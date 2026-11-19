"""LISTEN/NOTIFY「写端已提交、收端仍迟到」观测引擎。

一次观测使用三条连接：

  - idle：LISTEN 后保持空闲（不在事务里），写端提交后应立即收到通知；
  - held：LISTEN 后 BEGIN 并一直保持事务开启。PostgreSQL 会把这期间提交的
    通知挂起，直到该事务 COMMIT / ROLLBACK 才送达——这就是“写端已提交，
    收端仍迟到”；
  - writer：按配置跑两笔写端事务（每笔可重复发送，选择 COMMIT / ROLLBACK），
    之后再用单独一笔事务提交「收尾通知」。

观测分阶段记录两端“本阶段实收”，并与预期逐阶段对照：

  阶段 0  发送前基线         两端都应为 0
  阶段 1  第一笔写端事务     空闲端按规则收到；持事务端仍为 0
  阶段 2  第二笔写端事务     空闲端按规则收到；持事务端仍为 0
  阶段 3  收尾通知（单独提交）空闲端实收后，才允许结束 held 的事务
  阶段 4  结束 held 事务     held 一次性补收此前所有已提交通知

预期规则：
  - 同一笔事务内，相同频道 + 相同 payload 的多次 pg_notify 合并为 1 条；
  - 不同事务发送相同 payload，各算 1 条（跨事务不合并）；
  - 回滚的事务不产生通知；
  - held 在事务开启期间收不到通知，事务结束后才收到此前全部已提交通知
    （无论它自己是 COMMIT 还是 ROLLBACK）。

超时：任一阶段等待实收超过整体时限，即把该阶段标为“未确认”，保留此前及
本阶段已收到的内容，跳过后续阶段；收尾时回滚仍开启的 held 事务并关闭全部
连接。事务结束后才补收的通知单独记录（未计入阶段对照）。
"""
from __future__ import annotations

import re
import select
import time
from collections import Counter

import psycopg2
from psycopg2 import extensions
from psycopg2.extensions import quote_ident

DEFAULT_CHANNEL = "notify_late_demo"
DEFAULT_TIMEOUT = 8.0
MIN_TIMEOUT = 1.0
MAX_TIMEOUT = 60.0

MAX_REPEAT = 50              # 每笔事务里每个内容最多重复发送次数
MAX_PAYLOADS = 10            # 每笔事务最多几个不同内容
MAX_PAYLOAD_BYTES = 8000     # PostgreSQL 通知 payload 上限
MAX_CHANNEL_BYTES = 63       # 标识符 / 频道名长度上限

POLL_INTERVAL = 0.03         # 等待实收时的轮询间隔（秒）
SETTLE_SECONDS = 0.25        # 达到预期后再观察一小段，捕获多余通知
BASELINE_SECONDS = 0.15      # 发送前基线观察窗口
CLEANUP_DRAIN = 0.4          # 超时收尾时观察“迟到的补收”的窗口

OUTCOME_LABELS = {"commit": "COMMIT（提交）", "rollback": "ROLLBACK（回滚）"}

_CHANNEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class NotifyConfigError(ValueError):
    """观测参数不合法。"""


def default_config():
    return {
        "channel": DEFAULT_CHANNEL,
        "txn1": {"payloads": ["order.created"], "repeat": 3, "outcome": "commit"},
        "txn2": {"payloads": ["order.created"], "repeat": 3, "outcome": "commit"},
        "closing": {"send": True, "payload": "closing"},
        "end_with": "commit",
        "timeout_s": DEFAULT_TIMEOUT,
    }


# ---------------------------------------------------------------- 参数校验 --

def _parse_payloads(raw, field):
    if raw is None:
        lines = []
    elif isinstance(raw, str):
        lines = raw.splitlines()
    elif isinstance(raw, (list, tuple)):
        lines = [str(x) for x in raw]
    else:
        raise NotifyConfigError(f"{field} 的通知内容必须是多行文本或字符串列表")

    out, seen = [], set()
    for line in lines:
        payload = line.strip()
        if not payload:
            continue
        if len(payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise NotifyConfigError(
                f"{field} 有一条通知超过 {MAX_PAYLOAD_BYTES} 字节（PostgreSQL 上限）"
            )
        if payload in seen:
            continue          # 同一笔事务里重复列出同一内容没有意义，按合并处理
        seen.add(payload)
        out.append(payload)
    if len(out) > MAX_PAYLOADS:
        raise NotifyConfigError(f"{field} 最多 {MAX_PAYLOADS} 个不同内容")
    return out


def _parse_repeat(raw, field):
    if raw is None or raw == "":
        return 1
    if isinstance(raw, bool):
        raise NotifyConfigError(f"{field} 的重复次数必须是整数")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise NotifyConfigError(f"{field} 的重复次数必须是整数") from None
    if value < 1 or value > MAX_REPEAT:
        raise NotifyConfigError(f"{field} 的重复次数须在 1 到 {MAX_REPEAT} 之间")
    return value


def _parse_outcome(raw, field):
    token = str(raw or "commit").strip().lower()
    if token in ("commit", "committed", "提交"):
        return "commit"
    if token in ("rollback", "rolled_back", "回滚"):
        return "rollback"
    raise NotifyConfigError(f"{field} 只能是 commit（提交）或 rollback（回滚）")


def normalize_config(body):
    """把前端/接口传来的配置校验并规范化；出错抛 NotifyConfigError。"""
    if not isinstance(body, dict):
        raise NotifyConfigError("配置必须是 JSON 对象")
    if not body:
        body = default_config()

    channel = str(body.get("channel") or DEFAULT_CHANNEL).strip()
    if not channel:
        raise NotifyConfigError("频道名不能为空")
    if not _CHANNEL_RE.match(channel):
        raise NotifyConfigError("频道名只能用字母、数字、下划线，且不能以数字开头")
    if len(channel.encode("utf-8")) > MAX_CHANNEL_BYTES:
        raise NotifyConfigError(f"频道名不能超过 {MAX_CHANNEL_BYTES} 字节")

    try:
        timeout_s = float(body.get("timeout_s", DEFAULT_TIMEOUT))
    except (TypeError, ValueError):
        raise NotifyConfigError("超时时间必须是数字") from None
    if timeout_s < MIN_TIMEOUT or timeout_s > MAX_TIMEOUT:
        raise NotifyConfigError(
            f"超时时间须在 {MIN_TIMEOUT:g} 到 {MAX_TIMEOUT:g} 秒之间"
        )

    base = default_config()
    txns = {}
    for key, label in (("txn1", "第一笔写端事务"), ("txn2", "第二笔写端事务")):
        raw = body.get(key)
        if raw is None:
            raw = base[key]          # 未提供的整段沿用默认，便于局部覆盖
        if not isinstance(raw, dict):
            raise NotifyConfigError(f"{label} 配置必须是对象")
        merged = {**base[key], **raw}
        payloads = _parse_payloads(merged.get("payloads"), label)
        if not payloads:
            raise NotifyConfigError(f"{label} 至少需要一个通知内容（每行一个）")
        txns[key] = {
            "payloads": payloads,
            "repeat": _parse_repeat(merged.get("repeat"), label),
            "outcome": _parse_outcome(merged.get("outcome"), label),
        }

    raw_closing = body.get("closing")
    if raw_closing is None:
        raw_closing = base["closing"]
    if not isinstance(raw_closing, dict):
        raise NotifyConfigError("收尾通知配置必须是对象")
    raw_closing = {**base["closing"], **raw_closing}
    send = raw_closing.get("send", True)
    if isinstance(send, str):
        send = send.strip().lower() not in ("", "0", "false", "no", "off", "否")
    send = bool(send)
    closing_payload = str(raw_closing.get("payload") or "closing").strip()
    if send:
        if not closing_payload:
            raise NotifyConfigError("收尾通知内容不能为空")
        if len(closing_payload.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise NotifyConfigError(f"收尾通知超过 {MAX_PAYLOAD_BYTES} 字节")

    return {
        "channel": channel,
        "txn1": txns["txn1"],
        "txn2": txns["txn2"],
        "closing": {"send": send, "payload": closing_payload},
        "end_with": _parse_outcome(body.get("end_with", base["end_with"]),
                                   "结束持事务端的方式"),
        "timeout_s": timeout_s,
    }


# ---------------------------------------------------------------- 阶段计划 --

def _txn_stage(index, key, name, txn):
    delivered = list(txn["payloads"]) if txn["outcome"] == "commit" else []
    sent = len(txn["payloads"]) * txn["repeat"]
    if txn["outcome"] == "commit":
        detail = (
            f"写端发送 {sent} 次（{len(txn['payloads'])} 个不同内容，每个重复 "
            f"{txn['repeat']} 次）后 COMMIT。同一事务内相同内容合并为 1 条，"
            f"提交后送达空闲端；持事务端因事务未结束而暂不接收。"
        )
    else:
        detail = (
            f"写端发送 {sent} 次（{len(txn['payloads'])} 个不同内容，每个重复 "
            f"{txn['repeat']} 次）后 ROLLBACK。回滚事务不产生通知，两端都不应增加。"
        )
    return {
        "index": index,
        "key": key,
        "name": name,
        "detail": detail,
        "sender": key,
        "wait": "idle",
        "expected": {"idle": delivered, "held": []},
    }


def build_plan(config):
    """按配置生成阶段计划（纯函数，便于自测注入）。"""
    txn1, txn2 = config["txn1"], config["txn2"]
    closing = config["closing"]
    closing_delivered = [closing["payload"]] if closing["send"] else []
    cumulative = []
    for txn in (txn1, txn2):
        if txn["outcome"] == "commit":
            cumulative.extend(txn["payloads"])
    cumulative.extend(closing_delivered)

    stage1 = _txn_stage(1, "txn1", "第一笔写端事务", txn1)
    stage2 = _txn_stage(2, "txn2", "第二笔写端事务", txn2)
    if txn1["outcome"] == "commit" and set(txn2["payloads"]) & set(txn1["payloads"]):
        stage2["detail"] += " 与第一笔相同的内容来自另一事务，空闲端会分别计数、不合并。"

    if closing["send"]:
        closing_detail = (
            f"写端用单独一笔事务发送收尾通知 “{closing['payload']}” 并 COMMIT。"
            "空闲端应随即收到；确认实收后，才结束持事务端的事务。"
        )
    else:
        closing_detail = (
            "本次未发送收尾通知；本阶段只确认两端没有新增通知，随后结束持事务端。"
        )
    stage3 = {
        "index": 3,
        "key": "closing",
        "name": "收尾通知（单独提交）",
        "detail": closing_detail,
        "sender": "closing" if closing["send"] else None,
        "wait": "idle",
        "expected": {"idle": closing_delivered, "held": []},
    }

    end_label = OUTCOME_LABELS[config["end_with"]]
    stage4 = {
        "index": 4,
        "key": "release",
        "name": "结束持事务端",
        "detail": (
            f"用 {end_label} 结束 held 的事务。PostgreSQL 在其事务结束后一次性补发"
            "此前所有已提交通知，包括两笔写端事务与收尾通知；跨事务的相同内容"
            "分别计数。即使 held 自己回滚，已提交的通知仍会补收。"
        ),
        "sender": None,
        "wait": "held",
        "expected": {"idle": [], "held": cumulative},
    }

    baseline = {
        "index": 0,
        "key": "baseline",
        "name": "发送前基线",
        "detail": (
            "两条监听连接都已 LISTEN，持事务端已 BEGIN 并保持事务开启，写端尚未"
            "发送任何通知。两端预期均为 0，确保观测从发送前开始。"
        ),
        "sender": None,
        "wait": None,
        "expected": {"idle": [], "held": []},
    }
    return [baseline, stage1, stage2, stage3, stage4]


# ---------------------------------------------------------------- 连接封装 --

def _err_info(exc):
    pgcode = getattr(exc, "pgcode", None)
    diag = getattr(exc, "diag", None)
    msg = getattr(diag, "message_primary", None) if diag else None
    return pgcode, (msg or str(exc).strip())


def _exec(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.statusmessage
    finally:
        cur.close()


class _Conn:
    """一条 autocommit 连接；事务边界完全由显式 BEGIN/COMMIT/ROLLBACK 控制。"""

    def __init__(self, conninfo, application_name):
        self.conn = psycopg2.connect(**conninfo)
        self.conn.autocommit = True
        _exec(self.conn, "SELECT set_config('application_name', %s, false)",
              (application_name,))
        self.pid = self.conn.get_backend_pid()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class _Listener(_Conn):
    def __init__(self, conninfo, role, channel, log, t0):
        super().__init__(conninfo, f"notify-demo-{role}")
        self.role = role
        self.channel = channel
        self.log = log
        self.t0 = t0
        quoted = quote_ident(channel, self.conn)
        _exec(self.conn, "LISTEN " + quoted)
        log(role, "listen", detail=f"LISTEN {quoted}", pid=self.pid)

    def begin_hold(self):
        _exec(self.conn, "BEGIN")
        self.log(self.role, "begin", detail="BEGIN（保持事务开启）", pid=self.pid)

    def _take(self, sink):
        """把 psycopg2 已解析的通知移入本阶段收件箱，并标注到达时间。"""
        while self.conn.notifies:
            note = self.conn.notifies.pop(0)
            if note.channel != self.channel:
                continue
            item = {"payload": note.payload,
                    "t": round(time.monotonic() - self.t0, 3)}
            sink.append(item)
            self.log(self.role, "received", payload=note.payload, t=item["t"])

    def pump(self, sink, seconds):
        self._take(sink)
        end = time.monotonic() + max(0.0, seconds)
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            readable, _, _ = select.select(
                [self.conn.fileno()], [], [], min(remaining, POLL_INTERVAL)
            )
            if readable:
                self.conn.poll()
                self._take(sink)
        self._take(sink)

    def wait_for(self, sink, target, deadline):
        """等到收件箱达到 target 条；超过整体时限返回 False。"""
        while len(sink) < target:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self.pump(sink, min(POLL_INTERVAL, remaining))
        return True

    def transaction_status(self):
        try:
            return self.conn.get_transaction_status()
        except Exception:
            return None


# ---------------------------------------------------------------- 主流程 --

def _record_stage(stage, sinks, status, note=""):
    stage["actual"] = {
        "idle": [item["payload"] for item in sinks["idle"]],
        "held": [item["payload"] for item in sinks["held"]],
    }
    stage["arrivals"] = sinks
    if status is None:
        same = (Counter(stage["actual"]["idle"]) == Counter(stage["expected"]["idle"])
                and Counter(stage["actual"]["held"]) ==
                Counter(stage["expected"]["held"]))
        status = "ok" if same else "mismatch"
    stage["status"] = status
    stage["note"] = note
    return stage


def _mark_remaining(result, start_index, status, note):
    for stage in result["stages"][start_index:]:
        if stage["status"] == "pending":
            stage["status"] = status
            stage["note"] = note


def _totals(items):
    payloads = [item["payload"] for item in items]
    return {"count": len(payloads), "by_payload": dict(Counter(payloads))}


def run_observation(conninfo, config, plan=None):
    """跑一次观测并返回分阶段结果。

    plan 默认由 build_plan(config) 生成；自测可注入修改过的计划来验证
    超时/未确认路径，不影响前端调用。
    """
    cfg = normalize_config(config)
    plan = plan if plan is not None else build_plan(cfg)

    events = []
    t0 = time.monotonic()

    def log(who, kind, **kw):
        events.append({"t": round(time.monotonic() - t0, 3), "role": who,
                       "kind": kind, **kw})

    result = {
        "channel": cfg["channel"],
        "timeout_s": cfg["timeout_s"],
        "end_with": cfg["end_with"],
        "timed_out": False,
        "error": None,
        "stages": [
            {**stage, "expected": {"idle": list(stage["expected"]["idle"]),
                                   "held": list(stage["expected"]["held"])},
             "actual": {"idle": [], "held": []}, "arrivals": {"idle": [], "held": []},
             "status": "pending", "note": ""}
            for stage in plan
        ],
        "events": events,
        "end": None,
        "late": {"idle": [], "held": [], "note": ""},
        "duration": None,
    }
    deadline = t0 + cfg["timeout_s"]
    listeners = {}
    writer = None

    try:
        listeners["idle"] = _Listener(conninfo, "idle", cfg["channel"], log, t0)
        listeners["held"] = _Listener(conninfo, "held", cfg["channel"], log, t0)
        log("SYS", "ready",
            detail=f"两端已 LISTEN 频道 “{cfg['channel']}”；held 即将 BEGIN")
        listeners["held"].begin_hold()
        writer = _Conn(conninfo, "notify-demo-writer")
        log("writer", "connect", detail="写端连接已建立", pid=writer.pid)

        for stage in result["stages"]:
            index = stage["index"]
            if stage["status"] != "pending":
                continue
            if result["error"]:
                _mark_remaining(result, index, "skipped", "前序阶段出错，未执行")
                break
            if result["timed_out"]:
                _mark_remaining(result, index, "skipped", "前序阶段未确认，未执行")
                break
            if time.monotonic() >= deadline:
                result["timed_out"] = True
                stage["status"] = "unconfirmed"
                stage["note"] = "整体超时：本阶段未能在时限内开始"
                log("SYS", "timeout", stage=index,
                    detail="整体超时：本阶段未能在时限内开始")
                continue

            sinks = {"idle": [], "held": []}
            log("SYS", "stage_start", stage=index, name=stage["name"])

            # 阶段 0：发送前基线
            if stage["key"] == "baseline":
                window = min(BASELINE_SECONDS, max(deadline - time.monotonic(), 0.0))
                listeners["idle"].pump(sinks["idle"], window)
                listeners["held"].pump(sinks["held"], window)
                _record_stage(stage, sinks, None)
                log("SYS", "stage_done", stage=index, status=stage["status"])
                continue

            # 写端发送：两笔写端事务或收尾通知
            sender = stage["sender"]
            if sender:
                if sender == "closing":
                    sent_payloads = [cfg["closing"]["payload"]]
                    repeat = 1
                    outcome = "commit"
                else:
                    txn = cfg[sender]
                    sent_payloads = txn["payloads"]
                    repeat = txn["repeat"]
                    outcome = txn["outcome"]
                _exec(writer.conn, "BEGIN")
                for payload in sent_payloads:
                    for _ in range(repeat):
                        _exec(writer.conn, "SELECT pg_notify(%s, %s)",
                              (cfg["channel"], payload))
                log("writer", "sent", stage=index,
                    payloads=sent_payloads, repeat=repeat,
                    detail=f"发送 {len(sent_payloads) * repeat} 次")
                tag = _exec(writer.conn, "COMMIT" if outcome == "commit" else "ROLLBACK")
                stage["writer"] = {
                    "sent_count": len(sent_payloads) * repeat,
                    "distinct": len(sent_payloads),
                    "outcome": outcome,
                    "payloads": list(sent_payloads),
                    "repeat": repeat,
                    "tag": tag,
                }
                log("writer", "commit" if outcome == "commit" else "rollback",
                    stage=index, tag=tag,
                    detail=f"{OUTCOME_LABELS[outcome]}，服务器返回 {tag}")

            # 阶段 4：先结束 held 的事务（空闲端已确认收到收尾通知），再等它补收
            if stage["wait"] == "held":
                end_sql = "COMMIT" if cfg["end_with"] == "commit" else "ROLLBACK"
                tag = _exec(listeners["held"].conn, end_sql)
                result["end"] = {
                    "mode": cfg["end_with"],
                    "tag": tag,
                    "pid": listeners["held"].pid,
                    "note": "事务结束后 held 才收到此前全部已提交通知",
                }
                log("held", "end", tag=tag,
                    detail=f"结束持事务端：{OUTCOME_LABELS[cfg['end_with']]}，"
                           f"服务器返回 {tag}")

            # 等待目标端实收
            if stage["wait"] in ("idle", "held"):
                role = stage["wait"]
                listener = listeners[role]
                target = len(stage["expected"][role])
                confirmed = listener.wait_for(sinks[role], target, deadline)
                if not confirmed:
                    result["timed_out"] = True
                    note = (
                        f"超时：{role} 端在时限内只收到 {len(sinks[role])} 条，"
                        f"预期 {target} 条；已收到的内容保留"
                    )
                    _record_stage(stage, sinks, "unconfirmed", note)
                    log("SYS", "timeout", stage=index, role=role, note=note)
                    continue

            # 达到预期后，给两端各留一小段观察窗，捕获多余通知
            window = min(SETTLE_SECONDS, max(deadline - time.monotonic(), 0.0))
            listeners["idle"].pump(sinks["idle"], window)
            window = min(SETTLE_SECONDS, max(deadline - time.monotonic(), 0.0))
            listeners["held"].pump(sinks["held"], window)

            _record_stage(stage, sinks, None)
            log("SYS", "stage_done", stage=index, status=stage["status"])

    except Exception as exc:  # 连接/执行错误：保留已完成阶段，收尾释放连接
        pgcode, msg = _err_info(exc)
        result["error"] = f"{pgcode} {msg}".strip() if pgcode else msg
        log("SYS", "error", pgcode=pgcode, error=msg)
        for stage in result["stages"]:
            if stage["status"] == "pending":
                stage["status"] = "error"
                stage["note"] = "本次观测出错，该阶段未完成"
    finally:
        # 释放连接：held 若仍在事务里先回滚；之后短暂观察补收内容（不计入阶段）
        held = listeners.get("held")
        if held is not None:
            status = held.transaction_status()
            if status is not None and status != extensions.TRANSACTION_STATUS_IDLE:
                try:
                    _exec(held.conn, "ROLLBACK")
                    log("held", "rollback", detail="收尾时回滚 held 仍未结束的事务并释放连接")
                    if result["end"] is None:
                        result["end"] = {
                            "mode": "rollback",
                            "tag": "ROLLBACK",
                            "pid": held.pid,
                            "note": "超时/出错收尾：回滚 held 的事务以释放连接",
                        }
                except Exception as exc:
                    pgcode, msg = _err_info(exc)
                    log("SYS", "cleanup_error", pgcode=pgcode, error=msg)
        if result["timed_out"] or result["error"]:
            late = {"idle": [], "held": []}
            for role, listener in listeners.items():
                try:
                    listener.pump(late[role], CLEANUP_DRAIN)
                except Exception:
                    pass
            result["late"] = {
                "idle": late["idle"],
                "held": late["held"],
                "note": "事务结束后补收/迟到的内容，未计入上方阶段对照",
            }
            if late["held"]:
                log("held", "late_received",
                    detail="释放事务后补收 " + str(len(late["held"])) + " 条")

        for role, listener in listeners.items():
            listener.close()
            log("SYS", "closed", role=role, detail=f"{role} 连接已释放")
        if writer is not None:
            writer.close()
            log("SYS", "closed", role="writer", detail="writer 连接已释放")

        result["duration"] = round(time.monotonic() - t0, 3)
        result["totals"] = {
            "idle": _totals([item for stage in result["stages"]
                             for item in stage["arrivals"]["idle"]]),
            "held": _totals([item for stage in result["stages"]
                             for item in stage["arrivals"]["held"]]),
        }
        result["unconfirmed_stages"] = [s["index"] for s in result["stages"]
                                        if s["status"] == "unconfirmed"]
    return result
