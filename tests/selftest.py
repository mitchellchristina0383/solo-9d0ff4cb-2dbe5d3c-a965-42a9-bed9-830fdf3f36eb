"""端到端自测：隔离级别沙箱（原有用例）+ LISTEN/NOTIFY 迟到观测。

用法：
  docker compose up -d --build 之后：
      docker compose exec web python tests/selftest.py
  或对着任意可达的 PostgreSQL：
      PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py

也可只跑通知部分：python tests/selftest.py notify
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from engine import run_experiment  # noqa: E402
from notify_engine import (  # noqa: E402
    NotifyConfigError,
    build_plan,
    default_config,
    normalize_config,
    run_observation,
)
from notify_presets import NOTIFY_PRESETS  # noqa: E402
from presets import PRESETS  # noqa: E402
from script_parser import ScriptError, parse_script

FAILURES = []


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def preset(pid):
    return next(p for p in PRESETS if p["id"] == pid)


def run_preset(pid, timeout=6.0):
    p = preset(pid)
    steps = parse_script(p["script"])
    return run_experiment(conninfo(), p["init_sql"], steps, p["check_sql"], timeout)


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def check_value(rows, col_name, columns):
    idx = columns.index(col_name)
    return rows[0][idx]


def test_parser():
    print("== 脚本解析器 ==")
    steps = parse_script(preset("lock-wait")["script"])
    check("解析行锁等待样例得到 4 步", len(steps) == 4, f"got {len(steps)}")
    check("第 2 步是 B/blocked", steps[1].session == "B" and steps[1].mode == "blocked")
    check("其余步骤是 complete 模式", all(s.mode == "complete" for s in steps if s.index != 1))
    try:
        parse_script("SELECT 1;")
        check("头部之外的 SQL 报错", False)
    except ScriptError:
        check("头部之外的 SQL 报错", True)
    try:
        parse_script("@A complete\n")
        check("空步骤报错", False)
    except ScriptError:
        check("空步骤报错", True)
    try:
        parse_script("@A turbo\nSELECT 1;")
        check("未知模式报错", False)
    except ScriptError:
        check("未知模式报错", True)


def test_write_skew():
    print("== 两人同时撤班（写偏斜） ==")
    rc, ser = run_preset("shift-withdraw")
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_count = check_value(rc["check"]["rows"], "on_duty_count", rc["check"]["columns"])
    check("RC 撤班后无人值守（on_duty_count = 0）", rc_count == 0, f"got {rc_count}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    outcomes = sorted(o["outcome"] for o in ser["outcomes"].values())
    check("SER 恰一个提交、一个中止", outcomes == ["aborted", "committed"], str(ser["outcomes"]))
    has_40001 = any(e["kind"] == "step_error" and e.get("pgcode") == "40001"
                    for e in ser["events"])
    check("SER 时间线出现 40001 序列化失败", has_40001)
    ser_count = check_value(ser["check"]["rows"], "on_duty_count", ser["check"]["columns"])
    check("SER 仍有一人在岗（on_duty_count = 1）", ser_count == 1, f"got {ser_count}")


def test_lock_wait():
    print("== 行锁等待（blocked 模式） ==")
    rc, ser = run_preset("lock-wait")
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    blocked_ev = [e for e in rc["events"] if e["kind"] == "blocked" and e["session"] == "B"]
    check("B 被数据库确认阻塞", len(blocked_ev) == 1)
    check("阻塞者正是会话 A", bool(blocked_ev and blocked_ev[0]["by_other"]))
    resumed = [e for e in rc["events"]
               if e["kind"] == "step_ok" and e["session"] == "B" and e.get("after_blocked")]
    check("A 提交后 B 的 UPDATE 解除阻塞并完成", bool(resumed))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("RC 余额 = 700（B 在 A 的结果上继续）", rc_bal == 700, f"got {rc_bal}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    check("SER 下 A 提交、B 中止",
          ser["outcomes"]["A"]["outcome"] == "committed"
          and ser["outcomes"]["B"]["outcome"] == "aborted",
          str(ser["outcomes"]))
    ser_bal = check_value(ser["check"]["rows"], "balance", ser["check"]["columns"])
    check("SER 余额 = 900（只有 A 生效）", ser_bal == 900, f"got {ser_bal}")


def test_timeout_and_rerun():
    print("== 超时回滚与重跑 ==")
    rc, ser = run_preset("timeout-demo", timeout=3.0)
    for rnd, name in ((rc, "RC"), (ser, "SER")):
        check(f"{name} 轮判定为超时", rnd["timed_out"], str(rnd.get("error")))
        check(f"{name} 两事务都被回滚",
              rnd["outcomes"]["A"]["outcome"] == "rolled_back"
              and rnd["outcomes"]["B"]["outcome"] == "rolled_back",
              str(rnd["outcomes"]))
        bal = check_value(rnd["check"]["rows"], "balance", rnd["check"]["columns"])
        check(f"{name} 余额保持 1000（未提交的全部回滚）", bal == 1000, f"got {bal}")
    cancelled = any(e["kind"] == "step_error" and e.get("pgcode") == "57014"
                    for e in rc["events"])
    check("RC 时间线出现语句被取消（57014）", cancelled)

    # 超时之后立刻重跑另一组实验：仍应从初始数据开始并正常完成
    rc2, _ = run_preset("lock-wait")
    bal2 = check_value(rc2["check"]["rows"], "balance", rc2["check"]["columns"])
    check("超时后重跑仍从初始数据开始（锁等待样例 RC 余额 = 700）",
          not rc2["timed_out"] and bal2 == 700, f"got {bal2}")


def test_no_block_branch():
    print("== blocked 模式但语句未阻塞 ==")
    init = preset("lock-wait")["init_sql"]
    script = """@B blocked
UPDATE accounts SET balance = 0 WHERE id = 999;

@B complete
COMMIT;"""
    steps = parse_script(script)
    rc, _ = run_experiment(conninfo(), init, steps,
                           "SELECT balance FROM accounts WHERE id = 1;", 5.0)
    check("语句直接完成、本轮未超时", not rc["timed_out"] and not rc["error"])
    check("时间线记录“未阻塞”事件",
          any(e["kind"] == "no_block" for e in rc["events"]))
    bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("未命中任何行，余额不变（1000）", bal == 1000, f"got {bal}")


# ---------------------------------------------------------- 通知迟到观测自测 --

def notify_preset(pid):
    return next(p for p in NOTIFY_PRESETS if p["id"] == pid)


def run_notify(cfg, plan=None):
    return run_observation(conninfo(), cfg, plan=plan)


def payloads_of(stage, role):
    return stage["actual"][role]


def test_notify_config():
    print("== 通知观测：参数校验与阶段计划 ==")
    cfg = normalize_config({})
    check("默认配置三笔事务都提交", cfg["txn1"]["outcome"] == "commit"
          and cfg["txn2"]["outcome"] == "commit")
    check("默认重复次数 3", cfg["txn1"]["repeat"] == 3)
    bad_cases = [
        ({"channel": "1bad"}, "数字开头的频道名"),
        ({"channel": "with space"}, "含空格的频道名"),
        ({"txn1": {"payloads": ""}}, "第一笔没有通知内容"),
        ({"txn1": {"payloads": "a", "repeat": 0}}, "重复次数为 0"),
        ({"txn1": {"payloads": "a", "repeat": 51}}, "重复次数超上限"),
        ({"txn1": {"payloads": "a", "outcome": "maybe"}}, "未知总结方式"),
        ({"timeout_s": 0}, "超时过小"),
        ({"timeout_s": 61}, "超时过大"),
        ({"txn2": {"payloads": "x" * 9000}}, "payload 超长"),
    ]
    ok = True
    for body, _name in bad_cases:
        try:
            normalize_config(body)
            ok = False
            print("    未拒绝：", body)
        except NotifyConfigError:
            pass
    check("非法参数全部被拒绝", ok)

    cfg = normalize_config({"txn1": {"payloads": " a \n\n b \n a ", "repeat": 2}})
    check("通知内容去空白、同笔内去重", cfg["txn1"]["payloads"] == ["a", "b"],
          str(cfg["txn1"]["payloads"]))

    plan = build_plan(normalize_config(default_config()))
    check("阶段计划有 5 个阶段（基线 + 两笔 + 收尾 + 结束）", [s["index"] for s in plan] ==
          [0, 1, 2, 3, 4], str([s["index"] for s in plan]))
    check("阶段 4 预期 hold 端收到全部 3 条",
          len(plan[4]["expected"]["held"]) == 3, str(plan[4]["expected"]))
    check("阶段 4 预期空闲端没有新增", plan[4]["expected"]["idle"] == [])

    plan = build_plan(normalize_config({
        "txn1": {"payloads": "a", "repeat": 2, "outcome": "rollback"},
        "txn2": {"payloads": "a\nb", "repeat": 1, "outcome": "commit"},
    }))
    check("回滚事务的预期收件为空", plan[1]["expected"]["idle"] == [], str(plan[1]["expected"]))
    check("跨事务相同内容分别计数",
          plan[4]["expected"]["held"] == ["a", "b", "closing"],
          str(plan[4]["expected"]))


def test_notify_dedupe_cross_txn():
    print("== 通知观测：同笔合并、跨事务分别计数、迟到补收 ==")
    result = run_notify(notify_preset("dedupe-and-cross-txn")["config"])
    check("观测未超时、未出错", not result["timed_out"] and not result["error"],
          result.get("error"))
    check("五个阶段均符合预期",
          all(s["status"] == "ok" for s in result["stages"]),
          str([(s["index"], s["status"]) for s in result["stages"]]))
    stages = result["stages"]
    check("发送前基线两端均为 0",
          stages[0]["actual"]["idle"] == [] and stages[0]["actual"]["held"] == [])
    check("第一笔：同笔 3 次只算 1 条", stages[1]["actual"]["idle"] == ["order.created"],
          str(stages[1]["actual"]))
    check("第二笔：跨事务相同内容再算 1 条",
          stages[2]["actual"]["idle"] == ["order.created"], str(stages[2]["actual"]))
    check("持事务端在前三阶段保持 0 条（迟到）",
          all(s["actual"]["held"] == [] for s in stages[:4]))
    check("收尾通知单独提交后空闲端实收",
          stages[3]["actual"]["idle"] == ["closing"], str(stages[3]["actual"]))
    check("结束事务后持事务端一次性补收 3 条",
          stages[4]["actual"]["held"] == ["order.created", "order.created", "closing"],
          str(stages[4]["actual"]))
    check("空闲端在结束阶段没有新增", stages[4]["actual"]["idle"] == [])
    check("持事务端收尾按 COMMIT 结束", result["end"]["tag"] == "COMMIT")
    check("迟到通知确实是事务结束后才到达",
          all(item["t"] >= result["stages"][4]["arrivals"]["held"][0]["t"] - 0.001
              for item in result["stages"][4]["arrivals"]["held"]))
    check("全程合计：空闲端 3 条",
          result["totals"]["idle"]["count"] == 3, str(result["totals"]))


def test_notify_rollback_silent():
    print("== 通知观测：回滚不增加收件 ==")
    result = run_notify(notify_preset("rollback-silent")["config"])
    check("观测未超时、未出错", not result["timed_out"] and not result["error"])
    stages = result["stages"]
    check("回滚那笔两端都不增加",
          stages[1]["actual"]["idle"] == [] and stages[1]["actual"]["held"] == [],
          str(stages[1]["actual"]))
    check("回滚阶段的写端记录为 ROLLBACK",
          stages[1]["writer"]["outcome"] == "rollback"
          and stages[1]["writer"]["tag"] == "ROLLBACK")
    check("第二笔提交的两个内容空闲端均收到",
          stages[2]["actual"]["idle"] == ["订单.已提交A", "订单.已提交B"],
          str(stages[2]["actual"]))
    check("持事务端补收只含已提交内容",
          stages[4]["actual"]["held"] == ["订单.已提交A", "订单.已提交B", "closing"],
          str(stages[4]["actual"]))


def test_notify_held_rollback():
    print("== 通知观测：持事务端以回滚结束仍补收已提交通知 ==")
    result = run_notify(notify_preset("held-rollback")["config"])
    check("观测未超时、未出错", not result["timed_out"] and not result["error"])
    check("结束方式为 ROLLBACK", result["end"]["tag"] == "ROLLBACK")
    check("持事务端仍收到 alpha、beta、closing",
          result["stages"][4]["actual"]["held"] == ["alpha", "beta", "closing"],
          str(result["stages"][4]["actual"]))
    check("全部阶段符合预期",
          all(s["status"] == "ok" for s in result["stages"]),
          str([(s["index"], s["status"]) for s in result["stages"]]))


def test_notify_timeout():
    print("== 通知观测：超时保留已收内容、标注未确认、释放连接 ==")
    # 用 1 秒超时：前两笔能跑完，第三阶段来不及开始 → 未确认
    cfg = normalize_config({**default_config(), "timeout_s": 1.0})
    result = run_notify(cfg)
    check("整体判定为超时", result["timed_out"])
    check("存在未确认阶段", bool(result["unconfirmed_stages"]),
          str([(s["index"], s["status"]) for s in result["stages"]]))
    unconfirmed = [s for s in result["stages"] if s["status"] == "unconfirmed"]
    check("未确认阶段有说明文字", all(s["note"] for s in unconfirmed))
    check("未确认阶段之后标记为未执行",
          all(s["status"] in ("unconfirmed", "skipped")
              for s in result["stages"][unconfirmed[0]["index"]:]),
          str([(s["index"], s["status"]) for s in result["stages"]]))
    check("已收到内容被保留（第一笔空闲端 1 条）",
          result["stages"][1]["actual"]["idle"] == ["order.created"],
          str(result["stages"][1]["actual"]))
    check("收尾回滚了 held 的事务",
          result["end"] and result["end"]["mode"] == "rollback", str(result["end"]))
    check("释放后被回滚事务的补收单独记录、不计入阶段",
          len(result["late"]["held"]) >= 1 and result["late"]["note"],
          str(result["late"]))
    closed = [e for e in result["events"] if e["kind"] == "closed"]
    check("三条连接都记录了释放", len(closed) == 3, str(closed))

    # 另一个超时路径：结束事务后等不到预期补收 → 未确认但仍保留已收
    cfg = normalize_config({**default_config(), "timeout_s": 2.5})
    plan = build_plan(cfg)
    plan[4]["expected"]["held"] = plan[4]["expected"]["held"] + ["不会到达"]
    result = run_notify(cfg, plan=plan)
    check("补收不足时该阶段标未确认",
          result["stages"][4]["status"] == "unconfirmed",
          result["stages"][4]["status"])
    check("补收不足时已收到的 3 条仍保留",
          result["stages"][4]["actual"]["held"] ==
          ["order.created", "order.created", "closing"],
          str(result["stages"][4]["actual"]))
    check("未确认说明里包含实收/预期条数",
          "3" in result["stages"][4]["note"] and "4" in result["stages"][4]["note"],
          result["stages"][4]["note"])


def test_notify_closing_disabled():
    print("== 通知观测：取消收尾通知 ==")
    cfg = normalize_config({
        "channel": "notify_late_demo",
        "txn1": {"payloads": "only", "repeat": 2, "outcome": "commit"},
        "txn2": {"payloads": "only", "repeat": 2, "outcome": "commit"},
        "closing": {"send": False, "payload": "closing"},
        "end_with": "commit",
        "timeout_s": 6.0,
    })
    result = run_notify(cfg)
    check("观测未超时、未出错", not result["timed_out"] and not result["error"])
    check("收尾阶段两端都没有新增",
          result["stages"][3]["actual"]["idle"] == []
          and result["stages"][3]["actual"]["held"] == [],
          str(result["stages"][3]["actual"]))
    check("持事务端只补收两笔写端事务的 2 条",
          result["stages"][4]["actual"]["held"] == ["only", "only"],
          str(result["stages"][4]["actual"]))


def main(argv):
    only = argv[1] if len(argv) > 1 else None
    if only in (None, "core"):
        test_parser()
        test_write_skew()
        test_lock_wait()
        test_timeout_and_rerun()
        test_no_block_branch()
    if only in (None, "notify"):
        test_notify_config()
        test_notify_dedupe_cross_txn()
        test_notify_rollback_silent()
        test_notify_held_rollback()
        test_notify_timeout()
        test_notify_closing_disabled()
    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败：")
        for name in FAILURES:
            print("  -", name)
        return 1
    print("全部自测通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
