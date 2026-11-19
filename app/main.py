"""Flask 入口：页面 + 运行接口。

环境变量：PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PORT

两个入口：
  - /         并发写入观测台（隔离级别沙箱）
  - /notify   LISTEN/NOTIFY 迟到观测（写端已提交、收端仍迟到）
"""
import os
import threading

from flask import Flask, jsonify, render_template, request

from engine import run_experiment
from notify_engine import (
    DEFAULT_CHANNEL,
    DEFAULT_TIMEOUT as NOTIFY_DEFAULT_TIMEOUT,
    MAX_PAYLOAD_BYTES,
    MAX_REPEAT,
    MAX_TIMEOUT as NOTIFY_MAX_TIMEOUT,
    MIN_TIMEOUT as NOTIFY_MIN_TIMEOUT,
    NotifyConfigError,
    default_config,
    normalize_config,
    run_observation,
)
from notify_presets import NOTIFY_PRESETS
from presets import PRESETS
from script_parser import ScriptError, parse_script

app = Flask(__name__)

# 所有请求共用一个沙箱库，同一时刻只允许跑一轮实验，避免互相重置数据
RUN_LOCK = threading.Lock()
# 通知观测也连同一个沙箱库：同一时刻只允许一次观测，避免与隔离级别实验互相干扰
NOTIFY_LOCK = threading.Lock()

DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 60.0


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "sandbox"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/presets")
def get_presets():
    return jsonify(PRESETS)


@app.post("/api/run")
def run():
    body = request.get_json(force=True, silent=True) or {}
    init_sql = (body.get("init_sql") or "").strip()
    script = (body.get("script") or "").strip()
    check_sql = (body.get("check_sql") or "").strip()
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_TIMEOUT
    timeout_s = min(max(timeout_s, 1.0), MAX_TIMEOUT)

    if not init_sql:
        return jsonify({"error": "初始数据 SQL 不能为空"}), 400
    if not check_sql:
        return jsonify({"error": "核对查询不能为空"}), 400
    try:
        steps = parse_script(script)
    except ScriptError as exc:
        return jsonify({"error": f"脚本解析失败：{exc}"}), 400

    if not RUN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有实验正在运行，请稍候（所有会话共用一个沙箱库）"}), 409
    try:
        rounds = run_experiment(conninfo(), init_sql, steps, check_sql, timeout_s)
    finally:
        RUN_LOCK.release()
    return jsonify({"rounds": rounds, "timeout_s": timeout_s})


# ------------------------------------------------------------ 通知迟到观测 --

@app.get("/notify")
def notify_page():
    return render_template(
        "notify.html",
        notify_presets=NOTIFY_PRESETS,
        notify_defaults=default_config(),
        limits={
            "min_timeout": NOTIFY_MIN_TIMEOUT,
            "max_timeout": NOTIFY_MAX_TIMEOUT,
            "default_timeout": NOTIFY_DEFAULT_TIMEOUT,
            "max_repeat": MAX_REPEAT,
            "max_payload_bytes": MAX_PAYLOAD_BYTES,
            "default_channel": DEFAULT_CHANNEL,
        },
    )


@app.get("/api/notify/presets")
def notify_presets():
    return jsonify(NOTIFY_PRESETS)


@app.post("/api/notify/run")
def notify_run():
    body = request.get_json(force=True, silent=True) or {}
    try:
        config = normalize_config(body)
    except NotifyConfigError as exc:
        return jsonify({"error": f"参数错误：{exc}"}), 400

    if not NOTIFY_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有通知观测正在运行，请稍候"}), 409
    try:
        result = run_observation(conninfo(), config)
    finally:
        NOTIFY_LOCK.release()
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
