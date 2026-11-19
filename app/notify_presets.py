"""通知迟到观测的内置样例（前端一键载入）。"""

NOTIFY_PRESETS = [
    {
        "id": "dedupe-and-cross-txn",
        "name": "同笔合并 · 跨事务分别计数",
        "description": "两笔写端事务都提交，且发送相同内容。同一笔里重复 3 次只算 1 条；"
                       "跨事务的相同内容各算 1 条。空闲端立即收到；持事务端在事务结束后"
                       "才一次性补收全部 4 条（含收尾通知）。",
        "config": {
            "channel": "notify_late_demo",
            "txn1": {"payloads": "order.created", "repeat": 3, "outcome": "commit"},
            "txn2": {"payloads": "order.created", "repeat": 3, "outcome": "commit"},
            "closing": {"send": True, "payload": "closing"},
            "end_with": "commit",
            "timeout_s": 8.0,
        },
    },
    {
        "id": "rollback-silent",
        "name": "回滚不增加收件",
        "description": "第一笔写端事务发送 4 次后回滚，第二笔提交 2 个不同内容。"
                       "回滚那笔两端都不增加；空闲端只收到第二笔的 2 条，"
                       "持事务端事务结束后补收这 2 条。",
        "config": {
            "channel": "notify_late_demo",
            "txn1": {"payloads": "订单.已回滚", "repeat": 4, "outcome": "rollback"},
            "txn2": {"payloads": "订单.已提交A\n订单.已提交B", "repeat": 1,
                     "outcome": "commit"},
            "closing": {"send": True, "payload": "closing"},
            "end_with": "commit",
            "timeout_s": 8.0,
        },
    },
    {
        "id": "held-rollback",
        "name": "持事务端以回滚结束",
        "description": "写端两笔事务都提交；持事务端自己用 ROLLBACK 结束事务。"
                       "已由写端提交的通知仍会补发给它——收端的回滚不影响已提交的通知。",
        "config": {
            "channel": "notify_late_demo",
            "txn1": {"payloads": "alpha", "repeat": 2, "outcome": "commit"},
            "txn2": {"payloads": "beta", "repeat": 2, "outcome": "commit"},
            "closing": {"send": True, "payload": "closing"},
            "end_with": "rollback",
            "timeout_s": 8.0,
        },
    },
]
