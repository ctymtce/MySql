#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySql.py 使用示例。

数据库参数通过命令行或 JSON 文件传入，不在代码中保存账号和密码。

运行示例：
    python.exe demo.py --host 127.0.0.1 --port 3306 --user root \
        --password 你的密码 --database 数据库名
    python.exe demo.py --config config.json
"""

from __future__ import annotations

import argparse
import json
from pprint import pprint
from typing import Any

from MySql import Mysql


def _load_config() -> dict[str, Any]:
    """读取数据库配置并校验必需参数。"""
    parser = argparse.ArgumentParser(description="MySql.py 使用示例")
    parser.add_argument("--config", help="JSON 配置文件路径")
    parser.add_argument("--host", help="MySQL 地址")
    parser.add_argument("--port", type=int, help="MySQL 端口")
    parser.add_argument("--user", help="MySQL 用户")
    parser.add_argument("--password", help="MySQL 密码")
    parser.add_argument("--database", help="数据库名")
    parser.add_argument("--pools", type=int, help="连接池大小")
    args = parser.parse_args()

    config: dict[str, Any] = {"port": 3306, "pools": 10}
    if args.config:
        with open(args.config, "r", encoding="utf-8") as file:
            loaded = json.load(file)
        if not isinstance(loaded, dict):
            raise ValueError("JSON 配置必须是对象")
        config.update(loaded)

    overrides = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "pswd": args.password,
        "dbName": args.database,
        "pools": args.pools,
    }
    config.update(
        {
            key: value
            for key, value in overrides.items()
            if value is not None
        }
    )

    missing = [
        key
        for key in ("host", "user", "dbName")
        if not config.get(key)
    ]
    if missing:
        parser.error(
            "缺少数据库参数: " + ", ".join(missing)
            + "；请使用 --config 或命令行参数传入"
        )
    config.setdefault("pswd", "")
    return config


def main() -> None:
    """演示查询、总数统计和 SQL 历史读取。"""
    db = Mysql(**_load_config())
    try:
        row = db.GetAtom("user", None, {"fields": "id,enabled"})
        print("单行查询:")
        pprint(row)

        rows = db.GetMore(
            "user",
            None,
            {"fields": "id,enabled", "limit": 3, "order": "id desc"},
        )
        print("列表查询:")
        pprint(rows)
        print("总记录数:", db.GetTotal())

        print("最后一条 SQL:")
        print(db.getSqls(True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
