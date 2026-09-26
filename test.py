#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySql.py 的功能、事务、长连接及可选并发压力测试。

数据库参数优先级：
    1. ``--config`` 指定的 JSON 文件；
    2. ``--host/--port/--user/--password/--database/--pools`` 命令行参数；
    3. 其余未提供的参数使用通用默认值。

测试代码不读取环境变量。功能测试只创建一个随机命名的临时表，
不会修改现有 ``user`` 表；结束后会自动删除临时表。

运行：

    python test.py --config config.json
    python test.py --config config.json --stress
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from MySql import Mysql


DEFAULT_CONFIG: dict[str, Any] = {
    "port": 3306,
    "pools": 10,
}


def _load_config() -> tuple[dict[str, Any], bool]:
    """按“JSON 文件 > 命令行 > 默认值”的顺序合并测试参数。"""
    parser = argparse.ArgumentParser(description="MySql.py 数据库访问测试")
    parser.add_argument("--config", help="JSON 配置文件路径")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database")
    parser.add_argument("--pools", type=int)
    parser.add_argument(
        "--stress",
        action="store_true",
        help="额外执行 20 线程并发写入/读取测试",
    )
    args = parser.parse_args()

    config = dict(DEFAULT_CONFIG)
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
    return config, args.stress


def _temporary_table_name() -> str:
    """生成只包含字母数字下划线的临时表名。"""
    stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return f"mydao_selftest_{os.getpid()}_{stamp}"


def _create_test_table(db: Mysql, table: str) -> None:
    """创建测试表及唯一索引，覆盖自增、字符串、数值和时间类型。"""
    sql = f"""
        create table {db._quote_table(table)} (
            `id` bigint unsigned not null auto_increment,
            `name` varchar(64) not null default '',
            `score` decimal(12,2) not null default 0,
            `status` tinyint not null default 0,
            `created_at` datetime not null default current_timestamp,
            primary key (`id`),
            unique key `uk_name` (`name`)
        ) engine=InnoDB default charset=utf8mb4
    """
    result = db.Executes(sql, 9)
    assert result is True, db.GetError()
    # 第一次访问新表时立即加载结构，后续插入和字段过滤都使用缓存。
    assert db.GetPrimaryKey(table) == "id", db.GetError()


def _test_user_table(db: Mysql) -> None:
    """验证题目给出的 user 表示例调用。"""
    row = db.GetAtom("user")
    assert isinstance(row, dict), db.GetError()
    print(f"user 表示例: 首行 id={row.get('id')}")


def _test_crud(db: Mysql, table: str) -> None:
    """覆盖单行/多行增删改查、计数、替换和预览模式。"""
    first_id = db.AddAtom(
        table,
        {
            "name": "alice",
            "score": "10.50",
            "status": 0,
        },
    )
    assert isinstance(first_id, int) and first_id > 0, db.GetError()
    alice = db.GetAtom(table, {"name": "alice"})
    assert alice and alice["id"] == first_id
    assert alice["score"] == decimal.Decimal("10.50")

    changed = db.Updates(
        table,
        {
            "score": decimal.Decimal("12.50"),
            "status": 1,
        },
        {"id": first_id},
    )
    assert changed == 1, db.GetError()
    alice = db.GetAtom(table, first_id)
    assert alice and alice["status"] == 1

    bulk_id = db.AddMore(
        table,
        [
            {"name": "bob", "score": "5.00", "status": 1},
            {"name": "charlie", "score": "8.00", "status": 0},
        ],
    )
    assert isinstance(bulk_id, int), db.GetError()

    total = db.GetCount(table, {"status >=": 0})
    assert total == 3, (total, db.GetError())
    rows = db.GetData(
        table,
        {"status in": [0, 1]},
        {"order": "score desc", "limit": 10, "fields": "id,name,score,status"},
    )
    assert isinstance(rows, list) and len(rows) == 3
    assert [row["name"] for row in rows] == ["alice", "charlie", "bob"]

    # 不带 only_data 时，GetMore 会先 count 再查列表，同时写入
    # last_total；这里同时防止 WHERE 参数在 count 阶段丢失。
    rows_with_total = db.GetMore(
        table,
        {"status in": [0, 1]},
        {"limit": 10},
    )
    assert isinstance(rows_with_total, list) and len(rows_with_total) == 3
    assert db.last_total == 3, db.GetError()

    # Replaces 按唯一键 name 更新，而不是新增第四行。
    assert db.Replaces(
        table,
        [{"name": "bob", "score": "6.00", "status": 2}],
        fields="name",
    )
    bob = db.GetAtom(table, {"name": "bob"})
    assert bob and bob["score"] == decimal.Decimal("6.00")

    # preview 只生成 SQL 结构，不真正执行；随后 count 必须保持不变。
    preview = db.preview(True).AddAtom(
        table,
        {"name": "preview_only", "score": "1.00"},
    )
    db.preview(False)
    assert isinstance(preview, dict) and "insert" in preview["sql"].lower()
    assert db.GetCount(table, {"name": "preview_only"}) == 0

    # Packages 一次 SQL 更新多行不同值。
    affected = db.Packages(
        table,
        {
            "bob": {"score": "7.00"},
            "charlie": {"score": "9.00"},
        },
        {"name": ["bob", "charlie"]},
    )
    assert isinstance(affected, int) and affected >= 0, db.GetError()

    deleted = db.Deletes(table, {"name": "charlie"}, {"limit": 1})
    assert deleted == 1, db.GetError()
    print("CRUD/批量/替换/预览: OK")


def _test_join(db: Mysql, table: str) -> None:
    """验证 GetMore 的一对一 join、flat 和 prefix 组装。"""
    child_table = table + "_child"
    try:
        create_sql = f"""
            create table {db._quote_table(child_table)} (
                `id` bigint unsigned not null auto_increment,
                `parent_id` bigint unsigned not null,
                `note` varchar(64) not null default '',
                primary key (`id`),
                unique key `uk_parent` (`parent_id`)
            ) engine=InnoDB default charset=utf8mb4
        """
        assert db.Executes(create_sql, 9), db.GetError()
        alice = db.GetAtom(table, {"name": "alice"})
        assert alice
        assert db.AddAtom(
            child_table,
            {"parent_id": alice["id"], "note": "joined"},
        ), db.GetError()
        rows = db.GetMore(
            table,
            {"id": alice["id"]},
            {"join": {child_table + " child": "id:parent_id|flat=note|prefix=child_"}},
        )
        assert rows and rows[0]["child_note"] == "joined", db.GetError()
    finally:
        db.Executes(f"drop table if exists {db._quote_table(child_table)}", 9)
    print("join 组装: OK")


def _test_transactions(db: Mysql, table: str) -> None:
    """验证回滚和提交都固定在同一个连接上。"""
    assert db.Begin()
    rollback_id = db.AddAtom(
        table,
        {"name": "rollback_row", "score": "1.00"},
    )
    assert isinstance(rollback_id, int) and rollback_id > 0, db.GetError()
    assert db.GetAtom(table, {"id": rollback_id})
    assert db.Rollback()
    assert db.GetAtom(table, {"id": rollback_id}) is None

    assert db.Begin()
    commit_id = db.AddAtom(
        table,
        {"name": "commit_row", "score": "2.00"},
    )
    assert isinstance(commit_id, int) and commit_id > 0, db.GetError()
    assert db.Commit()
    committed = db.GetAtom(table, {"id": commit_id})
    assert committed and committed["name"] == "commit_row"
    print("事务回滚/提交: OK")


def _stress(db: Mysql, table: str, workers: int = 20, per_worker: int = 5) -> None:
    """验证多个线程共享一个 Mysql 对象时连接池不会串事务或串结果。"""
    def worker(index: int) -> list[int]:
        ids: list[int] = []
        for item in range(per_worker):
            name = f"stress_{index}_{item}_{datetime.datetime.now().timestamp()}"
            row_id = db.AddAtom(
                table,
                {"name": name, "score": str(index), "status": index % 2},
            )
            if not isinstance(row_id, int) or row_id <= 0:
                raise RuntimeError(db.GetError())
            row = db.GetAtom(table, {"id": row_id})
            if not row or row["name"] != name:
                raise RuntimeError("并发读取结果不匹配")
            ids.append(row_id)
        return ids

    all_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, index) for index in range(workers)]
        for future in as_completed(futures):
            all_ids.extend(future.result())
    assert len(all_ids) == workers * per_worker
    assert len(set(all_ids)) == len(all_ids)
    print(f"并发压力: {workers} 线程 / {len(all_ids)} 次写入读取 OK")


def main() -> None:
    config, stress_requested = _load_config()
    table = _temporary_table_name()
    db = Mysql(**config)
    try:
        _create_test_table(db, table)
        _test_user_table(db)
        _test_crud(db, table)
        _test_join(db, table)
        _test_transactions(db, table)
        if stress_requested:
            _stress(db, table)
        sqls = db.getSqls()
        assert isinstance(sqls, list)
        last_sql = db.getSqls(True)
        assert last_sql is None or isinstance(last_sql, str)
        assert isinstance(db.getSqls(as_string=True), str)
        print(f"SQL 历史条数: {len(sqls)}")
        print("全部功能测试通过")
    finally:
        # 只清理本轮创建的临时表，不触碰现有业务表。
        db.Executes(f"drop table if exists {db._quote_table(table)}", 9)
        db.close()


if __name__ == "__main__":
    main()
