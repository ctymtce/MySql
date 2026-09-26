#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySQL 数据访问层。

设计目标
--------
1. ``Mysql`` 是外部推荐入口：
   ``Mysql(**配置).GetAtom("user")``。
2. ``Mypdb`` 负责连接池、SQL 执行、事务和 MySQL 方言等底层能力。
3. ``Mysql(Mypdb)`` 负责查询、增删改和表结构缓存，是完整业务入口。
4. 连接池中的物理连接会长期复用，普通 CRUD 每次只从池中借出并归还，
   事务期间则固定使用同一连接。
5. 所有字段值使用 ``%s`` + 参数绑定，不直接拼接用户值；
   表名、字段名仍必须经过反引号包装和基本校验。
6. 不读取任何环境变量。数据库配置只能由调用方显式传入。

返回约定
--------
    - 查询一行：dict 或 None；
    - 查询多行：list[dict]；
    - INSERT/REPLACE：自增 ID（int，无自增时为 0）；
    - UPDATE/DELETE：受影响行数（int）；
    - 执行失败：False，并可由 ``GetError()`` 取回错误。
"""

import datetime as _datetime
import decimal, json, re, threading, time
from collections import deque
from typing import Any, Iterable, Mapping, Sequence

import mysql.connector
from mysql.connector import pooling
from mysql.connector.conversion import MySQLConverter


class Mypdb:
    """MySQL 底层数据访问类。

    本类负责连接池、长连接复用、参数化 SQL 执行、基础 CRUD 和事务。
    业务层通常不直接使用它，而应使用包含完整业务接口的 ``Mysql``。
    """

    _INT_TYPES = {
        "int",
        "integer",
        "tinyint",
        "smallint",
        "mediumint",
        "bigint",
        "boolean",
        "bool",
        "serial",
        "bit",
    }
    _FLOAT_TYPES = {"float", "double", "decimal", "real", "numeric"}
    _DATE_TYPES = {"date", "datetime", "timestamp", "time", "year"}
    _JOIN_OPTION_KEYS = {
        "join",
        "flat",
        "order",
        "group",
        "limit",
        "keyas",
        "alias",
        "fields",
        "having",
        "prefix",
        "defaults",
        "onmiss",
        "where",
        "only_data",
        "unlimit",
        "locking",
    }

    def __init__(
        self,
        host: Any = None,
        user: str | None = None,
        pswd: str | None = None,
        dbName: str | None = None,
        port: int = 3306,
        pools: int = 10,
        **kwargs: Any,
    ) -> None:
        """创建连接池并保存数据库配置。
        支持 ``host``、``port``、``user``、``pswd``、``dbName`` 和
        ``pools``。除这些字段外，还支持：
            encoding/charset、connection_timeout、pool_name、
            pool_reset_session、sql_history_size、alias。
        """
        # 也允许把整个配置字典作为第一个参数传入：
        # Mysql({"host": "...", "user": "..."})
        if isinstance(host, Mapping):
            config = dict(host)
            config.update(kwargs)
            host = config.get("host")
            port = config.get("port", 3306)
            user = config.get("user")
            pswd = config.get("pswd", config.get("password"))
            dbName = config.get("dbName", config.get("database"))
            pools = config.get("pools", config.get("pool_size", 10))
            kwargs = {
                key: value
                for key, value in config.items()
                if key
                not in {
                    "host",
                    "port",
                    "user",
                    "pswd",
                    "password",
                    "dbName",
                    "database",
                    "pools",
                    "pool_size",
                }
            }

        if not dbName:
            raise ValueError("数据库名 dbName 不能为空")
        if not host:
            raise ValueError("数据库地址 host 不能为空")
        if user is None:
            raise ValueError("数据库用户 user 不能为空")
        if not pools or int(pools) <= 0:
            raise ValueError("连接池大小 pools 必须是正整数")

        self._host = str(host)
        self._port = int(port)
        self._user = str(user)
        self._pswd = "" if pswd is None else str(pswd)
        self._dbName = str(dbName)
        self._pool_size = int(pools)
        self._alias = kwargs.get("alias")
        self._encoding = str(
            kwargs.get("encoding", kwargs.get("charset", "utf8mb4"))
        )
        self._connection_timeout = int(kwargs.get("connection_timeout", 3))
        self._pool_reset_session = bool(kwargs.get("pool_reset_session", True))
        self._preview = False
        self._warning: str | None = None
        self._converter = MySQLConverter(charset=self._encoding)

        history_size = max(1, int(kwargs.get("sql_history_size", 1000)))
        self._sqls: deque[str] = deque(maxlen=history_size)
        self._schema_cache: dict[str, dict[str, dict[str, Any]]] = {}
        self._primary_key_cache: dict[str, str | None] = {}
        self._schema_lock = threading.RLock()
        self._closed = False

        # 每个线程保存自己的事务状态。一个 Mysql 对象可以安全地被多个
        # 线程共享；不同线程仍然使用连接池里的不同物理连接。
        self._local = threading.local()
        self._pool: pooling.MySQLConnectionPool | None = None
        self._create_pool()

    # ------------------------------------------------------------------
    # 连接与配置
    # ------------------------------------------------------------------
    def _pool_name(self) -> str:
        """生成符合 MySQL Connector 长度限制且不会互相冲突的池名称。"""
        return f"mydao_{id(self):x}"

    def _pool_config(self, pool_name: str | None = None) -> dict[str, Any]:
        """组装 mysql.connector 的连接参数。"""
        config: dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._pswd,
            "database": self._dbName,
            "charset": self._encoding,
            "connection_timeout": self._connection_timeout,
            "autocommit": True,
            "pool_name": pool_name or self._pool_name(),
            "pool_size": self._pool_size,
            "pool_reset_session": self._pool_reset_session,
        }
        return config

    def _create_pool(self) -> None:
        """创建连接池。"""
        if self._closed:
            raise RuntimeError("Mysql 对象已关闭")
        for attempt in range(5):
            try:
                self._pool = pooling.MySQLConnectionPool(**self._pool_config())
            except mysql.connector.Error:
                if 4 == attempt: raise
                time.sleep(1)

    def _acquire(self) -> tuple[Any, bool]:
        """借出一个连接。
        返回值第二项表示连接是否由本次调用独占。事务连接由线程持有，
        业务执行完成后不能自动归还。
        """
        transaction = getattr(self._local, "transaction", None)
        if transaction and transaction.get("connection"):
            connection = transaction["connection"]
            try:
                connection.ping(reconnect=True, attempts=5, delay=0.1)
            except mysql.connector.Error as exc:
                self._error = str(exc)
                raise
            return connection, False
        connection = self._borrow_connection()
        return connection, True

    def _borrow_connection(self) -> Any:
        """获取并校验连接；失败后继续尝试连接 N 次。"""
        for attempt in range(10):
            try:
                if self._pool is None: self._create_pool()
                return self._pool.get_connection()
            except mysql.connector.Error:
                if 9 == attempt: raise
                time.sleep(0.05)

    @staticmethod
    def _release(connection: Any, borrowed: bool) -> None:
        """把非事务连接归还连接池。"""
        if borrowed and connection is not None:
            try:
                connection.close()
            except Exception:
                # 归还有时也会因服务端断开而失败；不能覆盖原始业务异常。
                pass

    def reconnect(self) -> bool:
        """重建连接池，常用于数据库服务重启后的显式恢复。"""
        self._close_pool()
        self._closed = False
        self._create_pool()
        return True

    def _close_pool(self) -> None:
        """尽力关闭池内所有物理连接。"""
        if self._pool is None:
            return
        queue = getattr(self._pool, "_cnx_queue", None)
        if queue is not None:
            while True:
                try:
                    connection = queue.get_nowait()
                except Exception:
                    break
                try:
                    connection.close()
                except Exception:
                    pass
        self._pool = None

    def close(self) -> None:
        """关闭当前对象；如需继续使用，请显式调用 reconnect()。"""
        self._close_pool()
        self._closed = True

    def getConfig(self, key: str | None = None) -> Any:
        """返回一条或全部连接配置。"""
        config = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "pswd": self._pswd,
            "dbName": self._dbName,
            "pools": self._pool_size,
            "encoding": self._encoding,
            "alias": self._alias,
            "driver": "mysql",
        }
        if key:
            return config.get(key)
        return config

    def getEncoding(self) -> str:
        """返回当前连接字符集。"""
        return self._encoding

    # ------------------------------------------------------------------
    # SQL 执行
    # ------------------------------------------------------------------
    @staticmethod
    def _add_history(history: deque[str], sql: str) -> None:
        """保存 SQL；deque 本身限制了最大长度，避免长期运行内存无限增长。"""
        history.append(sql)

    def execute(
        self,
        sql: str,
        etype: int = 0,
        multi: bool = True,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Any:
        """执行 SQL，并按 ``etype`` 返回对应结果。
        etype:
            0  SELECT，multi=True 返回全部行，否则返回一行；
            -1 DELETE，返回受影响行数；
            1  INSERT/REPLACE，返回 lastrowid；
            2  UPDATE，返回受影响行数；
            9  CREATE/DDL，成功返回 True；
            其它值执行成功返回 True。
        """
        if not sql or not str(sql).strip():
            self._warning = "SQL 不能为空"
            return False

        sql = str(sql)
        self._error = None
        self._warning = None
        connection = None
        borrowed = False
        cursor = None

        try:
            connection, borrowed = self._acquire()
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.execute(sql) if params is None else cursor.execute(sql, params)
            self._add_history(self._sqls, cursor.statement or sql)

            if etype == 0:
                if multi:
                    return list(cursor.fetchall())
                return cursor.fetchone()

            if etype == 1: return int(cursor.lastrowid or 0)
            if etype in (-1, 2): return int(cursor.rowcount)

            return True
        except mysql.connector.Error as exc:
            self._error = str(exc)
            self._errno = getattr(exc, "errno", None)
            return False
        except Exception as exc:
            self._error = str(exc)
            self._errno = None
            return False
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if connection is not None:
                self._release(connection, borrowed)

    def query(self, sql: str, multi: bool = True) -> Any:
        """执行查询 SQL。"""
        return self.execute(sql, 0, multi)

    def executes(
        self,
        sql: str,
        rows: Iterable[Sequence[Any] | Mapping[str, Any]],
        etype: int = 2,
    ) -> Any:
        """使用驱动级 ``executemany`` 批量执行参数化 SQL。

        批量 INSERT 常可直接使用 ``inserts``；本方法适合不同业务 SQL
        共用同一种参数结构的批处理。
        """
        sql = str(sql)
        self._add_history(self._sqls, sql)
        self._error = None
        connection = None
        borrowed = False
        cursor = None
        try:
            connection, borrowed = self._acquire()
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.executemany(sql, list(rows))
            if etype == 1:
                return int(cursor.lastrowid or 0)
            if etype in (-1, 2):
                return int(cursor.rowcount)
            return True
        except mysql.connector.Error as exc:
            self._error = str(exc)
            self._errno = getattr(exc, "errno", None)
            return False
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if connection is not None:
                self._release(connection, borrowed)

    # ------------------------------------------------------------------
    # 条件、字段和值
    # ------------------------------------------------------------------
    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        """安全包装表名或字段名。

        这里不允许空标识符和控制字符。标识符内部的反引号按 MySQL
        规则写成两个反引号，即使调用方已经传入 ``field`` 或 ``field``
        形式也可以正常工作。
        """
        parts = str(identifier).strip().split(".")
        if not parts or any(not part for part in parts):
            raise ValueError(f"非法字段或表名: {identifier!r}")
        for part in parts:
            if "\x00" in part or "\r" in part or "\n" in part:
                raise ValueError(f"非法字段或表名: {identifier!r}")
        return ".".join(
            "`" + part.strip("`").replace("`", "``") + "`"
            for part in parts
        )

    @staticmethod
    def _quote_table(table: str) -> str:
        """包装 ``[数据库.]表名``，数据库和表名分别加反引号。"""
        return Mysql._quote_identifier(table)

    @staticmethod
    def _split_operator(field_spec: str) -> tuple[str, str]:
        """从 ``field >=``、``field>=`` 等写法中提取字段与操作符。"""
        spec = str(field_spec).strip()
        match = re.match(
            r"^(.*?)(?:\s+|\s*)"
            r"(notnull|find_ni_set|find_in_set|between|match|"
            r"in|ni|bt|>=|<=|<>|!=|>|<|=|%|\*|\^|!|null)\s*$",
            spec,
            flags=re.IGNORECASE,
        )
        if match and match.group(1).strip():
            return match.group(1).strip(), match.group(2).lower()
        return spec.strip(), "="

    def _value_clause(
        self,
        value: Any,
        literal: bool,
    ) -> tuple[str, list[Any]]:
        """为一个值生成占位符或安全的 SQL 字符串。

        正常 DAO 查询走参数模式；``parseWhere`` 需要返回可直接查看的
        SQL 文本时才使用 literal 模式。
        """
        if literal:
            return self._sql_literal(value), []
        return "%s", [value]

    def _parse_where(
        self,
        conditions: Any,
        default_boolean: str = "and",
        depth: int = 0,
        literal: bool = False,
    ) -> tuple[str, list[Any]]:
        """把嵌套条件转换为 WHERE SQL 和参数列表。

        支持：
            {"id": 1}
            {"id >=": 10, "status in": [1, 2]}
            {"or": [{"city": 1, "sex": 0}, {"city": 2, "sex": 1}]}
            ["a=1", {"b": 2}]  # 顶层列表按 default_boolean 连接
        """
        if not conditions:
            return "", []
        if isinstance(conditions, (str, int, float, decimal.Decimal)):
            return str(conditions), []

        if isinstance(conditions, Mapping):
            items = list(conditions.items())
        elif isinstance(conditions, Sequence):
            items = [(index, value) for index, value in enumerate(conditions)]
        else:
            return str(conditions), []

        clauses: list[str] = []
        params: list[Any] = []

        for key, value in items:
            key_text = str(key)
            key_lower = key_text.lower()

            # 数字键或 and/or 键表示条件组，组内可继续嵌套树。
            if key_lower in {"and", "or"} or isinstance(key, int):
                boolean = key_lower if key_lower in {"and", "or"} else default_boolean
                group_values = (
                    value
                    if isinstance(value, (list, tuple))
                    else [value]
                )
                sub_clauses: list[str] = []
                for group_value in group_values:
                    sub_sql, sub_params = self._parse_where(
                        group_value,
                        boolean,
                        depth + 1,
                        literal,
                    )
                    if sub_sql:
                        sub_clauses.append(sub_sql)
                        params.extend(sub_params)
                if sub_clauses:
                    joined = f" {boolean} ".join(sub_clauses)
                    clauses.append(f"({joined})" if len(sub_clauses) > 1 else joined)
                continue

            field, operator = self._split_operator(key_text)
            try:
                quoted_field = self._quote_identifier(field)
            except ValueError:
                # 对不能识别的表达式保留原样，便于兼容复杂 SQL；常规
                # DAO 字段仍会走上面的校验分支。
                quoted_field = field

            # LIKE：% 是前后模糊，* 是尾部模糊，^ 和 ! 是对应 NOT LIKE。
            if operator in {"%", "*", "^", "!"}:
                if literal:
                    text = "" if value is None else str(value)
                    pattern = (
                        f"%{text}%"
                        if operator == "%"
                        else f"{text}%"
                    )
                    clause = (
                        f"{quoted_field} not like "
                        f"{self._sql_literal(pattern)}"
                        if operator in {"^", "!"}
                        else f"{quoted_field} like {self._sql_literal(pattern)}"
                    )
                    clauses.append(clause)
                else:
                    text = "" if value is None else str(value)
                    pattern = f"%{text}%" if operator == "%" else f"{text}%"
                    sql_op = "not like" if operator in {"^", "!"} else "like"
                    clauses.append(f"{quoted_field} {sql_op} %s")
                    params.append(pattern)
                continue

            if operator in {"between", "bt"}:
                if not isinstance(value, (list, tuple)) or len(value) < 2:
                    continue
                if literal:
                    clauses.append(
                        f"({quoted_field} between "
                        f"{self._sql_literal(value[0])} and "
                        f"{self._sql_literal(value[1])})"
                    )
                else:
                    clauses.append(
                        f"({quoted_field} between %s and %s)"
                    )
                    params.extend([value[0], value[1]])
                continue

            if operator in {"in", "ni"}:
                if value is None or value == "":
                    continue
                values = (
                    list(value)
                    if isinstance(value, (list, tuple, set))
                    else [value]
                )
                if not values:
                    continue
                sql_op = "not in" if operator == "ni" else "in"
                if literal:
                    values_sql = ", ".join(self._sql_literal(item) for item in values)
                    clauses.append(f"{quoted_field} {sql_op} ({values_sql})")
                else:
                    placeholders = ", ".join(["%s"] * len(values))
                    clauses.append(f"{quoted_field} {sql_op} ({placeholders})")
                    params.extend(values)
                continue

            if operator in {"null", "notnull"}:
                sql_op = "is not null" if operator == "notnull" else "is null"
                clauses.append(f"{quoted_field} {sql_op}")
                continue

            if operator == "match":
                clause, clause_params = self._value_clause(value, literal)
                clauses.append(
                    f"match({quoted_field}) against({clause})"
                )
                params.extend(clause_params)
                continue

            if operator in {"find_in_set", "find_ni_set"}:
                values = (
                    list(value)
                    if isinstance(value, (list, tuple, set))
                    else [value]
                )
                for item in values:
                    if item is None or item == "":
                        continue
                    value_sql, value_params = self._value_clause(item, literal)
                    prefix = "not " if operator == "find_ni_set" else ""
                    clauses.append(
                        f"{prefix}find_in_set({value_sql}, {quoted_field})"
                    )
                    params.extend(value_params)
                continue

            # 普通比较。None 单独翻译成 IS NULL / IS NOT NULL。
            if value is None:
                clauses.append(
                    f"{quoted_field} is not null"
                    if operator in {"!=", "<>"}
                    else f"{quoted_field} is null"
                )
                continue

            if isinstance(value, (list, tuple, set)):
                sql_op = "not in" if operator in {"!=", "<>"} else "in"
                values = list(value)
                if not values:
                    continue
                if literal:
                    values_sql = ", ".join(self._sql_literal(item) for item in values)
                    clauses.append(f"{quoted_field} {sql_op} ({values_sql})")
                else:
                    placeholders = ", ".join(["%s"] * len(values))
                    clauses.append(f"{quoted_field} {sql_op} ({placeholders})")
                    params.extend(values)
                continue

            operator = operator or "="
            value_sql, value_params = self._value_clause(value, literal)
            clauses.append(f"{quoted_field} {operator} {value_sql}")
            params.extend(value_params)

        return f" {default_boolean} ".join(clauses), params

    def parseWhere(
        self,
        whArr: Any,
        andordft: str = "and",
        depth: int = 0,
    ) -> str | None:
        """返回可直接查看的 WHERE 片段。"""
        sql, _ = self._parse_where(whArr, andordft, depth, literal=True)
        return sql or None

    def _sql_literal(self, value: Any) -> str:
        """把 Python 值转换成 SQL 字面量。

        该函数主要供 ``parseWhere`` 和少量元数据 SQL 使用；DAO 正常
        执行路径仍应优先使用参数绑定。
        """
        if value is None:
            return "NULL"
        if value is True:
            return "1"
        if value is False:
            return "0"
        if isinstance(value, (int, float, decimal.Decimal)):
            return str(value)
        if isinstance(value, _datetime.datetime):
            return "'" + value.isoformat(sep=" ") + "'"
        if isinstance(value, (_datetime.date, _datetime.time)):
            return "'" + value.isoformat() + "'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            raw = bytes(value)
            return "X'" + raw.hex() + "'"
        text = str(value)
        escaped = self._converter.escape(text)
        return f"'{escaped}'"

    def valCorrectize(
        self,
        val: Any,
        f: str | None = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> Any:
        """按字段值格式整理数据。

        返回 Python 值，供参数绑定使用：
            {"express": "count+1"} 保留表达式；
            {"json": {...}}         生成 column_create(...) 表达式；
            普通 dict/list            序列化成 JSON 字符串。
        """
        del f, exArr
        if isinstance(val, Mapping):
            if "express" in val:
                return str(val["express"])
            if "json" in val:
                return self.jsonCreate(val["json"])
            return json.dumps(val, ensure_ascii=False, separators=(",", ":"))
        if isinstance(val, (list, tuple, set)):
            return json.dumps(list(val), ensure_ascii=False, separators=(",", ":"))
        return val

    def jsonCreate(self, jArr: Any) -> str:
        """生成 ``column_create(...)`` 动态列表达式。"""
        if not isinstance(jArr, Mapping):
            values = jArr if isinstance(jArr, (list, tuple)) else [jArr]
            return (
                "column_create('', '')"
                if not values
                else "column_create("
                + ",".join(
                    "''," + self._sql_literal(value)
                    for value in values
                )
                + ")"
            )

        parts: list[str] = []
        for key, value in jArr.items():
            if isinstance(value, Mapping):
                parts.append(f"{self._sql_literal(key)},{self.jsonCreate(value)}")
            else:
                parts.append(f"{self._sql_literal(key)},{self._sql_literal(value)}")
        return "column_create(" + ",".join(parts) + ")"

    # ------------------------------------------------------------------
    # SQL 构造
    # ------------------------------------------------------------------
    def mkQuery(self, table: str, exArr: Mapping[str, Any] | None = None) -> str:
        """根据 DAO 扩展参数生成 SELECT SQL。"""
        exArr = dict(exArr or {})
        page = max(1, int(exArr.get("page", 1)))
        limit = int(exArr.get("limit", 100))
        fields = exArr.get("fields") or "*"
        where = exArr.get("where") or ""
        group = exArr.get("group")
        having = exArr.get("having")
        order = exArr.get("order")

        sql = f"select {fields} from {self._quote_table(table)}"
        if where:
            sql += f" where {where}"
        if group:
            sql += f" group by {group}"
        if having:
            sql += f" having {having}"
        if order:
            sql += f" order by {order}"

        if not exArr.get("unlimit"):
            start = (page - 1) * limit
            sql += f" limit {start},{limit}"
        if exArr.get("locking"):
            sql += " for update"
        return sql

    def count(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | float:
        """按条件计数，或统计传入的 SELECT 语句结果数。"""
        # 只有一个字符串参数且是 SELECT 时，按完整查询统计。
        if whArr is None and isinstance(table, str):
            text = table.strip()
            if re.match(r"^(select|with)\b", text, flags=re.IGNORECASE):
                return self._count_select_sql(text)

        where = whArr
        exArr = dict(exArr or {})
        if "where" in exArr:
            where_sql = exArr.get("where") or ""
            where_params: list[Any] = []
        else:
            where_sql, where_params = self._parse_where(where)
        exArr["where"] = where_sql

        if exArr.get("group"):
            exArr["unlimit"] = True
            clause = self.mkQuery(table, exArr)
            # 包一层子查询，正确处理 group/having。
            sql = f"select count(*) as C01 from ({clause}) A"
            row = self.execute(sql, 0, False, where_params)
        else:
            fields = str(exArr.get("fields") or "")
            if fields and "distinct" in fields.lower():
                count_field = fields
            else:
                count_field = "*"
            exArr["fields"] = f"count({count_field}) as C01"
            exArr["unlimit"] = True
            exArr.pop("order", None)
            exArr.pop("page", None)
            sql = self.mkQuery(table, exArr)
            row = self.execute(sql, 0, False, where_params)

        if isinstance(row, Mapping) and row.get("C01") is not None:
            value = row["C01"]
            return int(value) if float(value).is_integer() else float(value)
        return 0

    def _count_select_sql(self, sql: str) -> int:
        """把一条 SELECT 包成 ``count(*)`` 后执行。"""
        clean = sql.strip().rstrip(";")
        count_sql = f"select count(*) as C01 from ({clean}) A"
        row = self.execute(count_sql, 0, False)
        if isinstance(row, Mapping) and row.get("C01") is not None:
            return int(row["C01"])
        return 0

    def getAll(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]] | bool:
        """查询多行；非 ``only_data`` 时同时更新 ``last_total``。"""
        exArr = dict(exArr or {})
        where_sql, params = self._parse_where(whArr)
        exArr["where"] = where_sql
        if not exArr.get("only_data"):
            count_options = dict(exArr)
            count_options.pop("where", None)
            self.last_total = self.count(table, whArr, count_options)
            if self.last_total == 0:
                return []
        else:
            self.last_total = None
        sql = self.mkQuery(table, exArr)
        result = self.execute(sql, 0, True, params)
        return result if isinstance(result, list) else False

    def getOne(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None | bool:
        """查询一行。"""
        exArr = dict(exArr or {})
        exArr["limit"] = 1
        exArr["only_data"] = True
        rows = self.getAll(table, whArr, exArr)
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows

    def delete(self, table: str, whArr: Any, limit: int = 1) -> int | bool:
        """按条件删除，最多删除 ``limit`` 行。"""
        where_sql, params = self._parse_where(whArr)
        if not where_sql:
            self._warning = "DELETE 条件不能为空"
            return False
        sql = (
            f"delete from {self._quote_table(table)} "
            f"where {where_sql} limit {max(0, int(limit))}"
        )
        return self.execute(sql, -1, False, params)

    def remove(self, table: str, exArr: Any, limit: int = 1) -> int | bool:
        """delete 的兼容别名。"""
        return self.delete(table, exArr, limit)

    def insert(
        self,
        table: str,
        dataArr: Mapping[str, Any],
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool | dict[str, Any]:
        """插入一行。"""
        return self.inserts(table, [dataArr], exArr)

    def add(self, table: str, dataArr: Mapping[str, Any]) -> int | bool:
        """insert 的兼容别名。"""
        return self.insert(table, dataArr)

    def replace(
        self,
        table: str,
        dataArr: Mapping[str, Any],
    ) -> int | bool | dict[str, Any]:
        """REPLACE INTO 一行。"""
        return self.insert(table, dataArr, {"replaced": True})

    def inserts(
        self,
        table: str,
        dataArr: Sequence[Mapping[str, Any]],
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool | dict[str, Any]:
        """批量插入、REPLACE、INSERT IGNORE 或 ON DUPLICATE KEY UPDATE。"""
        rows = list(dataArr or [])
        if not rows or not rows[0]:
            self._warning = "插入数据不能为空"
            return False

        exArr = dict(exArr or {})
        fields = list(rows[0].keys())
        if not fields:
            return False

        # 所有行必须具有相同字段。字段缺失会让参数数量与列数不一致，
        # 因此这里提前拒绝，避免生成半截 SQL。
        for row in rows:
            if list(row.keys()) != fields:
                self._warning = "批量插入的每一行必须使用相同字段和顺序"
                return False

        quoted_fields = ",".join(self._quote_identifier(field) for field in fields)
        value_groups: list[str] = []
        params: list[Any] = []
        field_set = set(fields)

        for row in rows:
            placeholders: list[str] = []
            for field in fields:
                value = self.valCorrectize(row[field], field, exArr)
                if isinstance(value, str) and (
                    field in field_set
                    and isinstance(row[field], Mapping)
                    and (
                        "express" in row[field]
                        or "json" in row[field]
                    )
                ):
                    placeholders.append(value)
                else:
                    placeholders.append("%s")
                    params.append(value)
            value_groups.append("(" + ",".join(placeholders) + ")")

        replaced = bool(exArr.get("replaced", False))
        ignored = bool(exArr.get("ignored", False)) and not replaced
        delayed = bool(exArr.get("delayed", False))
        operation = "replace" if replaced else "insert"
        delay = "delayed" if delayed else ""
        ignore = "ignore" if ignored else ""
        on_duplicate = ""

        if exArr.get("ondups"):
            # 格式：update:name,mobile|ignore:id|express:score=score+1
            options = self._parse_duplicate_options(str(exArr["ondups"]))
            update_fields = options.get("update", [])
            ignored_fields = set(options.get("ignore", []))
            if update_fields:
                candidates = [field for field in update_fields if field in fields]
            else:
                candidates = [field for field in fields if field not in ignored_fields]

            assignments: list[str] = []
            for field in candidates:
                if field in ignored_fields:
                    continue
                assignments.append(
                    f"{self._quote_identifier(field)}="
                    f"values({self._quote_identifier(field)})"
                )
            for field, expression in options.get("express", {}).items():
                assignments.append(
                    f"{self._quote_identifier(field)}={expression}"
                )
            if assignments:
                on_duplicate = " on duplicate key update " + ",".join(assignments)

        sql = (
            f"{operation} {delay} {ignore} into {self._quote_table(table)}"
            f"({quoted_fields}) values {','.join(value_groups)}{on_duplicate}"
        )

        if self._preview:
            return {
                "type": operation,
                "ignore": ignore,
                "table": self._quote_table(table),
                "fields": quoted_fields,
                "values": value_groups,
                "sql": sql,
                "params": params,
            }
        return self.execute(sql, 1, False, params)

    @staticmethod
    def _parse_duplicate_options(text: str) -> dict[str, Any]:
        """解析 ``update:...|ignore:...|express:a=a+1``。"""
        result: dict[str, Any] = {"update": [], "ignore": [], "express": {}}
        for item in text.strip(" |").split("|"):
            item = item.strip()
            if not item:
                continue
            if ":" in item:
                key, value = item.split(":", 1)
            else:
                key, value = item, ""
            key = key.strip().lower()
            if key in {"update", "ignore"}:
                result[key] = [
                    field.strip()
                    for field in value.split(",")
                    if field.strip()
                ]
            elif key == "express":
                for expression in value.split(","):
                    if "=" in expression:
                        field, expr = expression.split("=", 1)
                        result["express"][field.strip()] = expr.strip()
        return result

    def update(
        self,
        table: str,
        valArr: Mapping[str, Any],
        whs: Any,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool:
        """按条件更新。"""
        if not valArr:
            return False
        exArr = dict(exArr or {})
        assignments: list[str] = []
        params: list[Any] = []
        for field, raw_value in valArr.items():
            value = self.valCorrectize(raw_value, field, exArr)
            quoted_field = self._quote_identifier(field)
            if isinstance(raw_value, Mapping) and "express" in raw_value:
                assignments.append(f"{quoted_field}={value}")
            else:
                assignments.append(f"{quoted_field}=%s")
                params.append(value)

        where_sql, where_params = self._parse_where(whs)
        if not where_sql:
            self._warning = "UPDATE 条件不能为空"
            return False
        params.extend(where_params)

        sql = (
            f"update {self._quote_table(table)} "
            f"set {','.join(assignments)} where {where_sql}"
        )
        if exArr.get("limit") is not None:
            sql += f" limit {max(0, int(exArr['limit']))}"
        return self.execute(sql, 2, False, params)

    def getDesc(self, table: str) -> dict[str, dict[str, Any]] | bool:
        """返回 ``SHOW FULL COLUMNS`` 整理后的字段描述。"""
        if str(table).strip().lower().startswith("select "):
            explained = self.query("explain " + str(table).strip(), False)
            if not explained:
                return False
            table = explained.get("table") or explained.get("Table")
        try:
            sql = f"show full columns from {self._quote_table(table)}"
        except ValueError:
            return False
        rows = self.execute(sql, 0, True)
        if not isinstance(rows, list):
            rows = self.execute(
                f"desc {self._quote_table(table)}",
                0,
                True,
            )
        if not isinstance(rows, list) or not rows:
            return False

        result: dict[str, dict[str, Any]] = {}
        for raw_row in rows:
            row = {
                self._as_text(key): self._as_text(value)
                for key, value in raw_row.items()
            }
            field = str(row.get("Field", ""))
            raw_type = str(row.get("Type", ""))
            lengths = re.search(r"\((\d+)(?:,(\d+))?\)", raw_type)
            base_type = re.sub(
                r"\([^)]*\)|\s+unsigned|\s+zerofill",
                "",
                raw_type,
                flags=re.IGNORECASE,
            ).strip().lower()
            item: dict[str, Any] = {
                "name": field,
                "type": base_type,
                "lens": int(lengths.group(1)) if lengths else None,
                "null": (
                    "NULL"
                    if str(row.get("Null", "")).upper() == "YES"
                    else "NOT NULL"
                ),
                "prik": "PK" if str(row.get("Key", "")).upper() == "PRI" else "",
                "unix": "UNI" if str(row.get("Key", "")).upper() == "UNI" else "",
                "indx": "MUL" if str(row.get("Key", "")).upper() == "MUL" else "",
                "deft": row.get("Default"),
                "auto": row.get("Extra", ""),
                "comm": row.get("Comment"),
                "unsigned": "unsigned" in raw_type.lower(),
            }
            if lengths and lengths.group(2) is not None:
                item["xArr"] = {
                    "len1": int(lengths.group(1)),
                    "len2": int(lengths.group(2)),
                    "dot": "." if int(lengths.group(2)) > 0 else "",
                }
            result[field] = item
        return result

    def getCreates(self, table: str) -> str | bool:
        """返回 CREATE TABLE SQL 文本。"""
        row = self.query(
            f"show create table {self._quote_table(table)}",
            False,
        )
        if not isinstance(row, Mapping):
            return False
        for key, value in row.items():
            if str(key).lower() in {"create table", "create view"}:
                return self._as_text(value)
        return row

    @staticmethod
    def _as_text(value: Any) -> Any:
        """把 Old MySQL Connector 偶尔返回的 bytes 元数据转成文本。"""
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).decode("utf-8", errors="replace")
        return value

    # ------------------------------------------------------------------
    # 事务
    # ------------------------------------------------------------------
    def Begin(self) -> bool:
        """开始事务；支持嵌套计数，最外层才真正开启。"""
        transaction = getattr(self._local, "transaction", None)
        if transaction is None:
            connection = self._borrow_connection()
            connection.ping(reconnect=True, attempts=1, delay=0)
            connection.autocommit = False
            transaction = {
                "connection": connection,
                "depth": 0,
            }
            self._local.transaction = transaction

        transaction["depth"] += 1
        if transaction["depth"] == 1:
            try:
                transaction["connection"].start_transaction()
                return True
            except mysql.connector.Error as exc:
                self._error = str(exc)
                return False
        return True

    def Commit(self) -> bool:
        """提交最外层事务。"""
        transaction = getattr(self._local, "transaction", None)
        if not transaction:
            self._warning = "没有活动事务"
            return False
        transaction["depth"] -= 1
        if transaction["depth"] > 0:
            return True

        connection = transaction["connection"]
        try:
            connection.commit()
            return True
        except mysql.connector.Error as exc:
            self._error = str(exc)
            return False
        finally:
            connection.autocommit = True
            connection.close()
            self._local.transaction = None

    def Rollback(self) -> bool:
        """回滚最外层事务并归还连接。"""
        transaction = getattr(self._local, "transaction", None)
        if not transaction:
            self._warning = "没有活动事务"
            return False
        transaction["depth"] -= 1
        if transaction["depth"] > 0:
            return True

        connection = transaction["connection"]
        try:
            connection.rollback()
            return True
        except mysql.connector.Error as exc:
            self._error = str(exc)
            return False
        finally:
            connection.autocommit = True
            connection.close()
            self._local.transaction = None

    # ------------------------------------------------------------------
    # 基础辅助接口
    # ------------------------------------------------------------------
    def getCount(self, sql: str) -> int | float:
        """统计已有 SELECT 的结果数。"""
        return self._count_select_sql(str(sql))

    def preview(self, enabled: bool = True) -> "Mypdb":
        """开启后，inserts 只返回将要执行的 SQL 结构，不真正写入。"""
        self._preview = bool(enabled)
        return self

    def lock(self, table: str, ltype: str = "read") -> bool:
        """表锁入口；业务类中可重写为固定连接版本。"""
        if not table or ltype.lower() not in {"read", "write"}:
            return False
        return bool(
            self.execute(
                f"lock tables {self._quote_table(table)} {ltype.lower()}",
                9,
            )
        )

    def unlock(self) -> bool:
        """释放当前连接上的表锁。"""
        return bool(self.execute("unlock tables", 9))

    def getSqls(
        self,
        lasted: bool = False,
        as_string: bool = False,
    ) -> list[str] | str | None:
        """返回 SQL 历史；``lasted=True`` 只取最后一条，``as_string=True`` 返回文本。"""
        history = list(self._sqls)
        if lasted:
            return history[-1] if history else None
        if as_string:
            return "\n".join(history) + ("\n" if history else "")
        return history

    def cleanSql(self) -> None:
        """清空 SQL 历史。"""
        self._sqls.clear()

    def getError(self) -> str | None:
        """返回最后一次错误。"""
        return getattr(self, "_error", None)

    def getWarning(self) -> str | None:
        """返回最后一次警告。"""
        return self._warning


class Mysql(Mypdb):
    """完整业务入口：在 Mypdb 底层能力上实现查询和写入接口。"""

    # 同名方法显式保留 Mysql 类入口，实际事务仍复用 Mypdb 中唯一的
    # 连接池实现，避免两套逻辑产生差异。
    def Begin(self) -> bool:
        """开始事务。"""
        return super().Begin()

    def Commit(self) -> bool:
        """提交事务。"""
        return super().Commit()

    def Rollback(self) -> bool:
        """回滚事务。"""
        return super().Rollback()

    def GetError(self) -> str | None:
        """返回最后一次错误。"""
        return super().getError()

    def MkQuery(
        self,
        table: str,
        exArr: Mapping[str, Any] | None = None,
    ) -> str:
        """复用 Mypdb 的 SQL 构造实现。"""
        return super().mkQuery(table, exArr)

    def GetCreates(self, table: str) -> str | bool:
        """复用 Mypdb 的建表语句查询。"""
        return super().getCreates(table)

    def IsTransaction(self) -> bool:
        """当前线程是否处于事务中。"""
        return bool(getattr(self._local, "transaction", None))

    def DbTransaction(self) -> Any:
        """返回当前事务连接；无事务时返回 False。"""
        transaction = getattr(self._local, "transaction", None)
        return transaction["connection"] if transaction else False

    # ------------------------------------------------------------------
    # 表结构缓存及字段处理
    # ------------------------------------------------------------------
    def LoadTableCache(self, table: str) -> dict[str, Any] | None:
        """加载并缓存表结构。"""
        with self._schema_lock:
            if table in self._schema_cache:
                return {
                    "types": self._schema_cache[table],
                    "prkey": self._primary_key_cache.get(table),
                }

        types = self.getDesc(table)
        if not types:
            return None
        primary_key = next(
            (
                field
                for field, description in types.items()
                if description.get("prik") == "PK"
            ),
            None,
        )
        with self._schema_lock:
            self._schema_cache[table] = types
            self._primary_key_cache[table] = primary_key
            return {"types": types, "prkey": primary_key}

    def ClearTableCache(self, table: str | None = None) -> None:
        """清空全部或指定表的结构缓存。"""
        with self._schema_lock:
            if table is None:
                self._schema_cache.clear()
                self._primary_key_cache.clear()
            else:
                self._schema_cache.pop(table, None)
                self._primary_key_cache.pop(table, None)

    def TableExists(self, table: str) -> bool:
        """表是否存在。"""
        return self.LoadTableCache(table) is not None

    def GetTypes(self, table: str) -> dict[str, dict[str, Any]] | bool:
        """返回表字段类型缓存。"""
        cache = self.LoadTableCache(table)
        return cache["types"] if cache else False

    def GetPrimaryKey(self, table: str) -> str | bool:
        """返回主键字段名。"""
        cache = self.LoadTableCache(table)
        if not cache:
            return False
        return cache.get("prkey") or False

    def IsPrimaryKey(self, table: str, field: str) -> bool:
        """判断字段是否为主键。"""
        types = self.GetTypes(table)
        return bool(
            types
            and types.get(field, {}).get("prik") == "PK"
        )

    def IsUniqueKey(self, table: str, field: str) -> bool:
        """判断字段是否为主键或唯一键。"""
        types = self.GetTypes(table)
        if not types:
            return False
        item = types.get(field, {})
        return item.get("prik") == "PK" or item.get("unix") == "UNI"

    def FtFields(self, table: str, fields: str) -> str:
        """过滤、校验并包装查询字段。

        支持 ``*``、``f1,f2``、``alias=f1``、``^f1,f2``（排除字段）、
        SQL 聚合表达式，以及 ``distinct`` 前缀。
        """
        if not fields or fields.strip() == "*":
            return "*"

        text = fields.strip()
        distinct = ""
        if text.lower().startswith("distinct "):
            distinct = "distinct "
            text = text[9:].strip()

        types = self.GetTypes(table)
        if not types:
            return fields
        valid_fields = list(types.keys())
        quoted: list[str] = []
        parts = self._split_sql_list(text)

        # 排除模式：^field1,field2
        if parts and parts[0].startswith("^"):
            excluded = {
                part.strip().lstrip("^").strip("`")
                for part in parts
                if part.strip().lstrip("^")
            }
            quoted.extend(
                self._quote_identifier(field)
                for field in valid_fields
                if field not in excluded
            )
            return distinct + ",".join(quoted) if quoted else "*"

        for part in parts:
            item = part.strip()
            if not item:
                continue
            lower = item.lower()
            if lower == "*":
                quoted.append("*")
                continue

            # 函数、算术和 CASE 等表达式不拆别名，直接保留。
            if "(" in item or any(op in item for op in "+-*/"):
                quoted.append(item)
                continue

            # 常见四种别名写法：alias=f1、f1 alias、f1 as alias、f1.`alias`
            if "=" in item:
                alias, real = item.split("=", 1)
                real = real.strip()
                if real.strip("`") in valid_fields:
                    quoted.append(
                        f"{self._quote_identifier(real)} "
                        f"{self._quote_identifier(alias)}"
                    )
                continue

            match = re.match(r"^(.*?)\s+(?:as\s+)?(.*?)$", item, re.I)
            if match:
                real = match.group(1).strip()
                alias = match.group(2).strip()
            else:
                real = item
                alias = None
            real_clean = real.strip("`")
            if real_clean not in valid_fields:
                continue
            expression = self._quote_identifier(real_clean)
            if alias:
                expression += " " + self._quote_identifier(alias.strip("`"))
            quoted.append(expression)

        if not quoted:
            return "*"
        return distinct + ",".join(quoted)

    @staticmethod
    def _split_sql_list(text: str) -> list[str]:
        """按顶层逗号拆分 SQL 字段列表，不拆函数括号内的逗号。"""
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        quote: str | None = None
        for char in text:
            if quote:
                current.append(char)
                if char == quote:
                    quote = None
                continue
            if char in {"'", '"', "`"}:
                quote = char
                current.append(char)
            elif char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth = max(0, depth - 1)
                current.append(char)
            elif char == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)
        parts.append("".join(current))
        return parts

    @staticmethod
    def _is_matrix(records: Any) -> bool:
        """判断是否为二维记录列表。"""
        return (
            isinstance(records, Sequence)
            and not isinstance(records, (str, bytes, bytearray))
            and all(isinstance(item, Mapping) for item in records)
        )

    def _filter_values(self, table: str, records: Any) -> Any:
        """删除未知字段并按数据库类型调整 Python 值。"""
        types = self.GetTypes(table)
        if not types or not records:
            return records

        def filter_one(record: Mapping[str, Any]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for field, value in record.items():
                description = types.get(field)
                if not description:
                    continue
                value = self.valCorrectize(value, field)
                base_type = str(description.get("type", "")).lower()
                if value is not None and base_type in self._INT_TYPES:
                    try:
                        value = int(value)
                    except (TypeError, ValueError):
                        pass
                elif value is not None and base_type in self._FLOAT_TYPES:
                    try:
                        value = float(value)
                    except (TypeError, ValueError):
                        pass
                elif base_type in self._DATE_TYPES and value == "":
                    continue
                result[field] = value
            return result

        if self._is_matrix(records):
            return [filter_one(record) for record in records]
        if isinstance(records, Mapping):
            return filter_one(records)
        return records

    def _fix_values(self, table: str, record: dict[str, Any]) -> None:
        """补齐 NOT NULL 且无默认值的字段。"""
        types = self.GetTypes(table)
        if not types:
            return
        for field, description in types.items():
            if field not in record:
                if description.get("prik") == "PK" or description.get("unix") == "UNI":
                    continue
                if (
                    description.get("deft") is None
                    and description.get("null") == "NOT NULL"
                    and "auto_increment" not in str(description.get("auto", "")).lower()
                ):
                    record[field] = ""
                continue

            if description.get("prik") == "PK":
                continue
            if "auto_increment" in str(description.get("auto", "")).lower():
                continue
            if (
                record[field] is None
                and description.get("null") == "NOT NULL"
                and description.get("deft") is None
            ):
                record[field] = ""
            if (
                record[field] == ""
                and str(description.get("type", "")).lower() in self._DATE_TYPES
            ):
                record.pop(field, None)

    # ------------------------------------------------------------------
    # DAO 查询
    # ------------------------------------------------------------------
    def FtExtras(self, table: str, exArr: dict[str, Any]) -> None:
        """把 ``join_user`` 形式的简写转换成 ``join`` 配置。"""
        prefixer = exArr.get("prefixer", table)
        joins = dict(exArr.get("join") or {})
        for key in list(exArr.keys()):
            if not key.lower().startswith("join"):
                continue
            value = exArr[key]
            if isinstance(value, str) and ":" in value:
                joins[str(prefixer) + key[4:]] = value
                exArr.pop(key, None)
        if joins:
            exArr["join"] = joins

    def _scalar_to_primary(self, table: str, value: Any) -> dict[str, Any]:
        """标量条件默认解释为主键，找不到主键时使用 id。"""
        field = self.GetPrimaryKey(table) or "id"
        return {str(field): value}

    def GetMore(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
        total: list[int] | None = None,
    ) -> list[dict[str, Any]] | bool:
        """查询多行，并支持分页、别名、keyas 和递归 join 组装。"""
        if not isinstance(table, str):
            self._warning = "表名必须是字符串"
            return False
        exArr = dict(exArr or {})
        if exArr:
            self.FtExtras(table, exArr)

        exArr["page"] = max(1, int(exArr.get("page", 1)))
        exArr["limit"] = int(exArr.get("limit", 20))
        if exArr.get("fields"):
            exArr["fields"] = self.FtFields(table, str(exArr["fields"]))

        if isinstance(whArr, (str, int, float, decimal.Decimal)):
            whArr = self._scalar_to_primary(table, whArr)

        exArr["only_data"] = bool(exArr.get("only_data", False))
        rows = self.getAll(table, whArr, exArr)
        if not isinstance(rows, list):
            return rows
        else:
            for row in rows:
                if isinstance(row, dict):
                    for field, value in row.items():
                        if isinstance(value, (_datetime.datetime, _datetime.date, _datetime.time)):
                            row[field] = value.strftime("%Y-%m-%d %H:%M:%S")
        if total is not None:
            total.append(int(self.last_total or 0))
        if not rows:
            return rows

        # alias 支持 "userId=id,type=1"：字段存在时复制字段值，
        # 字段不存在时直接写入常量。
        if exArr.get("alias"):
            alias_map = self._parse_key_value_options(str(exArr["alias"]))
            for row in rows:
                for alias, real in alias_map.items():
                    if real in row:
                        row[alias] = row[real]
                    else:
                        row[alias] = real

        if exArr.get("join") and not exArr.get("aggregated"):
            for join_table, join_rule in exArr["join"].items():
                rows = self._apply_join(rows, str(join_table), str(join_rule))

        if exArr.get("keyas"):
            rows = self._field_as_key(rows, str(exArr["keyas"]))
        return rows

    def GetTotal(self) -> int | None:
        """返回上次查询的总数；仅在 ``GetMore`` 时有效。"""
        return self.last_total

    def GetData(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | Sequence[Any] | int | float | None = None,
    ) -> list[dict[str, Any]] | bool:
        """只返回数据列表，不额外计算 total。"""
        if isinstance(exArr, (int, float, decimal.Decimal)):
            options = {"only_data": True, "limit": int(exArr)}
        else:
            options = dict(exArr or {})
            options["only_data"] = True
        return self.GetMore(table, whArr, options)

    def GetAtom(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None | bool:
        """返回一行记录；无数据时返回 None，执行失败时返回 False。"""
        options = dict(exArr or {})
        options["limit"] = 1
        options["only_data"] = True
        rows = self.GetMore(table, whArr, options)
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows

    def GetCount(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | float:
        """DAO 条件计数，同时兼容底层 ``GetCount(sql)`` 调用。"""
        return self.count(table, whArr, exArr)

    # ------------------------------------------------------------------
    # DAO 写入
    # ------------------------------------------------------------------
    def _existence_options(self, text: str) -> tuple[list[str], dict[str, Any]]:
        """解析 AddAtom 的 ``exists`` 参数。"""
        fields_text, _, actions_text = text.partition("|")
        fields = [
            item
            for item in re.split(r"[^A-Za-z0-9_]+", fields_text)
            if item
        ]
        actions: dict[str, Any] = {}
        for action in actions_text.strip(" |").split("|"):
            if not action:
                continue
            if "=" in action:
                key, value = action.split("=", 1)
                actions[key.strip().lower()] = value.strip()
            else:
                actions[action.strip().lower()] = ""
        return fields, actions

    def AddAtom(
        self,
        table: str,
        data: Mapping[str, Any] | None = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool:
        """添加一行；兼容 exists、ondups 和 join 写入。"""
        if not data or not isinstance(data, Mapping):
            return False
        exArr = dict(exArr or {})
        record = dict(data)
        self._fix_values(table, record)
        if not record:
            return False
        if exArr:
            self.FtExtras(table, exArr)
        record = self._filter_values(table, record)

        # exists 用于"存在则更新，不存在则插入"。它适合并发不高的
        # 普通业务；高并发唯一键场景建议直接使用 ondups。
        if exArr.get("exists"):
            exists_fields, actions = self._existence_options(str(exArr["exists"]))
            where = {
                field: record[field]
                for field in exists_fields
                if field in record
            }
            if where and self.GetAtom(table, where):
                if "update" in actions:
                    keys = {
                        field.strip()
                        for field in str(actions["update"]).split(",")
                        if field.strip()
                    }
                    update = {
                        key: value
                        for key, value in record.items()
                        if key not in where and (not keys or key in keys)
                    }
                    ignored = {
                        field.strip()
                        for field in str(actions.get("ignore", "")).split(",")
                        if field.strip()
                    }
                    update = {
                        key: value
                        for key, value in update.items()
                        if key not in ignored
                    }
                    if update:
                        return self.Updates(table, update, where, exArr)
                return True

        inserted_id = self.insert(table, record, exArr)
        if inserted_id is False:
            return False

        # 子表 join 写入规则：fields 指定来源字段，
        # "^" 前缀表示排除，字段后的 ".default" 表示缺省常量。
        for join_table, join_rule in (exArr.get("join") or {}).items():
            child_table, left_field, right_field, options = self._parse_join_rule(
                str(join_table),
                str(join_rule),
            )
            selected = options.get("fields")
            if not selected:
                continue
            joint_data: dict[str, Any] = {}
            for item in self._split_sql_list(str(selected)):
                for one in item.split(","):
                    one = one.strip()
                    if not one:
                        continue
                    if one.startswith("^"):
                        continue
                    source, _, default = one.partition(".")
                    if source in record:
                        joint_data[source] = record[source]
                    elif default:
                        joint_data[source] = default
            joint_data[right_field] = inserted_id
            if joint_data:
                self.AddAtom(
                    child_table,
                    joint_data,
                    {"ondups": options.get("ondups", "")},
                )
        if isinstance(inserted_id, Mapping):
            return dict(inserted_id)
        return int(inserted_id)

    def AddMore(
        self,
        table: str,
        dataArr: Sequence[Mapping[str, Any]] | None = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool:
        """批量添加多行。"""
        if not dataArr or not isinstance(dataArr, Sequence):
            return False
        records = [dict(record) for record in dataArr]
        for record in records:
            self._fix_values(table, record)
        records = self._filter_values(table, records)
        if not records:
            return False
        return self.inserts(table, records, exArr)

    def Updates(
        self,
        table: str,
        data: Mapping[str, Any] | None,
        whArr: Any = None,
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool:
        """按条件修改一行或多行，并支持简单 join 更新。"""
        if data is None or whArr is None or whArr == {} or whArr == []:
            return False
        exArr = dict(exArr or {})
        if exArr:
            self.FtExtras(table, exArr)
        cleaned = {
            key: value
            for key, value in dict(data).items()
            if value is not None
        }
        update = self._filter_values(table, cleaned)
        if not isinstance(update, Mapping) or not update:
            return False
        if isinstance(whArr, (str, int, float, decimal.Decimal)):
            whArr = self._scalar_to_primary(table, whArr)

        affected = self.update(table, update, whArr, exArr)
        if affected is False:
            return False

        # 简单 join 更新：join_table => "left:right|fields=..."。
        for join_table, join_rule in (exArr.get("join") or {}).items():
            child_table, left_field, right_field, options = self._parse_join_rule(
                str(join_table),
                str(join_rule),
            )
            selected = options.get("fields")
            if not selected or left_field not in whArr:
                continue
            fields = [
                item.strip()
                for item in self._split_sql_list(str(selected))
                if item.strip() and not item.strip().startswith("^")
            ]
            joint_update = {
                field: data[field]
                for field in fields
                if field in data
            }
            if not joint_update:
                continue
            where = {right_field: whArr[left_field]}
            if options.get("where"):
                where[options["where"]] = True
            if options.get("onmiss") == "insert":
                if not self.GetAtom(child_table, where):
                    self.AddAtom(
                        child_table,
                        {**joint_update, right_field: whArr[left_field]},
                    )
                    continue
            self.Updates(child_table, joint_update, where)
        return affected

    def Replaces(
        self,
        table: str,
        dataArr: Sequence[Mapping[str, Any]],
        fields: str = "id",
        exArr: Mapping[str, Any] | None = None,
    ) -> bool:
        """按指定唯一字段逐行更新或插入。"""
        keys = [
            item.strip()
            for item in str(fields).split(",")
            if item.strip()
        ]
        for original in dataArr or []:
            row = {
                key: value
                for key, value in dict(original).items()
                if value is not None
            }
            where = {
                key: row[key]
                for key in keys
                if key in row
            }
            if not where:
                return False
            result = (
                self.Updates(table, row, where, exArr)
                if self.GetAtom(table, where)
                else self.AddAtom(table, row, exArr)
            )
            if result is False:
                return False
        return True

    def Packages(
        self,
        table: str,
        dataArr: Mapping[Any, Mapping[str, Any]],
        whArr: Mapping[str, Sequence[Any]],
        exArr: Mapping[str, Any] | None = None,
    ) -> int | bool:
        """批量 CASE WHEN 更新，一次 SQL 完成多行不同值更新。"""
        del exArr
        if not dataArr or not whArr:
            return False
        where_field = next(iter(whArr))
        where_values = list(whArr[where_field])
        if len(dataArr) != len(where_values):
            return False

        update_fields: list[str] = []
        for row in dataArr.values():
            for field in row:
                if field not in update_fields:
                    update_fields.append(field)
        if not update_fields:
            return False

        assignments: list[str] = []
        params: list[Any] = []
        quoted_where = self._quote_identifier(where_field)
        for field in update_fields:
            chunks = [f"{self._quote_identifier(field)}=case {quoted_where}"]
            for case_value in where_values:
                row = dataArr.get(case_value, dataArr.get(str(case_value)))
                if row is None or field not in row:
                    continue
                chunks.append(f"when %s then %s")
                params.extend([case_value, row[field]])
            chunks.append("else " + self._quote_identifier(field) + " end")
            assignments.append(" ".join(chunks))

        placeholders = ",".join(["%s"] * len(where_values))
        params.extend(where_values)
        sql = (
            f"update {self._quote_table(table)} "
            f"set {','.join(assignments)} "
            f"where {quoted_where} in ({placeholders})"
        )
        return self.execute(sql, 2, False, params)

    def Deletes(
        self,
        table: str,
        whArr: Any = None,
        exArr: Mapping[str, Any] | int | None = None,
    ) -> int | bool:
        """按条件删除；默认只删除一行，unlimit 时删除全部匹配项。"""
        if not table or whArr is None or whArr == {} or whArr == []:
            return False
        if isinstance(exArr, (int, float, decimal.Decimal)):
            limit = int(exArr)
            options: dict[str, Any] = {}
        else:
            options = dict(exArr or {})
            if options.get("unlimit"):
                limit = 2**63 - 1
            else:
                limit = int(options.get("limit", 1))
        if options:
            self.FtExtras(table, options)
        if isinstance(whArr, (str, int, float, decimal.Decimal)):
            whArr = self._scalar_to_primary(table, whArr)

        deleted = self.delete(table, whArr, limit)
        if deleted is False:
            return False

        # 简单级联删除，只串联一层 join。
        for join_table, join_rule in (options.get("join") or {}).items():
            child_table, left_field, right_field, join_options = self._parse_join_rule(
                str(join_table),
                str(join_rule),
            )
            if left_field not in whArr:
                continue
            limit_child = 1 if self.IsUniqueKey(child_table, right_field) else 10000
            self.Deletes(
                child_table,
                {right_field: whArr[left_field]},
                {"limit": limit_child, **join_options},
            )
        return deleted

    # ------------------------------------------------------------------
    # join 辅助
    # ------------------------------------------------------------------
    def _parse_join_rule(
        self,
        table_spec: str,
        rule: str,
    ) -> tuple[str, str, str, dict[str, Any]]:
        """解析 ``table alias => left:right|flat=a,b|defaults=x=0``。"""
        main, _, options_text = rule.partition("|")
        if ":" not in main:
            raise ValueError(f"非法 join 规则: {rule!r}")
        left, right = main.split(":", 1)
        options = self._parse_join_options(options_text)
        table_name, alias = self._split_table_alias(table_spec)
        options["alias"] = alias
        return table_name, left.strip(), right.strip(), options

    @staticmethod
    def _split_table_alias(table_spec: str) -> tuple[str, str]:
        """拆出 ``db.table alias``，未写别名时根据表名生成合理默认值。"""
        text = table_spec.strip()
        match = re.match(r"^(.+?)\s+([A-Za-z0-9_]+)$", text)
        if match:
            table, alias = match.group(1), match.group(2)
        else:
            table = text
            base = table.rsplit(".", 1)[-1]
            alias = base.rsplit("_", 1)[-1] if "_" in base else base
        return table, alias

    @staticmethod
    def _parse_join_options(text: str) -> dict[str, Any]:
        """解析 join 规则右侧的小型 ``key=value`` 参数集合。"""
        options: dict[str, Any] = {}
        if not text:
            return options
        for item in text.split("|"):
            item = item.strip()
            if not item:
                continue
            if "=" in item:
                key, value = item.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key in {"flat", "fields"}:
                    options[key] = value
                elif key == "defaults":
                    options[key] = Mysql._parse_key_value_options(value)
                elif key == "where":
                    options[key] = value
                else:
                    options[key] = value
            else:
                options[item] = True
        return options

    @staticmethod
    def _parse_key_value_options(text: str) -> dict[str, str]:
        """解析 ``a=b,c=d`` 或 ``a=b&c=d`` 形式的配置。"""
        result: dict[str, str] = {}
        normalized = text.replace("&", ",")
        for item in normalized.split(","):
            if not item.strip():
                continue
            if "=" in item:
                key, value = item.split("=", 1)
                result[key.strip()] = value.strip()
        return result

    def _apply_join(
        self,
        rows: list[dict[str, Any]],
        table_spec: str,
        rule: str,
    ) -> list[dict[str, Any]]:
        """执行一轮 join，支持一对一扁平化和一对多树形组装。"""
        child_table, left_field, right_field, options = self._parse_join_rule(
            table_spec,
            rule,
        )
        left_values = list(
            dict.fromkeys(
                row[left_field]
                for row in rows
                if left_field in row and row[left_field] is not None
            )
        )
        if not left_values:
            return rows
        child_where: dict[str, Any] = {right_field + " in": left_values}
        child_options: dict[str, Any] = {
            "only_data": True,
            "limit": max(1000 * len(left_values), 1000),
        }
        if options.get("order"):
            child_options["order"] = options["order"]
        child_rows = self.GetMore(child_table, child_where, child_options)
        if not isinstance(child_rows, list):
            return rows

        unique = self.IsUniqueKey(child_table, right_field)
        if unique:
            child_by_key = {
                row.get(right_field): row
                for row in child_rows
            }
            if options.get("flat"):
                flat_fields = [
                    field.strip()
                    for field in str(options["flat"]).split(",")
                    if field.strip()
                ]
                defaults = options.get("defaults") or {}
                prefix = str(options.get("prefix", ""))
                for row in rows:
                    child = child_by_key.get(row.get(left_field), {})
                    for field in flat_fields:
                        row[prefix + field] = child.get(field, defaults.get(field))
                return rows

            alias = options.get("alias") or left_field
            defaults = options.get("defaults") or {}
            for row in rows:
                child = child_by_key.get(row.get(left_field))
                row[alias] = dict(child) if child else dict(defaults)
            return rows

        grouped: dict[Any, list[dict[str, Any]]] = {}
        for child in child_rows:
            grouped.setdefault(child.get(right_field), []).append(child)
        alias = options.get("alias") or left_field
        for row in rows:
            row[alias] = grouped.get(row.get(left_field), [])
        return rows

    @staticmethod
    def _field_as_key(
        rows: list[dict[str, Any]],
        field: str,
    ) -> dict[Any, dict[str, Any]]:
        """把结果列表按指定字段转换成字典。"""
        result: dict[Any, dict[str, Any]] = {}
        for row in rows:
            if field in row:
                result[row[field]] = row
        return result

    # ------------------------------------------------------------------
    # 通用执行、锁和调试
    # ------------------------------------------------------------------
    def Executes(self, sqlList: Any, etype: int = 0) -> Any:
        """执行一条或多条 SQL。"""
        single = isinstance(sqlList, str)
        statements = [sqlList] if single else list(sqlList or [])
        if not statements:
            return False
        results = [self.execute(str(sql), etype) for sql in statements]
        return results[0] if single else results

    def Locking(self, table: str | None = None, ltype: str = "read") -> bool:
        """给表加 READ 或 WRITE 锁。"""
        if not table or ltype.lower() not in {"read", "write"}:
            return False
        # MySQL 表锁属于连接级状态，普通 execute 会立即归还连接，
        # 因此这里临时固定一个事务连接，Release 时再解锁并归还。
        owned_transaction = False
        if not self.IsTransaction():
            if not self.Begin():
                return False
            owned_transaction = True
        ok = self.execute(
            f"lock tables {self._quote_table(table)} {ltype.lower()}",
            9,
        )
        if ok:
            self._local.table_lock_owned = owned_transaction
        elif owned_transaction:
            self.Rollback()
        return bool(ok)

    def Release(self, table: str | None = None) -> bool:
        """释放当前连接上的所有表锁。"""
        del table
        ok = bool(self.execute("unlock tables", 9))
        if getattr(self._local, "table_lock_owned", False):
            self._local.table_lock_owned = False
            self.Commit()
        return ok

    def Lock(self, table: str, ltype: str = "read") -> bool:
        """Locking 的底层别名。"""
        return self.Locking(table, ltype)

    def Unlock(self) -> bool:
        """Release 的底层别名。"""
        return self.Release()

    def MkWhere(self, whArr: Any) -> str | None:
        """生成 WHERE 文本。"""
        return self.parseWhere(whArr)

    def Encoding(
        self,
        encoding: str = "utf8mb4",
        oldencoding: list[str] | None = None,
    ) -> "Mysql":
        """切换连接字符集并重建池，确保后续长连接都使用新编码。"""
        if oldencoding is not None:
            oldencoding.append(self._encoding)
        self._encoding = str(encoding)
        self._converter = MySQLConverter(charset=self._encoding)
        self.reconnect()
        return self
