# MySql.py

`MySql.py` 是一个基于 `mysql.connector` 的 MySQL 数据访问层，提供连接池长连接、参数化 SQL、基础 CRUD、批量操作、事务、联表组装和表结构缓存。

## 文件说明

| 文件 | 说明 |
| --- | --- |
| `MySql.py` | 核心实现，对外使用 `Mysql` 类 |
| `demo.py` | 查询、总数统计和 SQL 历史示例 |
| `test.py` | 功能、事务、长连接和可选并发压力测试 |
| `README.md` | 使用和接口说明 |

## 环境要求

- Python 3.10 及以上
- MySQL 5.7、8.0 或兼容版本
- `mysql-connector-python`

安装依赖：

```powershell
python -m pip install mysql-connector-python
```

## 数据库配置

数据库参数由调用方显式传入。`MySql.py` 不读取环境变量，也不保存固定数据库配置。

```python
config = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "your_user",
    "pswd": "your_password",
    "dbName": "your_database",
    "pools": 10,
}
```

配置字段：

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | - | MySQL 地址 |
| `port` | 否 | `3306` | MySQL 端口 |
| `user` | 是 | - | 用户名 |
| `pswd` | 否 | `""` | 密码，也支持 `password` |
| `dbName` | 是 | - | 数据库名，也支持 `database` |
| `pools` | 否 | `10` | 连接池大小 |
| `charset` | 否 | `utf8mb4` | 字符集 |
| `connection_timeout` | 否 | `3` | 连接超时秒数 |
| `pool_reset_session` | 否 | `True` | 归还连接时是否重置会话 |
| `sql_history_size` | 否 | `1000` | SQL 历史上限 |

也可以将配置保存为不提交到仓库的 JSON 文件：

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "your_user",
  "pswd": "your_password",
  "dbName": "your_database",
  "pools": 10
}
```

## 快速开始

```python
from MySql import Mysql

config = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "your_user",
    "pswd": "your_password",
    "dbName": "your_database",
    "pools": 10,
}

db = Mysql(**config)
try:
    row = db.GetAtom("user", None, {"fields": "id,enabled"})
    print(row)
finally:
    db.close()
```

连接池会长期复用物理连接。应用运行期间应复用一个 `Mysql` 实例，不要为每次查询重复创建和关闭对象。

## 运行示例

```powershell
python demo.py --config config.json
```

也可以直接传入参数：

```powershell
python demo.py `
  --host 127.0.0.1 --port 3306 `
  --user your_user --password your_password `
  --database your_database
```

## 运行测试

```powershell
python test.py --config config.json
```

启用并发压力测试：

```powershell
python test.py --config config.json --stress
```

测试只创建随机命名的临时表，并在结束后自动清理，不会修改现有业务表。

## 查询接口

### GetAtom

返回一行记录。

```python
row = db.GetAtom("user", {"id": 1}, {"fields": "id,enabled"})
```

返回：

- 有数据：`dict`
- 无数据：`None`
- 执行失败：`False`

### GetMore

返回多行记录，支持分页、排序、字段选择、总数统计和联表组装。

```python
rows = db.GetMore(
    "user",
    {"status": 1},
    {
        "page": 1,
        "limit": 20,
        "order": "id desc",
        "fields": "id,name,status",
    },
)
total = db.GetTotal()
```

也可以直接接收总数：

```python
total = []
rows = db.GetMore("user", {"status": 1}, {"limit": 20}, total)
print(total[0])
```

### GetData

只返回列表，不额外执行总数统计。

```python
rows = db.GetData("user", {"status": 1}, {"limit": 100})
```

### GetCount

```python
total = db.GetCount("user", {"status": 1})
```

## 条件写法

```python
# 等值条件
where = {"status": 1, "type": "normal"}

# 比较、范围、模糊和 IN
where = {
    "id >=": 100,
    "score between": [60, 100],
    "name %": "张",
    "status in": [1, 2, 3],
}

# OR 条件
where = {
    "or": [
        {"status": 1, "type": "a"},
        {"status": 2, "type": "b"},
    ]
}
```

支持的主要运算符：

| 写法 | 含义 |
| --- | --- |
| `field` | 等于 |
| `field >`、`>=`、`<`、`<=`、`!=` | 比较 |
| `field in`、`field ni` | IN / NOT IN |
| `field between`、`field bt` | BETWEEN |
| `field %` | 前后模糊匹配 |
| `field *` | 尾部模糊匹配 |
| `field ^`、`field !` | NOT LIKE |
| `field null`、`field notnull` | NULL 判断 |
| `field match` | MySQL `MATCH ... AGAINST` |
| `field find_in_set` | `FIND_IN_SET` |

## 写入接口

### AddAtom

新增一行，返回自增 ID。

```python
user_id = db.AddAtom(
    "user",
    {
        "name": "alice",
        "status": 1,
    },
)
```

### AddMore

批量新增，返回首个自增 ID。

```python
first_id = db.AddMore(
    "user",
    [
        {"name": "alice", "status": 1},
        {"name": "bob", "status": 1},
    ],
)
```

### Updates

按条件更新，返回受影响行数。

```python
affected = db.Updates(
    "user",
    {"status": 2},
    {"id": user_id},
)
```

### Replaces

按指定唯一字段逐行更新或插入。

```python
ok = db.Replaces(
    "user",
    [{"name": "alice", "status": 2}],
    fields="name",
)
```

### Packages

一次 SQL 更新多行不同值。

```python
affected = db.Packages(
    "user",
    {
        "alice": {"score": "90.00"},
        "bob": {"score": "85.00"},
    },
    {"name": ["alice", "bob"]},
)
```

### Deletes

按条件删除，默认只删除一行。

```python
deleted = db.Deletes("user", {"id": user_id}, {"limit": 1})
```

删除所有匹配行：

```python
deleted = db.Deletes("user", {"status": 0}, {"unlimit": True})
```

## 事务

事务使用线程本地连接，同一事务内的多条 SQL 使用同一个物理连接。

```python
if not db.Begin():
    raise RuntimeError(db.GetError())

try:
    db.AddAtom("user", {"name": "alice", "status": 1})
    db.Updates("user", {"status": 2}, {"name": "alice"})
    if not db.Commit():
        raise RuntimeError(db.GetError())
except Exception:
    db.Rollback()
    raise
```

相关方法：

| 方法 | 说明 |
| --- | --- |
| `Begin()` | 开始事务 |
| `Commit()` | 提交事务 |
| `Rollback()` | 回滚事务 |
| `IsTransaction()` | 当前线程是否处于事务中 |
| `DbTransaction()` | 返回当前事务连接 |

## 表结构接口

```python
exists = db.TableExists("user")
columns = db.GetTypes("user")
primary_key = db.GetPrimaryKey("user")
is_primary = db.IsPrimaryKey("user", "id")
is_unique = db.IsUniqueKey("user", "name")
create_sql = db.GetCreates("user")
```

表结构会缓存，可通过以下方法清理：

```python
db.ClearTableCache("user")
db.ClearTableCache()
```

## SQL 和错误信息

```python
# 全部 SQL
sqls = db.getSqls()

# 最后一条 SQL
last_sql = db.getSqls(True)

# 拼接为文本
sql_text = db.getSqls(as_string=True)

# 清空历史
db.cleanSql()

# 最后一次错误和警告
error = db.GetError()
warning = db.getWarning()
```

## 原始 SQL

```python
rows = db.execute("select * from `user` where `id` = %s", 0, True, [1])
```

带参数执行：

```python
result = db.execute(
    "update `user` set `status` = %s where `id` = %s",
    2,
    False,
    [1, user_id],
)
```

执行多条 SQL：

```python
results = db.Executes(
    [
        "update `user` set `status` = 1 where `id` = 1",
        "update `user` set `status` = 2 where `id` = 2",
    ],
    2,
)
```

## SQL 预览

预览模式只返回将要执行的 SQL 结构，不写入数据库。

```python
preview = db.preview(True).AddAtom(
    "user",
    {"name": "test_preview", "status": 1},
)
db.preview(False)
print(preview["sql"])
```

## 锁

```python
db.Locking("user", "read")
# 执行读取操作
db.Release()
```

## 注意事项

1. `MySql.py` 只连接 MySQL，不包含其他数据库驱动代码。
2. 业务字段值使用参数绑定；表名和字段名会进行反引号包装和基本校验。
3. 不要在公开仓库提交真实数据库密码。推荐通过命令行或未纳入版本控制的 JSON 文件传入配置。
4. 普通查询会从连接池借出并归还连接；事务期间连接会固定到当前线程。
5. 应用退出或长期不使用时调用 `db.close()` 释放连接池。
