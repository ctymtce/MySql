#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySql.py 使用示例。

数据库参数通过命令行或 JSON 文件传入，不在代码中保存账号和密码。

运行示例：
    python.exe demo.py --host 127.0.0.1 --port 3306 --user root \
        --password 你的密码 --database 数据库名
    python.exe demo.py --config config.json
"""
kwinfos = {
    'host': '127.0.0.1',
    'port': 3305,
    'user': 'root',
    'pswd': 'root',
    'dbName': 'test',
    'pools': 5
}

from MySql import Mysql

Mydao = Mysql(**kwinfos)

rows, total = Mydao.GetList('user',None, {'fields': 'id, username,mobile', 'page': 1, 'limit':3})
print(rows)
print(total)