import json
import os, csv
import pymysql
from typing import Dict, Any, List
from mcp.server.fastmcp import FastMCP

# 创建 MCP Server 实例
mcp = FastMCP("SQLSyntherizer_MCP", host="0.0.0.0", port=8002)

def load_mysql_config(env_path: str = "../.env") -> dict:
    """
    从指定的 .env 文件读取 MySQL 连接配置并返回字典。

    参数
    ----
    env_path : str
        .env 文件路径，默认项目总目录下的 .env

    返回
    ----
    dict: {
        "host": str,
        "port": int,
        "user": str,
        "password": str,
        "database": str,
        "charset": str
    }
    """
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env 文件不存在: {env_path}")

    config = {
        "host": None,
        "port": None,
        "user": None,
        "password": None,
        "database": None,
        "charset": None,
    }

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()

            if key == "MYSQL_HOST":
                config["host"] = value
            elif key == "MYSQL_PORT":
                config["port"] = int(value)
            elif key == "MYSQL_USER":
                config["user"] = value
            elif key == "MYSQL_PASSWORD":
                config["password"] = value
            elif key == "MYSQL_DB":
                config["database"] = value
            elif key == "MYSQL_CHARSET":
                config["charset"] = value

    # 检查是否缺少必填项
    missing = [k for k, v in config.items() if v is None]
    if missing:
        raise ValueError(f".env 缺少必要配置: {missing}")

    return config

@mcp.tool()
async def get_mysql_schema() -> Dict[str, Any]:
    """
    功能
    ----
    从 .env 文件加载 MySQL 配置，连接数据库，获取所有表的表结构，并附带前两行样例数据。
    如果 .env 文件里没有 MySQL 配置，或者链接数据库失败，则返回报错信息

    参数
    ----
    无

    返回
    ----
    {
      "ok": true/false,
      "message": "提示信息",
      "schema": [
        {
          "table": "表名",
          "columns": [
            {"name": "字段名", "type": "类型", "nullable": true/false,
             "default": "默认值", "key": "PRI/UNI/MUL/''", "comment": "注释"}
          ],
          "sample_rows": [
            {"col1": "值1", "col2": "值2", ...},   # 最多两条
          ]
        }
      ]
    }
    """
    try:
        cfg = load_mysql_config("../.env")
    except Exception as e:
        return {"ok": False, "message": f"读取配置失败: {e}", "schema": []}

    try:
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            charset=cfg["charset"],
            connect_timeout=5,
        )
    except Exception as e:
        return {"ok": False, "message": f"数据库连接失败: {e}", "schema": []}

    schema_info: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            tables = []
            for r in cursor.fetchall():
                if isinstance(r, dict):
                    tables.append(list(r.values())[0])
                elif isinstance(r, (list, tuple)):
                    tables.append(r[0])

            if not tables:
                return {"ok": True, "message": "数据库中没有表", "schema": []}

            for t in tables:
                # 获取表结构
                cursor.execute(f"SHOW FULL COLUMNS FROM `{t}`;")
                cols = cursor.fetchall()
                col_list = []
                for c in cols:
                    if isinstance(c, dict):  # DictCursor
                        col_list.append({
                            "name": c["Field"],
                            "type": c["Type"],
                            "nullable": (c["Null"] == "YES"),
                            "default": c["Default"],
                            "key": c["Key"],
                            "comment": c["Comment"],
                        })
                    elif isinstance(c, (list, tuple)):  # tuple 游标（字段顺序固定）
                        # SHOW FULL COLUMNS 顺序: Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
                        col_list.append({
                            "name": c[0],
                            "type": c[1],
                            "nullable": (c[3] == "YES"),
                            "default": c[5],
                            "key": c[4],
                            "comment": c[8],
                        })

                # 获取样例数据
                cursor.execute(f"SELECT * FROM `{t}` LIMIT 2;")
                rows = cursor.fetchall()  # tuple 或 dict，直接原样放回
                schema_info.append({"table": t, "columns": col_list, "sample_rows": rows})
    finally:
        conn.close()

    return {"ok": True, "message": "获取表结构成功", "schema": schema_info}

@mcp.tool()
async def read_json_key(json_path: str, key: str = "SQL生成") -> Dict[str, Any]:
    """
    功能
    ----
    从指定 JSON 文件读取内容，并返回给定 key 的值。
    默认 key = "SQL生成"。
    用来获取需求分析助手给出的关于SQL生成的具体工作内容

    参数
    ----
    json_path : str
        JSON 文件路径
    key : str, optional
        要查找的键名，默认 "SQL生成"

    返回
    ----
    {
      "ok": true/false,
      "message": "提示信息",
      "value": <对应的值或 None>
    }
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {"ok": False, "message": f"未找到 JSON 文件: {json_path}", "value": None}
    except Exception as e:
        return {"ok": False, "message": f"读取 JSON 失败: {e}", "value": None}

    if key not in data:
        return {"ok": False, "message": f"JSON 中未找到 key: {key}", "value": None}

    return {"ok": True, "message": f"成功获取 key={key} 的值", "value": data[key]}

# ============ 启动 ============
if __name__ == "__main__":
    print("启动 SQLSyntherizer_MCP 工具 (SSE:8001)...")
    mcp.run(transport="sse")