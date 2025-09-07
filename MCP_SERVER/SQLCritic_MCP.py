import json
import os, csv, re
import pymysql
import sqlglot
import random
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Set
from mcp.server.fastmcp import FastMCP
from sqlglot import exp
from pymysql.cursors import DictCursor

# 创建 MCP Server 实例
mcp = FastMCP("SQLCritic_MCP", host="0.0.0.0", port=8003)

FORBIDDEN_TOKENS = [
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bREPLACE\b", r"\bMERGE\b",
    r"\bALTER\b", r"\bDROP\b", r"\bTRUNCATE\b", r"\bCREATE\b",
    r"\bGRANT\b", r"\bREVOKE\b", r"\bSET\b", r"\bCALL\b", r"\bEXEC\b",
    r"\bLOCK\b", r"\bUNLOCK\b", r"\bINTO\s+OUTFILE\b", r"\bLOAD\s+DATA\b",
    r"\bLOAD_FILE\s*\("
]

def _remove_comments_and_strings(sql: str) -> str:
    """
    Summary:
        粗略移除 SQL 中的注释与字面量字符串，便于做关键字扫描。
    Args:
        sql: 原始 SQL 字符串
    Returns:
        移除了注释与字符串后的 SQL 字符串（仅用于危险关键字扫描）
    """
    # 去行注释 -- ... 和 # ...
    s = re.sub(r"--[^\n]*", " ", sql)
    s = re.sub(r"#[^\n]*", " ", s)
    # 去块注释 /* ... */
    s = re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
    # 去单/双引号中的内容
    s = re.sub(r"'([^'\\]|\\.)*'", "''", s)
    s = re.sub(r'"([^"\\]|\\.)*"', '""', s)
    return s


def _fetch_schema_snapshot(env_path: str = "./.env", db_name: Optional[str] = None) -> Dict[str, Set[str]]:
    """
    Summary:
        读取 information_schema，返回 {表名 -> 列名集合} 的模式快照。
    Args:
        env_path: .env 文件路径
        db_name: 指定数据库名（默认读取 .env 中的 MYSQL_DB）
    Returns:
        dict: { table_name: {col1, col2, ...}, ... }
    """
    cfg = load_mysql_config(env_path)
    database = db_name or cfg["database"]
    conn = pymysql.connect(
        host=cfg["host"], port=cfg["port"], user=cfg["user"],
        password=cfg["password"], db=database, charset=cfg["charset"],
        cursorclass=pymysql.cursors.DictCursor, autocommit=True
    )
    try:
        tables: Dict[str, Set[str]] = {}
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TABLE_NAME, COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """, [database])
            for row in cur.fetchall():
                t = row["TABLE_NAME"]
                c = row["COLUMN_NAME"]
                tables.setdefault(t, set()).add(c)
        return tables
    finally:
        conn.close()


def _parse_sql_expressions(sql: str) -> List[sqlglot.Expression]:
    """
    Summary:
        使用 sqlglot 解析 SQL，返回顶层表达式列表（用于多语句判断与 AST 遍历）。
    Args:
        sql: 原始 SQL 字符串
    Returns:
        list[Expression]: 顶层表达式（一个元素表示单语句；多个表示多语句）
    """
    return sqlglot.parse(sql, read="mysql")


def _is_select_like(expr: sqlglot.Expression) -> bool:
    """
    Summary:
        判断顶层表达式是否属于只读查询（SELECT 或 WITH...SELECT）。
    Args:
        expr: 顶层 AST 表达式
    Returns:
        bool: True 仅当是只读查询
    """
    if isinstance(expr, exp.Select):
        return True
    if isinstance(expr, exp.With):
        # WITH 的主体通常是 SELECT/UNION 等，取其后续表达式
        return isinstance(expr.this, exp.Select) or isinstance(expr.this, exp.Union)
    # 也允许 UNION（UNION 也是只读）
    if isinstance(expr, exp.Union):
        return True
    return False


def _collect_real_table_aliases(root: sqlglot.Expression) -> Dict[str, str]:
    """
    Summary:
        收集真实表及其别名映射（不追踪子查询/CTE 产生的派生表）。
    Args:
        root: SQL AST 根节点
    Returns:
        dict: { alias_or_table_name_lower: real_table_name_lower }
    """
    mapping: Dict[str, str] = {}
    for t in root.find_all(exp.Table):
        real_name = (t.name or "").lower()
        if not real_name:
            continue
        alias = t.args.get("alias")
        alias_name = alias.this if (alias and hasattr(alias, "this")) else None
        alias_str = (alias_name.name if hasattr(alias_name, "name") else None) if alias_name else None
        if alias_str:
            mapping[alias_str.lower()] = real_name
        # 自身也可用作引用前缀
        mapping[real_name] = real_name
    return mapping


def _collect_derived_aliases(root: sqlglot.Expression) -> Set[str]:
    """
    Summary:
        收集派生表别名（CTE/Subquery），这些别名的列不在真实 schema 中，列校验时跳过。
    Args:
        root: SQL AST 根节点
    Returns:
        set[str]: 派生表别名（小写）
    """
    aliases: Set[str] = set()
    # CTE
    for cte in root.find_all(exp.CTE):
        if cte.alias:
            aliases.add(cte.alias.lower())
    # Subquery（FROM/JOIN 子查询）
    for sq in root.find_all(exp.Subquery):
        alias = sq.args.get("alias")
        alias_name = alias.this if (alias and hasattr(alias, "this")) else None
        alias_str = (alias_name.name if hasattr(alias_name, "name") else None) if alias_name else None
        if alias_str:
            aliases.add(alias_str.lower())
    return aliases


def _check_schema_columns(
    root: sqlglot.Expression,
    schema_tables: Dict[str, Set[str]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Summary:
        基于 AST 与 schema 快照完成表/列存在性检查。
    Args:
        root: SQL AST 根节点
        schema_tables: {table -> {columns}}
    Returns:
        (missing_tables, missing_columns, ambiguous_columns)
        - missing_tables: [{"table": "orders"}]
        - missing_columns: [{"table":"orders","column":"ammount","context":"select"}]
        - ambiguous_columns: [{"column":"id","candidates":["t1.id","t2.id"],"context":"order_by"}]
    """
    missing_tables: List[Dict[str, Any]] = []
    missing_columns: List[Dict[str, Any]] = []
    ambiguous_columns: List[Dict[str, Any]] = []

    real_aliases = _collect_real_table_aliases(root)  # alias -> real table
    derived_aliases = _collect_derived_aliases(root)  # derived-only aliases

    # 表存在性（真实表）
    real_tables_referred: Set[str] = set(real_aliases.values())
    for t in sorted(set(real_tables_referred)):
        if t not in {k.lower(): None for k in schema_tables}.keys():
            missing_tables.append({"table": t})

    # 列引用
    # context 映射：根据父节点类型大致判断列出现场景
    def clause_of(node: sqlglot.Expression) -> str:
        while node and node.parent:
            p = node.parent
            if isinstance(p, exp.Select):
                return "select"
            if isinstance(p, exp.Where):
                return "where"
            if isinstance(p, exp.Group):
                return "group_by"
            if isinstance(p, exp.Having):
                return "having"
            if isinstance(p, exp.Order):
                return "order_by"
            if isinstance(p, exp.Join):
                return "join_on"
            node = p
        return "unknown"

    all_real_tables = {t.lower() for t in schema_tables.keys()}

    for col in root.find_all(exp.Column):
        col_name = (col.name or "").lower()
        tbl_prefix = (col.table or "")
        ctx = clause_of(col)

        if not col_name:
            continue  # 异常节点，跳过

        if tbl_prefix:  # a.col 情况
            alias_lower = tbl_prefix.lower()
            if alias_lower in derived_aliases:
                # 来自派生表/CTE，列校验跳过
                continue
            real = real_aliases.get(alias_lower)
            if not real:
                # 前缀不是派生别名，也不是真实表别名 -> 无法解析，先跳过（也可加 warn）
                continue
            # 校验列是否存在
            if real not in all_real_tables:
                # 真实表自身缺失， missing_tables 会覆盖，这里不重复报
                continue
            if col_name not in {c.lower() for c in schema_tables.get(real, set())}:
                missing_columns.append({"table": real, "column": col_name, "context": ctx})
        else:
            # 未带前缀：在所有“可见真实表”里查
            candidates = []
            for alias, real in real_aliases.items():
                if real in all_real_tables and col_name in {c.lower() for c in schema_tables.get(real, set())}:
                    candidates.append(f"{real}.{col_name}")
            if len(candidates) == 0:
                # 可能来自派生表的列，但由于无前缀我们无法区分；保守起见，仍报缺失
                missing_columns.append({"table": "*", "column": col_name, "context": ctx})
            elif len(candidates) > 1:
                ambiguous_columns.append({"column": col_name, "candidates": candidates, "context": ctx})

    return missing_tables, missing_columns, ambiguous_columns

def _is_single_statement(sql: str) -> bool:
    """
    Summary:
        粗略判断是否为单语句（忽略注释与字符串中的分号）。
    Args:
        sql: 原始 SQL
    Returns:
        True 当且仅当顶层只有一条语句
    """
    scrubbed = _remove_comments_and_strings(sql)
    # 如果末尾带分号，把它去掉再看；若中间还存在非空内容后的分号，视为多语句
    trimmed = scrubbed.strip()
    if trimmed.endswith(";"):
        trimmed = trimmed[:-1].strip()
    # 如果剩余字符串里再出现分号，则判定为多语句
    return ";" not in trimmed

def _is_readonly_select(sql: str) -> bool:
    """
    Summary:
        判断是否只读查询（仅 SELECT/WITH/UNION），并扫描危险关键字。
    Args:
        sql: 原始 SQL
    Returns:
        True 当且仅当判定为只读查询
    """
    # 顶层应以 SELECT 或 WITH 开头（允许空白/括号）
    head = re.match(r"^\s*[\(]*\s*(select|with)\b", sql, flags=re.I)
    if head is None:
        return False
    # 危险关键字扫描（去注释与字符串后）
    scrubbed = _remove_comments_and_strings(sql)
    for pat in FORBIDDEN_TOKENS:
        if re.search(pat, scrubbed, flags=re.I):
            return False
    return True

def _connect_mysql(env_path: str = "../.env", db_name: Optional[str] = None) -> pymysql.connections.Connection:
    """
    Summary:
        根据 .env 建立 MySQL 连接。
    Args:
        env_path: .env 文件路径
        db_name: 可选指定数据库名，默认读 .env
    Returns:
        pymysql 的 Connection 对象
    """
    cfg = load_mysql_config(env_path)
    database = db_name or cfg["database"]
    conn = pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=database,
        charset=cfg["charset"],
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,  # 统一 dict 返回便于序列化
    )
    return conn

def _count_rows(cursor: pymysql.cursors.Cursor, sql: str) -> int:
    """
    Summary:
        统计原查询的行数：SELECT COUNT(*) FROM ( <sql> ) __t
        注意：如果原查询自带 LIMIT，则此 count 统计的是“被 LIMIT 限制后的行数”。
    Args:
        cursor: 已连接的游标
        sql: 原始只读查询
    Returns:
        行数（int）
    """
    count_sql = f"SELECT COUNT(*) AS __cnt FROM ( {sql.rstrip(';')} ) AS __t"
    cursor.execute(count_sql)
    row = cursor.fetchone()
    return int(row["__cnt"] if row and "__cnt" in row else 0)

def _preview_rows(cursor: pymysql.cursors.Cursor, sql: str, pool_limit: int = 200, sample_limit: int = 10) -> List[Dict[str, Any]]:
    """
    Summary:
        先 LIMIT 抓一小批（pool_limit）作为预览池，再在内存里随机抽样最多 sample_limit 行。
        这样避免昂贵的 ORDER BY RAND()。
    Args:
        cursor: 已连接的游标
        sql: 原始只读查询
        pool_limit: 预览池最大行数
        sample_limit: 最终返回的随机样本行数
    Returns:
        随机样本行的列表（每行为 dict）
    """
    preview_sql = f"SELECT * FROM ( {sql.rstrip(';')} ) AS __t LIMIT {int(pool_limit)}"
    cursor.execute(preview_sql)
    pool = cursor.fetchall() or []
    if not pool:
        return []
    if len(pool) <= sample_limit:
        return pool
    # 随机抽样
    idxs = random.sample(range(len(pool)), sample_limit)
    return [pool[i] for i in idxs]

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
async def static_check_sql(sql: str, env_path: str = "./.env", db_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Summary:
        对单条 SQL 进行静态分析（不执行）：检查是否多语句、是否只读、以及表/字段是否存在。
    Args:
        sql: 待检查的 SQL（单条查询；允许 WITH/UNION）
        env_path: .env 文件路径（用于读取 MySQL 连接配置）
        db_name: 指定数据库（默认读取 .env 中 MYSQL_DB）
    Returns:
        dict: 结构化检查结果（适合被上游 Agent 解析）
              {
                "stage": "static",
                "status": "pass|fail",
                "checks": {
                  "multi_statement": {"status": "pass|fail"},
                  "readonly": {"status": "pass|fail", "forbidden": [...]},
                  "schema_match": {
                    "status": "pass|warn|fail",
                    "missing_tables": [...],
                    "missing_columns": [...],
                    "ambiguous_columns": [...]
                  }
                },
                "findings": [...],
                "summary": "..."
              }
    """
    findings: List[Dict[str, Any]] = []
    checks: Dict[str, Any] = {
        "multi_statement": {"status": "pass"},
        "readonly": {"status": "pass", "forbidden": []},
        "schema_match": {"status": "pass", "missing_tables": [], "missing_columns": [], "ambiguous_columns": []},
    }

    # 1) 解析 AST（并用于多语句判断）
    try:
        exprs = _parse_sql_expressions(sql)
    except Exception as e:
        findings.append({
            "code": "SQL_PARSE_ERROR",
            "level": "error",
            "message": f"SQL 解析失败：{e}"
        })
        # 解析失败时，多语句/只读无法可靠判定，直接视为失败
        return {
            "stage": "static",
            "status": "fail",
            "checks": checks,
            "findings": findings,
            "summary": "SQL 解析失败，无法继续静态检查"
        }

    # 2) 多语句检查
    if len(exprs) != 1:
        checks["multi_statement"]["status"] = "fail"
        findings.append({
            "code": "MULTI_STATEMENT",
            "level": "error",
            "message": f"检测到 {len(exprs)} 个顶层语句，仅允许单语句查询"
        })

    # 3) 只读性检查（顶层必须为 SELECT/WITH/UNION；并做危险关键字扫描）
    if exprs:
        top = exprs[0]
        if not _is_select_like(top):
            checks["readonly"]["status"] = "fail"
            findings.append({
                "code": "NON_READONLY",
                "level": "error",
                "message": "仅允许只读查询（SELECT/WITH/UNION）"
            })

    # 危险关键字扫描（去掉注释/字符串后）
    scrubbed = _remove_comments_and_strings(sql)
    forbidden_hits = []
    for pat in FORBIDDEN_TOKENS:
        if re.search(pat, scrubbed, flags=re.I):
            forbidden_hits.append(pat)
    if forbidden_hits:
        checks["readonly"]["status"] = "fail"
        checks["readonly"]["forbidden"] = forbidden_hits
        findings.append({
            "code": "FORBIDDEN_TOKEN",
            "level": "error",
            "message": f"检测到危险关键字：{', '.join(forbidden_hits)}"
        })

    # 4) 表/列存在性（依赖信息模式）
    schema_tables = _fetch_schema_snapshot(env_path=env_path, db_name=db_name)
    missing_tables: List[Dict[str, Any]] = []
    missing_columns: List[Dict[str, Any]] = []
    ambiguous_columns: List[Dict[str, Any]] = []

    if exprs:
        mt, mc, ac = _check_schema_columns(exprs[0], schema_tables)
        missing_tables, missing_columns, ambiguous_columns = mt, mc, ac

    checks["schema_match"]["missing_tables"] = missing_tables
    checks["schema_match"]["missing_columns"] = missing_columns
    checks["schema_match"]["ambiguous_columns"] = ambiguous_columns

    # 设定 schema_match 的状态
    if missing_tables or missing_columns:
        checks["schema_match"]["status"] = "fail"
    elif ambiguous_columns:
        checks["schema_match"]["status"] = "warn"
    else:
        checks["schema_match"]["status"] = "pass"

    # 5) 汇总总体状态
    overall_status = "pass"
    if (
        checks["multi_statement"]["status"] == "fail" or
        checks["readonly"]["status"] == "fail" or
        checks["schema_match"]["status"] == "fail"
    ):
        overall_status = "fail"

    # 6) summary
    parts = []
    if checks["multi_statement"]["status"] == "fail":
        parts.append("存在多语句")
    if checks["readonly"]["status"] == "fail":
        parts.append("检测到非只读/危险关键字")
    if missing_tables:
        parts.append(f"{len(missing_tables)} 个表不存在")
    if missing_columns:
        parts.append(f"{len(missing_columns)} 个字段不存在")
    if ambiguous_columns:
        parts.append(f"{len(ambiguous_columns)} 个字段存在歧义")
    summary = "；".join(parts) if parts else "所有静态检查通过"

    return {
        "stage": "static",
        "status": overall_status,
        "checks": checks,
        "findings": findings,
        "summary": summary
    }


@mcp.tool()
async def dynamic_check_sql(
    sql: str,
    env_path: str = "../.env",
    db_name: Optional[str] = None,
    sample_limit: int = 10,
    preview_pool: int = 200
) -> Dict[str, Any]:
    """
    Summary:
        连接数据库后执行只读 SQL，返回总行数；若行数>0，随机抽取最多10行样本返回；
        若结果为空，返回提示信息以便上游 LLM 回到 schema 和样例数据排查；若执行报错，返回错误信息。
    Args:
        sql: 待执行的只读查询（单条语句；允许 WITH/UNION）
        env_path: .env 文件路径（默认 ../.env，与你的 get_mysql_schema 保持一致）
        db_name: 可选指定数据库名（默认读取 .env）
        sample_limit: 最多返回的样本行数（默认 10）
        preview_pool: 预览池大小，用于内存随机抽样（默认 200；避免昂贵的 ORDER BY RAND()）
    Returns:
        dict: 运行结果
              成功有数据：
              {
                "ok": true,
                "stage": "dynamic",
                "status": "success",
                "row_count": 12345,
                "sample_rows": [ {..}, ... up to 10 ],
                "message": "执行成功"
              }
              成功无数据：
              {
                "ok": true,
                "stage": "dynamic",
                "status": "success",
                "row_count": 0,
                "sample_rows": [],
                "message": "查询结果为空。请基于 get_mysql_schema 的表结构和样例数据检查筛选条件（时间、品类、人群等）是否合理。"
              }
              运行失败：
              {
                "ok": false,
                "stage": "dynamic",
                "status": "runtime_error",
                "error": {"code": <int|None>, "sqlstate": <str|None>, "message": "<错误描述>"},
                "message": "执行失败"
              }
    """
    # 基本安全闸：单语句 & 只读
    if not _is_single_statement(sql):
        return {
            "ok": False,
            "stage": "dynamic",
            "status": "runtime_error",
            "error": {"code": None, "sqlstate": None, "message": "仅允许单条 SQL 语句（检测到多语句）"},
            "message": "执行失败"
        }
    if not _is_readonly_select(sql):
        return {
            "ok": False,
            "stage": "dynamic",
            "status": "runtime_error",
            "error": {"code": None, "sqlstate": None, "message": "仅允许只读查询（SELECT/WITH/UNION），或检测到危险关键字"},
            "message": "执行失败"
        }

    # 连接并执行
    try:
        conn = _connect_mysql(env_path=env_path, db_name=db_name)
    except Exception as e:
        return {
            "ok": False,
            "stage": "dynamic",
            "status": "runtime_error",
            "error": {"code": None, "sqlstate": None, "message": f"数据库连接失败：{e}"},
            "message": "执行失败"
        }

    try:
        with conn.cursor() as cursor:
            # 统计行数
            try:
                total = _count_rows(cursor, sql)
            except Exception as ce:
                # 统计失败仍然尝试直接跑预览（兼容某些复杂语句）
                total = None
                count_err = ce
            else:
                count_err = None

            # 当 total 为 None 时说明 count 失败；我们直接执行预览池抓样本
            try:
                samples = _preview_rows(cursor, sql, pool_limit=int(preview_pool), sample_limit=int(sample_limit))
            except Exception as pe:
                # 预览也失败，就返回运行时错误
                err = {"code": getattr(pe, "args", [None])[0] if pe.args else None,
                       "sqlstate": getattr(pe, "sqlstate", None),
                       "message": str(pe)}
                return {"ok": False, "stage": "dynamic", "status": "runtime_error", "error": err, "message": "执行失败"}

            # 如果 count 成功，用它作为 row_count；否则用“样本是否为空”给出弱提示（不精确）
            if total is None:
                row_count = len(samples)  # 弱指示（仅用于非空/空判断）
                note = f"COUNT 统计失败：{count_err}; 已返回样本行用于参考。"
            else:
                row_count = int(total)
                note = "执行成功"

            if row_count == 0:
                return {
                    "ok": True,
                    "stage": "dynamic",
                    "status": "success",
                    "row_count": 0,
                    "sample_rows": [],
                    "message": "查询结果为空。请基于 get_mysql_schema 的表结构与样例数据，检查筛选条件（时间区间、品类、人群范围、聚合条件）是否合理。"
                }
            else:
                return {
                    "ok": True,
                    "stage": "dynamic",
                    "status": "success",
                    "row_count": row_count,
                    "sample_rows": samples,
                    "message": note
                }

    except Exception as e:
        err = {
            "code": getattr(e, "args", [None])[0] if e.args else None,
            "sqlstate": getattr(e, "sqlstate", None),
            "message": str(e)
        }
        return {"ok": False, "stage": "dynamic", "status": "runtime_error", "error": err, "message": "执行失败"}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@mcp.tool()
async def save_to_csv(
    sql: str,
    fetch_size: int = 1000,
    excel_friendly: bool = True,
) -> Dict[str, Any]:
    """
    Summary:
        执行只读 SQL（单语句）并将完整结果集保存为本地 CSV 文件。文件名自动生成（时间戳 + UUID），
        输出目录使用服务内部固定目录（例如 ./exports），不对外暴露可配置项。采用批量提取流式写入，适合大结果集。
    Args:
        sql: 待执行的只读查询（仅允许 SELECT / WITH / UNION；不允许多语句）
        fetch_size: 每批次抓取行数，默认 1000
        excel_friendly: True 时使用 UTF-8-SIG 编码（Excel 友好），False 时使用 UTF-8
    Returns:
        dict:
            成功：
            {
              "ok": true,
              "stage": "save",
              "status": "success" | "empty",
              "file_path": "<本地CSV路径>",
              "rows_written": <int>,
              "message": "保存成功（含统计信息）"
            }
            失败：
            {
              "ok": false,
              "stage": "save",
              "status": "runtime_error",
              "error": {"message": "<错误描述>"},
              "message": "保存失败"
            }
    """
    # 1) 安全闸：仅允许单语句 + 只读查询
    if not _is_single_statement(sql):
        return {
            "ok": False, "stage": "save", "status": "runtime_error",
            "error": {"message": "仅允许单条 SQL 语句（检测到多语句）"},
            "message": "保存失败"
        }
    if not _is_readonly_select(sql):
        return {
            "ok": False, "stage": "save", "status": "runtime_error",
            "error": {"message": "仅允许只读查询（SELECT/WITH/UNION），或检测到危险关键字"},
            "message": "保存失败"
        }

    # 2) 内部固定输出目录 & 自动文件名
    output_dir = "../exports"          # ← 内部约定目录
    os.makedirs(output_dir, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")         # 时间戳
    rand_id = uuid.uuid4().hex[:8]              # UUID 片段
    filename = f"query_{ts}_{rand_id}.csv"
    file_path = os.path.join(output_dir, filename)

    # 3) 连接数据库（所有连接信息仅来自 .env）
    try:
        conn = _connect_mysql()  # ← 不再接收 db_name/env_path，内部从 .env 读取
    except Exception as e:
        return {
            "ok": False, "stage": "save", "status": "runtime_error",
            "error": {"message": f"数据库连接失败：{e}"},
            "message": "保存失败"
        }

    rows_written = 0
    encoding = "utf-8-sig" if excel_friendly else "utf-8"

    # 4) 执行查询并流式写入 CSV
    try:
        with conn.cursor(DictCursor) as cursor, open(file_path, "w", newline="", encoding=encoding) as f:
            cursor.execute(sql)
            colnames = [desc[0] for desc in (cursor.description or [])]

            writer = csv.writer(f)
            if colnames:
                writer.writerow(colnames)

            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    writer.writerow([row.get(col) for col in colnames])
                rows_written += len(batch)

    except Exception as e:
        # 出错时清理半成品文件（可选策略：也可以选择保留用于排查）
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
        return {
            "ok": False, "stage": "save", "status": "runtime_error",
            "error": {"message": str(e)},
            "message": "保存失败"
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # 5) 返回结果
    if rows_written == 0:
        return {
            "ok": True, "stage": "save", "status": "empty",
            "file_path": file_path, "rows_written": 0,
            "message": "查询结果为空，已生成仅含表头的 CSV 文件。"
        }
    else:
        return {
            "ok": True, "stage": "save", "status": "success",
            "file_path": file_path, "rows_written": rows_written,
            "message": f"保存成功，共写入 {rows_written} 行。"
        }

@mcp.tool()
async def read_json_key(json_path: str, key: str = "检查SQL") -> Dict[str, Any]:
    """
    功能
    ----
    从指定 JSON 文件读取内容，并返回给定 key 的值。
    默认 key = "检查SQL"。
    用来获取需求分析助手给出的关于检查SQL的具体工作内容

    参数
    ----
    json_path : str
        JSON 文件路径
    key : str, optional
        要查找的键名，默认 "检查SQL"

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
    print("启动 SQLCritic_MCP 工具 (SSE:8003)...")
    mcp.run(transport="sse")
