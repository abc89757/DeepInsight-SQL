import json
import os, csv
from typing import Dict, Any, List, Optional
from mcp.server.fastmcp import FastMCP

# 创建 MCP Server 实例
mcp = FastMCP("Analyst_MCP", host="0.0.0.0", port=8004)

@mcp.tool()
async def read_json_key(json_path: str, key: str = "数据分析") -> Dict[str, Any]:
    """
    功能
    ----
    从指定 JSON 文件读取内容，并返回给定 key 的值。
    默认 key = "数据分析"。
    用来获取需求分析助手给出的关于数据分析助手的具体工作内容

    参数
    ----
    json_path : str
        JSON 文件路径
    key : str, optional
        要查找的键名，默认 "数据分析"

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

@mcp.tool()
async def open_csv_chunk(
    path: str,
    start: int = 1,
    limit: int = 100,
    has_header: bool = True,
    encoding: str = "utf-8",
    delimiter: Optional[str] = None
) -> Dict[str, Any]:
    """
    按 [start, start+limit-1]（1-based, 含）读取 CSV 的一段数据。

    参数
    ----
    path        : CSV 文件路径
    start       : 起始行号（1 表示数据部分的第一行；若 has_header=True，则表头不计入 start）
    limit       : 本批读取的最大行数（建议 ≤300，避免上下文过长）
    has_header  : 是否包含表头
    encoding    : 文件编码
    delimiter   : 分隔符（None 时自动嗅探，回退为','）

    返回
    ----
    {
      "ok": bool,
      "columns": [str],      # 表头；若无表头则空列表
      "rows": [[...]],       # 本批数据行（字符串列表）
      "range": [start, end], # 实际返回的数据行区间（1-based）
      "next_start": int|null,# 下一批建议起点（若不足 limit 则为 null）
      "path": str,           # 绝对路径
      "message": str
    }
    注意：range / start 都是针对“数据部分”的 1-based 行号；如果 has_header=True，表头行不计入行号。
    """
    try:
        if not os.path.isfile(path):
            return {"ok": False, "columns": [], "rows": [], "range": [start, start-1], "next_start": None, "path": path, "message": f"CSV 不存在: {path}"}

        if start < 1:
            start = 1
        if limit < 1:
            limit = 300

        # 自动识别分隔符（若未指定）
        sniffed_delim = delimiter
        try:
            if sniffed_delim is None:
                with open(path, "r", encoding=encoding, newline="") as f:
                    sample = f.read(65536)
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
                        sniffed_delim = dialect.delimiter
                    except Exception:
                        sniffed_delim = ","
        except Exception:
            sniffed_delim = delimiter or ","

        columns: List[str] = []
        rows: List[List[str]] = []

        with open(path, "r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=sniffed_delim)

            # 读取表头
            if has_header:
                try:
                    columns = next(reader)
                except StopIteration:
                    return {
                        "ok": True, "columns": [], "rows": [],
                        "range": [start, start-1], "next_start": None,
                        "path": os.path.abspath(path), "message": "空文件（仅表头或无数据）"
                    }

            # 跳过数据部分的前 start-1 行
            skipped = 0
            while skipped < (start - 1):
                try:
                    next(reader)
                    skipped += 1
                except StopIteration:
                    # 起点超过总行数
                    return {
                        "ok": True, "columns": columns, "rows": [],
                        "range": [start, start-1], "next_start": None,
                        "path": os.path.abspath(path), "message": "起始位置已超过数据行数"
                    }

            # 读取本批数据
            count = 0
            for row in reader:
                if count >= limit:
                    break
                rows.append(row)
                count += 1

        end = start + len(rows) - 1
        next_start = end + 1 if len(rows) == limit else None

        return {
            "ok": True,
            "columns": columns,
            "rows": rows,
            "range": [start, end],
            "next_start": next_start,
            "path": os.path.abspath(path),
            "message": "ok"
        }
    except Exception as e:
        return {
            "ok": False, "columns": [], "rows": [], "range": [start, start-1],
            "next_start": None, "path": path, "message": f"读取失败: {e}"
        }

@mcp.tool()
def append_analysis_txt(
        content: str,
        path: str = "../analyst_txt",
        filename: str = "",
        ext: str = "txt",
        encoding: str = "utf-8",
) -> Dict[str, Any]:
    """
    以“追加”的方式把 content 写入到 path/filename.txt。

    参数
    ----
    content  : 要写入的文本内容
    path     : 目录路径，默认 "../analyst_txt"
    filename : 文件名（不含扩展名），必填，建议填数据文件名
    ext      : 文件扩展名，默认 "txt"（可传 "log"/"md" 等）
    encoding : 文本编码，默认 "utf-8"

    返回
    ----
    {
      "ok": bool,
      "path": str,            # 绝对路径
      "created": bool,        # 此次是否创建了新文件
      "bytes_written": int,   # 本次写入的字节数
      "message": str
    }
    """
    try:
        if not filename:
            return {
                "ok": False,
                "path": "",
                "created": False,
                "bytes_written": 0,
                "message": "filename 不能为空",
            }

        # 规范扩展名
        ext_norm = ext if ext.startswith(".") else f".{ext}"

        # 目录与完整路径
        os.makedirs(path or ".", exist_ok=True)
        full_path = os.path.join(path, f"{filename}{ext_norm}")
        abs_path = os.path.abspath(full_path)

        created = not os.path.exists(abs_path)

        # 追加写入
        with open(abs_path, "a", encoding=encoding, newline="") as f:
            f.write(content)

        bytes_written = len(content.encode(encoding, errors="ignore"))

        return {
            "ok": True,
            "path": abs_path,
            "created": created,
            "bytes_written": bytes_written,
            "message": "append ok",
        }
    except Exception as e:
        return {
            "ok": False,
            "path": "",
            "created": False,
            "bytes_written": 0,
            "message": f"append failed: {e}",
        }


# ============ 启动 ============
if __name__ == "__main__":
    print("启动 Analyst_MCP 工具 (SSE:8004)...")
    mcp.run(transport="sse")