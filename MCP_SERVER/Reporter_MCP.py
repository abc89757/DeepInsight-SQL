import json
import os, csv
import uuid
import time
import pandas as pd
from typing import Dict, Any, List, Optional
from mcp.server.fastmcp import FastMCP
import matplotlib.pyplot as plt

# 创建 MCP Server 实例
mcp = FastMCP("Reporter_MCP", host="0.0.0.0", port=8005)

REPORT_OUT_DIR = "../report_md"   # 固定输出目录
IMAGE_OUT_DIR = "../report_img"

def _sanitize_filename(name: str) -> str:
    """净化文件名，去掉非法字符，避免保留名冲突"""
    illegal = '<>:"/\\|?*'
    safe = "".join("_" if ch in illegal else ch for ch in name).strip()
    reserved = {"CON","PRN","AUX","NUL","COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
                "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9"}
    if safe.upper() in reserved:
        safe = f"_{safe}"
    return safe or f"report_{int(time.time())}"

@mcp.tool()
async def read_json_key(json_path: str, key: str = "报告生成") -> Dict[str, Any]:
    """
    功能
    ----
    从指定 JSON 文件读取内容，并返回给定 key 的值。
    默认 key = "报告生成"。
    用来获取需求分析助手给出的关于报告生成的具体工作内容

    参数
    ----
    json_path : str
        JSON 文件路径
    key : str, optional
        要查找的键名，默认 "报告生成"

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
async def read_txt_file(path: str) -> Dict[str, Any]:
    """
    读取本地 txt 文件的全部内容并返回。

    参数
    ----
    path : str
        要读取的 txt 文件路径

    返回
    ----
    dict: {
        "ok": bool,
        "content": str,   # 文件内容
        "path": str,      # 文件绝对路径
        "message": str    # 说明信息
    }
    """
    try:
        if not os.path.isfile(path):
            return {"ok": False, "content": "", "path": path, "message": f"文件不存在: {path}"}

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        return {
            "ok": True,
            "content": content,
            "path": os.path.abspath(path),
            "message": "读取成功"
        }
    except Exception as e:
        return {
            "ok": False,
            "content": "",
            "path": path,
            "message": f"读取失败: {e}"
        }

@mcp.tool()
async def analyze_csv_stats(
    path: str,
    columns: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    has_header: bool = True,
    encoding: str = "utf-8"
) -> Dict[str, Any]:
    """
    对 CSV 文件中的数据进行统计分析。

    参数
    ----
    path : str
        CSV 文件路径
    columns : list[str], optional
        需要分析的列名；为空则自动检测所有数值型列
    metrics : list[str], optional
        需要计算的指标，可选:
        ["count", "sum", "mean", "min", "max", "std", "null_count"]
    has_header : bool
        是否包含表头
    encoding : str
        文件编码，默认 utf-8

    返回
    ----
    dict: {
        "ok": bool,
        "path": str,
        "stats": dict,
        "message": str
    }
    """
    try:
        if not os.path.isfile(path):
            return {"ok": False, "stats": {}, "path": path, "message": f"文件不存在: {path}"}

        # 读取数据
        header = 0 if has_header else None
        df = pd.read_csv(path, header=header, encoding=encoding)

        # 自动选列
        if columns:
            df = df[columns]
        else:
            df = df.select_dtypes(include=["number"])

        if df.empty:
            return {"ok": False, "stats": {}, "path": path, "message": "未找到数值型列"}

        # 默认指标
        if not metrics:
            metrics = ["count", "sum", "mean", "min", "max", "std", "null_count"]

        results = {}
        for col in df.columns:
            col_stats = {}
            series = df[col]
            for m in metrics:
                if m == "count":
                    col_stats["count"] = int(series.count())
                elif m == "sum":
                    col_stats["sum"] = float(series.sum())
                elif m == "mean":
                    col_stats["mean"] = float(series.mean())
                elif m == "min":
                    col_stats["min"] = float(series.min())
                elif m == "max":
                    col_stats["max"] = float(series.max())
                elif m == "std":
                    col_stats["std"] = float(series.std())
                elif m == "null_count":
                    col_stats["null_count"] = int(series.isna().sum())
            results[col] = col_stats

        return {
            "ok": True,
            "path": os.path.abspath(path),
            "stats": results,
            "message": "统计完成"
        }
    except Exception as e:
        return {"ok": False, "stats": {}, "path": path, "message": f"统计失败: {e}"}

# ---------------------------------------------
# 生成图表工具（带标题）
# ---------------------------------------------

@mcp.tool()
async def generate_chart(
    chart_type: str,
    title: str,
    data_path: str,
    x: Optional[str] = None,
    y: Optional[str] = None,
    group: Optional[str] = None,
    encoding: str = "utf-8",
    has_header: bool = True,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    根据 CSV 数据生成图表（带标题），并保存到指定目录 IMAGE_OUT_DIR（默认 ./charts）。

    工具用途
    --------
    - 读取本地 CSV 文件数据，根据给定的字段绘制折线图、柱状图或饼图。
    - 图表生成后保存为 PNG 文件，并返回文件路径信息（绝对路径 + 相对路径）。
    - 每个图表必须包含标题，确保图表在报告中可独立展示和解读。
    - 适用于自动化报告生成环节，将数据统计结果可视化。

    支持的图表类型
    --------------
    - "line" : 折线图
    - "bar"  : 柱状图（支持分组柱状）
    - "pie"  : 饼图（支持分组汇总）

    参数
    ----
    chart_type : str
        图表类型，必须是 {"line","bar","pie"} 之一。
    title : str
        图表标题（必填）。会自动清理全角空格、非断行空格等无效字符，
        若最终为空字符串则报错。
    data_path : str
        CSV 文件路径，必须存在且可读。
    x : str, optional
        X 轴字段（line/bar 必须，pie 可选，作为标签列）。
    y : str, optional
        Y 轴字段（line/bar 必须，pie 必须，作为数值列）。
    group : str, optional
        分组字段，用于多系列折线或分组柱状。
    encoding : str, default="utf-8"
        文件编码。
    has_header : bool, default=True
        CSV 文件是否包含表头。
    limit : int, optional
        限制加载的前 N 行，避免超大文件。

    返回
    ----
    dict: {
        "ok": bool,
            # 是否成功生成图表
        "chart_type": str,
            # 图表类型
        "title": str,
            # 最终使用的图表标题（已清理空格）
        "out_path": str,
            # 图像的绝对保存路径
        "message": str,
            # 操作提示信息
        "meta": {
            "rel_path": str,
                # 图像的相对路径（相对当前工作目录），便于前端或下载展示
            "rows_loaded": int,
                # 实际加载的行数
            "columns": list[str],
                # CSV 文件的列名
            "x": str|None,
            "y": str|None,
            "group": str|None
        }
    }

    异常处理
    --------
    - 若 CSV 文件不存在，返回 ok=False 与错误提示。
    - 若 x/y 列缺失，返回 ok=False 与提示信息。
    - 若生成饼图时数值为空或为零，返回 ok=False 与提示信息。
    - 若标题全为空白字符，返回 ok=False 与提示信息。
    - 其他异常捕获后统一返回 ok=False 与错误消息。

    用法示例
    --------
    1) 生成月度销售趋势折线图：
       generate_chart(
           chart_type="line",
           title="月度销售趋势",
           data_path="sales.csv",
           x="month",
           y="sales"
       )

    2) 生成各品类销量柱状图：
       generate_chart(
           chart_type="bar",
           title="各品类销量对比",
           data_path="sales.csv",
           x="category",
           y="amount"
       )

    3) 生成市场份额饼图：
       generate_chart(
           chart_type="pie",
           title="品类市场份额",
           data_path="sales.csv",
           x="category",
           y="amount"
       )
    """
    try:
        import re, uuid, os
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # ---- 1) 基础校验（修复“标题是空格”的问题）----
        chart_type = (chart_type or "").strip().lower()
        if chart_type not in {"line", "bar", "pie"}:
            return {"ok": False, "chart_type": chart_type, "title": title, "out_path": "",
                    "message": f"不支持的图表类型: {chart_type}（仅支持 line/bar/pie）"}

        # 归一化标题：去掉各种空白（包含 \u00A0 非断行空格），并折叠多空格
        title_raw = "" if title is None else str(title)
        title_norm = re.sub(r"\s+", " ", title_raw, flags=re.UNICODE).strip()
        if not title_norm:
            return {"ok": False, "chart_type": chart_type, "title": title, "out_path": "",
                    "message": "标题 title 不能为空或全为空白字符"}

        if not os.path.isfile(data_path):
            return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                    "message": f"数据文件不存在: {data_path}"}

        header = 0 if has_header else None
        df = pd.read_csv(data_path, header=header, encoding=encoding)
        if limit and isinstance(limit, int) and limit > 0:
            df = df.head(limit)

        if not has_header:
            df.columns = [f"col_{i}" for i in range(df.shape[1])]

        # ---- 2) 字段校验 ----
        if chart_type in {"line", "bar"}:
            if not x or not y:
                return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                        "message": "line/bar 图需要提供 x 与 y 字段"}
            if x not in df.columns or y not in df.columns:
                return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                        "message": f"列不存在：x={x} 或 y={y}"}
        else:  # pie
            if not y:
                return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                        "message": "pie 图需要提供 y（数值列），x 作为标签列可选"}
            if y not in df.columns or (x is not None and x not in df.columns):
                return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                        "message": f"列不存在：y={y} 或 x={x}"}

        # y 转数值
        if y:
            df[y] = pd.to_numeric(df[y], errors="coerce")

        # ---- 3) 作图（细节稳固）----
        fig, ax = plt.subplots(figsize=(9, 5))

        if chart_type == "line":
            plot_df = df[[x, y] + ([group] if group else [])].dropna(subset=[x, y]).copy()
            # x 若为日期尽量解析
            try:
                plot_df[x] = pd.to_datetime(plot_df[x], errors="ignore")
            except Exception:
                pass
            if group and group in plot_df.columns:
                for g, sub in plot_df.groupby(group):
                    sub_sorted = sub.sort_values(by=x)
                    ax.plot(sub_sorted[x], sub_sorted[y], label=str(g))
                ax.legend(title=group)
            else:
                plot_df = plot_df.sort_values(by=x)
                ax.plot(plot_df[x], plot_df[y])
            ax.set_xlabel(x or "")
            ax.set_ylabel(y or "")
            ax.set_title(title_norm)

        elif chart_type == "bar":
            plot_df = df[[x, y] + ([group] if group else [])].dropna(subset=[x, y]).copy()
            if group and group in plot_df.columns:
                agg_df = plot_df.groupby([x, group], dropna=False, sort=True)[y].sum().reset_index()
                pivot_df = agg_df.pivot(index=x, columns=group, values=y).fillna(0)
                pivot_df = pivot_df.sort_index()  # 按 x 排序，避免顺序混乱
                n_series = pivot_df.shape[1]
                idx = range(len(pivot_df))
                total_w = 0.8
                bar_w = total_w / max(n_series, 1)
                for i, col in enumerate(pivot_df.columns):
                    ax.bar([j + (i - n_series/2)*bar_w + bar_w/2 for j in idx],
                           pivot_df[col].values, width=bar_w, label=str(col))
                ax.set_xticks(list(idx))
                ax.set_xticklabels([str(v) for v in pivot_df.index], rotation=0)
                ax.legend(title=group)
            else:
                agg_df = plot_df.groupby(x, dropna=False, sort=True)[y].sum().reset_index()
                ax.bar(agg_df[x], agg_df[y])
            ax.set_xlabel(x or "")
            ax.set_ylabel(y or "")
            ax.set_title(title_norm)

        else:  # pie
            if x:
                plot_df = df[[x, y]].dropna(subset=[y]).copy()
                agg_df = plot_df.groupby(x, dropna=False, sort=True)[y].sum().reset_index()
                agg_df = agg_df.sort_values(y, ascending=False)
                labels = agg_df[x].astype(str).tolist()
                sizes  = agg_df[y].astype(float).tolist()
            else:
                plot_df = df[[y]].dropna().copy()
                sizes = plot_df[y].astype(float).tolist()
                labels = [f"item_{i+1}" for i in range(len(sizes))]
            if not sizes or float(pd.Series(sizes).fillna(0).sum()) == 0.0:
                return {"ok": False, "chart_type": chart_type, "title": title_norm, "out_path": "",
                        "message": "饼图没有可用的正数值"}
            ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
            ax.axis("equal")
            ax.set_title(title_norm)

        plt.tight_layout()

        # ---- 4) 正确保存（修复“存到文件夹失败”）----
        out_dir = os.path.normpath(IMAGE_OUT_DIR or "./charts")
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            pass  # 若创建失败，savefig 会抛错

        filename = f"chart_{chart_type}_{uuid.uuid4().hex[:8]}.png"
        out_path = os.path.normpath(os.path.join(out_dir, filename))  # ✅ 必须是“目录 + 文件名”
        plt.savefig(out_path, dpi=150)                                 # ✅ 不能把目录传给 savefig
        plt.close(fig)

        abs_path = os.path.abspath(out_path)
        rel_path = os.path.relpath(abs_path, os.getcwd())

        return {
            "ok": True,
            "chart_type": chart_type,
            "title": title_norm,
            "out_path": abs_path,
            "message": "图表生成成功",
            "meta": {
                "rel_path": rel_path,
                "rows_loaded": int(df.shape[0]),
                "columns": list(df.columns),
                "x": x, "y": y, "group": group
            }
        }

    except Exception as e:
        try:
            plt.close()
        except Exception:
            pass
        return {"ok": False, "chart_type": chart_type, "title": title, "out_path": "", "message": f"生成失败: {e}"}

@mcp.tool()
async def save_report_md(
    markdown_text: str,
    filename: str,
    ensure_suffix: bool = True,
    append_resources: Optional[List[str]] = None,
    add_index_section: bool = True,
    overwrite: bool = True
) -> Dict[str, Any]:
    """
    将文本保存为 Markdown 文件 (.md)，输出路径固定在 ../report_md/ 下。

    参数
    ----
    markdown_text : str
        要保存的 Markdown 正文
    filename : str
        文件名（不用带路径，可不带 .md 后缀）
    ensure_suffix : bool, default=True
        是否强制加上 ".md" 后缀
    append_resources : list[str], optional
        资源路径列表（如图表文件相对路径），可在文末生成下载清单
    add_index_section : bool, default=True
        是否在文末追加“下载附件”清单
    overwrite : bool, default=True
        False 时遇到同名文件会报错

    返回
    ----
    dict: {
      "ok": bool,
      "path": str,       # 文件绝对路径
      "rel_path": str,   # 相对路径（相对当前工作目录）
      "message": str,
      "meta": {
        "bytes": int,
        "filename": str,
        "resources_count": int
      }
    }
    """
    try:
        if not markdown_text or not isinstance(markdown_text, str):
            return {"ok": False, "path": "", "rel_path": "", "message": "内容不能为空", "meta": {}}

        os.makedirs(REPORT_OUT_DIR, exist_ok=True)

        fname = _sanitize_filename(filename)
        if ensure_suffix and not fname.lower().endswith(".md"):
            fname += ".md"

        out_path = os.path.join(REPORT_OUT_DIR, fname)

        if os.path.exists(out_path) and not overwrite:
            return {"ok": False, "path": out_path, "rel_path": "", "message": f"文件已存在: {out_path}", "meta": {}}

        # 追加下载清单
        text_to_write = markdown_text
        if add_index_section and append_resources:
            lines = ["", "## 下载附件", ""]
            for p in append_resources:
                lines.append(f"- [{os.path.basename(p)}]({p})")
            text_to_write = markdown_text.rstrip() + "\n" + "\n".join(lines) + "\n"

        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text_to_write)

        abs_path = os.path.abspath(out_path)
        rel_path = os.path.relpath(abs_path, os.getcwd())
        size = os.path.getsize(abs_path)

        return {
            "ok": True,
            "path": abs_path,
            "rel_path": rel_path,
            "message": "Markdown 保存成功",
            "meta": {
                "bytes": int(size),
                "filename": fname,
                "resources_count": int(len(append_resources) if append_resources else 0)
            }
        }

    except Exception as e:
        return {"ok": False, "path": "", "rel_path": "", "message": f"保存失败: {e}", "meta": {}}

# ============ 启动 ============
if __name__ == "__main__":
    print("启动 Reporter_MCP 工具 (SSE:8001)...")
    mcp.run(transport="sse")