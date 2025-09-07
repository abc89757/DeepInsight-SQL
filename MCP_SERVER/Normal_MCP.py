import requests
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from mcp.server.fastmcp import FastMCP
import matplotlib.pyplot as plt

# 创建 MCP Server 实例
mcp = FastMCP("Normal_MCP", host="0.0.0.0", port=8001)

def _http_get(url: str, params: Dict[str, Any] = None) -> requests.Response:
    headers = {"User-Agent": "WeatherTool/1.0", "Accept": "*/*"}
    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    return resp

def _strip_html(html: str, max_chars: int = 20000) -> Dict[str, Any]:
    """极简 HTML 提取：title + 文本"""
    # title
    title = ""
    try:
        lower = html.lower()
        start = lower.find("<title>")
        end = lower.find("</title>")
        if 0 <= start < end:
            title = html[start + 7:end].strip()
    except Exception:
        title = ""

    # 简单去标签
    out = []
    in_tag = False
    for ch in html:
        if ch == "<":
            in_tag = True
            continue
        if ch == ">":
            in_tag = False
            out.append(" ")
            continue
        if not in_tag:
            out.append(ch)
        if len(out) >= max_chars:
            break
    text = " ".join("".join(out).split())  # 压缩空白
    return {"title": title[:300], "text": text}

@mcp.tool()
async def get_weather(location: str, days: int = 1) -> Dict[str, Any]:
    """
    查询天气（使用 open-meteo 的免费服务）。

    参数:
      - location: 地名（中文/英文均可），例如："Nanning"、"北京"。
      - days: 需要返回的天数（1~7）。

    返回: 地理编码结果 + 每日最高/最低气温、降水概率等。无需 API Key。
    """
    days = max(1, min(int(days), 7))
    # 1) 地理编码
    geo = _http_get(
        "https://geocoding-api.open-meteo.com/v1/search",
        params={"name": location, "count": 1, "language": "zh", "format": "json"},
    ).json()
    if not geo.get("results"):
        return {"ok": False, "error": f"无法找到地点: {location}"}

    place = geo["results"][0]
    lat, lon = place.get("latitude"), place.get("longitude")

    # 2) 天气
    weather = _http_get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": lat,
            "longitude": lon,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_probability_max",
                "sunrise",
                "sunset",
            ],
            "timezone": "auto",
        },
    ).json()

    out = {k: v[:days] if isinstance(v, list) else v for k, v in weather.get("daily", {}).items()}

    return {
        "ok": True,
        "query": location,
        "resolved": {
            "name": place.get("name"),
            "country": place.get("country"),
            "admin1": place.get("admin1"),
            "latitude": lat,
            "longitude": lon,
        },
        "daily": out,
        "source": "open-meteo",
    }

@mcp.tool()
async def get_current_time(tz_offset: Optional[float] = None) -> Dict[str, Any]:
    """获取当前时间。

    参数：
      - tz_offset: 可选，时区偏移（单位：小时）。示例：8 表示 UTC+8；-7 表示 UTC-7。

    返回：包含 ISO8601、epoch 秒、可读字符串、以及使用的时区偏移。
    """
    now_utc = datetime.now(timezone.utc)
    offset = tz_offset if tz_offset is not None else 0.0
    # 将 offset 小时转秒
    tz = timezone.utc
    if tz_offset is not None:
        try:
            secs = int(tz_offset * 3600)
            tz = timezone(datetime.now(timezone.utc).utcoffset() or timezone.utc.utcoffset(None))  # noop for type
            tz = timezone.utc  # 基于 UTC 再手动偏移
            local_ts = now_utc.timestamp() + secs
            local_dt = datetime.fromtimestamp(local_ts, tz=timezone.utc)
            return {
                "iso": local_dt.isoformat().replace("+00:00", "Z"),
                "epoch": int(local_ts),
                "readable": local_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "tz_offset_hours": tz_offset,
            }
        except Exception:
            # 回退：直接返回 UTC
            pass
    return {
        "iso": now_utc.isoformat().replace("+00:00", "Z"),
        "epoch": int(now_utc.timestamp()),
        "readable": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "tz_offset_hours": 0.0,
    }

@mcp.tool()
async def fetch_url(url: str, max_chars: int = 20000, include_headers: bool = False) -> Dict[str, Any]:
    """
    抓取网页内容（GET）。

    参数:
      - url: 目标链接（http/https）。
      - max_chars: 最多返回的纯文本字符数（默认 20000）。
      - include_headers: 是否返回响应头。

    返回: status_code、final_url、title、text、length、(可选 headers)。
    """
    if not (url.startswith("http://") or url.startswith("https://")):
        return {"ok": False, "error": "仅支持 http/https 链接"}

    try:
        resp = _http_get(url)
    except Exception as e:
        return {"ok": False, "error": f"请求失败: {e}"}

    try:
        resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
    except Exception:
        pass

    html = resp.text or ""
    parsed = _strip_html(html, max_chars=max_chars)

    out: Dict[str, Any] = {
        "ok": True,
        "status_code": resp.status_code,
        "final_url": str(resp.url),
        "title": parsed.get("title"),
        "text": parsed.get("text"),
        "length": len(parsed.get("text", "")),
    }
    if include_headers:
        out["headers"] = dict(resp.headers)
    return out


# ============ 启动 ============
if __name__ == "__main__":
    print("启动 Normal_MCP 工具 (SSE:8001)...")
    mcp.run(transport="sse")