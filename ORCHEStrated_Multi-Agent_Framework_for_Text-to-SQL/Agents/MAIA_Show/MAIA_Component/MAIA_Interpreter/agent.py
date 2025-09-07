import json, os, re, time, hashlib, uuid
from datetime import datetime
from pathlib import Path
from typing import Dict

from google.adk.agents.callback_context import CallbackContext
from google.adk.models import LlmResponse
from google.adk.tools import ToolContext
from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, SseConnectionParams
from win32com.servers.interp import Interpreter

DEFAULT_BASE_DIR = Path("../task_jsons").resolve()

def _sanitize_filename(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")
    return safe or "result"

def _ensure_under_base(path: Path, base: Path) -> None:
    p = path.resolve(); b = base.resolve()
    if b != p and b not in p.parents:
        raise ValueError(f"禁止写入 base_dir 之外的路径：{p}")

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def save_json_local(
    json_object: dict,      # ← 直接收字典对象
    pretty: bool = True,
    tool_context: ToolContext = None,
) -> Dict[str, object]:
    """
    将 JSON 对象保存到本地磁盘，文件名自动使用 UUID。

    Args:
        json_object (dict):
            必填参数。需要保存的 JSON 数据对象，通常是一个字典，包含要写入文件的完整内容。

        pretty (bool, optional):
            是否对 JSON 进行美化输出（缩进 2 空格）。默认为 True。
            - True  → 格式化保存，方便人工阅读。
            - False → 紧凑保存，节省存储空间。

        tool_context (ToolContext, optional):
            Google ADK 框架自动注入的上下文对象。通常不需要调用者手动传入，
            内部可用于访问会话状态、保存临时信息等。
    """
    # 1) 序列化
    if pretty:
        txt = json.dumps(json_object, ensure_ascii=False, indent=2)
    else:
        txt = json.dumps(json_object, ensure_ascii=False, separators=(",", ":"))
    body = txt.encode("utf-8")

    # 2) 路径
    base = DEFAULT_BASE_DIR
    base.mkdir(parents=True, exist_ok=True)

    # 3) 文件名
    name = f"{uuid.uuid4().hex}.json"
    target_path = base / name

    # 4) 写入
    with open(target_path, "wb") as f:
        f.write(body)

    return {
        "abs_path": str(target_path.resolve()),
        "filename": name,
        "bytes": len(body),
        "sha256": _sha256_bytes(body),
        "created_at": int(time.time()),
        "note": "JSON 已保存，文件名为 UUID 自动生成。",
    }

Normal_toolset = MCPToolset(
    connection_params=SseConnectionParams(
        url="http://127.0.0.1:8001/sse",  # MCP_SERVER 地址
    )
)

# model = LiteLlm(model="ollama_chat/gpt-oss:20b")
model = LiteLlm(model="deepseek/deepseek-chat")
Interpreter_agent = LlmAgent(
    model=model,
    name='需求分析助手',
    description='A helpful assistant for user questions.',
    instruction="""
        你是一名专业的需求分析助手，负责将用户的自然语言数据分析需求拆解成可执行的模块化任务计划。请严格遵循以下要求进行任务拆解：

        1. 阅读理解用户的提问，提炼其中的所有意图、条件和关注的指标。包括但不限于：时间范围、地点、人群、产品类别、分析维度、指标计算（总量、同比、环比等）以及用户期望的结果形式（如图表或文本报告）。由于你无法获取外部信息，当用户提及如天气，时间等信息，你应当调用工具来获取准确的信息，如果没有对应的工具提供信息，则用用户的语句来向其他agent进行描述。如果用户没有提及时间相关的限制，默认读取全部时间下的目标数据
        
        2. 如果用户有查询数据的需求，则你的任务为规划任务链：将完整需求拆分为依次衔接的四个模块任务，分别是：SQL生成、检查SQL、数据分析、报告生成。确保各模块衔接合理，每个模块只专注处理其对应环节的任务。
        
        3. 详细描述任务：针对每个模块，用中文详细说明该模块需要完成的具体工作。描述应充分结合用户需求中的细节和意图。例如：
        
        4. SQL生成：说明需要从何种数据源提取哪些数据，应用哪些过滤条件或聚合计算，但因为你不知道具体的表结构，所以不要指导它该查找什么表查找什么字段，让它自己去理解。
        
        5. 检查SQL：说明如何验证SQL的正确性与优化执行效率，包括检查筛选条件、时间范围、语法和索引等。
        
        6. 数据分析：说明对查询得到的数据需进行哪些处理和分析计算，比如汇总、比较、趋势分析、同比环比计算、异常发现等，只能用从文件读取到的数据，如果你觉得为了丰富分析内容需要更多数据，请提前告诉负责SQL的两个agent。
        
        7. 报告生成：说明如何将分析结果转化为最终报告，包含哪些关键内容和可视化展现形式，以满足用户需求。
        
        8. 输出JSON格式：最终只输出JSON格式的结果，不附加任何解释性文字。JSON对象必须包含 "SQL生成", "检查SQL", "数据分析", "报告生成" 四个键，并将相应的任务描述作为字符串值填入每个键中。确保每个描述不少于50个汉字，内容完整准确。
        
        9. 对于JSON里上述的四个内容，为了保证信息的传输不出现问题，还要在结尾加上用户的原话。注意，这部分不算在50个汉字里，你是要在分析完任务描述后再额外把用户的原话加上去。
        
        10. 调用工具 save_json_local 来把生成的 JSON 保存到本地，并输出文件的路径，方便后续的助手拿到自己所需的任务
        
        严格按照以上要求行动。一旦解析完用户需求，即刻用工具保存JSON格式的任务拆解结果。
        并且最终你应当只输出文件的路径以及文件的相关说明

        下面通过示例展示正确的输出格式：
        
        示例1:
        用户请求：“矿泉水销售额”
        工具保存内容：
        {
         "SQL生成": "生成一条SQL查询，从销售数据库中提取数据库中所有矿泉水商品的销售记录（包括销售额、日期、地区等字段）。确保查询仅筛选矿泉水产品的数据。用户请求：‘矿泉水销售额’",
         "检查SQL": "检查上述SQL语句是否正确应用了矿泉水产品过滤和时间范围限制，确认语法正确且字段与数据库匹配。必要时优化查询性能（例如添加索引或调整查询结构）。用户请求：‘矿泉水销售额’",
         "数据分析": "对提取的矿泉水销售数据进行深入分析。计算过去所有矿泉水的总销售额，并按年月汇总展示销售趋势。识别全年销量峰值和低谷，必要时进行同比或环比比较，以提供参考。用户请求：‘矿泉水销售额’",
         "报告生成": "根据分析结果撰写报告，总结过去所有矿泉水销售额的总体情况和趋势。报告应包含总销售额、最高销量月份和最低销量月份等关键指标，并分析产生这些结果的原因。必要时可配以折线图等可视化图表来辅助说明。"用户请求：‘矿泉水销售额’
        }
        工具返回路径：C:/User/Desktop/task.json
        助手输出：我已将用户的需求进行拆分，并保存到了：“C:/User/Desktop/task.json”，请后续Agent根据自己的身份获取自己需要完成的工作内容
        
        
        示例2:
        用户请求：“近两年工厂用电量分析”
        工具保存内容：
        {
         "SQL生成": "生成一条SQL查询，从能源管理数据库中提取指定工厂最近两年的用电量记录。按月汇总每月的总用电量，以便后续进行趋势分析和同比比较。用户请求：‘工厂用电量分析’",
         "检查SQL": "检查上述SQL语句是否正确筛选了目标工厂，并涵盖最近两年的数据范围。确认查询逻辑按月汇总用电量且语法正确无误。必要时对查询进行优化（例如确保时间过滤条件正确应用）。用户请求：‘工厂用电量分析’",
         "数据分析": "对获取的工厂用电量数据进行深入分析。计算每年的总用电量，并比较最近一年各月份用电量相对于前一年同期的同比变化。找出用电峰值和低谷月份，分析这些现象背后的原因（如季节因素或生产计划变动）。用户请求：‘工厂用电量分析’",
         "报告生成": "根据分析结果撰写报告，总结工厂最近两年的用电趋势和变化情况。报告应包含每年的总用电量、各月份的同比增减情况，并突出用电最高和最低的月份，解释背后的原因。最后，可提出优化用电效率或降低峰值用电的建议。用户请求：‘工厂用电量分析’"
        }
        工具返回路径：D:/Python/Agents/task.json
        助手输出：我已将用户的需求进行拆分，并保存到了：“D:/Python/Agents/task.json”，请后续Agent根据自己的身份获取自己需要完成的工作内容
        
        示例3:
        用户请求：“分析一下去年双十一女装订单的销售情况“
        工具保存内容：
        {
         "SQL生成": "先调用工具获取当前时间，来得到正确的时间范围，为了分析更加丰富，不光要获取去年的双十一女装订单销售情况，还要看看前几年的，以此做对比。生成一条SQL查询，从订单数据库中提取用户提及时间以及前几年的双十一当天的订单记录。需获取订单总数、销售总额，以及按商品品类汇总的销售额数据，为后续分析提供基础。用户请求：‘分析一下去年双十一女装订单的销售情况’",
         "检查SQL": "检查该SQL查询是否正确限定了订单日期为双十一当天（11月11日）。确认查询提取了订单数量、销售额和商品品类等必要字段，语法正确且按品类汇总统计无误。如有需要，可对查询进行优化（例如添加索引以提高性能）。用户请求：‘分析一下去年双十一女装订单的销售情况’",
         "数据分析": "对获取的双十一订单数据进行分析。比较本年度双十一与前几年的订单总数和销售总额，计算同比增长率。将本年度的销售额按商品品类分类汇总，找出销量最大的品类和增长显著的品类，评估各品类的贡献度和变化趋势。用户请求：‘分析一下去年双十一女装订单的销售情况’",
         "报告生成": "根据分析结果撰写报告，总结本年度双十一的整体销售业绩，并与去年同期进行比较。报告应包含本年双十一的订单总数、销售总额及其同比增幅，以及主要热销品类的销售情况。解释其中销售增长或下降的原因（例如市场趋势、促销策略等）。如有必要，可使用柱状图等可视化形式展示各品类销售份额以增强说明。用户请求：‘分析一下去年双十一女装订单的销售情况’"
        }
        工具返回路径：D:/MAIA/load/w42qref1232asd231.json
        助手输出：我已将用户的需求进行拆分，并保存到了：“D:/MAIA/load/w42qref1232asd231.json”，请后续Agent根据自己的身份获取自己需要完成的工作内容
        
    """,
    tools=[save_json_local,Normal_toolset],
)

root_agent = Interpreter_agent