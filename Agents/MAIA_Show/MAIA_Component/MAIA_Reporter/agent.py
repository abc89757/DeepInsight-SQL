import mimetypes
import os

from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import ToolContext
from typing import Optional
from typing import Dict, Any, List, Optional
from google.genai import types
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, SseConnectionParams
from rich.diagnose import report

# 1) 扩充 MIME 映射：加入 Markdown
_EXT2MIME = {
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif":  "image/gif",
    ".bmp":  "image/bmp",
    ".svg":  "image/svg+xml",
    ".csv":  "text/csv",
    ".tsv":  "text/tab-separated-values",
    ".txt":  "text/plain",
    ".md":   "text/markdown",   # ← 新增
    ".json": "application/json",
    ".pdf":  "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls":  "application/vnd.ms-excel",
    ".zip":  "application/zip",
    ".gz":   "application/gzip",
    ".parquet": "application/octet-stream",
}

def _attach_charset_if_text(mime: str) -> str:
    if mime.startswith("text/") or mime in ("application/json",):
        if "charset=" not in mime:
            return f"{mime}; charset=utf-8"
    return mime

# 批量版：一次性把多个本地文件注册成 Artifact
async def save_local_files_as_artifacts(
    tool_context: ToolContext,
    paths: List[str],
    filenames: Optional[List[Optional[str]]] = None,
    mime_overrides: Optional[List[Optional[str]]] = None,
    per_file_max_size_mb: int = 256,
    total_max_size_mb: Optional[int] = 512,   # 可设总量限制；None 表示不限制
) -> Dict[str, Any]:
    """
    将多个本地文件一次性保存为 ADK Web 可展示/下载的 Artifacts。

    参数
    ----
    tool_context : ToolContext
        ADK 运行时上下文（用于注册 artifact）
    paths : list[str]
        本地文件路径列表
    filenames : list[str|None], optional
        与 paths 一一对应的展示文件名（可为 None 使用原文件名）
    mime_overrides : list[str|None], optional
        与 paths 一一对应的 MIME 覆盖值（可为 None 自动推断）
    per_file_max_size_mb : int, default 256
        单个文件最大允许大小（MB）
    total_max_size_mb : int|None, default 512
        本轮总字节上限（MB），None 表示不做总量限制

    返回
    ----
    dict: {
      "ok": bool,
      "message": str,
      "results": [
        {
          "ok": bool,
          "message": str,
          "source_path": str,
          "artifact_filename": str|None,
          "artifact_version": str|None,
          "size_bytes": int|None,
          "mime": str|None
        }, ...
      ],
      "meta": {
        "files_ok": int,
        "files_failed": int,
        "bytes_total": int
      }
    }

    说明
    ----
    - 自动根据扩展名或 mimetypes 推断 MIME（优先级：mime_overrides > _EXT2MIME > mimetypes.guess_type > octet-stream）
    - 支持 .md → text/markdown
    - 每个文件独立校验与处理，不因为单个失败而影响其他文件
    - 若 total_max_size_mb 不为 None，会在读取前预估总大小并拦截过大请求
    """
    import os, mimetypes
    from google.genai import types

    # 基本校验
    if not paths or not isinstance(paths, list):
        return {"ok": False, "message": "paths 不能为空", "results": [], "meta": {"files_ok": 0, "files_failed": 0, "bytes_total": 0}}

    filenames = filenames or [None] * len(paths)
    mime_overrides = mime_overrides or [None] * len(paths)

    if not (len(filenames) == len(paths) and len(mime_overrides) == len(paths)):
        return {"ok": False, "message": "filenames/mime_overrides 必须与 paths 等长（或省略）", "results": [], "meta": {"files_ok": 0, "files_failed": 0, "bytes_total": 0}}

    # 预估总大小（存在才检查）
    total_bytes_est = 0
    file_sizes = []
    for p in paths:
        if not p or not os.path.isfile(p):
            file_sizes.append(None)
            continue
        s = os.path.getsize(p)
        file_sizes.append(s)
        total_bytes_est += s

    if total_max_size_mb is not None and total_bytes_est > total_max_size_mb * 1024 * 1024:
        return {
            "ok": False,
            "message": f"总大小 {total_bytes_est} bytes 超过上限 {total_max_size_mb} MB",
            "results": [],
            "meta": {"files_ok": 0, "files_failed": len(paths), "bytes_total": total_bytes_est}
        }

    results = []
    bytes_total = 0
    files_ok = 0
    files_failed = 0

    # 逐文件处理
    for idx, path in enumerate(paths):
        try:
            if not path or not os.path.isfile(path):
                results.append({"ok": False, "message": f"文件不存在：{path}", "source_path": path, "artifact_filename": None, "artifact_version": None, "size_bytes": None, "mime": None})
                files_failed += 1
                continue

            size = file_sizes[idx] if file_sizes[idx] is not None else os.path.getsize(path)
            if per_file_max_size_mb and size > per_file_max_size_mb * 1024 * 1024:
                results.append({"ok": False, "message": f"文件过大：{size} bytes，超过 {per_file_max_size_mb}MB", "source_path": path, "artifact_filename": None, "artifact_version": None, "size_bytes": size, "mime": None})
                files_failed += 1
                continue

            with open(path, "rb") as f:
                data = f.read()

            # 名称与 MIME
            requested_name = filenames[idx]
            name = requested_name or os.path.basename(path) or "download.bin"
            ext = os.path.splitext(name)[1].lower()

            mime = mime_overrides[idx] or _EXT2MIME.get(ext) or (mimetypes.guess_type(name)[0]) or "application/octet-stream"
            mime = _attach_charset_if_text(mime)
            # 注册 artifact
            part = types.Part.from_bytes(data=data, mime_type=mime)
            version = await tool_context.save_artifact(name, part)

            results.append({
                "ok": True,
                "message": "已保存为 Artifact",
                "source_path": os.path.abspath(path),
                "artifact_filename": name,
                "artifact_version": version,
                "size_bytes": len(data),
                "mime": mime
            })
            bytes_total += len(data)
            files_ok += 1

        except Exception as e:
            results.append({
                "ok": False,
                "message": f"保存失败：{e}",
                "source_path": os.path.abspath(path) if path else "",
                "artifact_filename": None,
                "artifact_version": None,
                "size_bytes": None,
                "mime": None
            })
            files_failed += 1

    overall_ok = files_ok > 0 and files_failed == 0
    return {
        "ok": overall_ok,
        "message": f"完成，成功 {files_ok} 个，失败 {files_failed} 个",
        "results": results,
        "meta": {
            "files_ok": files_ok,
            "files_failed": files_failed,
            "bytes_total": bytes_total
        }
    }

Reporter_toolset = MCPToolset(
    connection_params=SseConnectionParams(
        url="http://127.0.0.1:8005/sse",  # MCP_SERVER 地址
    )
)

Normal_toolset = MCPToolset(
    connection_params=SseConnectionParams(
        url="http://127.0.0.1:8001/sse",  # MCP_SERVER 地址
    )
)

# mode = LiteLlm(model="ollama/deepseek-r1:32b")
mode = model = LiteLlm(model="deepseek/deepseek-chat")
Reporter_agent = LlmAgent(
    model= model,
    name='root_agent',
    description='A helpful assistant for user questions.',
    instruction="""
        你现在的身份是一名**报告生成专家**，负责撰写详细、结构化的最终数据分析报告。
        保存在本地的数据已经由数据分析助手进行了分批次的初步分析，但是由于是分批进行分析，分析的结果比较零散，需要由你进行总结和完善。
        
        请严格按照以下工作流程和要求完成任务：
        
        数据真实性与引用：
        - 只能引用工具返回或任务链中已给出的数字；若需外部行业数据，需标注“（外部来源，待核实）”，避免与本地数据混淆。
        - 同一指标的口径（时间范围、是否含退款等）需与任务链口径一致；如不一致，显式标注口径差异。


        ## 工作流程
        
        1. **读取任务链：** 使用工具 read_json_key 读取需求分析助手生成的 JSON 任务链文件，获取用户请求的背景信息、数据查询目标和分析目标等内容。
            这一步是为了提取：分析背景、时间范围、数据来源、核心问题、口径要求、可用文件清单。生成“写作提纲草案”（仅内部使用，不写入成稿）。
            
        2. **汇总分析材料：** 调用 read_txt_file 读取所有分析文本；抽取关键发现（趋势、分布、贡献度、异常）；合并为“要点池”。若存在维度冲突或口径不一致，优先以任务链口径为准，并在“方法与口径说明”里解释。
        
        3. **数据统计：**根据任务链与要点池，调用 analyze_csv_stats 生成必要指标（如总量、同比/环比、Top-K、占比、峰值/低谷位置与数值）来丰富报告内容。
        
        4. 撰写报告（Markdown）：
            - 结构（禁止出现“分批/批次”等措辞）：
            （1）. 报告摘要（200–300字）：背景、范围、目标与3–5条结论要点。
            （2）. 数据来源与处理方法：数据来源、时间范围、样本量/口径、清洗与分析方法（工具名可写，内部实现细节不可写）。
            （3）. 数据表现解读：整合“要点池”，从时间趋势、类别/产品贡献、区域差异、异常与原因假设、用户/主体行为等角度展开。每个角度至少给出一条可量化结论。
            （4）. 统计指标与趋势分析：列出关键指标表（总量、增速、占比、Top-K等），说明趋势、波动与可能驱动因素。
            （5）. 核心发现与结论：3–6条凝练要点，尽量落到“可执行判断”。
            （6）. 后续建议：业务策略建议（定位/商品/渠道/运营/风险）+ 数据侧建议（补数、监控、实验设计）。
            （7）. 下载附件：以清单形式给出数据，报告与图表的下载链接。
             语言与篇幅：专业、易懂，避免堆术语；全文建议 1800–3000 字。
        
        5. 使用工具 generate_chart 对关键指标进行可视化展示（如分类分布图、趋势折线图等）生成 2–5 张关键图表（柱状图/折线图/饼图/地图视能力）。生成的图表应作为报告内容的一部分，以辅助说明分析结果。
        
        6. **输出文件与链接：** 最后，使用工具 save_report_md 保存报告文本文件，然后使用工具 save_local_files_as_artifacts 把数据文件、图表、报告文档等文件保存成artifacts，方便提供给用户，并在报告末尾提供这些文件的下载链接，方便用户获取完整报告及配套图表。
        
        ## 报告内容要求
        
        - **多角度全面分析：** 报告必须从多个角度展开，如时间趋势变化、类别/产品贡献度、区域分布差异、异常数据识别、用户行为特征等（可根据具体数据维度调整），确保分析全面深入。
        - **内容详实有据：** 报告中每个部分的文字描述都必须充分、详实，用完整的句子阐述发现，并提供具体数据或统计结果作为支撑（避免只有笼统描述或空泛结论）。切忌过于简略，一句话带过。
        - **图表清晰解读：** 对于报告中的每个图表，需配备清晰的文字说明，解释图表所展示的信息、趋势和关键数值。例如，如果是趋势折线图，应指出峰值或低谷出现的时间及对应数值，以及整体走势变化情况。
        - **语言专业易懂：** 行文应清晰、专业，避免过多技术术语，必要时加以解释，以确保非技术背景的读者也能理解。语言风格正式且易于阅读，突出分析结果对业务的意义。
        
        
        ## 示例：双十一女装订单分析
        
        ## 报告摘要
        2023年双十一期间，女装品类总体保持电商销量前三的位置，但增速趋缓，整体成交额与去年基本持平，订单量略有上升，2023 年“双 11”综合电商各品类销售额排名均保持不变，家用电器、手机数码和服装销售 额依旧位居前三，个护美妆销售额位列第四。受到去年高基数的影响，个护美妆销售额同比下降 4.38,，食品饮料受益于必选消费属性而保持稳健增长；而随 着消费者愈发重视运动健身，运动户外类目也取得瞩目增长。)。本次促销中25-35岁的女性仍是消费主力，三线以下城市用户增长显著。在“全网最低价”策略下客单价明显下降，服饰类平均每笔订单金额约¥216元，同比下降约17%。冬季女装需求旺盛，羽绒服、羊毛衫等保暖类产品销量猛增。移动端几乎包揽全部订单来源，退货率则在大促后显著攀升，成为新的挑战点。本报告通过多维度数据分析，总结了双十一女装订单的用户画像、消费行为和销售结构变化，并提出后续优化建议。
        
        ## 数据来源与处理方法
        
        本次分析所用数据主要来自电商平台后台和权威第三方报告。一方面，我们提取了淘宝天猫**女装品类**在2023年双11期间（10月31日20时至11月11日24时）的订单记录，包括用户属性、订单金额、商品品类、下单时间等信息。我们对原始订单数据进行了清洗和脱敏处理，过滤异常订单和退款订单，然后使用Python和FineBI等工具进行统计汇总。另一方面，我们参考了星图数据、国家邮政局快递数据以及行业调研报告，除此之外，我们还可以从第三方数据中窥得一斑：中国国家邮政局数据显示，11月1日至11日，全国邮政快递企业共揽收快递包裹52.64亿件，同比增长23.2 2)，获取全网销售额、物流包裹量、用户调研等宏观指标用于对比印证。数据处理过程中，我们采用分组汇总（例如按年龄段、地区分布）、趋势线分析（如不同时段订单走势）和比例计算（如各子品类销售占比）等方法保证分析结果的准确可靠。所有图表均由整理后的数据生成，部分关键结论引用了公开报道的数据佐证。
        
        ## 数据表现解读
        
        2023年双十一女装品类总体销售表现平稳。**订单量**方面，由于参与用户基数扩大，女装订单数较去年略有增长，但增幅有限（估计在个位数百分比）。**销售额**方面，受平均客单价下降影响，女装总成交额与去年持平或略降，符合全网交易额增速放缓的大趋势。双十一期间女装依然是**全网销量前三大品类**之一，与家电、数码一起占据最主要成交额。然而，相比2022年疫情期间13.7%的全网增长，今年双十一全网交易额增速仅2.1%，服装类目表现疲软，被认为是“双十一史上最冷清”之一。这背后反映出消费者心态更趋理性，不再冲动囤货，**促销红利**对销量拉动减弱。此外，由于今年平台缩短了预售周期、简化促销规则，**下单高峰更集中**。据后台数据，女装订单高峰出现在10月31日20点开售当夜和11月10-11日，之后热度迅速回落。整体来看，2023年双十一女装板块没有出现爆发式增长，但在消费降级环境下仍取得了稳定的销售规模。
        
        ## 统计指标与趋势分析
        
        双十一女装消费的**用户群像**继续呈年轻化与多元化并存的趋势。一方面，平台数据显示女装消费者主要集中在**18–35岁**区间，占总购买人数约70%（其中25–34岁占比最高）。调查问卷也显示，参与双十一购物的人群中超过八成为25–45岁的城市白领人群。这说明年轻女性仍是线上女装的核心消费力，她们时尚敏感、热衷网购。然而另一方面，中年及银发族的线上渗透在提升。今年有越来越多**35岁以上**的用户开始参与大促买衣服，推动用户年龄结构更趋均衡。**地域分布**上，一线及东部沿海省市依旧贡献了最大的消费额。广东省以约24%的网络零售额占比高居第一，上海市和浙江省分列第二、三名，占比约16%和14%。前五大地区（如广东、上海、浙江、江苏、山东）合计贡献过半销售额，可见经济发达地区依然是女装消费主力。但值得关注的是，不发达地区的增长更快：青海、甘肃等西部省份今年双十一网络零售额同比增速均超过90%，显示下沉市场的消费潜力正被激发。
        
        从**客单价与消费金额**看，今年消费者更注重控制支出、追求性价比。平台数据表明，双十一期间女装单笔订单的**平均客单价**约¥200–250元，较去年明显下降。这与全网“以价换量”策略有关——各平台大打折扣券和直接降价，**低价商品**大量售出。订单金额分布上，小额订单占比提升，**¥100–300区间**的订单数最多（约占45%），而高于¥1000的大额订单仅约占3%，反映多数消费者购买以平价衣物为主。本次调研中有59%消费者表示只购买必需品，**实际支出未超预算**。另一方面，由于参与人数增加，总包裹量同比大增23%。这意味着虽然人均消费缩水，但**订单总量**增长弥补了客单价下滑，女装总体销量保持稳定。
        
        值得警惕的是，**退货率**在大促后大幅上升。服装类作为电商退货重灾区，有调查指出2023年电商平台全年退货件数约82亿件，其中女装退货率尤其高。今年双十一期间，不少女装店家反映大量订单在付款后很快申请退款，部分商家活动期退款率高达50%以上。“先凑单拿优惠，后退货理性消费”成为部分消费者的操作。一些头部女装品牌店甚至因**退货率飙升至70%**而几乎赚不到钱。统计数据显示，品牌店平日平均退货率约24%，2024年已升至45%，直播电商渠道的退货率甚至一度达到70%。双十一大促期间，这一比例显著高于平日水平。退货潮带来的物流逆流和售后成本，抵消了部分促销收益，也折射出当前消费者理性消费、谨慎试水的新常态。
        
        
        ## 核心发现与结论
        
        综合以上分析，我们得出以下核心发现：首先，**女装仍是双十一期间的核心品类**之一，但增长趋于饱和。今年双十一女装订单量小幅上扬但销售额未见大涨，反映出行业进入存量竞争阶段，促销对整体拉动效应减弱。其次，**消费者结构和行为正发生变化**：主要消费人群集中在25-35岁城市女性，但下沉市场用户快速涌入，新用户中约有2000万来自三线及以下城市；消费者心态更加理性务实，倾向于选择性价比高的商品，冲动消费减少。再次，**品类销售呈现季节热点**：今年秋冬女装（羽绒服、毛衣等）成为销量引擎，而非刚需或反季商品表现平平。这说明及时抓住季节潮流、快速响应需求的商家取得了优势。最后，**渠道与售后特点明显**：移动端依然是绝对主力，估计超过九成订单通过手机完成（2016年时移动端占比已达82%，如今这一比例更高）；同时超低价策略虽然扩大了订单规模，却带来了**居高不下的退货率**，侵蚀实际成交收益。
        
        总体而言，2023年双十一女装板块在挑战中保持了稳健：一方面，用户规模和订单量实现增长，显示市场需求仍在；但另一方面，交易额增长放缓、消费者愈发理性，预示行业需调整策略寻求新突破口。平台和商家必须正视**大促常态化**和**消费疲劳**的现实，更多关注如何提升用户体验和满意度，而不仅是追求短期数字增长。只有当消费者买得值、买得爽，品牌卖得有利润，双十一这一消费盛宴才能可持续地发挥价值。
        
        ## 后续建议
        
        针对本次分析结果，我们提出以下建议供参考：
        
        - **精准定位人群，丰富产品供给：**继续深耕年轻女性市场的同时，不忽视30岁以上及低线城市的新兴客群。根据不同人群偏好开发产品，如为年轻人推出设计新颖、平价的快时尚款，为成熟女性提供高品质、经典款式。利用大数据做用户画像，精准推荐适合各年龄层的女装，提高转化率。
        - **优化促销策略，提高性价比：\**在理性消费时代，简单粗暴的满减凑单已无法刺激长效购买。建议推出\**更直接透明**的优惠，例如会员限定价、老客返券等，避免复杂规则引发用户反感。同时严控商品品质，切忌以次充好。在低价让利的同时突出品牌质量和服务，真正实现“低价不低质”，借此培养用户忠诚度。
        - **加强退换货管理，改善售后体验：**高退货率已成为女装电商痛点。建议商家在商品详情页提供更详实的尺码说明、真人秀效果图，减少因尺码不符导致的退货。引入虚拟试衣等技术手段提升网购决策准确性。大促期间提前储备客服和物流资源，**快速响应退换货**请求，简化流程提升效率。对于恶意刷单退货行为，可通过风控手段识别并限制，净化消费环境。
        - **重视内容带货，打造购物体验：\**直播、电商内容化已成为新增长点。女装作为展示性强的品类，应积极拥抱直播带货和短视频种草，通过主播专业搭配指导、走秀展示提升商品吸引力。同时运营好店铺自播，增强与粉丝互动粘性。今年双十一\**品牌自播**取得亮眼成绩，快手女装品牌自播GMV同比提升155%，表明自有渠道运营潜力巨大。未来应结合内容电商趋势，提供更具娱乐性和互动性的购物体验，拉动销量增长。
        - **提前备战季节需求，灵活调整库存：\**本次双十一冬装类商品销售火爆，体现出应季供应链反应的成功。商家应根据销售数据和气候变化，提前\**备足当季热销品**（如秋冬的外套、春夏的连衣裙等）。同时保持库存管理的灵活性，对于销售滞缓的款式及时通过小促销或换季清仓处理，减少库存积压风险。此外，与工厂保持紧密协同，在大促前建立快速补单机制，避免爆款断货错失销售良机。
        
        通过以上措施，电商平台和女装商家有望在未来的双十一等大促节点中，实现**销量与口碑的双赢**：既促进销售增长，又提升消费者满意度，推动女装线上市场的良性发展。
        
        ## 下载附件链接
        
        - 数据下载链接：‘数据下载链接’
    """,
    tools=[save_local_files_as_artifacts,Reporter_toolset,Normal_toolset],

)
