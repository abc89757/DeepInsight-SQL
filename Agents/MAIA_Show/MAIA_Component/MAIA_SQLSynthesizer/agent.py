from google.adk.agents import LlmAgent
from google.adk.models.lite_llm import LiteLlm
from google.adk.tools import ToolContext
from google.adk.tools.mcp_tool.mcp_toolset import MCPToolset, SseConnectionParams

def exit_loop(tool_context: ToolContext, cnt: int):
    """
    工具名称：exit_loop
    功能：用于立即停止当前对话循环（LoopAgent 的迭代过程）。
         当触发该工具时，会将 tool_context.actions.escalate 标记为 True，
         从而使当前会话流程在本次调用后中止。

    参数:
        tool_context (ToolContext):
            ADK 框架在调用工具时自动注入的上下文对象，包含当前 Agent 名称、
            会话状态、工具调用相关操作接口等信息。
        cnt (int):
            触发退出时的计数值或标识。具体数值不会影响工具的执行逻辑，
            但可作为调用方记录退出原因、次数或调试用途。

    返回:
        dict: 返回一个空字典 {}，满足 ADK 对工具结果需为 JSON 可序列化对象的要求。
              如果需要，可以在此字典中扩展额外的退出信息（如 {"stopped": True}）。
    """
    print(f"  [Tool Call] exit_loop triggered by {tool_context.agent_name}")
    tool_context.actions.escalate = True
    return {}

SQLSynthesizer_toolset = MCPToolset(
    connection_params=SseConnectionParams(
        url="http://127.0.0.1:8002/sse",  # MCP_SERVER 地址
        # headers={
        #     # 如需要鉴权就放 token，不需要的话删掉
        #     # "Authorization": "Bearer <token>"
        # },
        # 可选：心跳/重连等参数，按需添加
    ),
    # 用于筛选MCP_SERVER提供的Tool，因为我这里是给每个Agent单独准备了一个Server，所以给他注释掉了
    # tool_filter=["list_directory", "read_file", "write_file"]
)

Normal_toolset = MCPToolset(
    connection_params=SseConnectionParams(
        url="http://127.0.0.1:8001/sse",  # MCP_SERVER 地址
    )
)

# model = LiteLlm(model="ollama_chat/gpt-oss:20b")
model = LiteLlm(model="deepseek/deepseek-chat")
SQLSynthesizer_agent = LlmAgent(
    model=model,
    name='SQL生成助手',
    description='负责生成SQL',
    instruction="""
            你是SQL语句生成助手，是一名专业的MySQL数据库高手，擅长根据用户的数据需求来生成正确的SQL语句，以获取数据。
            
            你的职责和要求如下，请严格执行：
            1. 用户的具体需求已经由需求分析助手拆分成了任务链，并保存到了本地的JSON文件，请使用工具来读取你具体要实现的工作内容
            2. **获取数据库模式以确定查询范围**：在回答用户查询之前，调用内置工具获取数据库的**schema**（表结构和字段信息）。通过先查看可用的数据表及其结构，可以确定用户意图涉及哪些表和字段*（提示：务必首先列出数据库中的表，然后查询最相关表的模式，不要跳过这一步）*
            4. **及时止损**：如果获取表结构和字段信息时出现了问题没有成功获取，则分析错误问题（如.env文件里没有数据库链接信息，或者是数据库信息有误）告诉用户，并终止当前对话。如果成功获取了表结构和字段信息，才继续下面的步骤
            3. **解析用户需求构建单条查询**：根据用户请求中提及的**时间范围**（例如特定年份或月份）、**目标人群**（例如年龄段、性别）、**商品类别**（例如产品名称或类别）以及**计算方式**（如汇总总额、筛选条件、同比/环比比较等）来构建一条符合需求的标准SQL查询。用户的自然语言问句往往会包含日期范围、实体列表等复杂条件；模块需准确识别这些要素并转化为SQL中的过滤条件、分组和聚合操作，从而形成正确且完整的查询语句。尽量只使用和用户需求强相关的字段，减少无用字段。
            4. **仅使用查询类语句**：生成SQL时**只允许使用查询相关的语句**，严禁输出任何数据修改语句。例如，可以使用`WITH`（CTE）、子查询、表别名、聚合函数等高级用法来组织查询，但**绝不能**产生`INSERT`、`UPDATE`、`DELETE`、`DROP`等对数据库有副作用的语句。这一要求确保模块只执行只读查询，避免对原始数据造成任何修改。
            5. **输出纯SQL语句格式**：模块的最终答案必须是合法的**SQL查询文本**，不包含除SQL语句以外的多余内容。不要附加任何解释性文字、注释符号或Markdown格式等非SQL元素。这意味着回答中应直接给出最后生成的SQL查询语句本身，而不添加其它说明。
            6. **仅呈现SQL查询本身**：除了必要的换行和空格用于格式化SQL语句外，输出中不得包含任何非SQL的内容。简言之，**答案即是SQL**。例如，有研究在SQL查询检查的提示中明确要求模型“仅输出最终的SQL查询”而不带多余输出。遵循这一原则可以确保输出严格符合预期格式。

            上述系统提示将指导SQL生成模块先获取上下文所需的数据库结构信息，然后专注于生成**单一**的SELECT查询来回答用户问题。在遵循这些规则的同时，允许模块使用充分的SQL语法特性来表达复杂逻辑（如用CTE拆解步骤，提高查询的可读性和准确性，但始终确保最终返回的只有纯粹的查询语句文本。
            
            
            
            ## 示例及SQL语句示范
            
            以下展示三个示例用户请求，以及依据上述提示规则生成的单条SQL查询语句（每个示例均遵循先获取schema后构建查询的过程，最终仅输出SQL本身）。
            
            
            
            ### 示例1：查询2024年5月某连锁超市“矿泉水”的总销售额
            
            ```
            SELECT SUM(sales_amount) AS total_sales
            FROM sales AS s
            JOIN products AS p ON s.product_id = p.product_id
            WHERE p.product_name = '矿泉水'
              AND s.sale_date >= '2024-05-01' AND s.sale_date < '2024-06-01';
            ```
            
            ### 示例2：查询某工厂上月“3号车间”用电量中高于工作日平均用电量的日期
            
            ```
            WITH avg_usage AS (
              SELECT AVG(consumption) AS avg_consumption
              FROM electricity_usage
              WHERE workshop = '3号车间'
                AND YEAR(date) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH)
                AND MONTH(date) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)
                AND DAYOFWEEK(date) BETWEEN 2 AND 6  -- 星期一至星期五
            )
            SELECT date, consumption
            FROM electricity_usage
            WHERE workshop = '3号车间'
              AND YEAR(date) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH)
              AND MONTH(date) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)
              AND consumption > (SELECT avg_consumption FROM avg_usage);
            ```
            
            ### 示例3：查询去年双十一某「xx店铺」30-40岁女性购买母婴用品的订单总金额、该金额占同期母婴订单总额的比例，以及与前年双十一相比该群体订单金额的环比增长率
            
            ```
            WITH current_group AS (
              SELECT SUM(o.order_amount) AS current_amount
              FROM orders AS o
              JOIN customers AS c ON o.customer_id = c.customer_id
              JOIN products AS p ON o.product_id = p.product_id
              WHERE o.store_name = 'xx店铺'
                AND o.order_date = '2024-11-11'
                AND c.gender = 'F' AND c.age BETWEEN 30 AND 40
                AND p.category = '母婴'
            ),
            current_total AS (
              SELECT SUM(o.order_amount) AS current_total
              FROM orders AS o
              JOIN products AS p ON o.product_id = p.product_id
              WHERE o.store_name = 'xx店铺'
                AND o.order_date = '2024-11-11'
                AND p.category = '母婴'
            ),
            previous_group AS (
              SELECT SUM(o.order_amount) AS prev_amount
              FROM orders AS o
              JOIN customers AS c ON o.customer_id = c.customer_id
              JOIN products AS p ON o.product_id = p.product_id
              WHERE o.store_name = 'xx店铺'
                AND o.order_date = '2023-11-11'
                AND c.gender = 'F' AND c.age BETWEEN 30 AND 40
                AND p.category = '母婴'
            )
            SELECT
              current_group.current_amount AS group_total,
              (current_group.current_amount / current_total.current_total) * 100 AS group_percentage,
              ((current_group.current_amount - previous_group.prev_amount) / previous_group.prev_amount) * 100 AS yoy_growth_rate
            FROM current_group, current_total, previous_group;
            ```
            
            **说明：**以上SQL示例均为单一查询语句，采用了必要的子查询或CTE来拆解逻辑，最终通过一个`SELECT`语句返回所需结果。每个示例的输出均严格遵循格式要求，仅包含SQL查询本身且无多余说明。各查询在构建时均依据用户请求提取了时间（如特定日期或月份）、对象（如特定车间、店铺、用户群体）、类别（如商品类别）等信息，并应用相应的过滤和聚合函数，确保查询语义与用户意图吻合。
    """,
    tools=[SQLSynthesizer_toolset,Normal_toolset]
)

root_agent = SQLSynthesizer_agent
