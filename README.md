# DeepInsight-SQL 多智能体数据洞察平台

**DeepInsight-SQL: A Multi-Agent Text-to-SQL Insight Platform**

![logo](https://github.com/abc89757/DeepInsight-SQL/blob/main/asset/Logo.png)

## 项目介绍

DeepInsight-SQL 多智能体数据洞察平台是一个基于多智能体协作的智能数据分析系统，支持用户通过自然语言直接查询 MySQL 数据库。平台对自然语言查询进行智能解读，并在 SQL 生成、校验、执行和结果分析等环节上实现了模块化分工与协作，从而自动完成整个查询流程。系统设计灵感源自“深度研究”协同式洞察任务流程，兼具高度的灵活性与自动化能力，可有效提升数据分析效率和准确性。

## 项目架构与功能

系统由多个智能体（Agent）协作组成，分别负责不同的任务环节：

- **需求分析**：解析用户输入的自然语言查询语句，提炼出核心查询意图和数据需求，为后续处理提供基础。
- **SQL 生成**：基于需求分析结果生成标准化的 SQL 查询语句，充分利用大规模语言模型（LLM）的理解和表达能力，提高生成效率和质量。
- **SQL 审计**：对生成的 SQL 查询语句进行静态和动态检查，检测潜在的安全风险（如 SQL 注入）和性能问题，并提供优化建议。
- **查询执行**：连接目标 MySQL 数据库，执行经过审计的 SQL 查询，获取查询结果数据。
- **数据分析**：对执行结果进行自动化分析，检测数据结构和分布特征，生成适当的图表和文本洞察，以结构化方式呈现数据价值。

此外，系统整体架构支持模块化扩展、流程闭环与多轮追问澄清机制，用户可以通过多次交互不断精炼查询意图并深入挖掘数据，从而提升查询效果和用户体验。整个系统设计具有良好的工程可维护性，便于后续功能的增加和优化。

![系统架构图](https://github.com/abc89757/DeepInsight-SQL/blob/main/asset/System%20Architecture.png)

## 技术创新点

- **多智能体协同架构**：各智能体分工明确，通过闭环交互完成查询任务，在保证灵活性的同时提高了系统的并行处理能力和扩展性。
- **融合大型语言模型（LLM）的分析生成能力**：利用大规模预训练语言模型的自然语言理解与生成优势，高质量自动生成 SQL 查询语句，提升查询构造的准确性和效率。
- **采用 Google ADK 和 MCP 协议**：通过标准化的 Agent Development Kit (ADK) 和多智能体控制协议 (MCP)，实现工具和服务的统一接入，支持在远程服务器和本地环境中的无缝部署与协作。
- **SQL 审计机制**：系统在 SQL 生成后进行静态和动态检查，能够识别并阻止 SQL 注入等安全风险，同时对查询性能进行评估，确保执行的安全性和稳定性。
- **自适应数据分析与可视化**：平台根据查询结果的结构和数据分布自动选择合适的分析方法，生成专业的图表和文本洞察，以结构化格式输出数据分析报告，帮助用户快速理解数据含义。

## 应用前景

- **金融、企业与政府等高合规性场景**：在对数据合规性和安全性要求极高的行业中提供可信的数据查询与分析解决方案。
- **替代传统 BI 工具的即席分析系统**：为用户提供更灵活的自助式数据查询方式，无需复杂的数据报表开发，即可进行快速的数据洞察。
- **企业数据运营与市场洞察**：支持企业在数据运营、产品分析和市场反馈等任务中进行深入洞察，助力业务决策和创新。



## 演示示例

![example](https://github.com/abc89757/DeepInsight-SQL/blob/main/asset/example.png)

该环节使用公开数据集进行功能验证，示例数据库来源： [Vehicle Sales Data (Kaggle)](https://www.kaggle.com/datasets/syedanwarafridi/vehicle-sales-data/data)

借助该数据集，可以直观展示平台在 **自然语言 → SQL → 数据洞察** 流程中的完整能力。

演示视频：[一个基于多智能体面向MySQL的类深度研究项目展示](https://www.bilibili.com/video/BV1YGYVztEdc/?vd_source=ab14dd58e8da7d9d38c94f04a95a87b0)

## 部署与使用

1. **克隆代码库**

   ```
   git clone https://github.com/your-repo/deepinsight-sql.git
   cd deepinsight-sql
   ```

2. **安装依赖环境**
    推荐使用 `Conda` 环境：

   ```
   conda env create -f environment.yml
   conda activate adk_env
   ```

3. **配置.env文件**

   在主目录下的 `.env` 文件中配置 `MySQL` 链接参数以及` LLM` 的 `API_KEY`，例如：

   ```
   # MySQL 配置
   MYSQL_HOST=127.0.0.1
   MYSQL_PORT=3306
   MYSQL_USER=root
   MYSQL_PASSWORD=123456
   MYSQL_DB=demo_data
   MYSQL_CHARSET=utf8mb4
   
   # LLM配置
   DEEPSEEK_API_KEY="sk-xxxxx"
   ```

   - **MySQL 配置**：修改为你自己的数据库地址、用户名和密码，`MYSQL_DB` 为实际要使用的数据库名。
   - **LLM 配置**：本项目默认使用 **DeepSeek v3** 模型，只需在 `DEEPSEEK_API_KEY` 中填入你的 `API Key` 即可。

   如想更换其他模型，除更改 `API_KEY` 以外，还要修改 `Agents/Component` 文件夹下所有 `agent.py` 文件里的模型信息：

   ```
   model = LiteLlm(model="deepseek/deepseek-chat")
   ```

   具体请参考 `google adk` 的官方文档 [Agent Development Kit](https://google.github.io/adk-docs/)

4. **启动所有服务**
    运行以下命令同时启动五个 MCP Server 和 ADK Web：

   ```
   python run_all.py
   ```

   - MCP Server 会监听端口 8001–8005
   - ADK Web 默认运行在 http://127.0.0.1:8000

5. **使用入口**
    在浏览器中访问：

   ```
   http://127.0.0.1:8000
   ```

   打开后选择相应的智能体（Agent），即可使用自然语言输入查询需求，系统将自动执行查询并返回分析结果。

以上步骤完成后，DeepInsight-SQL 平台即可正常运行，用户可以通过 Web 界面输入查询请求，体验端到端的智能数据洞察流程。

## 不足与改进空间

- **缺少分支与任务流灵活性**：目前系统基于顺序与循环式执行（SequentialAgent、LoopAgent），尚未支持分支式智能体流程设计，导致灵活性不足。
- **SQL 模块可控性有限**：虽然集成了 SQL 审计与验证机制，但在复杂 SQL（多表 JOIN、嵌套查询）场景下，生成逻辑仍存在一定不可控性，结果可解释性有待增强。
- **数据源支持范围有限**：当前主要面向 MySQL，未来需要扩展到 PostgreSQL、Oracle、SQL Server 等多种数据库，以及跨源查询。
- **可视化与洞察深度不足**：数据分析结果以自动图表与简要洞察为主，尚未形成复杂统计分析与预测模型支持。
- **缺乏长期运行与主动触发机制**：现有平台以用户交互为主，缺少定时监控、自动预警等主动式智能分析功能。
- **太耗token了**：真的太耗token了，受不了，以后引入分支办法分成低中高三种量级的报告让用户选算了
