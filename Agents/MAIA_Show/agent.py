from google.adk.tools.tool_context import ToolContext
from google.adk.agents import LoopAgent, SequentialAgent
from .MAIA_Component.MAIA_Analyst.agent import Analyst_agent
from .MAIA_Component.MAIA_Interpreter.agent import Interpreter_agent
from .MAIA_Component.MAIA_SQLCritic.agent import SQLCritic_agent
from .MAIA_Component.MAIA_SQLSynthesizer.agent import SQLSynthesizer_agent
from .MAIA_Component.MAIA_Reporter.agent import Reporter_agent


SQL_Agents = LoopAgent(
    name="SQL_Agents",
    sub_agents=[SQLSynthesizer_agent,SQLCritic_agent],
)

root_agent = SequentialAgent(
    name="MAIA",
    sub_agents=[Interpreter_agent,SQL_Agents,Analyst_agent,Reporter_agent]
)