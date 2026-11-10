"""The analytics profile's tools, built per run: log tools, release tools and at-risk tools.

Outside app/agent because the tools read Postgres through app.persistence; the runtime only
sees the ToolSetBuilder role. TOOL_TEXT is the schemas and recipes as text, so a figure that
appears in them was given to the model, not invented.
"""

from __future__ import annotations

import json

from app.agent.contracts import AgentRequest, ExecutionContext
from app.agent.runtime.tool_set import ToolSet
from app.config.database import SessionFactory, async_session
from app.services.analytics_agent import at_risk_tools
from app.services.analytics_agent.tools import RECIPES, RELEASE_TOOLS, build_tools

TOOL_TEXT = json.dumps(
    [RELEASE_TOOLS, RECIPES, at_risk_tools.AT_RISK_TOOLS, at_risk_tools.RECIPES], default=str
)


class AnalyticsToolSet:
    """Builds the seventeen analytics tools for the request's tenant on its own short sessions."""

    name = "analytics"

    def __init__(self, session_factory: SessionFactory = async_session):
        self.session_factory = session_factory

    def build(self, context: ExecutionContext, request: AgentRequest) -> ToolSet:
        """(context for acme) -> ToolSet(17 tools, TOOL_TEXT, scope_description=None)"""
        tools = build_tools(context.tenant, session_factory=self.session_factory)
        return ToolSet(tools=tuple(tools), tool_text=TOOL_TEXT)
