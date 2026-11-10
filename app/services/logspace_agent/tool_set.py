"""The logspace profile's tools, built per run from the request's raw feed filters.

The FeedScope type imports persistence models, so it is built here, outside app/agent, from the
plain values the channel put on the request (`scope`) and the context (`today`, `tz_name`).
"""

from __future__ import annotations

import json

from app.agent.contracts import AgentRequest, ExecutionContext
from app.agent.runtime.tool_set import ToolSet
from app.config.database import SessionFactory, async_session
from app.services.log_feed.scope import describe, scope_from
from app.services.logspace_agent.tools import LOGSPACE_TOOLS, build_tools

TOOL_TEXT = json.dumps(LOGSPACE_TOOLS)


class LogspaceToolSet:
    """Builds the six logspace tools for the request's tenant, day and filters."""

    name = "logspace"

    def __init__(self, session_factory: SessionFactory = async_session):
        self.session_factory = session_factory

    def build(self, context: ExecutionContext, request: AgentRequest) -> ToolSet:
        """The tools over the records the feed's filters fetched, and what those records are.

        (context today=2026-09-30, request scope={"user": "PEVANS"}) -> ToolSet(6 tools, ...,
        scope_description="2026-09-30 · user PEVANS") ; (context without tz_name) -> ValueError
        """
        if not context.tz_name:
            raise ValueError("the logspace profile needs tz_name on the request")
        scope = scope_from(dict(request.scope or {}), context.today)
        tools = build_tools(
            context.tenant,
            scope,
            context.tz_name,
            context.today,
            session_factory=self.session_factory,
            public_base_url=context.public_base_url,
        )
        return ToolSet(
            tools=tuple(tools),
            tool_text=TOOL_TEXT,
            scope_description=describe(scope, today=context.today),
        )
