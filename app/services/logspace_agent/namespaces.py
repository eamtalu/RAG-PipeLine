"""The `logspace` namespace: the ask box's six tools over the records the feed's filters fetched.

The FeedScope type imports persistence models, so it is built here, outside app/agent, from the
raw filter dict on the request and the day and zone on the context.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from app.agent.capabilities.spec import Namespace, ToolSpec
from app.agent.contracts import AgentRequest, ExecutionContext
from app.config.database import SessionFactory, async_session
from app.services.log_feed.scope import FeedScope, describe, scope_from
from app.services.logspace_agent.tools import LOGSPACE_TOOLS, run_logspace_tool

LOGSPACE_TOOL_TEXT = json.dumps(LOGSPACE_TOOLS)


def _scope(context: ExecutionContext, request: AgentRequest) -> FeedScope:
    return scope_from(dict(request.scope or {}), context.today)


def logspace_namespace(session_factory: SessionFactory = async_session) -> Namespace:
    """The six logspace tools; the describer refuses a context without a timezone, pre-model.

    () -> Namespace("logspace", 6 specs, runner on the app pool, tool_text=LOGSPACE_TOOL_TEXT)
    """

    def describe_records(context: ExecutionContext, request: AgentRequest) -> str:
        if not context.tz_name:
            raise ValueError("the logspace profile needs tz_name on the request")
        return describe(_scope(context, request), today=context.today)

    async def run(
        context: ExecutionContext, request: AgentRequest, name: str, args: Mapping[str, Any]
    ) -> str:
        async with session_factory() as db:
            return await run_logspace_tool(
                name,
                dict(args),
                db,
                context.tenant,
                scope=_scope(context, request),
                tz_name=context.tz_name or "",
                today=context.today,
                public_base_url=context.public_base_url,
            )

    return Namespace(
        name="logspace",
        specs=tuple(ToolSpec.from_dict("logspace", s) for s in LOGSPACE_TOOLS),
        runner=run,
        tool_text=LOGSPACE_TOOL_TEXT,
        describe=describe_records,
    )
