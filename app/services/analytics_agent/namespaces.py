"""The `logs` and `analytics` namespaces: today's analytics tools, registered unchanged.

Outside app/agent because the tools read Postgres through app.persistence; the channel that
composes the registry calls these builders. Each runner opens a session per call, never per run,
so no pooled connection is held while the model thinks (step 3).
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from app.agent.capabilities.spec import Namespace, ToolSpec
from app.agent.contracts import AgentRequest, ExecutionContext
from app.config.database import SessionFactory, async_session
from app.services.analytics_agent import (
    action_tools, advisory_tools, at_risk_tools, forecast_tools, presentation_tools, usage_tools,
)
from app.services.analytics_agent.tools import RECIPES, RELEASE_TOOLS, run_release_tool
from app.services.log_agent import tools as log_tools

#: The schemas and recipes as text, exactly as the analytics agent has always been given them:
#: a figure that appears here was given to the model, not invented. The log tools are not in it.
ANALYTICS_TOOL_TEXT = json.dumps(
    [RELEASE_TOOLS, RECIPES, at_risk_tools.AT_RISK_TOOLS, at_risk_tools.RECIPES,
     forecast_tools.FORECAST_TOOLS, forecast_tools.RECIPES], default=str
)
_AT_RISK_NAMES = frozenset(spec["name"] for spec in at_risk_tools.AT_RISK_TOOLS)


def logs_namespace(session_factory: SessionFactory = async_session) -> Namespace:
    """The eight read-only log tools (transactions, entries, metrics), with no extra text.

    () -> Namespace("logs", 8 specs, runner on the app pool)
    """

    async def run(
        context: ExecutionContext, request: AgentRequest, name: str, args: Mapping[str, Any]
    ) -> str:
        async with session_factory() as db:
            return await log_tools.execute_tool(name, dict(args), db, context.tenant)

    return Namespace(
        name="logs", specs=tuple(ToolSpec.from_dict("logs", s) for s in log_tools.TOOLS), runner=run
    )


def analytics_namespace(session_factory: SessionFactory = async_session) -> Namespace:
    """The four release tools, the five at-risk tools, the two advisory tools, the forecast tool
    and the usage tool (step 18), with their schemas and recipes as text.

    () -> Namespace("analytics", 13 specs, runner on the app pool, tool_text=ANALYTICS_TOOL_TEXT)
    """

    async def run(
        context: ExecutionContext, request: AgentRequest, name: str, args: Mapping[str, Any]
    ) -> str:
        async with session_factory() as db:
            if name in _AT_RISK_NAMES:
                return await at_risk_tools.run_at_risk_tool(name, dict(args), db, context.tenant)
            if name in advisory_tools.ADVISORY_TOOL_NAMES:
                return await advisory_tools.run_advisory_tool(db, context.tenant, name, args)
            if name in forecast_tools.FORECAST_TOOL_NAMES:
                return await forecast_tools.run_forecast_tool(db, context.tenant, name, args)
            if name in usage_tools.USAGE_TOOL_NAMES:
                # the usage figures name people, so the asker's own record decides
                return await usage_tools.run_usage_tool(db, context.tenant, request.principal.object_id, name, args)
            return await run_release_tool(name, dict(args), db, context.tenant)

    specs = tuple(
        ToolSpec.from_dict("analytics", s)
        for s in (*RELEASE_TOOLS, *at_risk_tools.AT_RISK_TOOLS, *advisory_tools.ADVISORY_TOOLS,
                  *forecast_tools.FORECAST_TOOLS, *usage_tools.USAGE_TOOLS)
    )
    return Namespace(name="analytics", specs=specs, runner=run, tool_text=ANALYTICS_TOOL_TEXT)


def presentation_namespace(enabled: bool) -> Namespace:
    """The three presentation tools (chart, table, follow-ups), or an empty namespace while the
    `ANALYTICS_AGENT_PRESENTATION` switch is off, so the profile may always list it.

    (True) -> Namespace("presentation", 3 specs) ; (False) -> Namespace("presentation", no specs)
    """

    async def run(
        context: ExecutionContext, request: AgentRequest, name: str, args: Mapping[str, Any]
    ) -> str:
        return await presentation_tools.run_presentation_tool(name, args)

    specs = (
        tuple(ToolSpec.from_dict("presentation", s) for s in presentation_tools.PRESENTATION_TOOLS)
        if enabled
        else ()
    )
    return Namespace(name="presentation", specs=specs, runner=run)


def actions_namespace(enabled: bool, session_factory: SessionFactory = async_session) -> Namespace:
    """The action tools (step 8 and step 18: one propose tool per real action and one demo tool,
    from the registry; step 14c: watch_now and stop_watching), or an empty namespace while the
    presentation switch is off, so the profile may always list it. The runner reads the asker's
    own record.

    (True) -> Namespace("actions", 9 specs) ; (False) -> Namespace("actions", no specs)
    """

    async def run(
        context: ExecutionContext, request: AgentRequest, name: str, args: Mapping[str, Any]
    ) -> str:
        async with session_factory() as db:
            return await action_tools.run_action_tool(
                db, context.tenant, request.principal.object_id, request.site_id, name, args,
                actor_name=getattr(request.principal, "display_name", None))

    specs = (
        tuple(ToolSpec.from_dict("actions", s) for s in action_tools.ACTION_TOOLS) if enabled else ()
    )
    return Namespace(name="actions", specs=specs, runner=run)
