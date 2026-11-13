"""Shared helpers for the agent tests: a runtime per profile and `ask` through the gateway.

Not collected by pytest (no `test_` prefix). `ask` is what the old `GroundedAgent.ask()` shim was:
it builds the request the way a channel would and returns the legacy dict, so a test reads as it
always did while the run goes through `run_request` like production.
"""

from __future__ import annotations

from datetime import date

from app import log_context
from app.agent.contracts import AgentRequest, ExecutionContext, Principal
from app.agent.gateway import build_context, run_request, snapshot_settings
from app.agent.profiles import load_profile
from app.agent.capabilities.registry import ToolRegistry
from app.agent.runtime.grounded import GroundedAgent
from app.config.database import async_session
from app.services.analytics_agent.namespaces import (
    analytics_namespace,
    logs_namespace,
    actions_namespace,
    presentation_namespace,
)
from app.services.logspace_agent.namespaces import logspace_namespace

TZ = "Europe/London"
TODAY = date(2026, 9, 30)


def tool_registry(session_factory=async_session) -> ToolRegistry:
    """Every namespace on the given session factory, as a channel composes it."""
    return ToolRegistry(
        [
            logs_namespace(session_factory),
            analytics_namespace(session_factory),
            logspace_namespace(session_factory),
            # the presentation tools are on in the tests, so a scripted run can draw a chart
            presentation_namespace(True),
            actions_namespace(True),
        ]
    )


def analytics_runtime(
    cc: str, *, session_factory=async_session, model=None, gate=None, router=None
) -> GroundedAgent:
    """The analytics profile on a registry over the given session factory, with overrides."""
    return GroundedAgent(
        cc,
        profile=load_profile("analytics"),
        registry=tool_registry(session_factory),
        model=model,
        gate=gate,
        router=router,
    )


def logspace_runtime(
    cc: str, *, session_factory=async_session, model=None, gate=None, router=None
) -> GroundedAgent:
    """The logspace profile on a registry over the given session factory, with overrides."""
    return GroundedAgent(
        cc,
        profile=load_profile("logspace"),
        registry=tool_registry(session_factory),
        model=model,
        gate=gate,
        router=router,
    )


def request_for(
    cc: str, question: str = "hi", *, profile: str = "analytics", **fields
) -> AgentRequest:
    """A request as a channel would build it; the logspace profile gets the test day and zone.

    ("acme") -> AgentRequest(tenant="acme", profile="analytics", text="hi", ...)
    """
    base = dict(
        request_id=log_context.request_id_var.get() or log_context.new_request_id(),
        tenant=cc,
        principal=Principal(tenant=cc, object_id="test-user"),
        profile=profile,
        text=question,
        conversation_id=None,
    )
    if profile == "logspace":
        base.update(tz_name=TZ, today=TODAY)
    base.update(fields)
    return AgentRequest(**base)


def context_for(cc: str, *, profile: str = "analytics", **fields) -> ExecutionContext:
    return build_context(request_for(cc, profile=profile, **fields), snapshot_settings())


async def ask(
    runtime: GroundedAgent,
    question: str,
    history=(),
    *,
    deadline_seconds: float | None = None,
    run_settings=None,
    **fields,
) -> dict:
    """Run one question through the gateway and return the legacy dict the channels read.

    (analytics runtime, "how many?") -> {"answer", "stop_reason", "tool_calls", ...}
    """
    request = request_for(
        runtime.customer_code,
        question,
        profile=runtime.profile.name,
        deadline_seconds=deadline_seconds,
        **fields,
    )
    result = await run_request(
        request, runtime=runtime, prior_turns=tuple(history), run_settings=run_settings
    )
    return result.to_legacy_dict()
