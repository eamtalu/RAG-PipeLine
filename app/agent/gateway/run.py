"""The entry every channel calls: snapshot, build the context once, run, log one line."""

from __future__ import annotations

import logging
import time
from typing import Sequence

from app import log_context
from app.agent.contracts import AgentRequest, AgentResult, RunSettings
from app.agent.gateway.context import build_context, snapshot_settings
from app.agent.gateway.ports import AgentRuntime

logger = logging.getLogger(__name__)


async def run_request(
    request: AgentRequest,
    *,
    runtime: AgentRuntime,
    prior_turns: Sequence[dict] = (),
    run_settings: RunSettings | None = None,
) -> AgentResult:
    """Run one request through the runtime under a context built here and nowhere else.

    (request, runtime, prior_turns) -> AgentResult ; a runtime error propagates to the channel
    """
    # Step 1: Snapshot settings if the channel did not, so that every value the run needs is
    # fixed before it starts and a settings change mid-run cannot reach it.
    run_settings = run_settings or snapshot_settings()
    # Step 2: Build the frozen context once, because a run that could rebuild it could also
    # change its own tenant or deadline.
    context = build_context(request, run_settings)
    started = time.monotonic()
    # Step 3: Bind the log context to the same ids the channel bound, so a direct caller is
    # covered too and every line of the run names the request and the tenant.
    with log_context.bind(request_id=context.request_id, tenant=context.tenant):
        result = await runtime.run(context, request, prior_turns)
        logger.info(
            "agent.run profile=%s profile_version=%s code_version=%s status=%s "
            "stop_reason=%s iterations=%d seconds=%.1f",
            context.profile,
            result.profile_version,
            result.code_version,
            result.status,
            result.stop_reason,
            result.iterations,
            time.monotonic() - started,
        )
    return result
