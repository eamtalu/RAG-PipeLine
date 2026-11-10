"""The entry every channel calls: snapshot, build the context once, run, store the trace, log."""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Sequence

from app import log_context
from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext, RunSettings
from app.agent.gateway.context import build_context, snapshot_settings
from app.agent.gateway.ports import AgentRuntime, TraceStore
from app.agent.runtime.trace import error_record

logger = logging.getLogger(__name__)


async def run_request(
    request: AgentRequest,
    *,
    runtime: AgentRuntime,
    prior_turns: Sequence[dict] = (),
    run_settings: RunSettings | None = None,
    trace_store: TraceStore | None = None,
) -> AgentResult:
    """Run one request through the runtime under a context built here and nowhere else.

    (request, runtime, prior_turns) -> AgentResult, its trace stored when a store is given ;
    a runtime error -> an error row stored, then the error propagates to the channel
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
        try:
            result = await runtime.run(context, request, prior_turns)
        except Exception as exc:
            # Step 4: A run that raised still leaves a row, because the failed runs are the ones
            # someone will look for; the channel's own error handling is unchanged.
            await _store(trace_store, context, error_record(context, request, exc).as_row())
            raise
        usage = result.usage or {}
        logger.info(
            "agent.run profile=%s request_id=%s trace_id=%s profile_version=%s code_version=%s "
            "status=%s stop_reason=%s iterations=%d tokens=%d seconds=%.1f",
            context.profile,
            context.request_id,
            context.trace_id,
            result.profile_version,
            result.code_version,
            result.status,
            result.stop_reason,
            result.iterations,
            int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0),
            time.monotonic() - started,
        )
        # Step 5: Store the redacted record after the answer exists, so storing can never delay
        # or change what the person gets.
        if result.trace is not None:
            await _store(trace_store, context, result.trace.as_row())
    return result


async def _store(
    trace_store: TraceStore | None, context: ExecutionContext, row: Mapping[str, Any]
) -> None:
    """Hand the row to the store; a store that fails is logged and never fails the run.

    (None, row) -> nothing ; (a raising store, row) -> one agent.trace_store_failed line
    """
    if trace_store is None:
        return
    try:
        await trace_store.save(row)
    except Exception:  # noqa: BLE001 - the answer must reach the person even when the store is down
        logger.exception(
            "agent.trace_store_failed trace_id=%s tenant=%s", context.trace_id, context.tenant
        )
