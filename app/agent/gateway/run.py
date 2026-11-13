"""The entry every channel calls: snapshot, build the context once, admit, run, store, account."""

from __future__ import annotations

import logging
import time
from dataclasses import replace
from typing import Any, Callable, Mapping, Sequence

from app import log_context
from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext, RunSettings
from app.agent.gateway.accounting import usage_rows
from app.agent.gateway.context import build_context, snapshot_settings
from app.agent.gateway.ports import Admission, AgentRuntime, TraceStore, UsageStore
from app.agent.profiles.version import code_version
from app.agent.router.failures import classify
from app.agent.runtime.trace import RunTrace, error_record

logger = logging.getLogger(__name__)

#: what the person reads when admission refuses the run, by the limit it would exceed
QUOTA_REFUSALS = {
    "tenant_in_flight": (
        "The assistant is busy answering other questions for your warehouse right now. "
        "Try again in a minute."
    ),
    "tenant_per_hour": (
        "Your warehouse has reached its hourly limit of questions. Try again later."
    ),
    "user_per_hour": "You have reached your hourly limit of questions. Try again later.",
}


async def run_request(
    request: AgentRequest,
    *,
    runtime: AgentRuntime,
    prior_turns: Sequence[dict] = (),
    run_settings: RunSettings | None = None,
    trace_store: TraceStore | None = None,
    admission: Admission | None = None,
    usage_store: UsageStore | None = None,
    on_event: Callable[[str, dict], None] | None = None,
) -> AgentResult:
    """Run one request through the runtime under a context built here and nowhere else.

    (request, runtime, prior_turns) -> AgentResult, its trace stored and its calls accounted
    when the stores are given ; over a quota -> a declined result, the runtime never runs ;
    a runtime error -> an error row stored, the calls accounted, the slot released, then the
    error propagates to the channel
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
        # Step 4: Admit before any model call, because a refusal must cost nothing and the
        # counters must include this run before the next worker reads them.
        admitted = False
        if admission is not None:
            reason, admitted = await _admit(admission, context)
            if reason is not None:
                result = _refused(context, request, reason)
                await _store(trace_store, context, result.trace.as_row())
                return result
        # Step 5: The gateway owns the collector, so a run that raised still has every tool
        # call and model call it made for the error row and the accounting; a channel that shows
        # progress listens to it (step 6).
        trace = RunTrace(listener=on_event)
        try:
            try:
                result = await runtime.run(context, request, prior_turns, trace=trace)
            except Exception as exc:
                # Step 6: A run that raised still leaves a row and its calls are accounted,
                # because the failed runs are the ones someone will look for and pay for; the
                # channel's own error handling is unchanged.
                logger.warning(
                    "agent.run_failed trace_id=%s kind=%s error=%s: %s",
                    context.trace_id,
                    classify(exc),
                    type(exc).__name__,
                    str(exc)[:200],
                )
                await _store(
                    trace_store, context, error_record(context, request, exc, trace).as_row()
                )
                await _account(usage_store, context, trace, run_settings)
                raise
        finally:
            # Step 7: Free the slot however the run ended, so a timeout or an exception can
            # never pin the tenant at its in-flight limit.
            if admitted:
                await _release(admission, context)
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
        # Step 8: Store the redacted record and account the calls after the answer exists, so
        # neither can delay or change what the person gets.
        if result.trace is not None:
            await _store(trace_store, context, result.trace.as_row())
        await _account(usage_store, context, trace, run_settings)
    return result


async def _admit(admission: Admission, context: ExecutionContext) -> tuple[str | None, bool]:
    """The limit the run would exceed and whether a slot was taken; a failing store lets it through.

    (under every limit) -> (None, True) ; (over the tenant hour) -> ("tenant_per_hour", False) ;
    (a raising store) -> (None, False) and one agent.admission_failed line
    """
    try:
        reason = await admission.admit(
            tenant=context.tenant,
            user_id=context.principal.object_id,
            trace_id=context.trace_id,
            deadline_at=context.deadline_at,
            now=context.started_at,
        )
    except Exception:  # noqa: BLE001 - quotas protect cost; an outage must not stop every answer
        logger.exception(
            "agent.admission_failed trace_id=%s tenant=%s", context.trace_id, context.tenant
        )
        return None, False
    if reason is not None:
        logger.warning(
            "agent.refused reason=%s tenant=%s user=%s trace_id=%s",
            reason,
            context.tenant,
            context.principal.object_id,
            context.trace_id,
        )
        return reason, False
    return None, True


async def _release(admission: Admission, context: ExecutionContext) -> None:
    try:
        await admission.release(tenant=context.tenant, trace_id=context.trace_id)
    except Exception:  # noqa: BLE001 - the row expires by its deadline anyway
        logger.exception(
            "agent.release_failed trace_id=%s tenant=%s", context.trace_id, context.tenant
        )


def _refused(context: ExecutionContext, request: AgentRequest, reason: str) -> AgentResult:
    """The declined result of a refused run, with its trace record, built without a profile."""
    result = AgentResult(
        answer=QUOTA_REFUSALS[reason],
        stop_reason="declined",
        model=context.model.name,
        declined=f"quota_{reason}",
        request_id=context.request_id,
        trace_id=context.trace_id,
        code_version=code_version(),
    )
    return replace(result, trace=RunTrace().finish(context, request, result))


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


async def _account(
    usage_store: UsageStore | None,
    context: ExecutionContext,
    trace: RunTrace,
    run_settings: RunSettings,
) -> None:
    """Add the run's calls to the usage store; a store that fails is logged, never fatal.

    (None, a trace) -> nothing ; (a store, a trace of 2 calls) -> one row per model added
    """
    if usage_store is None:
        return
    rows = usage_rows(trace, context, run_settings.prices)
    if not rows:
        return
    try:
        await usage_store.add(rows)
    except Exception:  # noqa: BLE001 - accounting must never cost the person their answer
        logger.exception(
            "agent.usage_store_failed trace_id=%s tenant=%s", context.trace_id, context.tenant
        )
