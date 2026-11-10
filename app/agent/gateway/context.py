"""The context builder, and the only module under app/agent that reads settings."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from app.agent.contracts import (
    AgentRequest,
    ExecutionContext,
    ModelSettings,
    ResilienceSettings,
    RunBudgets,
    RunSettings,
)
from app.settings import settings


def snapshot_settings() -> RunSettings:
    """Every setting a run needs, read once here so nothing inside the run reads them again.

    (settings analytics_agent_model="ollama:x") -> RunSettings(model=ModelSettings("ollama:x", ...))
    """
    return RunSettings(
        model=ModelSettings(
            name=settings.analytics_agent_model,
            ollama_base_url=settings.ollama_base_url,
            think=settings.analytics_agent_think,
            context_tokens=settings.analytics_agent_context_tokens,
            bedrock_region=settings.bedrock_region,
            max_tokens=settings.analytics_agent_max_tokens,
            fallback_models=tuple(
                m.strip() for m in settings.analytics_agent_fallback_models.split(",") if m.strip()
            ),
            gate_model=settings.analytics_agent_gate_model.strip(),
        ),
        budgets=RunBudgets(
            max_iterations=settings.analytics_agent_max_iterations,
            run_timeout_seconds=float(settings.analytics_agent_run_timeout_seconds),
            call_timeout_seconds=float(settings.analytics_agent_call_timeout_seconds),
        ),
        public_base_url=(settings.app_public_base_url or "").rstrip("/"),
        resilience=ResilienceSettings(
            bulkhead=settings.analytics_agent_provider_bulkhead,
            breaker_failures=settings.analytics_agent_breaker_failures,
            breaker_cooldown_seconds=float(settings.analytics_agent_breaker_cooldown_seconds),
        ),
    )


def build_context(
    request: AgentRequest, run_settings: RunSettings, *, now: datetime | None = None
) -> ExecutionContext:
    """The frozen context for one run: the request's identity plus the snapshot and a deadline.

    >>> from app.agent.contracts import Principal
    >>> req = AgentRequest("r1", "acme", Principal("acme", "u1"), "analytics", "how many?", None)
    >>> rs = RunSettings(ModelSettings("m", "", False, 1, "", 1), RunBudgets(10, 120.0), "")
    >>> ctx = build_context(req, rs, now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    >>> (ctx.trace_id != ctx.request_id, len(ctx.trace_id), ctx.deadline_at.isoformat())
    (True, 32, '2026-01-01T00:02:00+00:00')
    """
    started_at = now or datetime.now(timezone.utc)
    # Step 1: The deadline is absolute and fixed here, because the runtime measures its wait
    # from the context and must never reach for a setting mid-run.
    seconds = request.deadline_seconds or run_settings.budgets.run_timeout_seconds
    # Step 2: The trace id is fresh per run, because a stale-claim takeover reruns the same
    # request id and both runs must be kept; the stored row carries the request id beside it.
    return ExecutionContext(
        request_id=request.request_id,
        trace_id=uuid.uuid4().hex,
        tenant=request.tenant,
        principal=request.principal,
        profile=request.profile,
        conversation_id=request.conversation_id,
        conversation_type=request.conversation_type,
        started_at=started_at,
        deadline_at=started_at + timedelta(seconds=seconds),
        model=run_settings.model,
        budgets=run_settings.budgets,
        public_base_url=run_settings.public_base_url,
        # The server's date unless the channel said which day the question is about: the
        # analytics frame has always used the server date, and the logspace channel passes its own.
        tz_name=request.tz_name,
        today=request.today or date.today(),
        resilience=run_settings.resilience,
    )
