"""`POST /api/v1/analytics/agent/ask`: one question to the LangGraph analytics agent. Chunk 124.

JSON in, JSON out, the same result shape as `/logs/debug/ask` so a client can treat either agent
alike. Streaming is a later addition. The tenant comes from the `X-Customer-Code` header, as
everywhere in this API, and is bound into the tools; it is never a field the caller or the model
can set. This route is an adapter: it builds the request and hands it to the gateway.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app import log_context
from app.agent.contracts import AgentRequest, Principal
from app.agent.gateway import run_request, snapshot_settings
from app.services.agent_usage.store import usage_days
from app.agent.profiles import load_profile
from app.agent.runtime.grounded import GroundedAgent
from app.api.deps import (
    admission,
    get_current_customer,
    model_router,
    require_admin,
    tool_registry,
    trace_store,
    usage_store,
)
from app.api.service_auth import ANONYMOUS, current_caller
from app.config.database import get_session

router = APIRouter(prefix="/analytics/agent", tags=["analytics-agent"])


class HistoryTurn(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=4000)
    history: list[HistoryTurn] = Field(
        default_factory=list,
        max_length=40,
        description="Earlier turns of the same conversation, oldest first. Transitional: the web "
        "has no conversation store or sign-in yet, so the client sends its own turns; "
        "they are replayed as plain text without figures. Removed when the web gets a "
        "store.",
    )


def _runtime(customer: str) -> GroundedAgent:
    """The analytics profile on this process's registry; the profile lists its namespaces."""
    return GroundedAgent(
        customer, profile=load_profile("analytics"), registry=tool_registry(), router=model_router()
    )


def _principal(customer: str) -> Principal:
    """The service caller behind a web request, until people sign in (OIDC)."""
    caller = current_caller.get()
    return Principal.for_service(customer, caller.name if caller else ANONYMOUS)


@router.post("/ask")
async def ask(
    body: AskRequest,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Answer one question for the tenant; the response is the agent's legacy result dict.

    ({"question": "how many?"}, X-Customer-Code acme) -> {"answer", "stop_reason", ...}
    """
    # Step 1: End the transaction the tenant lookup began on the request session, so that its
    # pooled connection is free for the whole model wait; the tools open their own short sessions.
    await db.commit()
    # Step 2: Build the request with the bound request id, so the response's X-Request-ID and the
    # run's log lines name the same run; snapshot settings here so the 503 can name the model.
    request = AgentRequest(
        request_id=log_context.request_id_var.get() or log_context.new_request_id(),
        tenant=customer,
        principal=_principal(customer),
        profile="analytics",
        text=body.question,
        conversation_id=None,
    )
    run_settings = snapshot_settings()
    # Step 3: Run through the gateway and turn a provider failure into a clear 503 rather than a
    # bare 500.
    try:
        result = await run_request(
            request,
            runtime=_runtime(customer),
            prior_turns=tuple(t.model_dump() for t in body.history),
            run_settings=run_settings,
            trace_store=trace_store(),
            admission=admission(),
            usage_store=usage_store(),
        )
    except Exception as exc:  # noqa: BLE001 - the provider is down or misconfigured
        raise HTTPException(
            503, detail=f"the analytics agent could not run " f"({run_settings.model.name}): {exc}"
        )
    return result.to_legacy_dict()


@router.get("/usage")
async def usage(
    days: int = Query(7, ge=1, le=90, description="How many UTC days back, today included."),
    customer: str = Depends(get_current_customer),
    _admin: None = Depends(require_admin),
    db: AsyncSession = Depends(get_session),
):
    """The tenant's model usage per UTC day, newest first, with the rows per provider and model.

    (?days=7, X-Customer-Code acme, an admin caller) -> {"tenant": "acme", "since": "...",
    "days": [{"day", "calls", "failed_calls", "input_tokens", "output_tokens", "cached_tokens",
    "cost_usd" (null when any row of the day is unpriced), "latency_ms", "models": [...]}]}
    """
    since = datetime.now(timezone.utc).date() - timedelta(days=days - 1)
    rows = await usage_days(db, customer, since=since)
    # Step 1: Group the rows by day, keeping the store's order (newest day first), because the
    # person reads a day's total before its models.
    by_day: dict[str, dict] = {}
    for row in rows:
        day = by_day.setdefault(
            row.day.isoformat(),
            {"day": row.day.isoformat(), "calls": 0, "failed_calls": 0, "input_tokens": 0,
             "output_tokens": 0, "cached_tokens": 0, "cost_usd": 0.0, "latency_ms": 0,
             "models": []},
        )
        model = {
            "provider": row.provider, "model": row.model, "calls": row.calls,
            "failed_calls": row.failed_calls, "input_tokens": row.input_tokens,
            "output_tokens": row.output_tokens, "cached_tokens": row.cached_tokens,
            "cost_usd": None if row.cost_usd is None else float(row.cost_usd),
            "latency_ms": row.latency_ms,
        }
        day["models"].append(model)
        for key in ("calls", "failed_calls", "input_tokens", "output_tokens", "cached_tokens",
                    "latency_ms"):
            day[key] += model[key]
        # Step 2: A day with one unpriced row has no honest total, so its cost is null rather
        # than a partial sum that reads as the whole.
        if model["cost_usd"] is None or day["cost_usd"] is None:
            day["cost_usd"] = None
        else:
            day["cost_usd"] = round(day["cost_usd"] + model["cost_usd"], 6)
    return {"tenant": customer, "since": since.isoformat(), "days": list(by_day.values())}
