"""`POST /api/v1/analytics/agent/ask`: one question to the LangGraph analytics agent. Chunk 124.

JSON in, JSON out, the same result shape as `/logs/debug/ask` so a client can treat either agent
alike. Streaming is a later addition. The tenant comes from the `X-Customer-Code` header, as
everywhere in this API, and is bound into the tools; it is never a field the caller or the model
can set. This route is an adapter: it builds the request and hands it to the gateway.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app import log_context
from app.agent.contracts import AgentRequest, Principal
from app.agent.gateway import run_request, snapshot_settings
from app.api.deps import get_current_customer
from app.api.service_auth import ANONYMOUS, current_caller
from app.config.database import get_session
from app.services.analytics_agent.agent import AnalyticsAgent

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
            runtime=AnalyticsAgent(customer),
            prior_turns=tuple(t.model_dump() for t in body.history),
            run_settings=run_settings,
        )
    except Exception as exc:  # noqa: BLE001 - the provider is down or misconfigured
        raise HTTPException(
            503, detail=f"the analytics agent could not run " f"({run_settings.model.name}): {exc}"
        )
    return result.to_legacy_dict()
