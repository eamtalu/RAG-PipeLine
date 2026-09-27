"""`POST /api/v1/analytics/agent/ask`: one question to the LangGraph analytics agent. Chunk 124.

JSON in, JSON out, the same result shape as `/logs/debug/ask` so a client can treat either agent
alike. Streaming is a later addition. The tenant comes from the `X-Customer-Code` header, as
everywhere in this API, and is bound into the tools; it is never a field the caller or the model
can set.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_customer
from app.config.database import get_session
from app.services.analytics_agent.agent import AnalyticsAgent
from app.settings import settings

router = APIRouter(prefix="/analytics/agent", tags=["analytics-agent"])


class HistoryTurn(BaseModel):
    role: str = Field(..., pattern="^(user|assistant)$")
    content: str


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=4000)
    history: list[HistoryTurn] = Field(default_factory=list, max_length=40,
                                       description="Earlier turns of the same conversation, oldest first.")


@router.post("/ask")
async def ask(body: AskRequest, customer: str = Depends(get_current_customer),
              db: AsyncSession = Depends(get_session)):
    try:
        result = await AnalyticsAgent(db, customer).ask(
            body.question, history=[t.model_dump() for t in body.history])
    except Exception as exc:  # noqa: BLE001 - the provider is down or misconfigured
        raise HTTPException(503, detail=f"the analytics agent could not run ({settings.analytics_agent_model}): {exc}")
    return result
