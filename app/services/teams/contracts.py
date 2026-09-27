# COPIED VERBATIM from the edge repository (teams-agent-edge/app/contracts.py). Change there first.
# Exception, 2026-09-27: `Evidence` and `AnswerPayload.evidence` were added HERE first; the edge must mirror them.
"""The two messages that cross the edge/backend boundary.

These models are the contract with the backend worker (RAG FAST API, `teams_question_worker`).
Both sides validate against them, so a field added here must be optional or bumped in
`schema_version`. Keep this file dependency-free apart from pydantic so the backend can copy it
verbatim.
"""

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


class QuestionJob(BaseModel):
    """One question from Teams, published by the edge to SQS and consumed by the backend worker."""

    schema_version: int = 1
    job_id: str = Field(..., description="Edge-generated UUID; the worker echoes it in the answer.")
    tenant_id: str = Field(..., description="Entra tenant id of the customer, from Microsoft.")
    customer_code: str = Field(
        ..., description="Backend log space resolved from the tenant binding."
    )
    conversation_id: str = Field(
        ...,
        description="Teams conversation id; the reply goes here and "
        "conversation memory is keyed by it.",
    )
    activity_id: str | None = Field(default=None, description="Teams activity id of the question.")
    sender_object_id: str | None = Field(
        default=None, description="Entra object id of the sender, " "for audit only."
    )
    sender_name: str | None = None
    question: str = Field(..., min_length=1)
    enqueued_at: datetime = Field(default_factory=_now)


class EvidenceColumn(BaseModel):
    name: str
    align: Literal["left", "right"] = "left"


class Evidence(BaseModel):
    """The rows behind the answer, as data, so the edge draws a real table on the card instead of
    guessing widths from markdown. Optional: absent for answers that rest on no rows. ADDED on the
    backend side on 2026-09-27; mirror into the edge's contracts.py and its card builder."""

    title: str
    columns: list[EvidenceColumn]
    rows: list[list[str]]
    facts: dict[str, str] = Field(default_factory=dict, description="Small labelled facts under the table: window, sorted by, grain.")
    link: str | None = Field(default=None, description="Where the same view lives in the app, for an open-in button.")


class AnswerPayload(BaseModel):
    """The worker's answer, POSTed to the edge at /internal/answers."""

    schema_version: int = 1
    job_id: str
    conversation_id: str
    status: Literal["ok", "error"] = "ok"
    answer: str = Field(..., description="Markdown text. On status=error a short human sentence.")
    cited_transaction_ids: list[str] = Field(default_factory=list)
    tool_call_count: int = 0
    duration_seconds: float | None = None
    evidence: Evidence | None = None
