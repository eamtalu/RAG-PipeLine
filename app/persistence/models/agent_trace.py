# agent_trace.py - one row per agent run: what it did, redacted, for audit and troubleshooting
#
#   The gateway stores a run's TraceRecord (app/agent/runtime/trace.py) after the answer exists:
#   the question and the answer, every tool call with its arguments, result, timing and outcome,
#   every guard verdict, the model usage, the profile and code versions, and the error when the
#   run raised. Secrets are redacted and results capped before the row is built, so nothing here
#   is ever more sensitive than the logs. A run that is retried after a stale claim gets its own
#   trace id; `request_id` is what ties the attempts together.
#
#   customer_code is the soft tenant key, like agent_runs; a tenant purge deletes the rows.

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AgentTrace(Base):
    __tablename__ = "agent_traces"
    __table_args__ = (
        # troubleshooting reads "this tenant's runs, newest first" and "the attempts of one request"
        Index("ix_agent_traces_customer_started", "customer_code", "started_at"),
        Index("ix_agent_traces_customer_request", "customer_code", "request_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trace_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    request_id: Mapped[str] = mapped_column(String(64), nullable=False)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    principal_id: Mapped[str] = mapped_column(String(128), nullable=False)
    principal_kind: Mapped[str] = mapped_column(String(16), nullable=False)  # user | service
    profile: Mapped[str] = mapped_column(String(32), nullable=False)
    profile_version: Mapped[str] = mapped_column(String(16), nullable=False)
    code_version: Mapped[str] = mapped_column(String(16), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    conversation_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    conversation_type: Mapped[str] = mapped_column(String(16), nullable=False)
    # ok | declined | timeout | error
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    stop_reason: Mapped[str] = mapped_column(String(16), nullable=False)
    declined: Mapped[str | None] = mapped_column(String(32), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    iterations: Mapped[int] = mapped_column(Integer, nullable=False)
    retries: Mapped[int] = mapped_column(Integer, nullable=False)
    question: Mapped[str] = mapped_column(Text, nullable=False)
    answer: Mapped[str] = mapped_column(Text, nullable=False)
    # [{"name", "stage", "verdict", "detail", "ms"}] in run order
    guards: Mapped[list] = mapped_column(JSONB, nullable=False)
    # [{"namespace", "name", "args", "result", "ms", "ok"}] in run order; args hashed when sensitive
    tool_calls: Mapped[list] = mapped_column(JSONB, nullable=False)
    withheld_figures: Mapped[list] = mapped_column(JSONB, nullable=False)
    usage: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # model_calls, tokens
    error: Mapped[str | None] = mapped_column(Text, nullable=True)  # "Type: message" when raised
