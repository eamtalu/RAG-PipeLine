# agent_run.py - one row per agent run, claimed before the model is asked
#
#   The queue delivers a question at least once. Without a ledger a redelivered message ran the
#   model again and wrote its conversation turns twice. The consumer now inserts a row keyed by the
#   edge's job id (`request_id`) before it runs: the insert either wins the claim or finds the row.
#   A row that is `done` carries the stored answer, re-posted as it is; a row still `running` means
#   another consumer has the question, unless it is older than the queue's visibility window, in
#   which case that consumer is dead and the claim is taken over.
#
#   customer_code is the soft tenant key, like the log tables: the ledger is per tenant for purges
#   and, later, for the in-flight and daily quotas of docs/esmart-agent-architecture.md.

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

STATUS_RUNNING = "running"
STATUS_DONE = "done"


class AgentRun(Base):
    __tablename__ = "agent_runs"
    __table_args__ = (
        # sweeps and quotas read "this tenant's runs, newest first"
        Index("ix_agent_runs_customer_claimed", "customer_code", "claimed_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # The channel's own id for the request: the edge's job id for Teams. One run per request.
    request_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    customer_code: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    channel: Mapped[str] = mapped_column(String(16), nullable=False)  # "teams"
    status: Mapped[str] = mapped_column(String(16), nullable=False)  # running | done
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # How many consumers have claimed this run: 1, plus one per takeover of a stale claim.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    # The answer payload as posted to the channel, once `done`; replayed on redelivery.
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
