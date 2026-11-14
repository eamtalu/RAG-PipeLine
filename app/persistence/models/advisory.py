# advisory.py — what the agents of the eSmart Eye Teams app found (stage D, step 9)
#
#   An advisory is one thing worth a person's attention: a delivery likely to leave lines behind,
#   a stock count that disagrees with the system, a location that keeps turning up empty. An agent
#   runs on its own cadence, writes its findings here (one row per thing, by dedup_key, kept open
#   while the finding persists and resolved when it clears), and the people act on them:
#   acknowledge, assign, snooze, dismiss. advisory_agents is each agent's switch and cadence per
#   customer; advisory_agent_runs the ledger of every run.
#
#   customer_code is a real foreign key, as on the access tables.

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

SEVERITIES = ("high", "medium", "low")
ADVISORY_STATUSES = ("open", "acknowledged", "snoozed", "resolved", "dismissed")
AGENT_NAMES = ("delivery", "stock", "location", "forecast", "shortage", "data_quality", "access_watch")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Advisory(Base):
    __tablename__ = "advisories"
    __table_args__ = (
        UniqueConstraint("customer_code", "dedup_key", name="uq_advisories_dedup"),
        Index("ix_advisories_customer_status", "customer_code", "status", "opened_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    agent: Mapped[str] = mapped_column(String(32), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    evidence: Mapped[str | None] = mapped_column(Text, nullable=True)
    impact: Mapped[str | None] = mapped_column(Text, nullable=True)
    recommended: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)  # 0..1 where the agent has one
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    assignee_object_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    assignee_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    snoozed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dedup_key: Mapped[str] = mapped_column(String(200), nullable=False)
    source: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)  # what it points at
    # step 17: the kind of finding (kinds.py) and what it is about, in words, for the tab's groups
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="other", server_default="other")
    about: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    actions: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)  # the buttons it offers
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AdvisoryAgent(Base):
    __tablename__ = "advisory_agents"

    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True)
    agent: Mapped[str] = mapped_column(String(32), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    cadence_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300, server_default="300")
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # opened, resolved, error
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    updated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)


class AdvisoryAgentRun(Base):
    __tablename__ = "advisory_agent_runs"
    __table_args__ = (Index("ix_advisory_agent_runs_customer_started", "customer_code", "started_at"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    agent: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    opened: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    resolved: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
