# task.py — the reminders and schedules of the eSmart Eye Teams app (stage D, step 10)
#
#   A task is something that comes back at a time: a reminder (a note to oneself, with the latest
#   figures when it has a query attached) or a schedule (a question eSmart Eye runs for the
#   person on a rhythm, or whenever a condition holds). Both live on one table; `kind` tells them
#   apart. The schedule is data: `when` is the spec and `next_fire_at` the moment the consumer's
#   loop acts, so a restart loses nothing. teams_task_runs is the ledger, one row per planned
#   firing, unique, so a firing happens exactly once.
#
#   customer_code is a real foreign key, as on the access tables.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

TASK_KINDS = ("reminder", "schedule")
#: active: will fire; paused: kept, not firing; done: a reminder acted on or fired for good;
#: stopped: a schedule ended or deleted by its owner.
TASK_STATUSES = ("active", "paused", "done", "stopped")
TARGETS = ("me", "channel")
SHOW_AS = ("table", "chart", "summary")
RUN_OUTCOMES = ("sent", "skipped", "missed", "failed")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TeamsTask(Base):
    __tablename__ = "teams_tasks"
    __table_args__ = (
        Index("ix_teams_tasks_due", "status", "next_fire_at"),
        Index("ix_teams_tasks_owner", "customer_code", "owner_object_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    owner_object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    owner_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: The spec: {"kind": "once", "at": iso} | {"kind": "recurring", "weekdays": [0..6], "time": "16:00"}
    #: | {"kind": "condition", "metric": "fill_rate", "op": "<", "value": 0.97, "cooldown_minutes": 60}
    when: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    next_fire_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fire_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    #: The question re-run on fire (a reminder "with the latest figures", every schedule).
    query_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: {"kind": "advisory", "advisory_id": ...} | {"kind": "delivery", "delivery_number", "departure_date"}
    skip_if: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    target: Mapped[str] = mapped_column(String(16), nullable=False, default="me", server_default="me")
    #: The group a schedule posts to (step 12); None for "me".
    target_group_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_groups.id", ondelete="SET NULL"), nullable=True)
    show_as: Mapped[str] = mapped_column(String(16), nullable=False, default="table", server_default="table")
    until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_via: Mapped[str] = mapped_column(String(16), nullable=False, default="tab", server_default="tab")
    source: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)  # what it was made from
    #: The latest firing's words: {"text", "blocks", "at", "outcome"}; the card's "Right now".
    last_result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TeamsTaskRun(Base):
    __tablename__ = "teams_task_runs"
    __table_args__ = (
        UniqueConstraint("task_id", "planned_at", name="uq_teams_task_runs_planned"),
        Index("ix_teams_task_runs_customer_started", "customer_code", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("teams_tasks.id", ondelete="CASCADE"), nullable=False)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    planned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False, default="sent", server_default="sent")
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocks: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
