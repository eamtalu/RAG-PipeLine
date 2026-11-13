# watch.py — the watchlist of the eSmart Eye Teams app (stage D, step 14)
#
#   A watch is one thing a person chose to keep an eye on: a delivery, a route, a location, a
#   zone, a product or a measure (fill rate of a site). The consumer's watches loop refreshes each
#   watch on the cadence of its data, keeps its live value, and writes a watch_events line only
#   when the STATUS changes (never when a number moves). The person's unseen events are the
#   in-app alerts; watch_not_now remembers "Not now" answers to Ask's suggestions for a day.
#
#   customer_code is a real foreign key, as on the access tables. One active watch per person
#   and entity (a partial unique index), so adding something already watched finds that row.

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

KINDS = ("delivery", "route", "location", "zone", "product", "measure")
WATCH_STATUSES = ("active", "ended")
ENDED_REASONS = ("stopped", "delivered", "until", "access", "swapped", "not_found")
CREATED_VIA = ("tab", "eye", "ask", "search", "form", "undo")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Watch(Base):
    __tablename__ = "watches"
    __table_args__ = (
        Index("uq_watches_active", "customer_code", "owner_object_id", "kind", "entity_id", unique=True,
              postgresql_where=text("status = 'active'")),
        Index("ix_watches_due", "status", "next_check_at"),
        Index("ix_watches_owner", "customer_code", "owner_object_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    owner_object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    owner_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(128), nullable=False)
    #: What the kind needs to find the thing again: a delivery's row id, a zone's locations, a
    #: measure's site; never shown, never trusted from the page
    entity_ref: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    label: Mapped[str] = mapped_column(String(200), nullable=False)
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: The alert rule: {"rule": "tier_from", "tier": "at_risk"} | {"rule": "below"|"above", "level": 0.97}
    #: | {"rule": "status_change"} | {"rule": "show_only"}
    alert: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    also_teams: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", server_default="active")
    until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_value: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    last_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: A downgrade waits here until it has held for the settle time, so a bouncing tier is one event
    pending_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    pending_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    change_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_via: Mapped[str] = mapped_column(String(16), nullable=False, default="tab", server_default="tab")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)


class WatchEvent(Base):
    """One status change of one watch: the line in "What changed today", the toast, the unseen count."""

    __tablename__ = "watch_events"
    __table_args__ = (
        UniqueConstraint("watch_id", "seq", name="uq_watch_events_seq"),
        Index("ix_watch_events_owner", "customer_code", "owner_object_id", "at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    watch_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("watches.id", ondelete="CASCADE"), nullable=False)
    owner_object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    old_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    new_status: Mapped[str] = mapped_column(String(24), nullable=False)
    is_alert: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class WatchNotNow(Base):
    """A "Not now" to Ask's suggestion: no suggestion for this thing again until the moment passes."""

    __tablename__ = "watch_not_now"

    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True)
    owner_object_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    entity_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
