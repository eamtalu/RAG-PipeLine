# notification_group.py — notification groups of the eSmart Eye Teams app (stage D, step 12)
#
#   A group is a named route for what the agents and the schedules produce: the topics it takes
#   (advisory:delivery, advisory:stock, advisory:location, schedule), the lowest severity it
#   takes, the Teams channels it posts to (customer_notification_channels, by webhook) and the
#   people who see it in the app. Routing is one lookup at dispatch; nothing matching goes nowhere,
#   so a new channel receives nothing until a group takes it (owner's decision, 13 November 2026).
#
#   Step 13 adds the cap: a group sends at most `max_per_hour` advisory cards per clock hour, and
#   advisory_announcements is the ledger of what each group sent, held, the notice it sent when it
#   went quiet and the summary it sent once the hour was over. Held is recorded, not dropped, so
#   "which findings were in that summary?" stays answerable.
#
#   customer_code is a real foreign key, as on the access tables.

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

TOPICS = ("advisory:delivery", "advisory:stock", "advisory:location", "schedule")
SEVERITIES = ("high", "medium", "low")
ANNOUNCEMENT_STATUSES = ("sent", "held", "notice", "summary")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class NotificationGroup(Base):
    __tablename__ = "notification_groups"
    __table_args__ = (UniqueConstraint("customer_code", "name", name="uq_notification_groups_name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    topics: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    min_severity: Mapped[str] = mapped_column(String(8), nullable=False, default="high", server_default="high")
    max_per_hour: Mapped[int] = mapped_column(Integer, nullable=False, default=5, server_default="5")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now, nullable=False)


class NotificationGroupChannel(Base):
    __tablename__ = "notification_group_channels"

    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_groups.id", ondelete="CASCADE"), primary_key=True)
    channel_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("customer_notification_channels.id", ondelete="CASCADE"), primary_key=True)


class NotificationGroupMember(Base):
    __tablename__ = "notification_group_members"

    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_groups.id", ondelete="CASCADE"), primary_key=True)
    object_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    added_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)


class AdvisoryAnnouncement(Base):
    """One line of a group's ledger: an advisory sent or held in a clock hour, or the hour's notice
    or summary card (no advisory)."""

    __tablename__ = "advisory_announcements"
    __table_args__ = (Index("ix_advisory_announcements_window", "group_id", "window_start"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_groups.id", ondelete="CASCADE"), nullable=False)
    advisory_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("advisories.id", ondelete="CASCADE"), nullable=True)
    status: Mapped[str] = mapped_column(String(8), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
