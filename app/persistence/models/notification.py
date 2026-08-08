# notification.py — Alerting subsystem (rules → in-process event bus → channels)
#
#   Four tables make notifications data-driven and frontend-manageable, with durable
#   store-and-forward so an outage never drops an alert:
#
#   - customer_notification_channels: WHERE alerts go. One row per destination (a Teams channel,
#     a Slack channel, a WhatsApp group). A customer may have many → an event fans out to all of
#     them (or to the subset a rule targets).
#   - notification_rules: WHEN to alert. Data-driven (no redeploy): the frontend creates/edits these
#     and flips them draft → active → inactive. Only ACTIVE rules are evaluated. `rule_type` selects
#     a code evaluator; `match` (JSONB) parameterizes it.
#   - notification_events: durable OUTBOX of published events. Written at publish time so the event
#     survives a crash/outage; `payload` is stored so retries never recompute it (matters for digests).
#   - notification_deliveries: one row per (event × channel) = the per-channel sent/unsent tracker
#     with attempts / backoff / last_error, drained by the redelivery loop until delivered or dead.

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    String, Integer, Boolean, DateTime, Text, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class RuleStatus(str, enum.Enum):
    draft = "draft"        # created/edited but not live — never evaluated
    active = "active"      # published AND live — evaluated every cycle
    inactive = "inactive"  # deactivated — kept for history/re-enable, not evaluated


class RuleType(str, enum.Enum):
    status_match = "status_match"  # streaming: txn.status in match["statuses"] (+ optional method filter)
    text_match = "text_match"      # streaming: substring/regex over match["fields"] (default error_text)
    digest = "digest"              # windowed: one summary event per interval of matching transactions


class DeliveryStatus(str, enum.Enum):
    pending = "pending"        # created, not yet attempted (or claimed for retry)
    delivered = "delivered"    # accepted by the channel
    failed = "failed"          # last attempt failed; will be retried after next_attempt_at
    dead = "dead"              # exceeded max attempts — dead-lettered (surfaced, not retried)
    # Past its rule's burst cap for the window: represented by a rollup summary card instead of being
    # sent individually. RECORDED rather than skipped, so "which transactions were in that rollup?"
    # stays answerable — the cursor has already moved past them. Never claimed by the drain.
    suppressed = "suppressed"


# ---------------------------------------------------------------------------------------------------
# WHERE alerts go
# ---------------------------------------------------------------------------------------------------
class CustomerNotificationChannel(Base):
    __tablename__ = "customer_notification_channels"

    # a tenant may register several destinations; (customer, type, name) addresses one uniquely so the
    # same customer can have e.g. two distinct Teams channels ("ops" and "oncall").
    __table_args__ = (
        UniqueConstraint("customer_code", "channel_type", "name", name="uq_cust_notif_channel"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    channel_type: Mapped[str] = mapped_column(String(32))         # "teams" | "slack" | "whatsapp"
    name: Mapped[str] = mapped_column(String(128), default="default")
    # transport config; for webhook channels: {"webhook_url": "https://..."}. Secrets live here.
    config: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------------------------------
# WHEN to alert (data-driven, frontend-managed)
# ---------------------------------------------------------------------------------------------------
class NotificationRule(Base):
    __tablename__ = "notification_rules"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    rule_type: Mapped[str] = mapped_column(String(32))            # RuleType value
    # evaluator parameters, e.g. {"statuses": ["error"]} / {"pattern": "...", "is_regex": true} /
    # {"statuses": ["error"], "interval_seconds": 3600}
    match: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    severity: Mapped[str] = mapped_column(String(16), default="error", server_default="error")
    # optional fan-out narrowing: list of channel ids (as strings). Empty/None ⇒ all enabled channels.
    target_channel_ids: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # How far this rule has read the transaction feed, as a log_transactions.created_at (WRITE time,
    # not event time). NULL means "never run" and bootstraps to the lookback window, so activating a
    # rule alerts on recent data rather than replaying all history. Each rule owns its own position,
    # so activating or replaying one never disturbs another. See services/notifications/cursor.py.
    cursor_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # lifecycle: only `active` rows are evaluated (publish/deactivate flip this).
    status: Mapped[str] = mapped_column(String(16), default=RuleStatus.draft.value,
                                        server_default=RuleStatus.draft.value, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------------------------------
# Durable outbox of published events
# ---------------------------------------------------------------------------------------------------
class NotificationEvent(Base):
    __tablename__ = "notification_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # idempotency key — a rule emits a stable key (e.g. "txn-error:{txn_id}" or "digest:{cust}:{hour}")
    # so the same condition is published exactly once even though the worker polls repeatedly.
    dedup_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    rule_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)  # provenance (no FK)
    event_type: Mapped[str] = mapped_column(String(64))
    severity: Mapped[str] = mapped_column(String(16), default="error", server_default="error")
    title: Mapped[str] = mapped_column(String(512))
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # full structured context (the channel adapter renders from this on first send AND on every retry).
    payload: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    # optional channel-id allowlist the rule asked for (audit; dispatch also honors it).
    target_channel_ids: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)


# ---------------------------------------------------------------------------------------------------
# Per-(event × channel) delivery tracker — the sent/unsent ledger
# ---------------------------------------------------------------------------------------------------
class NotificationDelivery(Base):
    __tablename__ = "notification_deliveries"

    # one attempt-track per event per channel; never two rows for the same pair.
    __table_args__ = (
        UniqueConstraint("event_id", "channel_id", name="uq_notif_delivery_event_channel"),
        Index("ix_notif_deliveries_due", "status", "next_attempt_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("notification_events.id", ondelete="CASCADE"), index=True
    )
    # the destination row; SET NULL if the channel is later deleted (delivery then dead-letters).
    channel_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("customer_notification_channels.id", ondelete="SET NULL"), nullable=True
    )
    channel_type: Mapped[str] = mapped_column(String(32))  # denormalized for display/filtering

    status: Mapped[str] = mapped_column(String(16), default=DeliveryStatus.pending.value,
                                        server_default=DeliveryStatus.pending.value, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
