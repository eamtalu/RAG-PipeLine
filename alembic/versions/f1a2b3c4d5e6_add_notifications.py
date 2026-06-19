"""add notifications: channels, rules, events, deliveries

Revision ID: f1a2b3c4d5e6
Revises: a3e6b8c1d472
Create Date: 2026-06-19

Alerting subsystem (rules → in-process event bus → channels). Four tables:
  - customer_notification_channels: per-customer alert destinations (Teams/Slack/WhatsApp webhooks).
  - notification_rules: data-driven, frontend-managed rules (draft → active → inactive).
  - notification_events: durable outbox of published events (store-and-forward backbone).
  - notification_deliveries: per-(event × channel) sent/unsent tracker with backoff/retry.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "f1a2b3c4d5e6"
down_revision = "a3e6b8c1d472"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "customer_notification_channels",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("channel_type", sa.String(32), nullable=False),
        sa.Column("name", sa.String(128), nullable=False, server_default="default"),
        sa.Column("config", JSONB, nullable=False, server_default="{}"),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("customer_code", "channel_type", "name", name="uq_cust_notif_channel"),
    )
    op.create_index("ix_customer_notification_channels_customer_code",
                    "customer_notification_channels", ["customer_code"])

    op.create_table(
        "notification_rules",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("rule_type", sa.String(32), nullable=False),
        sa.Column("match", JSONB, nullable=False, server_default="{}"),
        sa.Column("severity", sa.String(16), nullable=False, server_default="error"),
        sa.Column("target_channel_ids", JSONB, nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_notification_rules_customer_code", "notification_rules", ["customer_code"])
    op.create_index("ix_notification_rules_status", "notification_rules", ["status"])

    op.create_table(
        "notification_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("dedup_key", sa.String(255), nullable=False),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("rule_id", UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False, server_default="error"),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("payload", JSONB, nullable=False, server_default="{}"),
        sa.Column("target_channel_ids", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dedup_key", name="uq_notification_events_dedup_key"),
    )
    op.create_index("ix_notification_events_dedup_key", "notification_events", ["dedup_key"])
    op.create_index("ix_notification_events_customer_code", "notification_events", ["customer_code"])
    op.create_index("ix_notification_events_created_at", "notification_events", ["created_at"])

    op.create_table(
        "notification_deliveries",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("event_id", UUID(as_uuid=True),
                  sa.ForeignKey("notification_events.id", ondelete="CASCADE"), nullable=False),
        sa.Column("channel_id", UUID(as_uuid=True),
                  sa.ForeignKey("customer_notification_channels.id", ondelete="SET NULL"), nullable=True),
        sa.Column("channel_type", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("event_id", "channel_id", name="uq_notif_delivery_event_channel"),
    )
    op.create_index("ix_notification_deliveries_event_id", "notification_deliveries", ["event_id"])
    op.create_index("ix_notification_deliveries_status", "notification_deliveries", ["status"])
    op.create_index("ix_notif_deliveries_due", "notification_deliveries", ["status", "next_attempt_at"])


def downgrade() -> None:
    op.drop_table("notification_deliveries")
    op.drop_index("ix_notification_events_created_at", table_name="notification_events")
    op.drop_index("ix_notification_events_customer_code", table_name="notification_events")
    op.drop_index("ix_notification_events_dedup_key", table_name="notification_events")
    op.drop_table("notification_events")
    op.drop_index("ix_notification_rules_status", table_name="notification_rules")
    op.drop_index("ix_notification_rules_customer_code", table_name="notification_rules")
    op.drop_table("notification_rules")
    op.drop_index("ix_customer_notification_channels_customer_code",
                  table_name="customer_notification_channels")
    op.drop_table("customer_notification_channels")
