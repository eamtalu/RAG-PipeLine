"""Watchlist: watches, their status-change events, and the "Not now" answers to Ask's suggestions

Revision ID: f1b6c9d3e487
Revises: e0a5b8c2d376
Create Date: 2026-11-13

Stage D, step 14. Three new tables; nothing existing changes. The condition schedules of step 10
("when fill rate drops below") are retired in the same step: anything triggered by a condition is
a measure watch now, so any remaining condition schedule is stopped with a note (none existed
live when this was written).
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f1b6c9d3e487"
down_revision = "e0a5b8c2d376"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watches",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("owner_object_id", sa.String(128), nullable=False),
        sa.Column("owner_name", sa.String(128), nullable=True),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("entity_id", sa.String(128), nullable=False),
        sa.Column("entity_ref", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("label", sa.String(200), nullable=False),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("alert", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("also_teams", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_value", postgresql.JSONB(), nullable=True),
        sa.Column("last_status", sa.String(24), nullable=True),
        sa.Column("last_summary", sa.Text(), nullable=True),
        sa.Column("last_changed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pending_status", sa.String(24), nullable=True),
        sa.Column("pending_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("change_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_via", sa.String(16), nullable=False, server_default="tab"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_reason", sa.String(32), nullable=True),
    )
    op.create_index("uq_watches_active", "watches", ["customer_code", "owner_object_id", "kind", "entity_id"],
                    unique=True, postgresql_where=sa.text("status = 'active'"))
    op.create_index("ix_watches_due", "watches", ["status", "next_check_at"])
    op.create_index("ix_watches_owner", "watches", ["customer_code", "owner_object_id", "status"])
    op.create_table(
        "watch_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("watch_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("watches.id", ondelete="CASCADE"), nullable=False),
        sa.Column("owner_object_id", sa.String(128), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("old_status", sa.String(24), nullable=True),
        sa.Column("new_status", sa.String(24), nullable=False),
        sa.Column("is_alert", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(), nullable=True),
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("watch_id", "seq", name="uq_watch_events_seq"),
    )
    op.create_index("ix_watch_events_owner", "watch_events", ["customer_code", "owner_object_id", "at"])
    op.create_table(
        "watch_not_now",
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True),
        sa.Column("owner_object_id", sa.String(128), primary_key=True),
        sa.Column("kind", sa.String(16), primary_key=True),
        sa.Column("entity_id", sa.String(128), primary_key=True),
        sa.Column("until", sa.DateTime(timezone=True), nullable=False),
    )
    # The condition schedules move to the watchlist: stop any that remain, with the reason in the note
    op.execute(
        "UPDATE teams_tasks SET status = 'stopped', next_fire_at = NULL, "
        "note = COALESCE(note || ' · ', '') || 'moved to the watchlist (step 14)' "
        "WHERE kind = 'schedule' AND status = 'active' AND \"when\"->>'kind' = 'condition'"
    )


def downgrade() -> None:
    op.drop_table("watch_not_now")
    op.drop_index("ix_watch_events_owner", table_name="watch_events")
    op.drop_table("watch_events")
    op.drop_index("ix_watches_owner", table_name="watches")
    op.drop_index("ix_watches_due", table_name="watches")
    op.drop_index("uq_watches_active", table_name="watches")
    op.drop_table("watches")
