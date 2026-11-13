"""Notification groups: routes by topic and severity to channels and people; a schedule names its group

Revision ID: d9f4a7b1e265
Revises: c8e3f6a9d054
Create Date: 2026-11-13

Stage D, step 12. Three tables (the groups, their channels, their members) and one nullable column on
teams_tasks (`target_group_id`, the group a schedule posts to). Additive only.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d9f4a7b1e265"
down_revision = "c8e3f6a9d054"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notification_groups",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("topics", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("min_severity", sa.String(8), nullable=False, server_default="high"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "name", name="uq_notification_groups_name"),
    )
    op.create_table(
        "notification_group_channels",
        sa.Column("group_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("notification_groups.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("channel_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("customer_notification_channels.id", ondelete="CASCADE"), primary_key=True),
    )
    op.create_table(
        "notification_group_members",
        sa.Column("group_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("notification_groups.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("object_id", sa.String(128), primary_key=True),
        sa.Column("added_by", sa.String(128), nullable=True),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.add_column("teams_tasks", sa.Column(
        "target_group_id", postgresql.UUID(as_uuid=True),
        sa.ForeignKey("notification_groups.id", ondelete="SET NULL"), nullable=True))


def downgrade() -> None:
    op.drop_column("teams_tasks", "target_group_id")
    op.drop_table("notification_group_members")
    op.drop_table("notification_group_channels")
    op.drop_table("notification_groups")
