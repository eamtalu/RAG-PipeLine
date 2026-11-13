"""Group caps: a group sends at most max_per_hour advisory cards per hour; the announcement ledger

Revision ID: e0a5b8c2d376
Revises: d9f4a7b1e265
Create Date: 2026-11-13

Stage D, step 13. One column on notification_groups (`max_per_hour`, default 5) and the ledger table
`advisory_announcements` (what each group sent or held per clock hour, and the hour's notice and
summary cards). Additive only; existing groups get the default cap.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e0a5b8c2d376"
down_revision = "d9f4a7b1e265"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("notification_groups", sa.Column("max_per_hour", sa.Integer(), nullable=False, server_default="5"))
    op.create_table(
        "advisory_announcements",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("group_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("notification_groups.id", ondelete="CASCADE"), nullable=False),
        sa.Column("advisory_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("advisories.id", ondelete="CASCADE"), nullable=True),
        sa.Column("status", sa.String(8), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_advisory_announcements_window", "advisory_announcements", ["group_id", "window_start"])


def downgrade() -> None:
    op.drop_index("ix_advisory_announcements_window", table_name="advisory_announcements")
    op.drop_table("advisory_announcements")
    op.drop_column("notification_groups", "max_per_hour")
