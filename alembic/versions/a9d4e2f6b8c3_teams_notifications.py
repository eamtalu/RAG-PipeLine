"""the Teams bell: a notification ledger and a routine's switch

Revision ID: a9d4e2f6b8c3
Revises: f8c3d1e5a7b2
Create Date: 2026-11-28

Chunk 252 (program step 3, slice 4a). teams_notifications is the ledger of what a person is
told under their Teams bell; teams_tasks.notify_bell says whether a routine's owner wants to be
told when it has run. Existing routines stay quiet.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a9d4e2f6b8c3"
down_revision = "f8c3d1e5a7b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("teams_tasks", sa.Column("notify_bell", sa.Boolean(), nullable=False,
                                           server_default=sa.text("false")))
    op.create_table(
        "teams_notifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("object_id", sa.String(128), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("subject_id", sa.String(128), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("text", sa.String(150), nullable=False, server_default=""),
        sa.Column("tab", sa.String(32), nullable=False, server_default="home"),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.String(300), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("kind", "subject_id", name="uq_teams_notifications_subject"),
    )
    op.create_index("ix_teams_notifications_queue", "teams_notifications", ["status", "created_at"])


def downgrade() -> None:
    op.drop_table("teams_notifications")
    op.drop_column("teams_tasks", "notify_bell")
