"""teams_conversations: the threads of the Ask history

Revision ID: f5d9b3c8e721
Revises: e4c8a2b7d619
Create Date: 2026-11-13

Stage D, step 6b of docs/esmart-agent-execution-plan.md. The history rail lists conversations
(a thread of follow-ups titled by its first question) rather than every question, as screen 03
draws it. This is the thread's header; the turns stay where they were. Additive only.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f5d9b3c8e721"
down_revision = "e4c8a2b7d619"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams_conversations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("conversation_id", sa.String(256), nullable=False),
        sa.Column("sender_object_id", sa.String(128), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_turn_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("turn_count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("customer_code", "conversation_id", name="uq_teams_conversations_conv"),
    )
    op.create_index("ix_teams_conversations_user_last", "teams_conversations",
                    ["customer_code", "sender_object_id", "last_turn_at"])


def downgrade() -> None:
    op.drop_index("ix_teams_conversations_user_last", table_name="teams_conversations")
    op.drop_table("teams_conversations")
