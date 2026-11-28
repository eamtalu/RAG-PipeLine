"""routine proposals, and why a routine paused itself

Revision ID: f8c3d1e5a7b2
Revises: e7b2c0d4f6a1
Create Date: 2026-11-28

Chunk 251 (program step 3, slice 3). A routine set up for a colleague is a row in
teams_task_proposals until they answer; accepting makes a teams_tasks row they own, marked with
who proposed it. teams_tasks also learns how many runs failed in a row and why it paused.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f8c3d1e5a7b2"
down_revision = "e7b2c0d4f6a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("teams_tasks", sa.Column("proposed_by", sa.String(128), nullable=True))
    op.add_column("teams_tasks", sa.Column("proposed_by_name", sa.String(128), nullable=True))
    op.add_column("teams_tasks", sa.Column("failed_in_a_row", sa.Integer(), nullable=False,
                                           server_default="0"))
    op.add_column("teams_tasks", sa.Column("pause_reason", sa.String(300), nullable=True))
    op.create_table(
        "teams_task_proposals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("sender_object_id", sa.String(128), nullable=False),
        sa.Column("sender_name", sa.String(128), nullable=True),
        sa.Column("receiver_object_id", sa.String(128), nullable=False),
        sa.Column("receiver_name", sa.String(128), nullable=True),
        sa.Column("fields", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("status", sa.String(16), nullable=False, server_default="waiting"),
        sa.Column("task_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("teams_tasks.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_teams_task_proposals_receiver", "teams_task_proposals",
                    ["customer_code", "receiver_object_id", "status"])
    op.create_index("ix_teams_task_proposals_sender", "teams_task_proposals",
                    ["customer_code", "sender_object_id", "created_at"])


def downgrade() -> None:
    op.drop_table("teams_task_proposals")
    for column in ("pause_reason", "failed_in_a_row", "proposed_by_name", "proposed_by"):
        op.drop_column("teams_tasks", column)
