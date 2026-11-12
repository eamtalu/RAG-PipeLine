"""History on teams_conversation_turns: who asked, where, the typed answer and its times

Revision ID: e4c8a2b7d619
Revises: d3b7e9f1a2c4
Create Date: 2026-11-12

Stage D, step 6 of docs/esmart-agent-execution-plan.md. The turns table held only the text the
agent replays; the Ask history of the eSmart Eye Teams app needs, per answer, who asked (the Entra
object id), the site the page looked at, the typed blocks, and when it was asked, answered and how
fresh its figures were. All nullable: rows before this change keep working and show as text only.
The index serves the one new read, "this person's last questions", newest first.
Additive only: nothing is dropped, renamed or made NOT NULL.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e4c8a2b7d619"
down_revision = "d3b7e9f1a2c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    table = "teams_conversation_turns"
    op.add_column(table, sa.Column("sender_object_id", sa.String(128), nullable=True))
    op.add_column(table, sa.Column("site_id", sa.String(16), nullable=True))
    op.add_column(table, sa.Column("blocks", postgresql.JSONB(), nullable=True))
    op.add_column(table, sa.Column("asked_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(table, sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(table, sa.Column("data_as_of", sa.DateTime(timezone=True), nullable=True))
    op.add_column(table, sa.Column("provisional", sa.Boolean(), nullable=True))
    # the history read: one person's turns, newest first, within the tenant
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_teams_conversation_turns_user_created",
            table,
            ["customer_code", "sender_object_id", "created_at"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    table = "teams_conversation_turns"
    with op.get_context().autocommit_block():
        op.drop_index("ix_teams_conversation_turns_user_created", table_name=table,
                      postgresql_concurrently=True)
    for column in ("provisional", "data_as_of", "answered_at", "asked_at", "blocks", "site_id",
                   "sender_object_id"):
        op.drop_column(table, column)
