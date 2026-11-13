"""Routines: the run ledger says how a run was triggered, by whom, and the person's own note

Revision ID: a7c3e5f9b012
Revises: f1b6c9d3e487
Create Date: 2026-11-13

Stage D, step 15a. The Tasks tab becomes Routines: a routine is run by the clock (as before), by
hand ("Run now", recorded at once), or ticked off by the person (done or skipped, with a note).
Four nullable-or-defaulted columns on `teams_task_runs`; nothing existing changes. Runs older than
30 days are purged by the tasks loop from this step on.
"""

import sqlalchemy as sa
from alembic import op

revision = "a7c3e5f9b012"
down_revision = "f1b6c9d3e487"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("teams_task_runs", sa.Column("trigger", sa.String(8), nullable=False, server_default="time"))
    op.add_column("teams_task_runs", sa.Column("by_object_id", sa.String(128), nullable=True))
    op.add_column("teams_task_runs", sa.Column("by_name", sa.String(128), nullable=True))
    op.add_column("teams_task_runs", sa.Column("note", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("teams_task_runs", "note")
    op.drop_column("teams_task_runs", "by_name")
    op.drop_column("teams_task_runs", "by_object_id")
    op.drop_column("teams_task_runs", "trigger")
