"""Teams tasks: the reminders and schedules, and the ledger of their firings

Revision ID: c8e3f6a9d054
Revises: b7f2d5e8c943
Create Date: 2026-11-13

Stage D, step 10 of docs/esmart-agent-execution-plan.md. One table for both kinds (`kind`
reminder | schedule), the spec as JSON and the next firing as a timestamp the consumer's loop
polls, so a restart loses nothing; one ledger row per planned firing, unique, so a firing
happens once. Additive only.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c8e3f6a9d054"
down_revision = "b7f2d5e8c943"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("owner_object_id", sa.String(128), nullable=False),
        sa.Column("owner_name", sa.String(128), nullable=True),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("when", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("next_fire_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_fired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fire_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("query_text", sa.Text(), nullable=True),
        sa.Column("skip_if", postgresql.JSONB(), nullable=True),
        sa.Column("target", sa.String(16), nullable=False, server_default="me"),
        sa.Column("show_as", sa.String(16), nullable=False, server_default="table"),
        sa.Column("until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_via", sa.String(16), nullable=False, server_default="tab"),
        sa.Column("source", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("last_result", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_teams_tasks_due", "teams_tasks", ["status", "next_fire_at"])
    op.create_index("ix_teams_tasks_owner", "teams_tasks", ["customer_code", "owner_object_id", "status"])
    op.create_table(
        "teams_task_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("teams_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("planned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(16), nullable=False, server_default="sent"),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("blocks", postgresql.JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.UniqueConstraint("task_id", "planned_at", name="uq_teams_task_runs_planned"),
    )
    op.create_index("ix_teams_task_runs_customer_started", "teams_task_runs", ["customer_code", "started_at"])


def downgrade() -> None:
    op.drop_table("teams_task_runs")
    op.drop_index("ix_teams_tasks_owner", table_name="teams_tasks")
    op.drop_index("ix_teams_tasks_due", table_name="teams_tasks")
    op.drop_table("teams_tasks")
