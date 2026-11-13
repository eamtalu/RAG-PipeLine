"""Advisories: what the agents found, each agent's switch and cadence, and the run ledger

Revision ID: b7f2d5e8c943
Revises: a6e1c4d9f832
Create Date: 2026-11-13

Stage D, step 9 of docs/esmart-agent-execution-plan.md. One row per thing an agent found, kept
open while it persists; the agents' switches per customer (seeded by the runner from the
registry: Delivery, Stock controller and Location on; the others off); the ledger of runs.
Additive only.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "b7f2d5e8c943"
down_revision = "a6e1c4d9f832"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "advisories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("agent", sa.String(32), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=True),
        sa.Column("impact", sa.Text(), nullable=True),
        sa.Column("recommended", sa.Text(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("assignee_object_id", sa.String(128), nullable=True),
        sa.Column("assignee_name", sa.String(128), nullable=True),
        sa.Column("acknowledged_by", sa.String(128), nullable=True),
        sa.Column("snoozed_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dedup_key", sa.String(200), nullable=False),
        sa.Column("source", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("actions", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_reason", sa.String(64), nullable=True),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("customer_code", "dedup_key", name="uq_advisories_dedup"),
    )
    op.create_index("ix_advisories_customer_status", "advisories", ["customer_code", "status", "opened_at"])
    op.create_table(
        "advisory_agents",
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True),
        sa.Column("agent", sa.String(32), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("cadence_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_result", postgresql.JSONB(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_by", sa.String(128), nullable=True),
    )
    op.create_table(
        "advisory_agent_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("agent", sa.String(32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("opened", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("resolved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index("ix_advisory_agent_runs_customer_started", "advisory_agent_runs", ["customer_code", "started_at"])


def downgrade() -> None:
    op.drop_table("advisory_agent_runs")
    op.drop_table("advisory_agents")
    op.drop_index("ix_advisories_customer_status", table_name="advisories")
    op.drop_table("advisories")
