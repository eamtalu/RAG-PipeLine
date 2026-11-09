"""agent_runs: one row per agent run, claimed before the model is asked

Revision ID: 3f727cb28a14
Revises: e6f7a8b9c0d1
Create Date: 2026-11-09

Step 5 of docs/esmart-agent-execution-plan.md. The Teams consumer claims a row keyed by the edge's
job id before it runs; a redelivered message finds the row and either leaves the question to the
consumer still running it or re-posts the stored answer. No model call is ever paid for twice.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "3f727cb28a14"
down_revision = "e6f7a8b9c0d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("request_id", sa.String(64), nullable=False),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("channel", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.UniqueConstraint("request_id", name="uq_agent_runs_request_id"),
    )
    op.create_index("ix_agent_runs_customer_code", "agent_runs", ["customer_code"])
    op.create_index("ix_agent_runs_customer_claimed", "agent_runs", ["customer_code", "claimed_at"])


def downgrade() -> None:
    op.drop_index("ix_agent_runs_customer_claimed", table_name="agent_runs")
    op.drop_index("ix_agent_runs_customer_code", table_name="agent_runs")
    op.drop_table("agent_runs")
