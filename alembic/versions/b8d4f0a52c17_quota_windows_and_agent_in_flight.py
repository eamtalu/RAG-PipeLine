"""quota_windows and agent_in_flight: the agent's shared admission counters

Revision ID: b8d4f0a52c17
Revises: a7c3e9d41b02
Create Date: 2026-11-10

Step 16 of docs/esmart-agent-execution-plan.md. The gateway admits a run before any model call:
hourly counts per tenant and per person in quota_windows, the live runs in agent_in_flight, both
changed by one atomic operation under a per-tenant lock so every worker sees one number.
"""

import sqlalchemy as sa
from alembic import op

revision = "b8d4f0a52c17"
down_revision = "a7c3e9d41b02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "quota_windows",
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("user_id", sa.String(128), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used", sa.Integer(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("customer_code", "user_id", "window_start"),
    )
    op.create_table(
        "agent_in_flight",
        sa.Column("trace_id", sa.String(64), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_agent_in_flight_customer_deadline", "agent_in_flight", ["customer_code", "deadline_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_agent_in_flight_customer_deadline", table_name="agent_in_flight")
    op.drop_table("agent_in_flight")
    op.drop_table("quota_windows")
