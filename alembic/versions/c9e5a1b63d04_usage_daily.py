"""usage_daily: the agent's model usage summed per tenant, UTC day, provider and model

Revision ID: c9e5a1b63d04
Revises: b8d4f0a52c17
Create Date: 2026-11-10

Step 19 of docs/esmart-agent-execution-plan.md. The gateway adds every run's model calls here
after the run: calls, failed calls, tokens, latency and the cost priced from the operator's list.
"""

import sqlalchemy as sa
from alembic import op

revision = "c9e5a1b63d04"
down_revision = "b8d4f0a52c17"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usage_daily",
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("calls", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_calls", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("input_tokens", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("output_tokens", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("cached_tokens", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("cost_usd", sa.Numeric(14, 6), nullable=True),
        sa.Column("latency_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("customer_code", "day", "provider", "model"),
    )


def downgrade() -> None:
    op.drop_table("usage_daily")
