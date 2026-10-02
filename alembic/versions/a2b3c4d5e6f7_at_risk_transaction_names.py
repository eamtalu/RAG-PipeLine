"""deliveries at risk: remember which picking screens a delivery went through

Revision ID: a2b3c4d5e6f7
Revises: f1c2d3e4a5b6
Create Date: 2026-10-02

A supervisor reviewing the month wants to look at one kind of picking at a time (Brighton Stock
Pick, JIT and Shorts, Milk, Freezer). The pick lines know their screen; the delivery row now keeps
the distinct set, so the history can filter on it without re-reading the pick lines, which are
dropped with the raw logs after 60 days. A delivery that spans two kinds appears under both.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a2b3c4d5e6f7"
down_revision = "f1c2d3e4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_deliveries",
                  sa.Column("transaction_names", postgresql.JSONB(), nullable=False, server_default="[]"))
    with op.get_context().autocommit_block():
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_deliveries_txn "
                   "ON analytics_at_risk_deliveries USING gin (transaction_names)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_at_risk_deliveries_txn")
    op.drop_column("analytics_at_risk_deliveries", "transaction_names")
