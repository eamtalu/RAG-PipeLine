"""deliveries at risk: rows the backfill reconstructed from the logs

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-10-03

A reconstructed row was written after the fact from the clocks in the logs, not watched minute by
minute. It fills the history and teaches the route profiles, but the accuracy score leaves it out.
"""

import sqlalchemy as sa
from alembic import op

revision = "c4d5e6f7a8b9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_deliveries",
                  sa.Column("reconstructed", sa.Boolean(), nullable=False, server_default=sa.text("false")))


def downgrade() -> None:
    op.drop_column("analytics_at_risk_deliveries", "reconstructed")
