"""analytics_metrics.source: transaction | record (18y, chunk 79).

The fold partitions definitions by their source table every cycle, so it is a promoted column;
server_default 'transaction' backfills every pre-R4b metric to exactly what it always was.

Revision ID: e7a92d4f1c58
Revises: d4f81b2c9e63
Create Date: 2026-08-28
"""

import sqlalchemy as sa
from alembic import op

revision = "e7a92d4f1c58"
down_revision = "d4f81b2c9e63"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_metrics",
                  sa.Column("source", sa.String(16), nullable=False,
                            server_default="transaction"))


def downgrade() -> None:
    op.drop_column("analytics_metrics", "source")
