"""distinct_sketch on the three rollup tables: count-distinct as an additive role (chunk 88, part 4).

A HyperLogLog sketch, 4 KB, NULL for every measure that is not a `distinct`. Registers union with
`max`, so a month's distinct count folds from its days exactly like a sum folds from its parts. Added
to the partitioned parents; PostgreSQL propagates the column to every partition.

Revision ID: b7e5d2c94a18
Revises: a9d4e17c3b52
Create Date: 2026-09-12
"""

import sqlalchemy as sa
from alembic import op

revision = "b7e5d2c94a18"
down_revision = "a9d4e17c3b52"
branch_labels = None
depends_on = None

_TABLES = ("analytics_hourly_rollups", "analytics_daily_rollups", "analytics_monthly_rollups")


def upgrade() -> None:
    for table in _TABLES:
        op.add_column(table, sa.Column("distinct_sketch", sa.LargeBinary(), nullable=True))


def downgrade() -> None:
    for table in _TABLES:
        op.drop_column(table, "distinct_sketch")
