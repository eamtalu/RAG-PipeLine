"""Chunk 115: a `latest` role on every rollup grain.

Every aggregation so far answers a question about a SET of rows. A level needs a different one -
what does it say NOW - and nothing could ask it.

Measured on tmp-live. A picker works a delivery line in several goes; 178 of 3,278 lines were picked
in more than one. Delivery 25810, item 104526, line 4 has nine rows over two hours, and its expected
quantity reads 9, 9, 9, 6, 15, 15, 15, 15, 15: the expectation is stamped on every row and revised as
the work goes on. Summing it gives 108 for a line that expects 15. Across the tenant, summing expected
gives 26,447 where taking each line's expectation once gives 22,778, so a shortfall built from the two
sums reads -3,729 where the truth is about -60.

ONE JSONB column rather than a number and a timestamp, because the value and its clock must never be
merged apart: two columns could pair one hour's reading with another hour's instant. The merge is
"take the later", which is why this is the only role here that is not an addition.

Nullable with no backfill. A rollup row written before this chunk has no latest and never will; the
recompute-and-replace fills it the next time that bucket is folded.

Revision ID: a3d61f8c04e7
Revises: f5a92c17d8b3
Create Date: 2026-09-16
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "a3d61f8c04e7"
down_revision = "f5a92c17d8b3"
branch_labels = None
depends_on = None

_TABLES = ("analytics_hourly_rollups", "analytics_daily_rollups", "analytics_monthly_rollups")


def upgrade() -> None:
    for table in _TABLES:
        op.add_column(table, sa.Column("latest", JSONB(), nullable=True))


def downgrade() -> None:
    for table in _TABLES:
        op.drop_column(table, "latest")
