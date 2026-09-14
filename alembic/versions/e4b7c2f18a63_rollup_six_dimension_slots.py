"""Chunk 102: six rollup dimension slots instead of four.

What decides whether pre-aggregation is worth anything is the CARDINALITY of the dimensions, not how
many there are. Measured over 9,569 live facts at day grain: warehouse and user store 0.004 rows per
fact (245x), the same plus transaction and status 0.015 (68x), and SIX low-cardinality dimensions
still only 0.032 (31x). Four was therefore the only thing preventing a genuinely useful wide, shallow
cube.

It buys nothing for high cardinality - warehouse, user, item and lot is already 0.308 rows per fact,
and adding slots does not change that - which is why six and not more.

The unique constraints have to be rebuilt because the slots are part of the key. They keep
NULLS NOT DISTINCT: an unset slot is a value, not an unknown, or every partial-dimension row would be
distinct from every other and the recompute-and-replace would append instead of replacing.

Revision ID: e4b7c2f18a63
Revises: c1a83e7d5f92
Create Date: 2026-09-14
"""

import sqlalchemy as sa
from alembic import op

revision = "e4b7c2f18a63"
down_revision = "c1a83e7d5f92"
branch_labels = None
depends_on = None

_TABLES = {
    "analytics_hourly_rollups": ("uq_analytics_hourly_bucket", "bucket_start"),
    "analytics_daily_rollups": ("uq_analytics_daily_bucket", "business_date"),
    "analytics_monthly_rollups": ("uq_analytics_monthly_bucket", "month_start"),
}
_OLD = ("dim1", "dim2", "dim3", "dim4")
_NEW = _OLD + ("dim5", "dim6")


def upgrade() -> None:
    for table, (constraint, bucket) in _TABLES.items():
        op.add_column(table, sa.Column("dim5", sa.String(128), nullable=True))
        op.add_column(table, sa.Column("dim6", sa.String(128), nullable=True))
        op.drop_constraint(constraint, table, type_="unique")
        op.create_unique_constraint(
            constraint, table,
            ["customer_code", "definition_id", "measure_name", bucket, *_NEW],
            postgresql_nulls_not_distinct=True)


def downgrade() -> None:
    for table, (constraint, bucket) in _TABLES.items():
        op.drop_constraint(constraint, table, type_="unique")
        op.create_unique_constraint(
            constraint, table,
            ["customer_code", "definition_id", "measure_name", bucket, *_OLD],
            postgresql_nulls_not_distinct=True)
        op.drop_column(table, "dim6")
        op.drop_column(table, "dim5")
