"""rollup rows and record facts carry the WMS company and warehouse

Revision ID: e1b6c4d8f0a5
Revises: d0a5b3c7e9f4
Create Date: 2026-11-26

Chunk 232 (step 54). A rollup row says which place it belongs to, so the pre-computed figures
can be limited by site. The three rollup tables are small (under 100,000 rows together on the
dev server), so adding the columns and swapping the unique keys is quick; the locks are taken
with a timeout all the same, because the fact writer touches these tables every cycle. Existing
rows keep an empty place until their buckets are refolded.
"""

import sqlalchemy as sa
from alembic import op

revision = "e1b6c4d8f0a5"
down_revision = "d0a5b3c7e9f4"
branch_labels = None
depends_on = None

#: table, unique key, bucket column
_ROLLUPS = (("analytics_hourly_rollups", "uq_analytics_hourly_bucket", "bucket_start"),
            ("analytics_daily_rollups", "uq_analytics_daily_bucket", "business_date"),
            ("analytics_monthly_rollups", "uq_analytics_monthly_bucket", "month_start"))
_DIMS = "dim1, dim2, dim3, dim4, dim5, dim6"


def _swap_key(table: str, key: str, bucket: str, place: str) -> None:
    op.drop_constraint(key, table, type_="unique")
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {key} UNIQUE NULLS NOT DISTINCT "
               f"(customer_code, definition_id, measure_name, {bucket}, {place}{_DIMS})")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    for table, key, bucket in _ROLLUPS:
        op.add_column(table, sa.Column("company", sa.String(16), nullable=True))
        op.add_column(table, sa.Column("warehouse", sa.String(64), nullable=True))
        _swap_key(table, key, bucket, "company, warehouse, ")
    op.add_column("analytics_record_facts", sa.Column("company", sa.String(16), nullable=True))
    op.add_column("analytics_record_facts", sa.Column("warehouse", sa.String(16), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.drop_column("analytics_record_facts", "warehouse")
    op.drop_column("analytics_record_facts", "company")
    for table, key, bucket in _ROLLUPS:
        _swap_key(table, key, bucket, "")
        op.drop_column(table, "warehouse")
        op.drop_column(table, "company")
