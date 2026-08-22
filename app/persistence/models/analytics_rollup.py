# analytics_rollup.py — the grain cascade: facts -> hourly -> daily -> monthly.
#
#   Three tables, one file, because they are one structure at three resolutions. Each level reads only
#   the level below, so the fact table is scanned once per cycle rather than once per grain.
#
#   TWO DECISIONS ARE LOAD-BEARING HERE.
#
#   *Generic storage.* A definition identifier, a fixed number of dimension slots, and additive measure
#   slots — not a bespoke table per metric. Adding a metric is a row plus a backfill, never a migration,
#   which is the whole point of the registry.
#
#   *The measure slots are named for their additive ROLE, not numbered.* `sum_value`, `count_value`,
#   `sum_sq`, `min_value`, `max_value`, `histogram` is the complete set of additive primitives, fixed by
#   the composition table in section 11: sums and counts direct, averages as sum+count, variance as
#   sum/sum_sq/count, percentiles as a 20-bucket log histogram, first and last as min/max.
#
#   Naming them makes invariant 8 STRUCTURAL rather than conventional. There is no column an average
#   could be written into, so "a rollup stores additive components, never finished answers" stops being
#   a rule someone has to catch in review and becomes unrepresentable. Averaging twelve monthly averages
#   is not the yearly average, and this schema cannot be asked to try.
#
#   The cost, chosen deliberately (correction log C5): a definition needing one sum and two counts
#   cannot share one set of role columns, so a row is keyed per (definition, MEASURE, dimensions,
#   bucket). Consumption emits three rows per bucket instead of one — about 3x on the hourly table.
#
#   Folding is then one uniform operation across every level and every metric: sums add, counts add,
#   mins take the min, maxes take the max, histograms add element-wise. Nothing consults which measure
#   it is looking at, which is the "registry, not an if-chain" requirement satisfied by the schema.

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import (BigInteger, Date, DateTime, Index, Numeric, String, UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

#: How many dimension slots a rollup row carries.
#:
#: Four, not more: consumption uses two (method, transaction_name), and a metric sliced by more than
#: four dimensions has cardinality that defeats pre-aggregation anyway — the doc's own "honest limit",
#: where genuinely ad-hoc exploration falls back to a bounded fact-table scan instead. Four leaves room
#: for method + transaction_name + item + warehouse, which is the widest chart worth pre-computing.
#:
#: Slots rather than JSONB because a dimension is what every query GROUPS BY and filters on, and a
#: JSONB key cannot carry a composite btree index that serves both.
DIMENSION_SLOTS = 4


class RollupColumns:
    """Everything the three grains share. A mixin, so a level cannot drift from its neighbours: the
    cascade only works if each level stores exactly what the level above needs to fold."""

    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    #: Which definition this row belongs to. Not a foreign key: `analytics_metrics` is unpartitioned
    #: while these tables are partitioned, and a FK from a partitioned child made the log partitions
    #: undroppable once already (see log_entry_assignment's docstring).
    definition_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    #: Which of the definition's measures. Part of the key, per C5.
    measure_name: Mapped[str] = mapped_column(String(64), nullable=False)

    # --- dimension slots ---
    # Positional, interpreted through the definition's `dimensions` list. A row is meaningless without
    # its definition, which is why definition_id is in every index that matters.
    dim1: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dim2: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dim3: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dim4: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # --- additive roles, and ONLY additive roles ---
    sum_value: Mapped[object | None] = mapped_column(Numeric(30, 6), nullable=True)
    count_value: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    sum_sq: Mapped[object | None] = mapped_column(Numeric(40, 6), nullable=True)
    min_value: Mapped[object | None] = mapped_column(Numeric(30, 6), nullable=True)
    max_value: Mapped[object | None] = mapped_column(Numeric(30, 6), nullable=True)
    #: 20-bucket log histogram. JSONB because bucket counts ADD, which is the only reason percentiles
    #: are storable at all: no hll or tdigest extension is available on this server.
    histogram: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    #: Every write is recompute-and-replace, never increment — an additive upsert double-counts on the
    #: first retry. This records when the replacement happened, so a stale level is visible.
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


# Entity
class AnalyticsHourlyRollup(RollupColumns, Base):
    """Hourly buckets. Retained 90 days: the shortest-lived level, because a request older than that
    resolves to daily anyway."""

    __tablename__ = "analytics_hourly_rollups"

    __table_args__ = (
        UniqueConstraint("customer_code", "definition_id", "measure_name", "bucket_start",
                         "dim1", "dim2", "dim3", "dim4",
                         name="uq_analytics_hourly_bucket", postgresql_nulls_not_distinct=True),
        Index("ix_analytics_hourly_read", "customer_code", "definition_id", "bucket_start"),
        # Cut DAILY though the bucket is hourly: a day's worth of hourly rows is the unit retention
        # drops, and 24 rows per key per day makes a daily partition the right size.
        {"postgresql_partition_by": "RANGE (bucket_start)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: Start of the hour, UTC. Partition key, so NOT NULL: a fact with no event_time cannot be placed in
    #: an hour and is excluded from this level rather than bucketed into a DEFAULT partition where
    #: retention could never reclaim it.
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


# Entity
class AnalyticsDailyRollup(RollupColumns, Base):
    """Daily buckets on the tenant-LOCAL business date. Kept forever.

    Local, not UTC: `business_date` is what an operator means by "yesterday", and for a UK warehouse the
    two diverge by an hour for half the year. Weekly derives from here at read time using ISO Monday
    weeks, which is why there is no weekly table.
    """

    __tablename__ = "analytics_daily_rollups"

    __table_args__ = (
        UniqueConstraint("customer_code", "definition_id", "measure_name", "business_date",
                         "dim1", "dim2", "dim3", "dim4",
                         name="uq_analytics_daily_bucket", postgresql_nulls_not_distinct=True),
        Index("ix_analytics_daily_read", "customer_code", "definition_id", "business_date"),
        # YEARLY partitions: kept forever, and a year of daily rows per key is small.
        {"postgresql_partition_by": "RANGE (business_date)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_date: Mapped[date_type] = mapped_column(Date, nullable=False)


# Entity
class AnalyticsMonthlyRollup(RollupColumns, Base):
    """Monthly buckets. Kept forever and deliberately NOT partitioned: roughly 300K rows over five
    years, so there is nothing worth pruning and partitioning would add planning cost for no gain."""

    __tablename__ = "analytics_monthly_rollups"

    __table_args__ = (
        UniqueConstraint("customer_code", "definition_id", "measure_name", "month_start",
                         "dim1", "dim2", "dim3", "dim4",
                         name="uq_analytics_monthly_bucket", postgresql_nulls_not_distinct=True),
        Index("ix_analytics_monthly_read", "customer_code", "definition_id", "month_start"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    #: First day of the month, on the tenant-local business date, so it composes from daily exactly.
    month_start: Mapped[date_type] = mapped_column(Date, nullable=False)
