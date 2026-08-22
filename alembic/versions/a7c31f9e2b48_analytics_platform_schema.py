"""analytics platform schema (Phase 1)

Nine tables for the warehouse analytics platform. Cites
docs/analytics-ml-architecture/final_architecture.md, per that document's instruction that Alembic
migrations for this work name it.

The DDL is emitted verbatim rather than through `op.create_table` for one reason: five of these tables
are RANGE-partitioned, and PostgreSQL rejects a primary key that omits the partition key. Their identity
is a `UNIQUE NULLS NOT DISTINCT` containing that key instead, exactly as `log_transactions` does it, and
`primary_key=True` on the model is the ORM's row identity only. Generating the statements from the models
and checking them in keeps that asymmetry visible rather than buried in dialect kwargs.

PostgreSQL also requires such a constraint to contain EVERY partition-key column, which shaped two keys:

  - `analytics_facts` is keyed on (customer_code, source_transaction_id, event_time). That is F3's
    identity, not a weaker one: `event_time` holds the source transaction's `started_at`, which is the
    value F3 names. Keying on it also keeps the partition key in the predicate reads carry, so reads
    prune; keying on `source_started_at` would satisfy F3 literally and prune nothing.
  - `analytics_fact_ledger` includes `recorded_at`, which is functionally dependent on the revision --
    a given version was written once, at one instant -- so it neither weakens nor widens the key.

Every partitioned table gets a DEFAULT partition. That is not defensive: `analytics_facts.event_time` is
nullable because `log_transactions.started_at` is, and without DEFAULT an insert of a NULL key fails
outright and takes the batch with it.

The runway is then built through `partitioning.ensure_coverage`'s own helpers, so bounds and names come
from the single source of truth the worker uses. Each table is provisioned at ITS OWN grain: the fact
table and its ledger monthly, the hourly rollups daily, the daily rollups yearly.

Retention is declared in `log_partition_worker`: KEEP_FOREVER for `analytics_facts`,
`analytics_fact_ledger` and `analytics_daily_rollups`, whose raw source is dropped at 60 days so a
dropped partition here could not be rebuilt from anything; RETENTION_DAYS for the hourly rollups (90) and
the quality issues (365). A table registered as partitioned WITHOUT such a policy silently inherits the
log tables' 60 days, which for the fact table would mean losing it a month at a time.

Revision ID: a7c31f9e2b48
Revises: e4b28f5c9107
Create Date: 2026-08-22
"""
from datetime import date, timedelta

from alembic import op

from app.persistence import partitioning as pt

revision = "a7c31f9e2b48"
down_revision = "e4b28f5c9107"
branch_labels = None
depends_on = None

#: Emitted in order: each CREATE TABLE followed by its indexes. Generated from the models, so the
#: migration and the ORM cannot disagree about a column.
_DDL = (
    """CREATE TABLE analytics_pending_windows (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	job_id UUID, 
	range_start TIMESTAMP WITH TIME ZONE NOT NULL, 
	range_end TIMESTAMP WITH TIME ZONE NOT NULL, 
	consumed_at TIMESTAMP WITH TIME ZONE, 
	created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	attempts INTEGER DEFAULT '0' NOT NULL, 
	last_error TEXT, 
	last_attempt_at TIMESTAMP WITH TIME ZONE, 
	abandoned_at TIMESTAMP WITH TIME ZONE, 
	available_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL, 
	PRIMARY KEY (id)
)""",
    """CREATE INDEX ix_analytics_pending_customer_consumed ON analytics_pending_windows (customer_code, consumed_at)""",
    """CREATE INDEX ix_analytics_pending_due ON analytics_pending_windows (consumed_at, abandoned_at, available_at)""",
    """CREATE INDEX ix_analytics_pending_windows_customer_code ON analytics_pending_windows (customer_code)""",
    """CREATE INDEX ix_analytics_pending_windows_job_id ON analytics_pending_windows (job_id)""",
    """CREATE TABLE analytics_facts (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	source_transaction_id UUID NOT NULL, 
	source_started_at TIMESTAMP WITH TIME ZONE, 
	source_version_hash VARCHAR(64) NOT NULL, 
	revision BIGINT DEFAULT '1' NOT NULL, 
	event_time TIMESTAMP WITH TIME ZONE, 
	business_date DATE, 
	duration_ms INTEGER, 
	method VARCHAR(128), 
	transaction_name VARCHAR(128), 
	transaction_type VARCHAR(32), 
	status VARCHAR(16), 
	item_number VARCHAR(128), 
	lot_number VARCHAR(64), 
	order_number VARCHAR(64), 
	delivery_number VARCHAR(64), 
	warehouse VARCHAR(16), 
	warehouse_id VARCHAR(16), 
	from_location VARCHAR(32), 
	to_location VARCHAR(32), 
	user_name VARCHAR(64), 
	device_id VARCHAR(64), 
	device_name VARCHAR(64), 
	quantity NUMERIC(20, 6), 
	quantity_classification VARCHAR(16), 
	attributes JSONB DEFAULT '{}' NOT NULL, 
	
	CONSTRAINT uq_analytics_facts_source UNIQUE NULLS NOT DISTINCT (customer_code, source_transaction_id, event_time)
)
 PARTITION BY RANGE (event_time)""",
    """CREATE INDEX ix_analytics_facts_created ON analytics_facts (created_at)""",
    """CREATE INDEX ix_analytics_facts_customer_code ON analytics_facts (customer_code)""",
    """CREATE INDEX ix_analytics_facts_customer_date ON analytics_facts (customer_code, business_date)""",
    """CREATE INDEX ix_analytics_facts_customer_event ON analytics_facts (customer_code, event_time)""",
    """CREATE TABLE analytics_fact_ledger (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	recorded_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	reason TEXT, 
	source_transaction_id UUID NOT NULL, 
	source_started_at TIMESTAMP WITH TIME ZONE, 
	source_version_hash VARCHAR(64) NOT NULL, 
	revision BIGINT DEFAULT '1' NOT NULL, 
	event_time TIMESTAMP WITH TIME ZONE, 
	business_date DATE, 
	duration_ms INTEGER, 
	method VARCHAR(128), 
	transaction_name VARCHAR(128), 
	transaction_type VARCHAR(32), 
	status VARCHAR(16), 
	item_number VARCHAR(128), 
	lot_number VARCHAR(64), 
	order_number VARCHAR(64), 
	delivery_number VARCHAR(64), 
	warehouse VARCHAR(16), 
	warehouse_id VARCHAR(16), 
	from_location VARCHAR(32), 
	to_location VARCHAR(32), 
	user_name VARCHAR(64), 
	device_id VARCHAR(64), 
	device_name VARCHAR(64), 
	quantity NUMERIC(20, 6), 
	quantity_classification VARCHAR(16), 
	attributes JSONB DEFAULT '{}' NOT NULL, 
	
	CONSTRAINT uq_analytics_ledger_version UNIQUE NULLS NOT DISTINCT (customer_code, source_transaction_id, source_started_at, revision, recorded_at)
)
 PARTITION BY RANGE (recorded_at)""",
    """CREATE INDEX ix_analytics_fact_ledger_customer_code ON analytics_fact_ledger (customer_code)""",
    """CREATE INDEX ix_analytics_ledger_customer_recorded ON analytics_fact_ledger (customer_code, recorded_at)""",
    """CREATE INDEX ix_analytics_ledger_revision ON analytics_fact_ledger (customer_code, revision)""",
    """CREATE TABLE analytics_metrics (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	name VARCHAR(128) NOT NULL, 
	description TEXT, 
	dimensions JSONB DEFAULT '[]' NOT NULL, 
	measures JSONB DEFAULT '[]' NOT NULL, 
	filter JSONB DEFAULT '{}' NOT NULL, 
	grains JSONB DEFAULT '[]' NOT NULL, 
	status VARCHAR(16) DEFAULT 'draft' NOT NULL, 
	backfilled_through DATE, 
	created_by VARCHAR(128), 
	created_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_analytics_metrics_name UNIQUE (customer_code, name)
)""",
    """CREATE INDEX ix_analytics_metrics_customer_code ON analytics_metrics (customer_code)""",
    """CREATE INDEX ix_analytics_metrics_customer_status ON analytics_metrics (customer_code, status)""",
    """CREATE TABLE analytics_hourly_rollups (
	id UUID NOT NULL, 
	bucket_start TIMESTAMP WITH TIME ZONE NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	definition_id UUID NOT NULL, 
	measure_name VARCHAR(64) NOT NULL, 
	dim1 VARCHAR(128), 
	dim2 VARCHAR(128), 
	dim3 VARCHAR(128), 
	dim4 VARCHAR(128), 
	sum_value NUMERIC(30, 6), 
	count_value BIGINT, 
	sum_sq NUMERIC(40, 6), 
	min_value NUMERIC(30, 6), 
	max_value NUMERIC(30, 6), 
	histogram JSONB, 
	computed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	
	CONSTRAINT uq_analytics_hourly_bucket UNIQUE NULLS NOT DISTINCT (customer_code, definition_id, measure_name, bucket_start, dim1, dim2, dim3, dim4)
)
 PARTITION BY RANGE (bucket_start)""",
    """CREATE INDEX ix_analytics_hourly_read ON analytics_hourly_rollups (customer_code, definition_id, bucket_start)""",
    """CREATE INDEX ix_analytics_hourly_rollups_customer_code ON analytics_hourly_rollups (customer_code)""",
    """CREATE TABLE analytics_daily_rollups (
	id UUID NOT NULL, 
	business_date DATE NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	definition_id UUID NOT NULL, 
	measure_name VARCHAR(64) NOT NULL, 
	dim1 VARCHAR(128), 
	dim2 VARCHAR(128), 
	dim3 VARCHAR(128), 
	dim4 VARCHAR(128), 
	sum_value NUMERIC(30, 6), 
	count_value BIGINT, 
	sum_sq NUMERIC(40, 6), 
	min_value NUMERIC(30, 6), 
	max_value NUMERIC(30, 6), 
	histogram JSONB, 
	computed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	
	CONSTRAINT uq_analytics_daily_bucket UNIQUE NULLS NOT DISTINCT (customer_code, definition_id, measure_name, business_date, dim1, dim2, dim3, dim4)
)
 PARTITION BY RANGE (business_date)""",
    """CREATE INDEX ix_analytics_daily_read ON analytics_daily_rollups (customer_code, definition_id, business_date)""",
    """CREATE INDEX ix_analytics_daily_rollups_customer_code ON analytics_daily_rollups (customer_code)""",
    """CREATE TABLE analytics_monthly_rollups (
	id UUID NOT NULL, 
	month_start DATE NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	definition_id UUID NOT NULL, 
	measure_name VARCHAR(64) NOT NULL, 
	dim1 VARCHAR(128), 
	dim2 VARCHAR(128), 
	dim3 VARCHAR(128), 
	dim4 VARCHAR(128), 
	sum_value NUMERIC(30, 6), 
	count_value BIGINT, 
	sum_sq NUMERIC(40, 6), 
	min_value NUMERIC(30, 6), 
	max_value NUMERIC(30, 6), 
	histogram JSONB, 
	computed_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_analytics_monthly_bucket UNIQUE NULLS NOT DISTINCT (customer_code, definition_id, measure_name, month_start, dim1, dim2, dim3, dim4)
)""",
    """CREATE INDEX ix_analytics_monthly_read ON analytics_monthly_rollups (customer_code, definition_id, month_start)""",
    """CREATE INDEX ix_analytics_monthly_rollups_customer_code ON analytics_monthly_rollups (customer_code)""",
    """CREATE TABLE analytics_tenant_state (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	analytics_watermark TIMESTAMP WITH TIME ZONE, 
	source_watermark TIMESTAMP WITH TIME ZONE, 
	unsealed_share NUMERIC(6, 5), 
	oldest_unsealed_at TIMESTAMP WITH TIME ZONE, 
	open_tickets INTEGER DEFAULT '0' NOT NULL, 
	abandoned_tickets INTEGER DEFAULT '0' NOT NULL, 
	quarantined_rows BIGINT DEFAULT '0' NOT NULL, 
	facts_total BIGINT DEFAULT '0' NOT NULL, 
	revision BIGINT DEFAULT '0' NOT NULL, 
	last_cycle_at TIMESTAMP WITH TIME ZONE, 
	last_error TEXT, 
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_analytics_tenant_state_customer UNIQUE (customer_code)
)""",
    """CREATE INDEX ix_analytics_tenant_state_customer_code ON analytics_tenant_state (customer_code)""",
    """CREATE TABLE analytics_quality_issues (
	id UUID NOT NULL, 
	customer_code VARCHAR(64) NOT NULL, 
	detected_at TIMESTAMP WITH TIME ZONE NOT NULL, 
	source_transaction_id UUID, 
	source_started_at TIMESTAMP WITH TIME ZONE, 
	reason VARCHAR(64) NOT NULL, 
	detail TEXT, 
	observed JSONB DEFAULT '{}' NOT NULL
)
 PARTITION BY RANGE (detected_at)""",
    """CREATE INDEX ix_analytics_quality_customer_detected ON analytics_quality_issues (customer_code, detected_at)""",
    """CREATE INDEX ix_analytics_quality_issues_customer_code ON analytics_quality_issues (customer_code)""",
    """CREATE INDEX ix_analytics_quality_reason ON analytics_quality_issues (customer_code, reason)""",
)

#: The nine tables. Reversed for the downgrade.
_TABLES = (
    "analytics_pending_windows",
    "analytics_facts",
    "analytics_fact_ledger",
    "analytics_metrics",
    "analytics_hourly_rollups",
    "analytics_daily_rollups",
    "analytics_monthly_rollups",
    "analytics_tenant_state",
    "analytics_quality_issues",
)

#: Runway provisioned now: a month either side of today at every grain, which for a yearly table means
#: this year and the neighbouring ones. The partition worker extends it on its own schedule; this only
#: has to be enough that the first write cannot fail before the worker's first tick.
_RUNWAY_DAYS = 31


def upgrade() -> None:
    for statement in _DDL:
        op.execute(statement)

    # DEFAULT partitions FIRST, so a NULL key is insertable from the moment the table exists.
    for table in _TABLES:
        if table in pt.BY_TABLE:
            op.execute(pt.create_default_sql(table))

    # Then the dated runway, at each table's own grain, via the same helpers the worker uses.
    today = date.today()
    days = pt.days_between(today - timedelta(days=_RUNWAY_DAYS), today + timedelta(days=_RUNWAY_DAYS))
    for t in pt.PARTITIONED:
        if not t.table.startswith("analytics_"):
            continue
        for start in sorted({pt.period_start(t.grain, d) for d in days}):
            op.execute(pt.create_partition_sql(t.table, start))


def downgrade() -> None:
    # CASCADE takes the partitions with the parent; nothing outside this set references these tables.
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
