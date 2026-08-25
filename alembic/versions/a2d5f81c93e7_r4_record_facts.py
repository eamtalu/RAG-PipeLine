"""R4: analytics_record_facts

Cites docs/analytics-ml-architecture/final_architecture.md sections 18a (the open decision) and 18d.

A SEPARATE TABLE rather than a second row type in `analytics_facts`, and the decision was measured
rather than argued: `_read_dirty_facts` selects the whole table with no grain predicate and
`group_fold` has no notion of grain, so record rows fold into the same buckets as their parent. One
transaction plus three of its records inflated the seed definition's quantity from 10 to 40 - 4x,
silently. Avoiding that would need every definition to carry a grain filter, and forgetting one
produces a plausible-looking wrong total.

Partitioned MONTHLY on `event_time` and KEEP_FOREVER, matching `analytics_facts`: the reason to capture
a record is a question somebody asks next year, and raw entries are gone in 60 days.

Registered in `partitioning.PARTITIONED` and in `log_partition_worker.KEEP_FOREVER` in the same change.
A partitioned table registered in neither retention collection silently inherits the log tables' 60
days, which for a keep-forever table would mean the worker dropping the thing nothing can rebuild - and
a test asserts that cannot happen.
"""

from alembic import op
import sqlalchemy as sa

revision = "a2d5f81c93e7"
down_revision = "f4c82e9b6d31"
branch_labels = None
depends_on = None

TABLE = "analytics_record_facts"


def upgrade() -> None:
    op.execute(f"""
        CREATE TABLE {TABLE} (
            id UUID NOT NULL,
            customer_code VARCHAR(64) NOT NULL,
            source_transaction_id UUID NOT NULL,
            source_started_at TIMESTAMPTZ,
            record_index INTEGER NOT NULL,
            event_time TIMESTAMPTZ,
            business_date DATE,
            method VARCHAR(128),
            transaction_name VARCHAR(128),
            mi_program VARCHAR(64),
            mi_transaction VARCHAR(64),
            attributes JSONB NOT NULL DEFAULT '{{}}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_analytics_record_facts_id
                UNIQUE NULLS NOT DISTINCT (source_transaction_id, record_index, event_time)
        ) PARTITION BY RANGE (event_time)
    """)
    op.execute(f"CREATE INDEX ix_{TABLE}_customer_code ON {TABLE} (customer_code)")
    op.execute(f"CREATE INDEX ix_{TABLE}_business_date ON {TABLE} (business_date)")
    op.execute(f"CREATE INDEX ix_{TABLE}_method ON {TABLE} (method)")
    op.execute(f"CREATE INDEX ix_{TABLE}_transaction_name ON {TABLE} (transaction_name)")
    # A DEFAULT partition, matching every other partitioned analytics table: a record whose parent has
    # no parsable timestamp has a NULL event_time and must still land somewhere.
    op.execute(f"CREATE TABLE {TABLE}_default PARTITION OF {TABLE} DEFAULT")


def downgrade() -> None:
    op.execute(f"DROP TABLE IF EXISTS {TABLE}")
