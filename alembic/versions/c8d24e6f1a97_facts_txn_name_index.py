"""analytics_facts: index for per-transaction fact counts (chunk 77).

The registry console's detail endpoint answers "how many facts has this transaction produced";
without an index that is a full per-partition scan of the tenant's facts. Composite leads with
`customer_code` (this is a multi-tenant table), then `transaction_name`, then `event_time` so the
min/max span read is index-only too.

Plain CREATE INDEX rather than CONCURRENTLY, deliberately: `analytics_facts` is a partitioned
parent and PostgreSQL cannot build a partitioned index concurrently. The table holds ~200k rows and
migrations run with the worker stopped, so the brief lock is cheaper than the per-partition
build-and-attach dance CONCURRENTLY would require.

Revision ID: c8d24e6f1a97
Revises: b5e19f7c3a84
Create Date: 2026-08-28
"""

from alembic import op

revision = "c8d24e6f1a97"
down_revision = "b5e19f7c3a84"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_analytics_facts_customer_txn_event", "analytics_facts",
                    ["customer_code", "transaction_name", "event_time"])


def downgrade() -> None:
    op.drop_index("ix_analytics_facts_customer_txn_event", table_name="analytics_facts")
