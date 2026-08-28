"""Record capture hardening (18x, chunk 78).

- `analytics_tenant_state.record_facts_total`: the record grain's volume counter, moved
  incrementally by the fold exactly as `facts_total` is.
- Composite indexes on `analytics_record_facts`: the fold's dirty-bucket reads scan by tenant +
  time on both axes, the presence diff probes by tenant + window, and the registry console counts
  a name's records. Plain CREATE INDEX: a partitioned parent cannot be indexed CONCURRENTLY, the
  table is empty everywhere (expand has never been on), and migrations run with the worker stopped.

Revision ID: d4f81b2c9e63
Revises: c8d24e6f1a97
Create Date: 2026-08-28
"""

import sqlalchemy as sa
from alembic import op

revision = "d4f81b2c9e63"
down_revision = "c8d24e6f1a97"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_tenant_state",
                  sa.Column("record_facts_total", sa.BigInteger(), nullable=False,
                            server_default="0"))
    op.create_index("ix_analytics_record_facts_customer_event", "analytics_record_facts",
                    ["customer_code", "event_time"])
    op.create_index("ix_analytics_record_facts_customer_date", "analytics_record_facts",
                    ["customer_code", "business_date"])
    op.create_index("ix_analytics_record_facts_customer_txn_event", "analytics_record_facts",
                    ["customer_code", "transaction_name", "event_time"])


def downgrade() -> None:
    op.drop_index("ix_analytics_record_facts_customer_txn_event",
                  table_name="analytics_record_facts")
    op.drop_index("ix_analytics_record_facts_customer_date", table_name="analytics_record_facts")
    op.drop_index("ix_analytics_record_facts_customer_event", table_name="analytics_record_facts")
    op.drop_column("analytics_tenant_state", "record_facts_total")
