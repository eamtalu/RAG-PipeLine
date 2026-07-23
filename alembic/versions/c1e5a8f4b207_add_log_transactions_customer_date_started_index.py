"""add composite index (customer_code, date, started_at) on log_transactions

Revision ID: c1e5a8f4b207
Revises: b9d4f2a7c318
Create Date: 2026-07-23

The paginated day-scoped feed runs
    WHERE customer_code = ? AND date = ? ORDER BY started_at ASC LIMIT n OFFSET m
(see app/api/v1/logs.py view_transactions). Without a matching index Postgres either scans the wrong
index (page 1 ~1s) or sorts the whole day on disk for deep offsets (external merge). This composite
matches the filter prefix (customer_code, date) and the sort key (started_at), so pages become an
ordered index range scan with no sort and OFFSET skips within the index at any depth. Its
(customer_code, date) prefix also serves the pager's COUNT and makes ix_log_transactions_customer_date
redundant (left in place; optional cleanup later).

Created CONCURRENTLY so it does not lock log_transactions on the live DB; CONCURRENTLY cannot run
inside a transaction, so use alembic's autocommit_block (same pattern as b9d4f2a7c318). Note:
CREATE INDEX CONCURRENTLY blocks on any long-lived `idle in transaction` session; the worker singleton
now commits (app/worker.py) so it is not one, but check pg_stat_activity before deploying.
"""

from alembic import op

revision = "c1e5a8f4b207"
down_revision = "b9d4f2a7c318"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_log_transactions_customer_date_started "
            "ON log_transactions (customer_code, date, started_at)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_log_transactions_customer_date_started")
