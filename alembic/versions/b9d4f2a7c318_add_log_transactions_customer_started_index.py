"""add composite index (customer_code, started_at DESC) on log_transactions

Revision ID: b9d4f2a7c318
Revises: a2c7e9d13f5b
Create Date: 2026-07-22

The transaction list + text feed both run
    WHERE customer_code = :c ORDER BY started_at DESC NULLS LAST LIMIT :n
(see app/api/v1/logs.py list_transactions / view_transactions). None of the existing indexes serve
this per-customer top-N-by-time query: the standalone ix_log_transactions_started_at is not scoped
to a customer, and the composites are (customer_code, date) and (customer_code, user_name). EXPLAIN
on tmp-live (223k transactions) showed a bitmap scan of ~143k matching rows followed by a sort of
~60k rows just to return 50, i.e. seconds per list load. This composite lets Postgres walk the newest
N rows for a customer directly (index scan, no sort), turning that into a ~N-row lookup.

Created CONCURRENTLY so it does not lock log_transactions on the live DB; CONCURRENTLY cannot run
inside a transaction, so use alembic's autocommit_block (same pattern as the enum migrations).
"""

from alembic import op

revision = "b9d4f2a7c318"
down_revision = "a2c7e9d13f5b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_log_transactions_customer_started "
            "ON log_transactions (customer_code, started_at DESC NULLS LAST)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_log_transactions_customer_started")
