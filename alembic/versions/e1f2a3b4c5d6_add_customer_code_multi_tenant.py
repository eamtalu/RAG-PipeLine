"""add customer_code (multi-tenant log segregation) + per-customer dedup

Revision ID: e1f2a3b4c5d6
Revises: d4e7a1b9c206
Create Date: 2026-06-12

Each customer runs a different M3 WMS deployment; their logs must be physically segregated. We
denormalize customer_code onto jobs, log_entries and log_transactions so every query, the Stage 2
grouper, and the debugging agent stay scoped to one customer. Two correctness points:

  - Stage 2 keys open transactions by (thread, user_ctx); .NET thread ids (e.g. [94]) collide across
    customers, so grouping must be partitioned by customer (handled in code). customer_code on
    log_entries / log_transactions is what makes that partition cheap.
  - The content-dedup unique key moves from (entry_hash) to (customer_code, entry_hash): two customers
    can emit an identical line, and a global unique would silently drop the second one.

Existing rows are backfilled to settings.default_customer_code, then the column is made NOT NULL.
"""

from alembic import op
import sqlalchemy as sa

from app.settings import settings

revision = "e1f2a3b4c5d6"
down_revision = "d4e7a1b9c206"
branch_labels = None
depends_on = None

_TABLES = ("jobs", "log_entries", "log_transactions")
_DEFAULT = settings.default_customer_code


def upgrade() -> None:
    # 1. add nullable, 2. backfill, 3. enforce NOT NULL, + per-table index
    for table in _TABLES:
        op.add_column(table, sa.Column("customer_code", sa.String(64), nullable=True))
        op.execute(
            sa.text(f"UPDATE {table} SET customer_code = :code WHERE customer_code IS NULL")
            .bindparams(code=_DEFAULT)
        )
        op.alter_column(table, "customer_code", nullable=False)
        op.create_index(f"ix_{table}_customer_code", table, ["customer_code"])

    # 4. swap the global entry_hash unique index for a per-customer composite unique
    op.drop_index("uq_log_entries_entry_hash", table_name="log_entries")
    op.create_index("ix_log_entries_entry_hash", "log_entries", ["entry_hash"])  # keep plain lookups
    op.create_index(
        "uq_log_entries_customer_hash", "log_entries", ["customer_code", "entry_hash"], unique=True
    )

    # 5. composite indexes for the common tenant-scoped transaction filters
    op.create_index("ix_log_transactions_customer_date", "log_transactions", ["customer_code", "date"])
    op.create_index("ix_log_transactions_customer_user", "log_transactions", ["customer_code", "user_name"])


def downgrade() -> None:
    op.drop_index("ix_log_transactions_customer_user", table_name="log_transactions")
    op.drop_index("ix_log_transactions_customer_date", table_name="log_transactions")

    op.drop_index("uq_log_entries_customer_hash", table_name="log_entries")
    op.drop_index("ix_log_entries_entry_hash", table_name="log_entries")
    op.create_index("uq_log_entries_entry_hash", "log_entries", ["entry_hash"], unique=True)

    for table in _TABLES:
        op.drop_index(f"ix_{table}_customer_code", table_name=table)
        op.drop_column(table, "customer_code")
