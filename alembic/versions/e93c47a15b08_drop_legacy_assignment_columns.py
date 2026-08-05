"""log_entries: drop the legacy transaction_id / seq columns

Revision ID: e93c47a15b08
Revises: d5b830e14f72
Create Date: 2026-08-05

The current assignment lives in log_entry_assignment. These two columns and their index are the last
remnants of the write amplification that took the box down:

    log_entries: n_tup_upd = 105,838,123  n_tup_hot_upd = 162  -> 0.0% HOT

Leaving them would keep the door open — anything writing them silently reintroduces the churn, and
the index on transaction_id is maintained on every INSERT for a column nothing reads.

Cost. DROP COLUMN in PostgreSQL is a CATALOG operation, not a rewrite. Measured on a 48 MB table
before writing this migration: 0.1s, relfilenode unchanged, size unchanged. The heap keeps the old
values until each row is next rewritten, and the space comes back through normal vacuum — so this is
safe even on the multi-GB production table.

An earlier plan document claimed this step "rewrites the raw table" and deferred it to the
partitioning pass. That was wrong, and measurement corrected it.

The lock IS an ACCESS EXCLUSIVE for the duration, so it briefly blocks readers of log_entries;
at 0.1s that is a blip, not an outage.

Irreversible in practice: downgrade recreates the columns EMPTY. The data they held was a duplicate
of log_entry_assignment, which remains the source of truth, so nothing is lost - but a downgrade
cannot repopulate them and no code reads them anyway.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e93c47a15b08"
down_revision = "d5b830e14f72"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # the index goes with the column, but drop it explicitly so the intent is on the record
    op.drop_index("ix_log_entries_transaction_id", table_name="log_entries", if_exists=True)
    op.drop_column("log_entries", "transaction_id")
    op.drop_column("log_entries", "seq")


def downgrade() -> None:
    # Recreated EMPTY: log_entry_assignment holds the real assignment and this migration cannot
    # reverse-derive per-row values without rewriting the table it was designed not to touch.
    op.add_column("log_entries", sa.Column("seq", sa.Integer(), nullable=True))
    op.add_column("log_entries",
                  sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_log_entries_transaction_id", "log_entries", ["transaction_id"])
