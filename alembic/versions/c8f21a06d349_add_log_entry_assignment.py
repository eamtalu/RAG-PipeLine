"""log_entry_assignment: move the grouping result off log_entries

Revision ID: c8f21a06d349
Revises: b4e17d92c8a3
Create Date: 2026-08-05

Stage 2 writes the grouping result back onto the raw table (transaction_id / seq) and clears it again
through an ON DELETE SET NULL cascade. transaction_id is indexed, so every rewrite touches the heap
AND the index, and the unsealed tail is regrouped repeatedly before it seals. Measured on production:

    log_entries: n_tup_upd = 105,838,123   n_tup_hot_upd = 162   -> 0.0% HOT
                 dead tuples 345,382 (15.3%)

~55 rewrites per row. Moving the assignment into its own table makes log_entries insert-only and
moves the churn to a small table designed to be replaced.

This migration is purely ADDITIVE - one new table and two indexes. It does not touch log_entries, so
there is no rewrite of the multi-GB heap, and nothing reads or writes the new table until the code
that uses it ships. Safe on its own.

Dropping log_entries.transaction_id / seq is deliberately NOT part of this migration. That is the only
step that rewrites the raw table, and it should ride with the partitioning pass which rewrites it
anyway.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c8f21a06d349"
down_revision = "b4e17d92c8a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "log_entry_assignment",
        # PK, not merely a FK: "at most one current assignment per entry" becomes a database
        # guarantee rather than something the writer has to remember.
        sa.Column("entry_id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("customer_code", sa.String(length=64), nullable=False),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False,
                  # clock_timestamp(), not now(): now() is transaction_timestamp(), so every row
                  # written by one regroup would share an identical stamp.
                  server_default=sa.text("clock_timestamp()")),

        # Deleting a transaction drops its assignments - what ON DELETE SET NULL used to do, without
        # touching the raw rows. Deleting an entry drops its assignment, which keeps the existing
        # purge chain intact: jobs -> entries -> assignments.
        sa.ForeignKeyConstraint(["entry_id"], ["log_entries.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["transaction_id"], ["log_transactions.id"], ondelete="CASCADE"),
    )

    # the hot read: "this transaction's entries, in order".
    op.create_index("ix_log_entry_assignment_txn", "log_entry_assignment",
                    ["transaction_id", "seq"])
    # tenant-scoped cleanup and consistency checks.
    op.create_index("ix_log_entry_assignment_customer", "log_entry_assignment", ["customer_code"])


def downgrade() -> None:
    op.drop_index("ix_log_entry_assignment_customer", table_name="log_entry_assignment")
    op.drop_index("ix_log_entry_assignment_txn", table_name="log_entry_assignment")
    op.drop_table("log_entry_assignment")
