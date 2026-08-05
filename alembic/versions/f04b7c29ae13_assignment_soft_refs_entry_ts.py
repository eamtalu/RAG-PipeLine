"""log_entry_assignment: soft references + entry_ts, ready for daily partitioning

Revision ID: f04b7c29ae13
Revises: e93c47a15b08
Create Date: 2026-08-05

Step 1 of docs/plan/2026-08-05_20-32_daily-partitioning.md. Three changes, each forced by something
measured against a real PostgreSQL instance rather than reasoned about.

1. DROP BOTH FOREIGN KEYS.
   A foreign key makes the referenced table's partitions impossible to remove:

       ALTER TABLE ... DETACH PARTITION  -> ERROR: violates foreign key constraint
       DROP TABLE <partition>            -> ERROR: other objects depend on it

   Retention IS dropping partitions, so the FKs and partitioning are mutually exclusive. Removing
   them also made the hottest write path ~4x faster - 200k assignment inserts went from 1,060 ms
   (two FK triggers, 200,000 calls each) to 249 ms - which stands on its own merits even if
   partitioning never happens.

   The cost: deletes no longer cascade. Every path that removes entries or transactions now deletes
   the matching assignments explicitly. Four such paths, each covered by a test in
   tests/test_assignment_soft_refs_chunk21.py.

2. ADD entry_ts, backfilled from log_entries.timestamp.
   The table has no time column, so there is nothing to partition it on. Denormalising the owning
   entry's timestamp means an assignment always lands in the same daily partition as its entry, so
   the pair can be dropped together. Nullable, because log_entries.timestamp is.

3. REPLACE THE PRIMARY KEY WITH UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts).
   Not a primary key: PostgreSQL silently forces PK columns to NOT NULL, and entry_ts must stay
   nullable. NULLS NOT DISTINCT (PG15+; production is 16.14) keeps the "one assignment per entry"
   guarantee for timestamp-less entries, which a plain UNIQUE would not - it treats two NULLs as
   different.

   COLUMN ORDER IS LOAD-BEARING. Measured on 300k rows, looking up by entry_id alone:

       (entry_id)             0.045 ms  index scan
       (entry_ts, entry_id)  10.8   ms  SEQUENTIAL SCAN
       (entry_id, entry_ts)   0.046 ms  index scan

   Three hot paths filter on entry_id with no time bound, so partition-key-first would be a 240x
   regression. The published codex design puts the partition key first; this deliberately does not.

The backfill is a single UPDATE joined on the entry id. The table is small relative to log_entries
(one row per grouped entry, and only for the retained window), so this is a bounded operation.
"""

import sqlalchemy as sa
from alembic import op

revision = "f04b7c29ae13"
down_revision = "e93c47a15b08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- 1. soft references -------------------------------------------------
    op.drop_constraint("log_entry_assignment_entry_id_fkey",
                       "log_entry_assignment", type_="foreignkey")
    op.drop_constraint("log_entry_assignment_transaction_id_fkey",
                       "log_entry_assignment", type_="foreignkey")

    # --- 2. the future partition key ----------------------------------------
    op.add_column("log_entry_assignment",
                  sa.Column("entry_ts", sa.DateTime(timezone=True), nullable=True))
    op.execute("""
        UPDATE log_entry_assignment a
        SET entry_ts = e.timestamp
        FROM log_entries e
        WHERE e.id = a.entry_id
    """)
    # Supports dropping a day's assignments alongside its entry partition, and the tenant+day
    # consistency checks retention runs before a drop.
    op.create_index("ix_log_entry_assignment_entry_ts", "log_entry_assignment", ["entry_ts"])

    # --- 3. constraint swap --------------------------------------------------
    # PK -> UNIQUE NULLS NOT DISTINCT, so entry_ts can stay nullable AND be in the key.
    op.drop_constraint("log_entry_assignment_pkey", "log_entry_assignment", type_="primary")
    op.execute("""
        ALTER TABLE log_entry_assignment
        ADD CONSTRAINT uq_log_entry_assignment_entry
        UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)
    """)


def downgrade() -> None:
    # Reverting the constraint requires entry_ts to be gone first (a PK cannot contain a nullable
    # column), so the order here is the mirror of upgrade().
    op.drop_constraint("uq_log_entry_assignment_entry",
                       "log_entry_assignment", type_="unique")
    op.drop_index("ix_log_entry_assignment_entry_ts", table_name="log_entry_assignment")
    op.drop_column("log_entry_assignment", "entry_ts")
    op.create_primary_key("log_entry_assignment_pkey", "log_entry_assignment", ["entry_id"])

    # Restoring the FKs will FAIL if any orphan rows accumulated while they were absent. That is the
    # point of the explicit deletes added alongside this migration; if it errors, find the orphans:
    #   SELECT count(*) FROM log_entry_assignment a
    #   LEFT JOIN log_entries e ON e.id = a.entry_id WHERE e.id IS NULL;
    op.create_foreign_key("log_entry_assignment_entry_id_fkey", "log_entry_assignment",
                          "log_entries", ["entry_id"], ["id"], ondelete="CASCADE")
    op.create_foreign_key("log_entry_assignment_transaction_id_fkey", "log_entry_assignment",
                          "log_transactions", ["transaction_id"], ["id"], ondelete="CASCADE")
