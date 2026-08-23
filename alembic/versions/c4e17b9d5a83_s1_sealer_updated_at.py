"""S1: log_transactions.updated_at, the cursor index, and the sealer's partial index

Cites docs/analytics-ml-architecture/final_architecture.md section 18 (S1).

WHY updated_at EXISTS
---------------------
S1 makes sealing an explicit UPDATE instead of a side effect of re-insertion. An UPDATE does not
refresh `created_at`, and the notification cursor only ever moves forward from its stored position, so
a row sealed by the sealer would never re-enter the feed and `stability.py`'s `incomplete AND sealed`
alert could never fire. The cursor reads `updated_at` instead.

BACKFILLED TO created_at, NOT TO now()
--------------------------------------
`ADD COLUMN ... NOT NULL DEFAULT now()` would be instant (PG11+ stores the default in the catalog with
no rewrite) and is the wrong thing. Every existing row would report "written at migration time", so
every notification rule would see its entire retained history as brand new. Dedup would absorb the
alerts, but each rule would first scan ~500k rows it has already processed.

Backfilling to `created_at` means a row nothing ever updates behaves exactly as it did before the
column existed, which is the property that makes this migration boring.

Done in three steps so the NOT NULL is safe: add nullable, backfill per partition, then set NOT NULL.
Per partition rather than one statement because the parent is partitioned 95 ways - one UPDATE would
hold a lock across all of them for the duration; per partition keeps each lock short and lets the work
be interrupted and resumed. A `server_default` is set as well, so a concurrent insert during the
migration cannot produce the NULL that the final step would then reject.

THE TWO INDEXES
---------------
Both follow migration b3d914c7ea52's recipe, because `CREATE INDEX CONCURRENTLY` is not supported on a
partitioned parent: `CREATE INDEX ON ONLY parent` registers an INVALID parent index (metadata only),
then each partition is built CONCURRENTLY and ATTACHed, and PostgreSQL marks the parent valid once all
are attached. Partitions created later by the partition worker build their local indexes automatically
from the parent definition.

  ix_log_transactions_customer_updated  (customer_code, updated_at)
      The cursor's window query is `customer_code = ? AND updated_at >= ? AND < ? ORDER BY
      updated_at`, so the index must serve both the filter and the sort (CLAUDE.md rule 4). Before S1
      that query ran on `created_at`, which HAS a single-column index (b3d914c7ea52) but no composite
      one - fast only because the feed is small and recent.

  ix_log_transactions_unsealed  (customer_code, ended_at) WHERE NOT sealed
      The sealer's tenant enumeration and its UPDATE both filter exactly this. `NOT sealed` is 2.1% of
      rows, so the partial index is roughly fifty times smaller than a full one, and without it every
      tick is a full scan of a 60-day partition set.

NOT DONE HERE, deliberately: `ix_log_transactions_created_at` is now unused by the cursor and is left
in place. The index redesign is deferred in section 18 pending pg_stat_statements with the analytics
and reconcile workers ENABLED - the earlier "never scanned" measurement was taken with them off, and
that is exactly the mistake this document already records once.
"""

import sqlalchemy as sa
from alembic import op

revision = "c4e17b9d5a83"
down_revision = "f1a92d3b7c60"
branch_labels = None
depends_on = None

PARENT = "log_transactions"
CURSOR_INDEX = "ix_log_transactions_customer_updated"
UNSEALED_INDEX = "ix_log_transactions_unsealed"


def _partitions(conn) -> list[str]:
    """Every leaf partition of the parent, DEFAULT included.

    Read from `pg_inherits` rather than derived from partition names: the name is a convention, the
    inheritance link is the truth, and a partition missed here would leave a parent index invalid
    forever.
    """
    return list(conn.execute(sa.text("""
        SELECT c.relname FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = CAST(:parent AS regclass)
        ORDER BY c.relname
    """), {"parent": PARENT}).scalars().all())


def upgrade() -> None:
    conn = op.get_bind()
    parts = _partitions(conn)

    # ---- 1. the column, nullable and WITH NO DEFAULT.
    #
    # The default is deliberately withheld until step 4, and the first version of this migration got
    # it wrong in a way worth recording. `ADD COLUMN ... DEFAULT now()` does not leave existing rows
    # NULL: PostgreSQL evaluates the default once and stores it as the column's "missing value" for
    # every pre-existing row. The backfill in step 2 was written `WHERE updated_at IS NULL`, so it
    # matched nothing, and all 397 local rows ended up holding one identical timestamp 62 days away
    # from their own `created_at`.
    #
    # That is precisely the outcome this migration exists to avoid. Since the notification cursor now
    # reads this column, every rule would have seen its entire retained history as newly written and
    # rescanned it. Add nullable, backfill, then constrain.
    op.execute(f"ALTER TABLE {PARENT} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")

    # ---- 2. backfill per partition, so no single statement locks all 95 at once.
    #
    # Unconditional rather than `WHERE updated_at IS NULL`: the condition is what hid the bug above,
    # and an unconditional assignment is idempotent here anyway because the source column never moves.
    for part in parts:
        op.execute(f"UPDATE {part} SET updated_at = created_at")

    # ---- 3. now it can be NOT NULL. The cursor's range filter and ORDER BY would silently drop a
    #         NULL row from the feed, which is the one failure notifications/cursor.py forbids.
    op.execute(f"ALTER TABLE {PARENT} ALTER COLUMN updated_at SET NOT NULL")

    # ---- 4. the default, LAST, so it applies only to rows inserted from now on. Belt and braces:
    #         `_write_transaction` stamps the column explicitly, and this means a future insert path
    #         that forgets to cannot produce the NULL that step 3 forbids.
    op.execute(f"ALTER TABLE {PARENT} ALTER COLUMN updated_at SET DEFAULT now()")

    # ---- 4. the two indexes, ON ONLY then per-partition CONCURRENTLY then ATTACH.
    op.execute(f"CREATE INDEX IF NOT EXISTS {CURSOR_INDEX} ON ONLY {PARENT} "
               f"(customer_code, updated_at)")
    op.execute(f"CREATE INDEX IF NOT EXISTS {UNSEALED_INDEX} ON ONLY {PARENT} "
               f"(customer_code, ended_at) WHERE NOT sealed")

    with op.get_context().autocommit_block():
        for part in parts:
            cur_child = f"{part}_customer_updated_idx"
            op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {cur_child} "
                       f"ON {part} (customer_code, updated_at)")
            op.execute(f"ALTER INDEX {CURSOR_INDEX} ATTACH PARTITION {cur_child}")

            uns_child = f"{part}_unsealed_idx"
            op.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {uns_child} "
                       f"ON {part} (customer_code, ended_at) WHERE NOT sealed")
            op.execute(f"ALTER INDEX {UNSEALED_INDEX} ATTACH PARTITION {uns_child}")


def downgrade() -> None:
    # Dropping a parent index cascades to its attached partition indexes, so the children need no
    # separate DROP. Not CONCURRENTLY: a partitioned index cannot be dropped concurrently.
    op.execute(f"DROP INDEX IF EXISTS {UNSEALED_INDEX}")
    op.execute(f"DROP INDEX IF EXISTS {CURSOR_INDEX}")
    # The column goes last: the cursor reads it, so it must outlive the indexes that serve it.
    op.execute(f"ALTER TABLE {PARENT} DROP COLUMN IF EXISTS updated_at")
