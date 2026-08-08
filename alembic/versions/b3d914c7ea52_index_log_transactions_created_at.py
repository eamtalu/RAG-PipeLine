"""index log_transactions.created_at for incremental (cursor-based) readers

Revision ID: b3d914c7ea52
Revises: a1f6d70b3e92
Create Date: 2026-08-08

Step 1 of docs/plan/2026-08-08_notification-architecture.html.

`created_at` is the WRITE time of a transaction row, as opposed to `started_at` which is when the log
line happened. Any consumer that wants "what changed since I last looked" must read on write time:
a week-old file backfilled today produces rows with an OLD `started_at` but a NEW `created_at`, so a
reader cursoring on `started_at` would silently never see them.

The notification rule engine is the first such consumer. ML feature extraction is expected to be the
second. Neither can work without this index - the cursor query filters and orders on `created_at`, and
without an index that is a sequential scan of every partition on every tick.

WHY NOT A PLAIN `CREATE INDEX CONCURRENTLY`
-------------------------------------------
`log_transactions` is partitioned (migration a1f6d70b3e92), and PostgreSQL refuses:

    ERROR:  cannot create index on partitioned table "log_transactions" concurrently

Verified against the live database, not assumed. The supported non-blocking recipe for a partitioned
table is three steps:

  1. `CREATE INDEX ... ON ONLY <parent>` - registers an INVALID parent index. Metadata only: it
     defines the shape without scanning or locking any data.
  2. `CREATE INDEX CONCURRENTLY` on each partition individually - this is where the work happens, and
     CONCURRENTLY *is* allowed on a leaf partition, so writes keep flowing.
  3. `ALTER INDEX <parent> ATTACH PARTITION <child>` for each. Once every partition is attached
     PostgreSQL marks the parent index valid automatically.

Partitions created LATER by the partition worker need no special handling: `CREATE TABLE ...
PARTITION OF` builds the matching local index automatically from the parent definition.

Step 1 is DDL and must be transactional; steps 2 and 3 must not be, because CONCURRENTLY cannot run
inside a transaction. Hence the autocommit block around the per-partition loop only.
"""

import sqlalchemy as sa
from alembic import op

revision = "b3d914c7ea52"
down_revision = "a1f6d70b3e92"
branch_labels = None
depends_on = None

PARENT = "log_transactions"
INDEX = "ix_log_transactions_created_at"


def _partitions(conn) -> list[str]:
    """Every leaf partition of the parent, DEFAULT included.

    Read from `pg_inherits` rather than derived from partition names: the name is a convention, the
    inheritance link is the truth, and a partition missed here would leave the parent index invalid
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

    # 1. the parent index, ON ONLY — metadata, no data touched, no long lock.
    op.execute(f'CREATE INDEX IF NOT EXISTS {INDEX} ON ONLY {PARENT} (created_at)')

    # 2 + 3. build each partition's index without blocking writes, then attach it.
    with op.get_context().autocommit_block():
        for part in parts:
            child = f"{part}_created_at_idx"
            op.execute(f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {child} ON {part} (created_at)')
            # ATTACH is idempotent in effect: re-attaching an already-attached index is a no-op error
            # only if the index is missing, which the CREATE above just guaranteed.
            op.execute(f'ALTER INDEX {INDEX} ATTACH PARTITION {child}')


def downgrade() -> None:
    # Dropping the parent cascades to the attached partition indexes, so the children need no
    # separate DROP. Not CONCURRENTLY: a partitioned index cannot be dropped concurrently either.
    op.execute(f"DROP INDEX IF EXISTS {INDEX}")
