"""partition log_entries, log_transactions and log_entry_assignment by UTC day

Revision ID: a1f6d70b3e92
Revises: f04b7c29ae13
Create Date: 2026-08-05

Step 3 of docs/plan/2026-08-05_20-32_daily-partitioning.md.

Retention was `DELETE` + `VACUUM`, both of which read the whole table — on a heap that reached 40 GB,
on a disk with bad sectors. Partitioned by day it becomes `DROP TABLE <partition>`: a file unlink,
no row scan, no dead tuples. Reads of one day touch one partition.

This is a full table REWRITE and it is OFFLINE. Stop the workers before running it.

The DOWNGRADE is real and verified (plain tables, original primary keys, original two-column dedup,
foreign keys intact, no rows lost), but it must be paired with a CODE rollback. `parse_insert.py`
names all three dedup columns in its `ON CONFLICT`; against the narrowed two-column constraint that
fails outright with "no unique or exclusion constraint matching the ON CONFLICT specification", so
reverting the schema without reverting the code stops ingestion dead.

All three tables are done in ONE migration on purpose. PostgreSQL DDL is transactional, so a failure
anywhere rolls the whole thing back; splitting it per table could leave `log_entries` partitioned
while `log_entry_assignment` was not, and their co-partitioning is the property that lets retention
drop a day from both without stranding rows.

Order per table — rename, create bare parent, copy, verify, drop old, THEN build indexes:

  1. `ALTER TABLE x RENAME TO x_old`
  2. `CREATE TABLE x (LIKE x_old INCLUDING DEFAULTS) PARTITION BY RANGE (key)`
  3. DEFAULT partition + one partition per day the data spans, plus today .. today+precreate
  4. `INSERT INTO x SELECT * FROM x_old`
  5. verify the copied row count against the source, and ABORT if they differ
  6. `DROP TABLE x_old`
  7. add constraints and indexes

Indexes come last for two reasons. Building them after the bulk load is far faster than maintaining
them during it, and the old table holds the index and constraint NAMES until it is dropped — creating
them earlier would collide.

Things that are easy to get wrong and are handled explicitly here:

- **Partition bounds are pinned to UTC.** A `timestamptz` bound written `'2026-08-05'` is resolved in
  the session's TimeZone when the partition is created; on a Europe/London server every partition
  would sit an hour off the day it is named after. The same trap applies to deriving the data's day
  range, so that uses `AT TIME ZONE 'UTC'` rather than a bare `::date`.
- **Every partition key is nullable**, and a PRIMARY KEY silently forces NOT NULL — which would make
  the NULL-timestamp entries the parser genuinely produces un-insertable. So each PK becomes a
  `UNIQUE NULLS NOT DISTINCT` containing the key, and a DEFAULT partition catches the NULL-key rows.
- **The key is in the unique but never first.** Leading with it measured 240x slower on lookups by id
  alone (plan §2.2).
- **`log_entries` dedup grows the key**: `(customer_code, entry_hash)` becomes
  `(customer_code, entry_hash, timestamp)`, because a unique on a partitioned table must contain
  every partition column. Safe only because `entry_hash` is a sha256 over the raw line INCLUDING its
  timestamp text, so an identical replay parses to the same instant and routes to the same partition.
- **`LIKE` does not copy foreign keys**, so `job_id -> jobs ON DELETE CASCADE` is re-added by hand.
  `logspace_cleanup` purges a tenant through that cascade; losing it would silently orphan rows.
"""

from datetime import date

import sqlalchemy as sa
from alembic import op

from app.persistence import partitioning as pt
from app.settings import settings

revision = "a1f6d70b3e92"
down_revision = "f04b7c29ae13"
branch_labels = None
depends_on = None


# Rebuilt verbatim after the copy. Held here as explicit DDL rather than reflected from the live
# catalogue so the migration is reviewable and reproducible on any database, not only this one.
INDEXES: dict[str, list[str]] = {
    "log_entries": [
        "CREATE INDEX ix_log_entries_customer_code ON log_entries (customer_code)",
        "CREATE INDEX ix_log_entries_mi_transaction ON log_entries (mi_transaction)",
        'CREATE INDEX ix_log_entries_timestamp ON log_entries ("timestamp")',
    ],
    "log_transactions": [
        "CREATE INDEX ix_log_transactions_customer_code ON log_transactions (customer_code)",
        "CREATE INDEX ix_log_transactions_customer_date ON log_transactions (customer_code, date)",
        "CREATE INDEX ix_log_transactions_customer_date_started"
        " ON log_transactions (customer_code, date, started_at)",
        "CREATE INDEX ix_log_transactions_customer_started"
        " ON log_transactions (customer_code, started_at DESC NULLS LAST)",
        "CREATE INDEX ix_log_transactions_customer_user ON log_transactions (customer_code, user_name)",
        "CREATE INDEX ix_log_transactions_date ON log_transactions (date)",
        "CREATE INDEX ix_log_transactions_delivery_number ON log_transactions (delivery_number)",
        "CREATE INDEX ix_log_transactions_flow_id ON log_transactions (flow_id)",
        "CREATE INDEX ix_log_transactions_item_number ON log_transactions (item_number)",
        "CREATE INDEX ix_log_transactions_job_id ON log_transactions (job_id)",
        "CREATE INDEX ix_log_transactions_method ON log_transactions (method)",
        "CREATE INDEX ix_log_transactions_order_number ON log_transactions (order_number)",
        "CREATE INDEX ix_log_transactions_reqid ON log_transactions (reqid)",
        "CREATE INDEX ix_log_transactions_sealed ON log_transactions (sealed)",
        "CREATE INDEX ix_log_transactions_started_at ON log_transactions (started_at)",
        "CREATE INDEX ix_log_transactions_status ON log_transactions (status)",
        "CREATE INDEX ix_log_transactions_transaction_name ON log_transactions (transaction_name)",
        "CREATE INDEX ix_log_transactions_transaction_type ON log_transactions (transaction_type)",
        "CREATE INDEX ix_log_transactions_user_date ON log_transactions (user_name, date)",
        "CREATE INDEX ix_log_transactions_user_name ON log_transactions (user_name)",
        "CREATE INDEX ix_log_transactions_warehouse ON log_transactions (warehouse)",
    ],
    "log_entry_assignment": [
        "CREATE INDEX ix_log_entry_assignment_customer ON log_entry_assignment (customer_code)",
        "CREATE INDEX ix_log_entry_assignment_entry_ts ON log_entry_assignment (entry_ts)",
        "CREATE INDEX ix_log_entry_assignment_txn ON log_entry_assignment (transaction_id, seq)",
    ],
}

# Identity uniques replacing the old primary keys. The partition key is present (PostgreSQL demands
# it) but never leading (plan §2.2), and NULLS NOT DISTINCT so two rows sharing an id with a NULL key
# still conflict — otherwise identity would stop being enforced for exactly the DEFAULT-partition rows.
CONSTRAINTS: dict[str, list[str]] = {
    "log_entries": [
        'ALTER TABLE log_entries ADD CONSTRAINT uq_log_entries_id'
        ' UNIQUE NULLS NOT DISTINCT (id, "timestamp")',
        'ALTER TABLE log_entries ADD CONSTRAINT uq_log_entries_customer_hash'
        ' UNIQUE NULLS NOT DISTINCT (customer_code, entry_hash, "timestamp")',
        "ALTER TABLE log_entries ADD CONSTRAINT log_entries_job_id_fkey"
        " FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE",
    ],
    "log_transactions": [
        "ALTER TABLE log_transactions ADD CONSTRAINT uq_log_transactions_id"
        " UNIQUE NULLS NOT DISTINCT (id, started_at)",
        "ALTER TABLE log_transactions ADD CONSTRAINT log_transactions_job_id_fkey"
        " FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE",
    ],
    "log_entry_assignment": [
        "ALTER TABLE log_entry_assignment ADD CONSTRAINT uq_log_entry_assignment_entry"
        " UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)",
    ],
}


def _data_days(conn, old_table: str, key: str) -> list[date]:
    """Every UTC day the existing rows span, widened to today .. today + precreate.

    `AT TIME ZONE 'UTC'` rather than a bare `::date`: casting a timestamptz to date uses the session's
    TimeZone, which would put the boundary rows in the wrong day and leave the real first/last day
    with no partition to land in.
    """
    lo, hi = conn.execute(sa.text(
        f'SELECT min(("{key}" AT TIME ZONE \'UTC\')::date), '
        f'       max(("{key}" AT TIME ZONE \'UTC\')::date) FROM {old_table}'
    )).one()
    today = conn.execute(sa.text("SELECT (now() AT TIME ZONE 'UTC')::date")).scalar()
    # Delegated so the widening rules and the corrupt-timestamp guard are unit-tested rather than
    # only exercised by running a migration.
    return pt.migration_days(lo, hi, today, ahead=settings.log_partition_precreate_days)


def _partition(conn, table: str, key: str) -> None:
    old = f"{table}_old"
    op.execute(f"ALTER TABLE {table} RENAME TO {old}")
    # Bare parent: LIKE copies columns, types, NOT NULL and defaults, but no indexes, no constraints
    # and no foreign keys. Those are added at the end, after the load and after `old` releases its
    # hold on their names.
    op.execute(f'CREATE TABLE {table} (LIKE {old} INCLUDING DEFAULTS) PARTITION BY RANGE ("{key}")')
    op.execute(pt.create_default_sql(table))
    for day in _data_days(conn, old, key):
        op.execute(pt.create_partition_sql(table, day))

    expected = conn.execute(sa.text(f"SELECT count(*) FROM {old}")).scalar()
    copied = conn.execute(sa.text(f"INSERT INTO {table} SELECT * FROM {old}")).rowcount
    # Verify BEFORE dropping the source. A silent short copy here is unrecoverable data loss, and the
    # only moment it can still be caught for free is while the original rows are still on disk.
    if copied != expected:
        raise RuntimeError(
            f"{table}: copied {copied} rows but {old} holds {expected}. "
            f"Aborting so the original table is preserved — nothing has been dropped.")

    op.execute(f"DROP TABLE {old}")
    for stmt in CONSTRAINTS[table]:
        op.execute(stmt)
    for stmt in INDEXES[table]:
        op.execute(stmt)


def upgrade() -> None:
    conn = op.get_bind()
    for t in pt.PARTITIONED:
        _partition(conn, t.table, t.key)


def _unpartition(conn, table: str, key: str) -> None:
    """Collapse the partitions back into one plain table.

    The old PRIMARY KEYs are restored rather than left as uniques, so a rollback lands on exactly the
    schema that preceded this migration. `log_entries` dedup narrows back to
    `(customer_code, entry_hash)`; that is only safe because the wider key was a SUPERSET, so any data
    the partitioned table accepted is still unique on the narrower one — unless a customer's timezone
    was changed and lines were re-ingested while partitioned, which would have created rows that now
    collide. The migration surfaces that as a constraint violation rather than dropping rows.
    """
    new = f"{table}_part"
    op.execute(f"ALTER TABLE {table} RENAME TO {new}")
    op.execute(f"CREATE TABLE {table} (LIKE {new} INCLUDING DEFAULTS)")
    expected = conn.execute(sa.text(f"SELECT count(*) FROM {new}")).scalar()
    copied = conn.execute(sa.text(f"INSERT INTO {table} SELECT * FROM {new}")).rowcount
    if copied != expected:
        raise RuntimeError(f"{table}: copied {copied} of {expected} rows on downgrade; aborting.")
    op.execute(f"DROP TABLE {new} CASCADE")


def downgrade() -> None:
    conn = op.get_bind()
    for t in pt.PARTITIONED:
        _unpartition(conn, t.table, t.key)

    op.execute("ALTER TABLE log_entries ADD CONSTRAINT log_entries_pkey PRIMARY KEY (id)")
    op.execute("ALTER TABLE log_entries ADD CONSTRAINT uq_log_entries_customer_hash"
               " UNIQUE (customer_code, entry_hash)")
    op.execute("ALTER TABLE log_entries ADD CONSTRAINT log_entries_job_id_fkey"
               " FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE")
    op.execute("ALTER TABLE log_transactions ADD CONSTRAINT log_transactions_pkey PRIMARY KEY (id)")
    op.execute("ALTER TABLE log_transactions ADD CONSTRAINT log_transactions_job_id_fkey"
               " FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE")
    op.execute("ALTER TABLE log_entry_assignment ADD CONSTRAINT uq_log_entry_assignment_entry"
               " UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)")
    for table, stmts in INDEXES.items():
        for stmt in stmts:
            op.execute(stmt)
