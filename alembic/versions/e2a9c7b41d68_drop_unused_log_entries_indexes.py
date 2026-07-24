"""drop unused / damaged indexes on log_entries (cut insert write-amplification)

Revision ID: e2a9c7b41d68
Revises: d2f6b9c04a18
Create Date: 2026-07-24

Each log_entries insert maintained ~13 indexes on a 40 GB table; on the failing/slow production disk
that write-amplification (random writes) dominated ingest time. These six indexes were read 0-1 times
ever (pg_stat_user_indexes) - pure dead weight on writes - plus ix_log_entries_job_id, which is no
longer read in the hot path (the pending-range query now uses INSERT ... RETURNING) and was sitting on
a bad disk sector. Dropping them roughly halves per-insert index work.

Correctness is unaffected: the primary key and the UNIQUE(customer_code, entry_hash) dedup index are
kept, and the job_id FOREIGN KEY (ON DELETE CASCADE) remains - only its index is removed (cascade
deletes of a job now seq-scan, which is rare / admin-only).

On the live box these were already dropped with DROP INDEX CONCURRENTLY, so upgrade() uses IF EXISTS
and is a no-op there; on a fresh build it drops the ones earlier migrations created.
"""

from alembic import op

revision = "e2a9c7b41d68"
down_revision = "d2f6b9c04a18"
branch_labels = None
depends_on = None

# index name -> the single column it covered (for downgrade recreation)
_INDEXES = {
    "ix_log_entries_entry_hash": "entry_hash",
    "ix_log_entries_entry_type": "entry_type",
    "ix_log_entries_mi_program": "mi_program",
    "ix_log_entries_thread": "thread",
    "ix_log_entries_user_ctx": "user_ctx",
    "ix_log_entries_job_id": "job_id",
}


def upgrade() -> None:
    # IF EXISTS = idempotent: a no-op on the live box (already dropped concurrently), and a real drop
    # on a fresh build. Plain (non-concurrent) drop is fine here: on a fresh build the table is empty,
    # on the box it does nothing.
    for name in _INDEXES:
        op.execute(f"DROP INDEX IF EXISTS {name}")


def downgrade() -> None:
    # Recreate the single-column btree indexes. Expensive on a populated table, but downgrade is rare.
    for name, col in _INDEXES.items():
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON log_entries ({col})")
