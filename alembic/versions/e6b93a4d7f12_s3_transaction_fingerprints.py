"""S3: log_transactions.row_fingerprint and members_fingerprint

Cites docs/analytics-ml-architecture/final_architecture.md section 18 (S3).

Two columns, both NULLABLE, and no backfill. That is the whole migration strategy.

A NULL never equals a recomputed digest, so the first regroup pass after deploying treats every
existing row as changed and rewrites it exactly once, filling both columns in as a side effect of the
work the pipeline was going to do anyway. A NOT NULL column would have required a backfill that
recomputed the derivation OUTSIDE the pipeline that owns it - a second implementation of `compute`,
`_is_sealed` and `_merged_attrs` living in a migration, which is precisely the kind of duplicate that
drifts and then disagrees.

Deliberately NOT indexed. They are read by id for a row the rebuild already holds, never searched, and
an index on a column that changes on every real write is pure cost. Same reasoning that makes the seal
flip a non-HOT update: `log_entries` measured 105.8M updates and 162 HOT, i.e. 0.0%.

No CONCURRENTLY dance is needed here for the same reason: `ADD COLUMN` with no default and no index is
a catalogue-only change on PostgreSQL 11+, so it takes a brief lock and touches no data.
"""

from alembic import op
import sqlalchemy as sa

revision = "e6b93a4d7f12"
down_revision = "d8f52c6a1b94"
branch_labels = None
depends_on = None

PARENT = "log_transactions"


def upgrade() -> None:
    op.execute(f"ALTER TABLE {PARENT} ADD COLUMN IF NOT EXISTS row_fingerprint VARCHAR(64)")
    op.execute(f"ALTER TABLE {PARENT} ADD COLUMN IF NOT EXISTS members_fingerprint VARCHAR(64)")


def downgrade() -> None:
    op.execute(f"ALTER TABLE {PARENT} DROP COLUMN IF EXISTS members_fingerprint")
    op.execute(f"ALTER TABLE {PARENT} DROP COLUMN IF EXISTS row_fingerprint")
