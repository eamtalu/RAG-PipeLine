"""log_entries: drop the ON DELETE SET NULL cascade from transaction_id

Revision ID: d5b830e14f72
Revises: c8f21a06d349
Create Date: 2026-08-05

Moving the assignment into log_entry_assignment removes the EXPLICIT rewrite of log_entries, but not
the implicit one. `log_entries_transaction_id_fkey` is ON DELETE SET NULL, so every time Stage 2
deletes a window's transactions Postgres UPDATES every entry that pointed at them - blanking the
column. Measured directly: a regroup of 6 entries still performed 6 row updates with the new code in
place, because of this cascade alone.

That makes it the second half of the same problem, and the table cannot be append-only while it
exists.

Dropping the constraint is a catalog-only operation: no table rewrite, no scan, instant even on the
multi-GB heap. The `transaction_id` / `seq` COLUMNS are deliberately left in place - dropping those
is the only step that rewrites the table, so it should ride with the partitioning pass. Until then
they simply hold their last written values and nothing reads them; log_entry_assignment is the source
of truth.

The index on transaction_id is also left alone for now: it is what makes the (deferred) column drop
and any historical comparison cheap, and it is no longer written on the hot path.
"""

import sqlalchemy as sa
from alembic import op

revision = "d5b830e14f72"
down_revision = "c8f21a06d349"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("log_entries_transaction_id_fkey", "log_entries", type_="foreignkey")


def downgrade() -> None:
    op.create_foreign_key(
        "log_entries_transaction_id_fkey", "log_entries", "log_transactions",
        ["transaction_id"], ["id"], ondelete="SET NULL",
    )
