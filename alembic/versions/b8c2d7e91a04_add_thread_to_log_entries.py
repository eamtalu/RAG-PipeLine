"""add thread column to log_entries (concurrency-safe Stage 2 grouping)

Revision ID: b8c2d7e91a04
Revises: a1b2c3d4e5f6
Create Date: 2026-06-11

Stage 2 grouping was a single-stack REQUEST→RESPONSE state machine that mis-stitched interleaved
requests when the M3 server processes multiple users concurrently (confirmed: transactions
containing 2+ users). The fix demultiplexes by thread — one request's internal MI work stays on
one thread — so we persist the thread id (already parsed, previously dropped).

Existing rows are backfilled from raw_body's header line (`... (user) [thread] LEVEL ...`).
"""

from alembic import op
import sqlalchemy as sa

revision = "b8c2d7e91a04"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("log_entries", sa.Column("thread", sa.String(16), nullable=True))
    op.create_index("ix_log_entries_thread", "log_entries", ["thread"])
    # Backfill: extract the [thread] token from the first line of raw_body. We match the ") ["
    # immediately before it so nested "(user)" groups like "((null)) [60]" are handled too.
    # e.g.  "2026-06-05 10:38:53,465 (BECWHLO) [60] INFO  ..." -> "60".
    op.execute(
        r"""
        UPDATE log_entries
        SET thread = substring(split_part(raw_body, chr(10), 1) from '\) \[([^\]]+)\]')
        WHERE thread IS NULL AND raw_body IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ix_log_entries_thread", table_name="log_entries")
    op.drop_column("log_entries", "thread")
