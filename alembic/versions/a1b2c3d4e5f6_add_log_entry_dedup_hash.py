"""add entry_hash dedup column to log_entries

Revision ID: a1b2c3d4e5f6
Revises: f6a0c4d18e25
Create Date: 2026-06-08

Content-level dedup: entry_hash = sha256(raw_body). A UNIQUE index lets Stage 1 insert with
ON CONFLICT DO NOTHING, so the same log line is never stored twice (handles re-ingestion,
growing active files, and overlap between rotated files). Existing rows keep NULL hashes
(NULLs are distinct in a unique index); they simply don't participate in dedup.
"""

from alembic import op
import sqlalchemy as sa

revision = "a1b2c3d4e5f6"
down_revision = "f6a0c4d18e25"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("log_entries", sa.Column("entry_hash", sa.String(64), nullable=True))
    op.create_index("uq_log_entries_entry_hash", "log_entries", ["entry_hash"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_log_entries_entry_hash", table_name="log_entries")
    op.drop_column("log_entries", "entry_hash")
