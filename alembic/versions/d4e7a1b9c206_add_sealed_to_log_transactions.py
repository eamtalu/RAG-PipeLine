"""add sealed flag to log_transactions (incremental Stage 2 grouping)

Revision ID: d4e7a1b9c206
Revises: c9d3e8f02b15
Create Date: 2026-06-11

Stage 2 used to FULL-rebuild log_transactions on every ingest (O(whole table)) and reassign random
uuid4 ids — not scalable for continuous ingestion and broke any saved transaction id. Now grouping is
incremental: a transaction whose end is older than the seal window is SEALED and never recomputed, so
only the recent "live tail" is reprocessed each cycle. `sealed` marks those finalized rows.
(Transaction ids are also made deterministic in code — uuid5 of the anchor entry hash — so a regroup
reproduces the same id instead of a new random one.)
"""

from alembic import op
import sqlalchemy as sa

revision = "d4e7a1b9c206"
down_revision = "c9d3e8f02b15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "log_transactions",
        sa.Column("sealed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index("ix_log_transactions_sealed", "log_transactions", ["sealed"])


def downgrade() -> None:
    op.drop_index("ix_log_transactions_sealed", table_name="log_transactions")
    op.drop_column("log_transactions", "sealed")
