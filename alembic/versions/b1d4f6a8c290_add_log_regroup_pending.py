"""add log_regroup_pending (dirty-window tracking for scoped Stage 2 regroup)

Revision ID: b1d4f6a8c290
Revises: a7b1c2d3e4f5
Create Date: 2026-06-13

Stage 2 regroup was all-or-tail: regroup_all (whole table) or regroup_incremental (unsealed tail).
Neither cheaply + losslessly back-fills a file into an already-sealed time region. This table records
the time range each ingest touched so a SCOPED, padded regroup (regroup_window) can rebuild only the
affected window — triggered by the console finalize endpoint or when the watcher's queue drains empty.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "b1d4f6a8c290"
down_revision = "a7b1c2d3e4f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "log_regroup_pending",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("range_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("range_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_log_regroup_pending_customer_consumed",
        "log_regroup_pending", ["customer_code", "consumed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_log_regroup_pending_customer_consumed", table_name="log_regroup_pending")
    op.drop_table("log_regroup_pending")
