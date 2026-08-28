"""log_regroup_runs.range_start/range_end - the ranged rebuild (chunk 70)

Revision ID: e8b26a7c4d91
Revises: d5f81c3a9e26
Create Date: 2026-08-28
"""
import sqlalchemy as sa
from alembic import op

revision = "e8b26a7c4d91"
down_revision = "d5f81c3a9e26"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("log_regroup_runs",
                  sa.Column("range_start", sa.DateTime(timezone=True), nullable=True))
    op.add_column("log_regroup_runs",
                  sa.Column("range_end", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("log_regroup_runs", "range_end")
    op.drop_column("log_regroup_runs", "range_start")
