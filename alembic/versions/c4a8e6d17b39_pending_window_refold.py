"""analytics_pending_windows.refold_rollups: a ticket that asks for the rollups to be rebuilt even when no
fact in its range changed (chunk 91).

Two callers need this. Activating a metric with a past `rollups_from` publishes tickets over facts the
fold has already seen, so the diff says unchanged and, without this flag, no dirty bucket and no rollup
row would ever be built for the new metric. Flipping a transaction's `show` switch gates the rollups
only, so it changes no fact either. Both were silent no-ops before this column.

Revision ID: c4a8e6d17b39
Revises: b7e5d2c94a18
Create Date: 2026-09-13
"""

import sqlalchemy as sa
from alembic import op

revision = "c4a8e6d17b39"
down_revision = "b7e5d2c94a18"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_pending_windows",
                  sa.Column("refold_rollups", sa.Boolean(), nullable=False, server_default=sa.false()))


def downgrade() -> None:
    op.drop_column("analytics_pending_windows", "refold_rollups")
