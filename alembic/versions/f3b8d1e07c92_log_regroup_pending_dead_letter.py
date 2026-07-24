"""log_regroup_pending: dead-letter tracking (attempts / last_error / abandoned_at)

Revision ID: f3b8d1e07c92
Revises: e2a9c7b41d68
Create Date: 2026-07-24

Adds retry-budget columns so a stitch window that keeps failing finalize is retried a bounded number
of times and then ABANDONED, instead of retried forever (a poison window on a dead disk block would
otherwise burn the statement_timeout every cycle). finalize_pending bumps `attempts` + `last_error` +
`last_attempt_at` on each failure and, once attempts reaches settings.log_regroup_max_attempts, sets
`abandoned_at`; the open-pending query then excludes abandoned rows.

log_regroup_pending is tiny (~thousands of rows) and all four columns are added with a constant
default / NULL, so this is a metadata-only ADD COLUMN (no table rewrite) — safe/fast even on the
degraded disk.
"""

import sqlalchemy as sa
from alembic import op

revision = "f3b8d1e07c92"
down_revision = "e2a9c7b41d68"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("log_regroup_pending",
                  sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"))
    op.add_column("log_regroup_pending", sa.Column("last_error", sa.Text(), nullable=True))
    op.add_column("log_regroup_pending",
                  sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("log_regroup_pending",
                  sa.Column("abandoned_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("log_regroup_pending", "abandoned_at")
    op.drop_column("log_regroup_pending", "last_attempt_at")
    op.drop_column("log_regroup_pending", "last_error")
    op.drop_column("log_regroup_pending", "attempts")
