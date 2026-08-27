"""log_regroup_runs.kind - finalize | full (chunk 69)

One table tracks both run flavours. A RUNNING kind='full' row doubles as the tenant's maintenance
flag: both worker sweeps skip the tenant while one is fresh, so a tracked full rebuild cannot race
the live stitcher (the collision that cost the 2026-08-27 repair four attempts).

Revision ID: d5f81c3a9e26
Revises: b7e34c9a2f58
Create Date: 2026-08-27
"""
import sqlalchemy as sa
from alembic import op

revision = "d5f81c3a9e26"
down_revision = "b7e34c9a2f58"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # server_default backfills every existing row as 'finalize' (they all were), then stays as the
    # default for rows written by any not-yet-restarted process.
    op.add_column("log_regroup_runs",
                  sa.Column("kind", sa.String(16), nullable=False, server_default="finalize"))


def downgrade() -> None:
    op.drop_column("log_regroup_runs", "kind")
