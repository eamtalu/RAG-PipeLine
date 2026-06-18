"""add live progress (phase + progress) to log_ssh_fetch_runs

Revision ID: a3e6b8c1d472
Revises: d7f1a2c3b4e5
Create Date: 2026-06-18

Adds a coarse `phase` (listing/fetching/regrouping/done) and a `progress` JSONB to the async
fetch-run record so the frontend's existing poll of GET /logs/fetch-remote/runs/{id} can show
per-file / per-source progress mid-flight instead of just a binary running→completed status.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "a3e6b8c1d472"
down_revision = "d7f1a2c3b4e5"
branch_labels = None
depends_on = None

ssh_fetch_phase = sa.Enum("listing", "fetching", "regrouping", "done", name="logsshfetchphase")


def upgrade() -> None:
    ssh_fetch_phase.create(op.get_bind(), checkfirst=True)
    op.add_column(
        "log_ssh_fetch_runs",
        sa.Column("phase", ssh_fetch_phase, nullable=True),
    )
    op.add_column(
        "log_ssh_fetch_runs",
        sa.Column("progress", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("log_ssh_fetch_runs", "progress")
    op.drop_column("log_ssh_fetch_runs", "phase")
    ssh_fetch_phase.drop(op.get_bind(), checkfirst=True)
