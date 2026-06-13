"""add log_regroup_runs (async finalize status tracking)

Revision ID: c2e5a9f3b471
Revises: b1d4f6a8c290
Create Date: 2026-06-13

POST /logs/regroup/finalize is now non-blocking: it records a log_regroup_runs row and runs the
scoped regroup in the background, so a long regroup can't time out the HTTP request. The frontend
polls GET /logs/regroup/runs/{id} for running -> completed/failed, like it already polls ingest Jobs.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "c2e5a9f3b471"
down_revision = "b1d4f6a8c290"
branch_labels = None
depends_on = None

log_regroup_run_status = sa.Enum("running", "completed", "failed", name="logregrouprunstatus")


def upgrade() -> None:
    op.create_table(
        "log_regroup_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("status", log_regroup_run_status, nullable=False, index=True),
        sa.Column("windows", sa.Integer, nullable=True),
        sa.Column("pending_consumed", sa.Integer, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("result", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("log_regroup_runs")
    log_regroup_run_status.drop(op.get_bind(), checkfirst=True)
