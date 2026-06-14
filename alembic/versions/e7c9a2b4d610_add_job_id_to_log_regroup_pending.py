"""add job_id to log_regroup_pending (per-upload finalize signal)

Revision ID: e7c9a2b4d610
Revises: c2e5a9f3b471
Create Date: 2026-06-14

The console banner ("this upload still needs finalize") was driven off the optimistic POST /logs/ingest
response, which fires before Stage 1 has run and so can't know whether the file actually added a
window — making the banner appear, then clear itself. Linking each dirty-window row to the ingest job
lets GET /logs/jobs/{id} report an ACCURATE per-upload pending_regroup (an open row exists for this
job) once the job completes. Nullable: rows written before this column stay valid (finalize is still
tenant-wide and ignores job_id).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "e7c9a2b4d610"
down_revision = "c2e5a9f3b471"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "log_regroup_pending",
        sa.Column("job_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_log_regroup_pending_job_id",
        "log_regroup_pending", ["job_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_log_regroup_pending_job_id", table_name="log_regroup_pending")
    op.drop_column("log_regroup_pending", "job_id")
