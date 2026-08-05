"""log_regroup_pending: available_at backoff gate for Stage 2 retries

Revision ID: b4e17d92c8a3
Revises: a7c93f0e5b21
Create Date: 2026-08-05

Stage 2 had a dead-letter cap (3 attempts) but no delay between attempts: a failing window was
retried on the very next finalize tick, so the whole budget was spent within seconds — before a
transient condition (busy disk, held lock, backed-up I/O) could possibly clear. The retries were
bounded but useless.

`available_at` is the gate. finalize_pending pushes it into the future on a transient failure using
the shared backoff (app/services/queueing/retry_policy.py), and the open-window query filters
`available_at <= now()`.

Also adds a partial index for the stitch worker's "which tenants have work due?" query. It is the
hot path of a loop that now runs every second, and it must never degenerate into a scan.

log_regroup_pending is small (thousands of rows) and the column is added with a constant default, so
this is a metadata-only ADD COLUMN — no table rewrite, safe on the degraded disk. Existing rows
become immediately eligible, which is the correct behaviour on upgrade.
"""

import sqlalchemy as sa
from alembic import op

revision = "b4e17d92c8a3"
down_revision = "a7c93f0e5b21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "log_regroup_pending",
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False,
                  # clock_timestamp(), not now(): this column is written AND compared by the
                  # database clock, so no app-host clock skew can make a row look not-yet-due.
                  server_default=sa.text("clock_timestamp()")),
    )
    # Partial: only OPEN windows are indexed, so the worker's poll stays cheap no matter how much
    # consumed history accumulates.
    op.create_index(
        "ix_log_regroup_pending_due",
        "log_regroup_pending",
        ["available_at", "customer_code"],
        postgresql_where=sa.text("consumed_at IS NULL AND abandoned_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_log_regroup_pending_due", table_name="log_regroup_pending")
    op.drop_column("log_regroup_pending", "available_at")
