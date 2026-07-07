"""add SSH hardening columns (head fingerprint + source circuit breaker / status)

Revision ID: b3f9a1c05d27
Revises: e5a2c9f10b34
Create Date: 2026-07-07

Chunk 1 of the SSH log-fetch hardening (see docs/ssh-log-fetch-hardening-and-per-customer-poller.md):

- log_ssh_file_checkpoints.head_fingerprint: sha256 of the file head, used to detect log rotation
  (a path reused by different content) so incremental resume never misses bytes. Nullable, lazily
  backfilled.
- log_ssh_sources.last_attempt_at: when a fetch was last ATTEMPTED (success or failure); last_ok_at
  stays last SUCCESS. Lets the UI show "when did it last run" even for an unhealthy source.
- log_ssh_sources.consecutive_failures + auto_disabled_at: the outage circuit breaker. After a
  sustained failure the poller flips the source to manual-only; auto_disabled_at marks it (vs an
  operator disable) so the UI can prompt a bounded resume.

The `cancelled` value on the LogSshFetchRunStatus enum is added by a later migration (it needs a
standalone ALTER TYPE ... ADD VALUE), alongside the run-history / cancel endpoints.
"""

from alembic import op
import sqlalchemy as sa

revision = "b3f9a1c05d27"
down_revision = "e5a2c9f10b34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "log_ssh_file_checkpoints",
        sa.Column("head_fingerprint", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "log_ssh_sources",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "log_ssh_sources",
        sa.Column("consecutive_failures", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "log_ssh_sources",
        sa.Column("auto_disabled_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("log_ssh_sources", "auto_disabled_at")
    op.drop_column("log_ssh_sources", "consecutive_failures")
    op.drop_column("log_ssh_sources", "last_attempt_at")
    op.drop_column("log_ssh_file_checkpoints", "head_fingerprint")
