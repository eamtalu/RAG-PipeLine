"""add 'seed' to the log_ssh_fetch_runs mode enum

Revision ID: d1e4f7a92c63
Revises: c7d2e9a41f38
Create Date: 2026-07-08

Enables a "start from now, zero backfill" fetch: seed every present file's checkpoint to its current
end without ingesting, so enabling auto-poll afterwards only follows new lines forward. Postgres
requires ALTER TYPE ... ADD VALUE outside a transaction, so use alembic's autocommit_block.
Downgrade is a no-op (Postgres can't drop an enum value without recreating the type).
"""

from alembic import op

revision = "d1e4f7a92c63"
down_revision = "c7d2e9a41f38"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE logsshfetchmode ADD VALUE IF NOT EXISTS 'seed'")


def downgrade() -> None:
    pass
