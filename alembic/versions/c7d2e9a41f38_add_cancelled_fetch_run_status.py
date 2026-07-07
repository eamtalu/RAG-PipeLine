"""add 'cancelled' to the log_ssh_fetch_runs status enum

Revision ID: c7d2e9a41f38
Revises: b3f9a1c05d27
Create Date: 2026-07-07

Chunk 5 of the SSH log-fetch hardening: the cancel endpoint
(POST /logs/fetch-remote/runs/{id}/cancel) marks a run `cancelled`, a distinct terminal state from
`failed`. Postgres requires ALTER TYPE ... ADD VALUE to run outside a transaction block, so we use
alembic's autocommit_block. Removing an enum value is not supported by Postgres without recreating
the type, so downgrade is a no-op (the extra label is harmless if unused).
"""

from alembic import op

revision = "c7d2e9a41f38"
down_revision = "b3f9a1c05d27"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE logsshfetchrunstatus ADD VALUE IF NOT EXISTS 'cancelled'")


def downgrade() -> None:
    # Postgres cannot drop an enum value without recreating the type; leaving 'cancelled' in place is
    # harmless. Intentionally a no-op.
    pass
