"""row security on the registry tables, with the agent reader's policy

Revision ID: f2c7d5e9a1b6
Revises: e1b6c4d8f0a5
Create Date: 2026-11-27

Chunk 238 (step 57, plan step 1b). The agent's data tools get a database user of their own,
`rag_agent`, which the database itself keeps inside one customer. This migration switches row
security on for every table in the scoped data registry and adds one policy per table. The
application's own user owns the tables and PostgreSQL applies no row rule to an owner, so
nothing changes for ingestion, the web API or any write.

Switching row security on changes the catalogue only (no table is rewritten), but it needs a
short exclusive lock, so every statement runs in its own transaction with a lock timeout:
no lock outlives its statement and ingestion never waits behind the whole migration. The role
itself is created by the database superuser, not here; the
select rights are granted here when the role already exists, and by
`scripts/grant_agent_reader.py` after it is created.
"""

from alembic import op
from sqlalchemy import text

from app.services.scoped_data import database_lock

revision = "f2c7d5e9a1b6"
down_revision = "e1b6c4d8f0a5"
branch_labels = None
depends_on = None


def _run_each_in_its_own_transaction(statements: list[str]) -> None:
    """Run catalogue changes one by one, each committed at once.

    In one transaction the exclusive locks would pile up until the end, and ingestion would
    wait behind all of them. A statement that cannot get its lock within three seconds is
    tried again, because the lock is only ever held for an instant by others.
    """
    bind = op.get_bind()
    with op.get_context().autocommit_block():
        bind.execute(text("SET lock_timeout = '3s'"))
        for statement in statements:
            for attempt in range(1, 6):
                try:
                    bind.execute(text(statement))
                    break
                except Exception as exc:  # noqa: BLE001 - only a lock timeout is retried
                    if "lock timeout" not in str(exc) or attempt == 5:
                        raise
        bind.execute(text("RESET lock_timeout"))


def upgrade() -> None:
    _run_each_in_its_own_transaction(database_lock.policy_statements())
    has_role = op.get_bind().scalar(text("SELECT 1 FROM pg_roles WHERE rolname = :role"),
                                    {"role": database_lock.ROLE})
    if has_role:
        _run_each_in_its_own_transaction(database_lock.grant_statements())


def downgrade() -> None:
    _run_each_in_its_own_transaction(database_lock.removal_statements())
