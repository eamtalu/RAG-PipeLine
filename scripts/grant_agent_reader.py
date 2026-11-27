"""Give the agent's reader user its select rights, after the superuser has created it.

    PYTHONPATH=. venv/bin/python scripts/grant_agent_reader.py

Runs as the application's own user, which owns the tables. Safe to repeat: rights are taken
away first and granted again for the tables the registry lists today, so run it again whenever
a table joins or leaves the registry.
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import text

from app.config.database import async_session
from app.services.scoped_data import database_lock


async def run() -> int:
    async with async_session() as db:
        has_role = await db.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = :role"),
                                   {"role": database_lock.ROLE})
        if not has_role:
            print(f"the role {database_lock.ROLE} does not exist yet; create it first")
            return 1
        statements = database_lock.grant_statements()
        for statement in statements:
            await db.execute(text(statement))
        await db.commit()
    print(f"{database_lock.ROLE}: select granted on {len(statements) - 1} tables")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
