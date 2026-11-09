"""Re-wrap every tenant secret under the active master key (docs/secrets-store.md, Rotation).

Run from the repo root with the new key already added to SECRETS_MASTER_KEYS and named in
SECRETS_ACTIVE_KEY_ID: `PYTHONPATH=$PWD python scripts/rotate_secrets_master_key.py`.
"""

import asyncio

from app.config.database import async_session, engine
from app.services.secrets import envelope, secret_store


async def main() -> None:
    master = envelope.MasterKeys.from_settings()
    async with async_session() as db:
        moved = await secret_store.rewrap_all(db)
        await db.commit()
    print(f"re-wrapped {moved} secret(s) under master key {master.active!r}")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
