"""Move every plain webhook URL left in a channel's config into the secrets store, once.

Rows created before step 8 carry `config.webhook_url` in plain text. Run from the repo root with
SECRETS_MASTER_KEYS configured: `PYTHONPATH=$PWD python scripts/seal_channel_secrets.py`.
"""

import asyncio

from sqlalchemy import select

from app.config.database import async_session, engine
from app.persistence.models.notification import CustomerNotificationChannel
from app.services.notifications import channel_secrets


async def main() -> None:
    moved = 0
    async with async_session() as db:
        rows = (await db.execute(select(CustomerNotificationChannel))).scalars().all()
        for row in rows:
            if not any(
                isinstance((row.config or {}).get(f), str) for f in channel_secrets.SECRET_FIELDS
            ):
                continue
            row.config = await channel_secrets.seal_config(
                db,
                customer_code=row.customer_code,
                channel_id=row.id,
                config={},
                previous=row.config,
            )
            moved += 1
        await db.commit()
    print(f"sealed the secrets of {moved} channel(s)")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
