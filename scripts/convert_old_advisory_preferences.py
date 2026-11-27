"""Turn the preferences saved under the two old Advisories areas into one saved view each.

    PYTHONPATH=. venv/bin/python scripts/convert_old_advisory_preferences.py

Run once after deploying chunk 242. Safe to repeat: a second run finds nothing to convert.
"""

from __future__ import annotations

import asyncio

from app.config.database import async_session
from app.services.preferences import conversion


async def run() -> None:
    async with async_session() as db:
        converted = await conversion.convert_old_advisories(db)
        await db.commit()
    print(f"{converted} subject(s) got a saved view from their old Advisories preferences")


if __name__ == "__main__":
    asyncio.run(run())
