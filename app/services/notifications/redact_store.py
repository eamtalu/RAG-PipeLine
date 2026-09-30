"""Redact alerts stored before chunk 131, so the Activity page and any retry stop carrying secrets.

Reports ids and counts only, never the text itself. Safe to run again: a clean row is left alone.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.notification import NotificationEvent
from app.services.notifications.redact import redact_secrets, redact_value


async def redact_stored_events(db: AsyncSession, customer_code: str, *, apply: bool) -> dict:
    rows = (await db.execute(
        select(NotificationEvent).where(NotificationEvent.customer_code == customer_code)
    )).scalars().all()
    changed: list[str] = []
    for row in rows:
        title, summary, payload = redact_secrets(row.title), redact_secrets(row.summary), redact_value(row.payload)
        if (title, summary, payload) == (row.title, row.summary, row.payload):
            continue
        changed.append(str(row.id))
        if apply:
            row.title, row.summary, row.payload = title, summary, payload
    if apply and changed:
        await db.flush()
    return {"scanned": len(rows), "changed": len(changed), "ids": changed, "applied": apply}
