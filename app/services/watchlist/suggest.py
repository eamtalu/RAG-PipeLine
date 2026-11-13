"""The Watchlist tab's Suggested box (stage D, step 14): one thing worth watching that the person
is not watching yet, taken from the open HIGH advisories they may see (the app has no view
tracking, so "you opened it four times" is not something it can say). Used by mirror.py.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.access.records import AccessRecord
from app.services.advisories import store as advisories_store
from app.services.watchlist import store


def entity_of(source: dict | None) -> tuple[str, str] | None:
    """The watchable thing behind an advisory's source, or None.

    >>> entity_of({"kind": "delivery", "delivery_number": "D-1"}), entity_of({"kind": "count", "item": "I1", "location": "A-01"})
    (('delivery', 'D-1'), ('product', 'I1'))
    >>> entity_of({"kind": "location", "location": "A-01"}), entity_of({"kind": "item", "item": "I2"}), entity_of(None)
    (('location', 'A-01'), ('product', 'I2'), None)
    """
    s = source or {}
    kind = s.get("kind")
    if kind == "delivery" and s.get("delivery_number"):
        return "delivery", str(s["delivery_number"])
    if kind in ("count", "item") and s.get("item"):
        return "product", str(s["item"])
    if kind == "location" and s.get("location"):
        return "location", str(s["location"])
    return None


async def suggestion_for(db: AsyncSession, customer_code: str, record: AccessRecord, *, now: datetime) -> dict | None:
    """One unwatched thing from the open HIGH advisories the person may see, or None.

    (HIGH stock advisory on item I1, not watched) -> {"kind": "product", "entity_id": "I1", "text": "..."}
    """
    watched = {(w.kind, w.entity_id) for w in await store.active_for_owner(db, customer_code, record.object_id)}
    for row in await advisories_store.live_rows(db, customer_code, limit=50):
        if row.severity != "high" or not record.may_see(row.site_id):
            continue
        entity = entity_of(row.source)
        if entity is None or entity in watched:
            continue
        if await store.is_not_now(db, customer_code, record.object_id, entity[0], entity[1], now=now):
            continue
        kind, entity_id = entity
        return {"kind": kind, "entity_id": entity_id, "label": entity_id,
                "text": f"A HIGH advisory is open on it: {row.title}. Add it to your watchlist?",
                "advisory_id": str(row.id)}
    return None
