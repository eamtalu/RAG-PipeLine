"""The row the edge reads for one person's preferences: `USER#<id>#PREFS`.

It holds every area resolved for that person (`areas`) and what they set themselves (`own`), so
the app can draw at once and can tell "my choice" from "my role's default". Rewritten after
every command that could have moved it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.services import access
from app.services.preferences import store
from app.services.teams.mirror import MirrorWriter, envelope

ROW_TYPE = "user_prefs"


async def user_payload(db: AsyncSession, record: access.AccessRecord) -> dict:
    """One person's row: {"areas": {area: resolved values}, "own": {area: what they set}}."""
    return {"areas": await store.resolve(db, record), "own": await store.own(db, record)}


async def mirror_user(db: AsyncSession, writer: MirrorWriter, customer_code: str, object_id: str,
                      *, now: datetime | None = None) -> bool:
    """Write one person's row; False when they have no active record (nothing is written).

    (an active person) -> True, the row is put ; (unknown or deactivated) -> False
    """
    record = await access.access_record_for(db, customer_code, object_id)
    if record is None or not record.is_active:
        return False
    now = now or datetime.now(timezone.utc)
    await writer.put(envelope(ROW_TYPE, customer_code, user_id=object_id,
                              payload=await user_payload(db, record), as_of=now.isoformat(),
                              now=now.timestamp()))
    return True
