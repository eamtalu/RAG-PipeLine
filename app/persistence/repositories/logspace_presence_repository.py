"""Repository for logspace presence — who is currently in a log space.

Presence is ephemeral and self-declared (not auth): opening a space upserts a row keyed by
(customer_code, name), leaving removes it, and stale rows are swept after a TTL. Reads filter out stale
rows (`since < fresh_after`) so a crashed client never shows as "present" indefinitely.
"""

from datetime import datetime, timezone

from fastapi import Depends
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.logspace_presence import LogspacePresence


class LogspacePresenceRepository:
    """Database access for the logspace_presence table."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def upsert(self, customer_code: str, name: str, note: str | None = None) -> LogspacePresence:
        """Insert a presence row, or refresh `since` + `note` if this person is already present."""
        now = datetime.now(timezone.utc)
        row = await self.db.scalar(
            select(LogspacePresence).where(
                LogspacePresence.customer_code == customer_code,
                LogspacePresence.name == name,
            )
        )
        if row is None:
            row = LogspacePresence(customer_code=customer_code, name=name, note=note, since=now)
            self.db.add(row)
        else:
            row.note = note
            row.since = now
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def list_for_code(self, customer_code: str, *,
                            fresh_after: datetime | None = None) -> list[LogspacePresence]:
        stmt = select(LogspacePresence).where(LogspacePresence.customer_code == customer_code)
        if fresh_after is not None:
            stmt = stmt.where(LogspacePresence.since >= fresh_after)
        stmt = stmt.order_by(LogspacePresence.since.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def list_all(self, *, fresh_after: datetime | None = None) -> list[LogspacePresence]:
        """Every presence row across all spaces, for batch-enriching the list endpoints (no N+1)."""
        stmt = select(LogspacePresence)
        if fresh_after is not None:
            stmt = stmt.where(LogspacePresence.since >= fresh_after)
        stmt = stmt.order_by(LogspacePresence.customer_code.asc(), LogspacePresence.since.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def remove(self, customer_code: str, presence_id: str) -> bool:
        row = await self.db.scalar(
            select(LogspacePresence).where(
                LogspacePresence.id == presence_id,
                LogspacePresence.customer_code == customer_code,
            )
        )
        if row is None:
            return False
        await self.db.delete(row)
        await self.db.commit()
        return True

    async def sweep(self, older_than: datetime) -> int:
        """Bulk-delete presence rows last refreshed before `older_than`. Returns the count removed."""
        result = await self.db.execute(
            delete(LogspacePresence).where(LogspacePresence.since < older_than)
        )
        await self.db.commit()
        return result.rowcount or 0


def get_logspace_presence_repository(
    db: AsyncSession = Depends(get_session),
) -> LogspacePresenceRepository:
    """FastAPI dependency — provides LogspacePresenceRepository with the request session injected."""
    return LogspacePresenceRepository(db)
