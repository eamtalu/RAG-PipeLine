"""The watchlist rows (stage D, step 14): adding a watch under the limit (or finding the one that
exists), ending and resuming one, the alert rule, the events of a status change with their seen
flag, the "Not now" answers, and the claim the tick makes. Used by runner.py, commands.py,
mirror.py and suggest.py.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.watch import KINDS, Watch, WatchEvent, WatchNotNow
from app.services.watchlist import registry
from app.settings import settings

NOT_NOW_HOURS = 24


class Full(Exception):
    """The person watches the limit already; carries their watches quietest first for the swap."""

    def __init__(self, watches: list[Watch], limit: int):
        super().__init__(f"you watch {limit} things already")
        self.watches, self.limit = watches, limit


def _key(raw) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(raw))
    except (ValueError, TypeError, AttributeError):
        return None


async def active_for_owner(db: AsyncSession, customer_code: str, owner_object_id: str) -> list[Watch]:
    """The person's active watches, newest first."""
    return list((await db.execute(select(Watch).where(
        Watch.customer_code == customer_code, Watch.owner_object_id == owner_object_id, Watch.status == "active")
        .order_by(Watch.created_at.desc()))).scalars())


async def count_active(db: AsyncSession, customer_code: str, owner_object_id: str) -> int:
    return int(await db.scalar(select(func.count()).select_from(Watch).where(
        Watch.customer_code == customer_code, Watch.owner_object_id == owner_object_id, Watch.status == "active")) or 0)


async def get(db: AsyncSession, customer_code: str, watch_id) -> Watch | None:
    key = _key(watch_id)
    if key is None:
        return None
    return await db.scalar(select(Watch).where(Watch.customer_code == customer_code, Watch.id == key))


async def find_active(db: AsyncSession, customer_code: str, owner_object_id: str, kind: str, entity_id: str) -> Watch | None:
    return await db.scalar(select(Watch).where(
        Watch.customer_code == customer_code, Watch.owner_object_id == owner_object_id, Watch.kind == kind,
        Watch.entity_id == entity_id, Watch.status == "active"))


async def quietest_first(db: AsyncSession, customer_code: str, owner_object_id: str, *, now: datetime) -> list[Watch]:
    """The person's active watches with the least to say first: fewest events in the last week,
    then the one that changed longest ago (never changed first), then the oldest."""
    week = now - timedelta(days=7)
    recent = (select(WatchEvent.watch_id, func.count().label("n"))
              .where(WatchEvent.customer_code == customer_code, WatchEvent.owner_object_id == owner_object_id,
                     WatchEvent.at >= week).group_by(WatchEvent.watch_id).subquery())
    rows = (await db.execute(
        select(Watch, func.coalesce(recent.c.n, 0)).outerjoin(recent, recent.c.watch_id == Watch.id)
        .where(Watch.customer_code == customer_code, Watch.owner_object_id == owner_object_id, Watch.status == "active")
        .order_by(func.coalesce(recent.c.n, 0), Watch.last_changed_at.asc().nulls_first(), Watch.created_at))).all()
    return [row[0] for row in rows]


async def add(db: AsyncSession, customer_code: str, *, owner_object_id: str, owner_name: str | None, kind: str,
              entity_id: str, entity_ref: dict[str, Any], label: str, site_id: str | None, alert: dict[str, Any],
              until: datetime | None, via: str, now: datetime) -> tuple[Watch, bool]:
    """Add a watch for a person, or find the active one they already have for the same thing;
    (watch, whether it is new). Full when the person is at the limit.

    (first "D-48213") -> (watch, True) ; (again) -> (same watch, False) ; (11th thing) -> Full
    """
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    # Step 1: One person adds under one lock, so two taps at once cannot both see nine watches
    # and both land an eleventh
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"watch:{customer_code}:{owner_object_id}"})
    existing = await find_active(db, customer_code, owner_object_id, kind, entity_id)
    if existing is not None:
        return existing, False
    limit = int(settings.teams_watches_limit)
    if await count_active(db, customer_code, owner_object_id) >= limit:
        raise Full(await quietest_first(db, customer_code, owner_object_id, now=now), limit)
    # Step 2: Insert under the partial unique index, so even a second process adding the same
    # thing at the same moment ends with one row, which both then read back
    values = dict(id=uuid.uuid4(), customer_code=customer_code, owner_object_id=owner_object_id, owner_name=owner_name,
                  kind=kind, entity_id=entity_id, entity_ref=dict(entity_ref or {}), label=label[:200], site_id=site_id,
                  alert=dict(alert), also_teams=False, status="active", until=until, next_check_at=now, change_count=0,
                  created_via=via, created_at=now, updated_at=now)
    stmt = insert(Watch).values(**values).on_conflict_do_nothing(
        index_elements=["customer_code", "owner_object_id", "kind", "entity_id"], index_where=text("status = 'active'"))
    await db.execute(stmt)
    watch = await find_active(db, customer_code, owner_object_id, kind, entity_id)
    if watch is None:
        raise ValueError("the watch could not be added")
    return watch, watch.id == values["id"]


def end(watch: Watch, reason: str, *, now: datetime) -> None:
    """Close a watch for good with the reason; its place is free from now."""
    watch.status, watch.ended_reason, watch.ended_at, watch.updated_at = "ended", reason, now, now
    watch.next_check_at = None


async def resume(db: AsyncSession, watch: Watch, *, now: datetime) -> None:
    """Reopen an ended watch (a receipt's Undo); ValueError when the same thing is watched again meanwhile."""
    clash = await find_active(db, watch.customer_code, watch.owner_object_id, watch.kind, watch.entity_id)
    if clash is not None and clash.id != watch.id:
        raise ValueError("you are already watching it again")
    if await count_active(db, watch.customer_code, watch.owner_object_id) >= int(settings.teams_watches_limit):
        raise ValueError("your watchlist is full again")
    watch.status, watch.ended_reason, watch.ended_at, watch.updated_at = "active", None, None, now
    watch.next_check_at = now


def set_alert(watch: Watch, alert: dict[str, Any], *, until: datetime | None, now: datetime) -> None:
    watch.alert, watch.until, watch.updated_at, watch.next_check_at = dict(alert), until, now, now


async def record_event(db: AsyncSession, watch: Watch, *, old: str | None, new: str, summary: str,
                       value: dict[str, Any] | None, is_alert: bool, now: datetime) -> WatchEvent:
    """One status change: the ledger line and the watch's own stamp, in the caller's transaction,
    so a restart repeats nothing (the status is persisted with the line)."""
    watch.change_count = int(watch.change_count or 0) + 1
    watch.last_status, watch.last_changed_at, watch.updated_at = new, now, now
    event = WatchEvent(id=uuid.uuid4(), customer_code=watch.customer_code, watch_id=watch.id,
                       owner_object_id=watch.owner_object_id, seq=watch.change_count, at=now, old_status=old,
                       new_status=new, is_alert=is_alert, summary=summary, value=value)
    db.add(event)
    return event


async def events_since(db: AsyncSession, customer_code: str, owner_object_id: str, since: datetime, *,
                       limit: int = 200) -> list[WatchEvent]:
    """The person's events since the moment, newest first."""
    return list((await db.execute(select(WatchEvent).where(
        WatchEvent.customer_code == customer_code, WatchEvent.owner_object_id == owner_object_id, WatchEvent.at >= since)
        .order_by(WatchEvent.at.desc()).limit(limit))).scalars())


async def unseen_count(db: AsyncSession, customer_code: str, owner_object_id: str) -> int:
    return int(await db.scalar(select(func.count()).select_from(WatchEvent).where(
        WatchEvent.customer_code == customer_code, WatchEvent.owner_object_id == owner_object_id,
        WatchEvent.seen_at.is_(None))) or 0)


async def mark_seen(db: AsyncSession, customer_code: str, owner_object_id: str, event_ids, *, now: datetime) -> int:
    """Mark the named events seen, or every unseen one when none are named; how many changed."""
    stmt = update(WatchEvent).where(WatchEvent.customer_code == customer_code,
                                    WatchEvent.owner_object_id == owner_object_id, WatchEvent.seen_at.is_(None))
    keys = [k for k in (_key(e) for e in (event_ids or [])) if k is not None]
    if event_ids and not keys:
        return 0
    if keys:
        stmt = stmt.where(WatchEvent.id.in_(keys))
    result = await db.execute(stmt.values(seen_at=now))
    return int(result.rowcount or 0)


async def not_now(db: AsyncSession, customer_code: str, owner_object_id: str, kind: str, entity_id: str, *,
                  now: datetime, hours: float = NOT_NOW_HOURS) -> datetime:
    """Remember a "Not now" for a thing until the window passes; when it ends."""
    until = now + timedelta(hours=hours)
    stmt = insert(WatchNotNow).values(customer_code=customer_code, owner_object_id=owner_object_id, kind=kind,
                                      entity_id=entity_id, until=until)
    await db.execute(stmt.on_conflict_do_update(
        index_elements=["customer_code", "owner_object_id", "kind", "entity_id"], set_={"until": until}))
    return until


async def is_not_now(db: AsyncSession, customer_code: str, owner_object_id: str, kind: str, entity_id: str, *,
                     now: datetime) -> bool:
    until = await db.scalar(select(WatchNotNow.until).where(
        WatchNotNow.customer_code == customer_code, WatchNotNow.owner_object_id == owner_object_id,
        WatchNotNow.kind == kind, WatchNotNow.entity_id == entity_id))
    return until is not None and until > now


async def claim_due(db: AsyncSession, *, now: datetime, customer_code: str | None = None, limit: int = 1000) -> list[Watch]:
    """The active watches whose check is due, locked for this tick (a second consumer skips them)."""
    q = select(Watch).where(Watch.status == "active", (Watch.next_check_at.is_(None)) | (Watch.next_check_at <= now))
    if customer_code is not None:
        q = q.where(Watch.customer_code == customer_code)
    return list((await db.execute(q.order_by(Watch.next_check_at.asc().nulls_first()).limit(limit)
                                  .with_for_update(skip_locked=True))).scalars())


async def owners_with_watches(db: AsyncSession, customer_code: str) -> list[str]:
    """Everyone of a customer with an active watch or an event today, so their rows get rewritten."""
    rows = (await db.execute(select(Watch.owner_object_id).where(
        Watch.customer_code == customer_code, Watch.status == "active").distinct())).scalars()
    return sorted(set(rows))


async def customers_with_watches(db: AsyncSession) -> list[str]:
    rows = (await db.execute(select(Watch.customer_code).where(Watch.status == "active").distinct())).scalars()
    return sorted(set(rows))


def cadence_of(kind: str) -> timedelta:
    return timedelta(seconds=registry.CADENCE_SECONDS.get(kind, 300))
