"""The registry of how far each incremental reader has consumed, and what retention may do about it.

Retention drops partitions past 60 days. It already refuses to drop a day Stage 2 has not finished
stitching. A slow READER needs the same protection: dropping day 70 while ML sits at day 70 destroys
that data permanently, and ML would simply skip the gap without anything recording it.

So every incremental consumer publishes one number here, and retention gates on the minimum.

The hard call in this module is what to do about a consumer that stops reporting:

    Blocking forever fills the disk — a total outage for everyone.
    Never blocking loses data for one consumer — bad, but contained.

So a stale consumer stops blocking and is logged CRITICAL. That is choosing the survivable failure,
and making it loud rather than letting it be discovered later. The staleness threshold is generous
enough that an ordinary deploy or restart never trips it, or the alarm would be noise and get ignored.
"""

import logging
from datetime import date as date_type, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.consumer_cursor import ConsumerCursor
from app.persistence.models.notification import NotificationRule, RuleStatus
from app.settings import settings

logger = logging.getLogger(__name__)

#: The name notifications publishes under. One entry for the subsystem, not one per rule — rules are
#: an internal detail, and retention only cares about the oldest position the subsystem still needs.
NOTIFICATIONS = "notifications"


def blocks(day: date_type, *, min_position: datetime | None) -> bool:
    """Whether this UTC day must be kept because some consumer has not finished reading it.

    `position` means "everything strictly before here is consumed", so a day is safe only once the
    slowest reader has passed its END. A consumer partway through still blocks: dropping the day would
    lose the remainder, and its cursor would simply move past the gap without noticing.

    No consumers means nothing is blocked. That default matters — failing closed here would freeze
    retention on an empty registry and fill the disk.
    """
    if min_position is None:
        return False
    day_end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    return min_position < day_end


def is_live(updated_at: datetime, *, now: datetime | None = None) -> bool:
    """Whether this consumer is still reporting, as opposed to gone."""
    now = now or datetime.now(timezone.utc)
    return (now - updated_at).total_seconds() <= settings.consumer_cursor_stale_after_seconds


async def report(db: AsyncSession, consumer: str, *, position: datetime) -> None:
    """Publish this consumer's position. Upsert — one row per consumer, forever.

    Does not commit; the caller owns the transaction boundary, so the published position lands with
    whatever work justified it rather than getting ahead of it.
    """
    now = datetime.now(timezone.utc)
    stmt = pg_insert(ConsumerCursor).values(consumer=consumer, position=position, updated_at=now)
    await db.execute(stmt.on_conflict_do_update(
        index_elements=[ConsumerCursor.consumer],
        set_={"position": position, "updated_at": now}))


async def min_live_position(db: AsyncSession) -> datetime | None:
    """The oldest position among consumers that are still reporting, or None if there are none.

    Stale consumers are excluded AND logged CRITICAL. Excluding them is deliberate — a consumer that
    died weeks ago would otherwise hold retention hostage until the disk filled — but it does mean
    that consumer is about to lose data, which is exactly the kind of thing that must never happen
    quietly.
    """
    rows = (await db.execute(select(ConsumerCursor.consumer, ConsumerCursor.position,
                                    ConsumerCursor.updated_at))).all()
    live, stale = _split_by_liveness(rows, now=datetime.now(timezone.utc))
    _alarm_on_stale(stale)
    return min((position for _c, position, _u in live), default=None)


def _split_by_liveness(rows: list, *, now: datetime) -> tuple[list, list]:
    """(still reporting, gone). One pass, so a row cannot end up in both or neither."""
    live, stale = [], []
    for row in rows:
        (live if is_live(row[2], now=now) else stale).append(row)
    return live, stale


def _alarm_on_stale(stale: list) -> None:
    """Announce consumers that have stopped reporting and are therefore no longer protected.

    CRITICAL because it is silent otherwise: the consumer keeps running (or does not), retention
    quietly moves past it, and the gap is only discovered when someone asks why the data is missing.
    """
    if not stale:
        return
    logger.critical(
        "Consumer cursor(s) stale and now IGNORED by retention, so data they have not read may be "
        "dropped: %s. Restart them or remove their registry rows.",
        ", ".join(f"{c} (last seen {u.isoformat()})" for c, _p, u in stale))


async def notifications_position(db: AsyncSession) -> datetime | None:
    """The oldest position across notifications' ACTIVE streaming rules, or None.

    None when nothing needs protecting — no active rules, or none that have run yet. A NULL
    `cursor_at` means "never run", not "at the beginning of time"; treating it as the latter would pin
    retention at the epoch and stop it dropping anything, ever.

    SQL's `MIN` already skips NULLs (measured, not assumed), so the explicit `isnot(None)` below is
    redundant for correctness. It is kept to state the intent at the point it matters, since the
    behaviour depends on aggregate semantics a reader would otherwise have to know by heart.
    """
    return await db.scalar(
        select(func.min(NotificationRule.cursor_at)).where(
            NotificationRule.status == RuleStatus.active.value,
            NotificationRule.cursor_at.isnot(None)))


async def report_notifications(db: AsyncSession) -> None:
    """Publish notifications' position, if it has one to publish."""
    position = await notifications_position(db)
    if position is not None:
        await report(db, NOTIFICATIONS, position=position)
