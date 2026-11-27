"""Rebuilding stored rollups from the stored facts, one local day at a time.

Used after a change to what a rollup row holds (chunk 232 gave every row its place): the rows
already stored keep their old shape until their buckets are folded again. Each day is folded
straight from the facts with the writer's own fold, under the writer's tenant lock, and the disk
is looked at before the next day. It queues no ticket on purpose (chunk 235): a ticket makes the
fact writer compare the facts with the source transactions, which are kept 60 days only, and a
rebuild has no business touching facts. Used by `scripts/refold_rollups_by_day.py`; nothing in
the serving path imports this.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Awaitable, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import select, text

from app.config.database import SessionFactory
from app.services.analytics import capture, consume, registry, rollups
from app.settings import settings

#: Positions in a `/proc/diskstats` line, after the three leading columns (major, minor, name).
_WRITES_COMPLETED, _MS_WRITING = 4, 7


@dataclass(slots=True)
class Progress:
    """What a run did: the days refolded, and where and why it stopped early, if it did."""

    days_done: list[date] = field(default_factory=list)
    stopped_at: date | None = None
    reason: str | None = None


def days_between(start: date, end: date) -> list[date]:
    """Every day from `start` to `end`, both included, oldest first.

    >>> days_between(date(2026, 9, 29), date(2026, 10, 1))
    [datetime.date(2026, 9, 29), datetime.date(2026, 9, 30), datetime.date(2026, 10, 1)]
    >>> days_between(date(2026, 10, 2), date(2026, 10, 1))
    []
    """
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def local_day(day: date, tz: str) -> tuple[datetime, datetime]:
    """The UTC instants at which a tenant-local day starts and ends.

    >>> lo, hi = local_day(date(2026, 9, 10), "Europe/London")
    >>> lo.isoformat(), hi.isoformat()
    ('2026-09-09T23:00:00+00:00', '2026-09-10T23:00:00+00:00')
    """
    zone = ZoneInfo(tz)
    lo = datetime.combine(day, time.min, tzinfo=zone)
    hi = datetime.combine(day + timedelta(days=1), time.min, tzinfo=zone)
    return lo.astimezone(timezone.utc), hi.astimezone(timezone.utc)


def write_wait_ms(before: str, after: str, device: str) -> float | None:
    """How long one write waited on a disk, on average, between two `/proc/diskstats` readings.

    None when the device is in neither reading; 0.0 when no write completed in between.

    >>> a = "   8       0 sda 100 0 800 50 200 0 1600 1000 0 0 0"
    >>> b = "   8       0 sda 100 0 800 50 210 0 1680 3500 0 0 0"
    >>> write_wait_ms(a, b, "sda"), write_wait_ms(a, a, "sda"), write_wait_ms(a, b, "sdb")
    (250.0, 0.0, None)
    """
    def counters(text: str) -> tuple[int, int] | None:
        for line in text.splitlines():
            parts = line.split()
            if len(parts) > 3 + _MS_WRITING and parts[2] == device:
                return int(parts[3 + _WRITES_COMPLETED]), int(parts[3 + _MS_WRITING])
        return None

    first, second = counters(before), counters(after)
    if first is None or second is None:
        return None
    writes, waited = second[0] - first[0], second[1] - first[1]
    return 0.0 if writes <= 0 else waited / writes


async def fold_day(session_factory: SessionFactory, customer_code: str, day: date, tz: str, *,
                   now: datetime | None = None) -> dict:
    """Fold every rollup bucket of one local day again from the stored facts; the fold's counts.

    (10 Sep with facts) -> {"definitions": 2, "rows_written": 14, ...} ; (a day with none) -> its
    buckets are emptied
    """
    lo, hi = local_day(day, tz)
    now = now or datetime.now(timezone.utc)
    # Step 1: Leave the hourly level alone for a day older than hourly rows are kept, otherwise
    # the fold writes hourly rows that retention has already dropped once.
    is_hourly_kept = day >= now.date() - timedelta(days=rollups.HOURLY_RETENTION_DAYS)
    hours = rollups.hours_in(lo, hi - timedelta(hours=1)) if is_hourly_kept else set()
    async with session_factory() as db:
        # Step 2: Take the fact writer's tenant lock, so that the rollup tables still have one
        # writer at a time and a live fold of the same bucket cannot interleave with this one.
        await db.execute(select(consume.writer_lock(customer_code)))
        await db.execute(text(
            f"SET LOCAL statement_timeout = {settings.analytics_fold_statement_timeout_ms}"))
        # Step 3: Fold with the writer's own function and switches, because a second way to
        # compute a total would be a second answer to the same question.
        await registry.ensure_seed(db, customer_code)
        stats = await rollups.fold_all(
            db, customer_code, await registry.active_definitions(db, customer_code),
            hours=hours, dates={day}, rec_hours=hours, rec_dates={day},
            hidden=await capture.hidden_names(db, customer_code), tz=tz, now=now, changed=None)
        await db.commit()
    return stats


async def refold_range(session_factory: SessionFactory, customer_code: str, *, start: date,
                       end: date, tz: str, max_write_wait_ms: float,
                       read_write_wait: Callable[[], Awaitable[float | None]],
                       pause_seconds: float = 0.0,
                       on_day: Callable[[date, float | None], None] | None = None) -> Progress:
    """Refold the rollups of every day in the range, oldest first, one day at a time.

    (3 days, healthy disk) -> Progress(days_done=[d1, d2, d3])
    ; (disk slow before d2) -> Progress(days_done=[d1], stopped_at=d2, reason="disk ...")
    """
    progress = Progress()
    for day in days_between(start, end):
        # Step 1: Look at the disk before adding work, because this server's disk stalls under
        # load and everything else on it, ingestion first, waits behind a stalled write.
        waited = await read_write_wait()
        if waited is not None and waited > max_write_wait_ms:
            progress.stopped_at = day
            progress.reason = f"disk write wait {waited:.0f} ms is above {max_write_wait_ms:.0f} ms"
            return progress
        # Step 2: Fold one day in its own transaction, so that a failure keeps the days already
        # done and the lock is never held across the pause.
        await fold_day(session_factory, customer_code, day, tz)
        progress.days_done.append(day)
        if on_day is not None:
            on_day(day, waited)
        if pause_seconds:
            await asyncio.sleep(pause_seconds)
    return progress
