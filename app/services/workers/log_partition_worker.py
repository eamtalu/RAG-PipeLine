"""Keeps the daily log partitions provisioned ahead of ingestion, and drops them past retention.

Two jobs per tick, both idempotent — which is what makes an hourly cadence and the occasional missed
tick harmless.

**Create** covers today through today + `log_partition_precreate_days`. This is the half that must not
fail quietly: an insert into a day with no partition fails outright with "no partition of relation
found for row", so exhausting the runway stops Stage 1 dead. The failure is silent until the runway
runs out days later, so a creation error is logged CRITICAL and the remaining coverage is reported on
every tick rather than only when something is built.

**Drop** reclaims disk by unlinking a day's file instead of `DELETE` + `VACUUM` reading the whole
table — the reason the tables were partitioned at all. Being irreversible, it sits behind three gates:

  1. the day is older than `log_partition_retention_days`;
  2. no OPEN `log_regroup_pending` window overlaps it, so data Stage 2 has not stitched yet is never
     destroyed;
  3. entries lag transactions by one day (see `droppable_days`).

Creation runs FIRST and independently of the drop. A drop failure reclaims no disk and retries next
tick, which is survivable; a creation failure is not, so it must never be prevented by one.

Concurrency: this runs inside the singleton worker process, which already holds a session-scoped
advisory lock, so there is exactly one instance. The DDL is `IF NOT EXISTS` / `IF EXISTS` regardless,
so a second runner would be harmless rather than an error.
"""

import asyncio
import logging
from datetime import date as date_type

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence import partitioning as pt
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.settings import settings

logger = logging.getLogger(__name__)

#: Tables whose partitions must outlive the transactions that can reference them by one day.
#: A transaction spans at most the seal window, so one starting at 23:58 owns entries until ~00:13 the
#: next day. `log_entry_assignment` is co-partitioned with `log_entries` and so lags identically —
#: if the two schedules ever diverged, a day of assignments would be left pointing at entries that no
#: longer exist.
_LAG_ONE_DAY = frozenset({"log_entries", "log_entry_assignment"})


async def db_today(db: AsyncSession) -> date_type:
    """Today in UTC, per the DATABASE clock.

    Not the app host's clock. Partition bounds are cut on the database's notion of time, and the two
    machines' clocks have already drifted far enough in this project to make tests flap; a worker that
    disagreed about what day it is would build the wrong runway or drop the wrong day.
    """
    return await db.scalar(text("SELECT (now() AT TIME ZONE 'UTC')::date"))


def droppable_days(table: str, covered: list[date_type], today: date_type) -> list[date_type]:
    """Which of `covered` are past retention for `table`, oldest first.

    `log_transactions` is dropped at the retention cutoff. `log_entries` and `log_entry_assignment`
    are held one day LONGER, and that asymmetry is the midnight rule rather than a safety margin: a
    transaction starting at 23:58 owns entries into the next day, so dropping day N's entries while a
    day N-1 transaction still references them would leave that transaction rendering half-empty, with
    nothing anywhere recording that its body used to exist.

    The direction matters. Dropping transactions ahead of entries leaves at most one boundary day of
    assignments pointing at a transaction that is gone — harmless, since nothing queries by a
    transaction id that no longer exists, and self-healing when that entry day is dropped a day later.
    Dropping entries ahead of transactions is the one that loses information a reader would notice.
    """
    retention = settings.log_partition_retention_days + (1 if table in _LAG_ONE_DAY else 0)
    return pt.expired_days(covered, today, retention_days=retention)


def _overlaps(day: date_type, start, end) -> bool:
    """Whether a stitch window `[start, end]` touches `day`.

    Inclusive at the boundaries, because windows are padded and routinely straddle midnight; a
    comparison that missed the edge would drop the very day a straddling window is about to rebuild.
    """
    return start < pt.day_end(day) and end >= pt.day_start(day)


async def days_blocked_by_pending(db: AsyncSession, days: list[date_type]) -> set[date_type]:
    """Of `days`, those overlapped by an OPEN stitch window.

    Open means neither consumed nor abandoned — the same definition the read gate and
    `GET /regroup/status` use, so an operator sees one consistent notion of outstanding work.
    Consumed windows have already been stitched; abandoned ones are parked awaiting a human, and
    letting either pin retention would mean one dead-lettered window stops disk being reclaimed
    forever.

    Overlap is inclusive of the boundaries, because stitch windows are padded and routinely straddle
    midnight; a comparison that missed the boundary would drop the very day a straddling window is
    about to rebuild.
    """
    if not days:
        return set()
    # One query over the whole candidate span, then map to days in Python. Asking per day would be
    # one round-trip per expired partition, and the set is small either way.
    rows = (await db.execute(
        select(LogRegroupPending.range_start, LogRegroupPending.range_end).where(
            LogRegroupPending.consumed_at.is_(None),
            LogRegroupPending.abandoned_at.is_(None),
            LogRegroupPending.range_start < pt.day_end(max(days)),
            LogRegroupPending.range_end >= pt.day_start(min(days)),
        )
    )).all()
    return {d for d in days for start, end in rows if _overlaps(d, start, end)}


async def _create_runway(db: AsyncSession, today: date_type) -> int:
    days = pt.coverage_days(today, ahead=settings.log_partition_precreate_days)
    return await pt.ensure_coverage(db, days=days)


async def _drop_each(db: AsyncSession, candidates: dict[str, list[date_type]],
                     blocked: set[date_type]) -> list[str]:
    """Drop each candidate day that nothing is holding open. Returns the partition names removed."""
    dropped = []
    for table, days in candidates.items():
        for day in days:
            if day in blocked:
                continue
            await db.execute(text(pt.drop_partition_sql(table, day)))
            dropped.append(pt.partition_name(table, day))
    return dropped


async def _expired_candidates(db: AsyncSession, today: date_type) -> dict[str, list[date_type]]:
    """Per table, the days past ITS retention (entries and assignments lag by one, see droppable_days).

    Gathered for all three tables before anything is dropped, so the pending gate below can be applied
    once across their union: a day held open must protect entries, transactions and assignments
    together, and gating them separately would leave exactly the torn state the gate exists to prevent.
    """
    return {t.table: droppable_days(t.table, await pt.covered_days(db, t.table), today)
            for t in pt.PARTITIONED}


async def _drop_expired(db: AsyncSession, today: date_type) -> list[str]:
    """Drop every partition past retention that no open stitch window protects. Returns their names.

    The pending check is done ONCE over the union of candidate days rather than per table, so a day
    held open protects entries, transactions and assignments together — protecting only some of them
    would leave exactly the torn state the gate exists to prevent.
    """
    candidates = await _expired_candidates(db, today)
    blocked = await days_blocked_by_pending(
        db, sorted({d for days in candidates.values() for d in days}))
    if blocked:
        logger.info("Partition retention: holding %d day(s) with unstitched windows: %s",
                    len(blocked), ", ".join(str(d) for d in sorted(blocked)))
    return await _drop_each(db, candidates, blocked)


async def run_once(db: AsyncSession) -> dict:
    """One full cycle: extend the runway, then drop what has expired.

    The two halves are isolated from each other on purpose. A drop failure reclaims no disk and
    retries next tick; a creation failure eventually stops ingestion. Letting the first prevent the
    second would turn a survivable problem into an outage.
    """
    today = await db_today(db)
    stats: dict = {"today": today.isoformat(), "created": 0, "dropped": [], "errors": []}

    try:
        stats["created"] = await _create_runway(db, today)
    except Exception as exc:
        # CRITICAL rather than ERROR: nothing breaks NOW, so this is invisible until the runway is
        # exhausted days later and every insert starts failing. It needs to page someone while there
        # is still runway left to fix it in.
        logger.critical("Partition creation FAILED (%s). Ingestion will stop once the existing "
                        "runway is exhausted — fix this before then.", exc, exc_info=True)
        stats["errors"].append(f"create: {exc}")

    try:
        stats["dropped"] = await _drop_expired(db, today)
    except Exception as exc:
        logger.warning("Partition retention failed (%s) — no disk reclaimed, retrying next tick.",
                       exc, exc_info=True)
        stats["errors"].append(f"drop: {exc}")

    await db.commit()
    # Reported every tick, not only when something was built: a shortfall is the signal that matters,
    # and "created 0" alone cannot distinguish "fully provisioned" from "creation has been broken for
    # a week".
    stats["days_ahead"] = await days_of_runway(db, today)
    return stats


async def _runway_for(db: AsyncSession, table: str, today: date_type) -> int:
    """Days ahead of `today` that one table is provisioned; -1 when it has no future partition."""
    days = [d for d in await pt.covered_days(db, table) if d >= today]
    return (max(days) - today).days if days else -1


async def days_of_runway(db: AsyncSession, today: date_type) -> int:
    """How many days ahead of `today` every partitioned table is provisioned.

    The MINIMUM across tables, because ingestion fails as soon as ANY of them lacks the day being
    written — reporting the best-covered table would hide exactly the problem this measures.
    """
    return min([await _runway_for(db, t.table, today) for t in pt.PARTITIONED], default=-1)


def report(stats: dict) -> None:
    """Log one tick's outcome.

    The runway warning fires on EVERY tick it applies to, not only when something changed: a shortfall
    is the signal that matters, and "created 0" alone cannot distinguish "fully provisioned" from
    "creation has been broken for a week".
    """
    if stats["created"] or stats["dropped"]:
        logger.info("Partition maintenance: created=%d dropped=%d (%s), %d day(s) of runway",
                    stats["created"], len(stats["dropped"]),
                    ", ".join(stats["dropped"]) or "-", stats["days_ahead"])
    if stats["days_ahead"] < settings.log_partition_min_runway_days:
        logger.critical("Only %d day(s) of partition runway left (want >= %d). Ingestion STOPS when "
                        "it reaches zero.", stats["days_ahead"], settings.log_partition_min_runway_days)


async def _tick() -> None:
    """One iteration, in its own session. Errors never kill the loop — both halves are idempotent, so
    the next tick simply redoes whatever did not land.

    No `except asyncio.CancelledError: raise` here: since Python 3.8 CancelledError inherits from
    BaseException, so shutdown propagates through the handler below untouched and an explicit re-raise
    would be redundant branching that reads as if it were load-bearing.
    """
    try:
        async with async_session() as db:
            stats = await run_once(db)
    except Exception:
        logger.exception("Partition worker tick failed — retrying next tick")
        return
    report(stats)


async def run_log_partition_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Log partition worker started (interval=%ds, precreate=%dd, retention=%dd)",
                settings.log_partition_worker_interval_seconds,
                settings.log_partition_precreate_days,
                settings.log_partition_retention_days)
    while True:
        await _tick()
        await asyncio.sleep(settings.log_partition_worker_interval_seconds)
