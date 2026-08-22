"""Keeps every partitioned table provisioned ahead of ingestion, and drops what is past retention.

Two jobs per tick, both idempotent — which is what makes an hourly cadence and the occasional missed
tick harmless.

**Create** covers today through today + `log_partition_precreate_days`. This is the half that must not
fail quietly: an insert into a day with no partition fails outright with "no partition of relation
found for row", so exhausting the runway stops Stage 1 dead. The failure is silent until the runway
runs out days later, so a creation error is logged CRITICAL and the remaining coverage is reported on
every tick rather than only when something is built.

**Drop** reclaims disk by unlinking a day's file instead of `DELETE` + `VACUUM` reading the whole
table — the reason the tables were partitioned at all. Being irreversible, it sits behind four gates:

  1. the partition is past ITS retention, which may be `log_partition_retention_days`, a per-table
     override, or never (see `KEEP_FOREVER` and `retention_days_for`);
  2. no OPEN `log_regroup_pending` window overlaps it, so data Stage 2 has not stitched yet is never
     destroyed;
  3. no live consumer is still reading it (see `consumer_cursors`);
  4. entries lag transactions by one day (see `droppable_days`).

Each table declares its own GRAIN in `partitioning.PARTITIONED`, so a partition is not necessarily a
day. Every gate here therefore compares against the partition's own span rather than a date: keying
on the first day of a monthly partition would expire it up to 30 days early, release it while a writer
or reader was still inside it, and report a freshly created month as zero runway.

Creation runs FIRST and independently of the drop. A drop failure reclaims no disk and retries next
tick, which is survivable; a creation failure is not, so it must never be prevented by one.

Concurrency: this runs inside the singleton worker process, which already holds a session-scoped
advisory lock, so there is exactly one instance. The DDL is `IF NOT EXISTS` / `IF EXISTS` regardless,
so a second runner would be harmless rather than an error.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import date as date_type, datetime, timedelta, timezone

from sqlalchemy import distinct, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence import partitioning as pt
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.services import consumer_cursors
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.settings import settings

logger = logging.getLogger(__name__)

#: Tables whose partitions must outlive the transactions that can reference them by one day.
#: A transaction spans at most the seal window, so one starting at 23:58 owns entries until ~00:13 the
#: next day. `log_entry_assignment` is co-partitioned with `log_entries` and so lags identically —
#: if the two schedules ever diverged, a day of assignments would be left pointing at entries that no
#: longer exist.
_LAG_ONE_DAY = frozenset({"log_entries", "log_entry_assignment"})

#: Tables whose partitions are NEVER dropped. `droppable_days` returns nothing for these, whatever
#: `log_partition_retention_days` is set to, so no configuration change can reach them.
#:
#: The analytics fact table, its ledger and the daily rollups. They are not merely long-lived: their raw
#: source is dropped at 60 days, so a dropped partition here cannot be rebuilt from anything at all.
#: Before this set existed there was no way to express that, and registering such a table would have had
#: the worker delete it a month at a time.
#:
#: `analytics_monthly_rollups` is kept forever too but is absent, because it is not partitioned: this set
#: governs partition drops and nothing else.
KEEP_FOREVER: frozenset[str] = frozenset({
    "analytics_facts",
    "analytics_fact_ledger",
    "analytics_daily_rollups",
})

#: Retention in days for tables that do NOT follow `log_partition_retention_days`, keyed by table.
#: A table listed in KEEP_FOREVER ignores this. Every partitioned analytics table appears in one
#: collection or the other: appearing in neither means silently inheriting the log tables' 60 days, and
#: a test asserts that cannot happen.
RETENTION_DAYS: dict[str, int] = {
    # The shortest-lived level: a request older than this resolves to the daily grain anyway.
    "analytics_hourly_rollups": 90,
    # Long enough to explain a total that was questioned months later, bounded so a permanently broken
    # source cannot grow it forever.
    "analytics_quality_issues": 365,
}


async def db_today(db: AsyncSession) -> date_type:
    """Today in UTC, per the DATABASE clock.

    Not the app host's clock. Partition bounds are cut on the database's notion of time, and the two
    machines' clocks have already drifted far enough in this project to make tests flap; a worker that
    disagreed about what day it is would build the wrong runway or drop the wrong day.
    """
    return await db.scalar(text("SELECT (now() AT TIME ZONE 'UTC')::date"))


def retention_days_for(table: str) -> int | None:
    """How many days `table` keeps a partition after its LAST day, or None to keep it forever.

    Read at call time, not frozen into a module constant, because the log retention is an environment
    setting and a constant would pin whatever it happened to be at import.
    """
    if table in KEEP_FOREVER:
        return None
    base = RETENTION_DAYS.get(table, settings.log_partition_retention_days)
    return base + (1 if table in _LAG_ONE_DAY else 0)


def droppable_days(table: str, covered: list[date_type], today: date_type) -> list[date_type]:
    """Which of `covered` partition starts are past retention for `table`, oldest first.

    A keep-forever table returns nothing at all. That is the only branch here that can prevent data
    loss rather than cause it, so it comes first and it is total: no grain, cutoff or setting is
    consulted afterwards.

    `log_transactions` is dropped at the retention cutoff. `log_entries` and `log_entry_assignment`
    are held one day LONGER, and that asymmetry is the midnight rule rather than a safety margin: a
    transaction starting at 23:58 owns entries into the next day, so dropping day N's entries while a
    day N-1 transaction still references them would leave that transaction rendering half-empty, with
    nothing anywhere recording that its body used to exist.

    The direction matters. Dropping transactions ahead of entries leaves at most one boundary day of
    assignments pointing at a transaction that is gone — harmless, since nothing queries by a
    transaction id that no longer exists, and self-healing when that entry day is dropped a day later.
    Dropping entries ahead of transactions is the one that loses information a reader would notice.

    The comparison is made against each partition's last day, via the table's grain. For the three
    daily log tables that is the same as its first, so their behaviour is unchanged.
    """
    retention = retention_days_for(table)
    if retention is None:
        return []
    return pt.expired_days(covered, today, retention_days=retention, grain=pt.grain_of(table))


#: One candidate partition: which table, and which period start within it.
Period = tuple[str, date_type]


def _period_bounds(table: str, start: date_type) -> tuple[datetime, datetime]:
    """The `[start, end)` UTC instants one partition spans, at its table's grain.

    Half-open, matching the partition bounds themselves. Every gate below compares against these
    rather than against a day, because a monthly partition asked about by its first day would be
    released while a writer or a reader was still working in the middle of it.
    """
    grain = pt.grain_of(table)
    first = pt.period_start(grain, start)
    return pt.day_start(first), pt.day_start(pt.next_period_start(grain, first))


def _overlaps(bounds: tuple[datetime, datetime], start, end) -> bool:
    """Whether a stitch window `[start, end]` touches the half-open partition span `bounds`.

    Inclusive at the window's boundaries, because windows are padded and routinely straddle midnight;
    a comparison that missed the edge would drop the very partition a straddling window is about to
    rebuild.
    """
    lo, hi = bounds
    return start < hi and end >= lo


async def periods_blocked_by_pending(db: AsyncSession, periods: list[Period]) -> set[Period]:
    """Of `periods`, those overlapped by an OPEN stitch window.

    Open means neither consumed nor abandoned — the same definition the read gate and
    `GET /regroup/status` use, so an operator sees one consistent notion of outstanding work.
    Consumed windows have already been stitched; abandoned ones are parked awaiting a human, and
    letting either pin retention would mean one dead-lettered window stops disk being reclaimed
    forever.
    """
    if not periods:
        return set()
    bounds = {p: _period_bounds(*p) for p in periods}
    # One query over the whole candidate span, then map to periods in Python. Asking per partition
    # would be one round-trip each, and the set is small either way.
    lo = min(b[0] for b in bounds.values())
    hi = max(b[1] for b in bounds.values())
    rows = (await db.execute(
        select(LogRegroupPending.range_start, LogRegroupPending.range_end).where(
            LogRegroupPending.consumed_at.is_(None),
            LogRegroupPending.abandoned_at.is_(None),
            LogRegroupPending.range_start < hi,
            LogRegroupPending.range_end >= lo,
        )
    )).all()
    return {p for p, b in bounds.items() for start, end in rows if _overlaps(b, start, end)}


async def periods_blocked_by_consumers(db: AsyncSession, periods: list[Period]) -> set[Period]:
    """Of `periods`, those some incremental reader has not finished consuming.

    Same shape as `periods_blocked_by_pending`, and applied alongside it: Stage 2 must have finished
    WRITING a partition and every consumer must have finished READING it before it can go. Dropping a
    partition a consumer has not reached destroys that data permanently, and the consumer's cursor
    would simply move past the gap without noticing.

    A consumer that has stopped reporting is excluded upstream (and alarmed), so a dead reader cannot
    hold retention hostage until the disk fills.
    """
    if not periods:
        return set()
    floor = await consumer_cursors.min_live_position(db)
    return {p for p in periods
            if consumer_cursors.blocks_until(_period_bounds(*p)[1], min_position=floor)}


#: The tables whose partitions the analytics health gate can hold. The SOURCE only.
#:
#: Deliberately not the analytics tables themselves: gating those on analytics health would be circular,
#: and the fact table and its ledger are KEEP_FOREVER anyway. What has to survive a broken analytics
#: platform is the data a repair would read.
HEALTH_GATED: frozenset[str] = frozenset({"log_entries", "log_transactions", "log_entry_assignment"})


@dataclass(frozen=True)
class AnalyticsHealth:
    """Whether analytics is in a state where losing source data would be unrecoverable.

    `holding` is the answer; the two tenant lists are the explanation, because "retention is blocked"
    with no named cause is an alert nobody can act on.
    """

    holding: bool
    reason: str
    unhealthy_tenants: list[str]
    #: Tenants that ARE unhealthy but have been so for longer than the cap, so the hold was released for
    #: them. Their totals are now permanently unprovable once the partitions go, which is why the release
    #: is logged CRITICAL rather than merely noted.
    expired_tenants: list[str]


async def analytics_health(db: AsyncSession, *, now: datetime | None = None) -> AnalyticsHealth:
    """Which tenants are broken in a way that makes source retention dangerous.

    Unhealthy means a dead-lettered ticket (a range that will never be diffed) or a recorded cycle error.
    It deliberately does NOT mean an open ticket: those are the normal steady state at roughly one every
    70 seconds.

    "Unused" is not "unhealthy". A tenant with no state, or one that has never folded (NULL frontier),
    is not broken -- and the analytics worker ships disabled, so that is the normal condition on any
    instance that has not adopted it. Reading it as a fault would freeze partition drops everywhere.

    The hold is capped at `analytics_retention_hold_max_days`, measured from the tenant's last successful
    cycle. Past that the hold is released and logged CRITICAL: blocking forever fills the disk, which is
    a total outage, while losing the ability to prove one tenant's totals is contained. This is the same
    trade `consumer_cursors` makes for a consumer that has stopped reporting.
    """
    now = now or datetime.now(timezone.utc)
    rows = (await db.execute(select(
        AnalyticsTenantState.customer_code, AnalyticsTenantState.last_error,
        AnalyticsTenantState.last_cycle_at))).all()
    if not rows:
        return AnalyticsHealth(False, "no analytics state: the platform is unused here", [], [])

    dead = {cc for cc in (await db.execute(select(distinct(AnalyticsPendingWindow.customer_code))
                                          .where(AnalyticsPendingWindow.abandoned_at.isnot(None)))
                          ).scalars().all()}

    cap = timedelta(days=settings.analytics_retention_hold_max_days)
    holding, expired, reasons = [], [], []
    for cc, last_error, last_cycle_at in rows:
        why = []
        if cc in dead:
            why.append("abandoned ticket(s)")
        if last_error:
            why.append("last cycle failed")
        if not why:
            continue
        # A tenant that has never run cannot have been broken "since the epoch": NULL means never, and
        # treating it as long-ago would expire the cap instantly and make this gate a no-op.
        if last_cycle_at is not None and (now - last_cycle_at) > cap:
            expired.append(cc)
        else:
            holding.append(cc)
            reasons.append(f"{cc}: {', '.join(why)}")

    if expired:
        logger.critical(
            "Analytics has been unhealthy for more than %d days for %s. RELEASING the source-retention "
            "hold so the disk does not fill: their partitions can now be dropped, and once they are, "
            "those tenants' totals become permanently unprovable. Fix analytics or accept the loss.",
            settings.analytics_retention_hold_max_days, ", ".join(sorted(expired)))

    return AnalyticsHealth(bool(holding),
                           "; ".join(reasons) if reasons else "all tenants healthy",
                           sorted(holding), sorted(expired))


async def periods_blocked_by_analytics(db: AsyncSession, periods: list[Period],
                                       *, now: datetime | None = None) -> set[Period]:
    """Of `periods`, the SOURCE ones held because analytics could not be repaired without them.

    Fails CLOSED, unlike the consumer-cursor default. An error reading health is not evidence that
    analytics is fine, and the cost of holding one extra cycle is a day of disk against permanently
    unprovable totals. The cap still bounds how long that can go on.
    """
    if not periods or not settings.analytics_retention_gate_enabled:
        return set()
    candidates = {p for p in periods if p[0] in HEALTH_GATED}
    if not candidates:
        return set()
    try:
        health = await analytics_health(db, now=now)
    except Exception:
        logger.exception("Could not read analytics health; HOLDING source retention this cycle rather "
                         "than assuming it is safe to drop")
        return candidates
    if not health.holding:
        return set()
    logger.warning("Holding source retention for %d partition(s): %s",
                   len(candidates), health.reason)
    return candidates


async def periods_blocked(db: AsyncSession, periods: list[Period]) -> set[Period]:
    """Three holds, and all are required: Stage 2 must have finished WRITING a partition, every live
    consumer must have finished READING it, and analytics must not be in a state where losing the source
    would make a wrong total unprovable."""
    return (await periods_blocked_by_pending(db, periods)
            | await periods_blocked_by_consumers(db, periods)
            | await periods_blocked_by_analytics(db, periods))


async def days_blocked_by_pending(db: AsyncSession, days: list[date_type]) -> set[date_type]:
    """Of `days`, those overlapped by an OPEN stitch window.

    The DAILY view of `periods_blocked_by_pending`, kept because the three log tables are all daily and
    every caller and test here speaks in days. It asks about `log_entries` so the answer is the day
    itself; the three log tables share a grain, so which one is named cannot change the result.
    """
    return {d for _t, d in await periods_blocked_by_pending(db, [("log_entries", d) for d in days])}


async def days_blocked_by_consumers(db: AsyncSession, days: list[date_type]) -> set[date_type]:
    """Of `days`, those some incremental reader has not finished consuming.

    The daily view of `periods_blocked_by_consumers`, for the same reason as above.
    """
    return {d for _t, d in await periods_blocked_by_consumers(db, [("log_entries", d) for d in days])}


async def _create_runway(db: AsyncSession, today: date_type) -> int:
    days = pt.coverage_days(today, ahead=settings.log_partition_precreate_days)
    return await pt.ensure_coverage(db, days=days)


async def _drop_each(db: AsyncSession, candidates: dict[str, list[date_type]],
                     blocked: set[Period]) -> list[str]:
    """Drop each candidate partition that nothing is holding open. Returns the names removed.

    `blocked` is keyed on (table, period start) rather than on a bare date, because two tables at
    different grains can have candidates that share a start date while spanning different ranges.
    """
    dropped = []
    for table, starts in candidates.items():
        for start in starts:
            if (table, start) in blocked:
                continue
            await db.execute(text(pt.drop_partition_sql(table, start)))
            dropped.append(pt.partition_name(table, start))
    return dropped


async def _expired_candidates(db: AsyncSession, today: date_type) -> dict[str, list[date_type]]:
    """Per table, the partition starts past ITS retention (see `droppable_days` for the lag and for
    keep-forever tables, which yield nothing here).

    Gathered for every table before anything is dropped, so the gates below can be applied in one pass:
    a period held open must protect entries, transactions and assignments together, and gating them
    separately would leave exactly the torn state the gate exists to prevent.
    """
    return {t.table: droppable_days(t.table, await pt.covered_days(db, t.table), today)
            for t in pt.PARTITIONED}


async def _drop_expired(db: AsyncSession, today: date_type) -> list[str]:
    """Drop every partition past retention that no open stitch window protects. Returns their names.

    The gates are applied ONCE over the union of candidate partitions rather than per table, so a
    period held open protects entries, transactions and assignments together — protecting only some of
    them would leave exactly the torn state the gate exists to prevent.
    """
    candidates = await _expired_candidates(db, today)
    periods = sorted({(table, start) for table, starts in candidates.items() for start in starts})
    # Two independent holds, both required: Stage 2 must have finished WRITING the partition, and
    # every live consumer must have finished READING it.
    blocked = await periods_blocked(db, periods)
    if blocked:
        logger.info("Partition retention: holding %d partition(s) still needed by a writer or a "
                    "reader: %s", len(blocked),
                    ", ".join(pt.partition_name(t, d) for t, d in sorted(blocked)))
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
    """Days ahead of `today` that one table is provisioned; -1 when it has no future partition.

    Measured to the LAST day of the newest covered partition, not its first. One monthly partition
    created on the 1st is a month of runway; keying on the start would report zero and trip the
    CRITICAL runway alarm on every tick for the rest of the month. For a daily table the two are the
    same day, so this is unchanged for the log tables.
    """
    grain = pt.grain_of(table)
    ends = [pt.period_end(grain, s) for s in await pt.covered_days(db, table)]
    future = [e for e in ends if e >= today]
    return (max(future) - today).days if future else -1


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
