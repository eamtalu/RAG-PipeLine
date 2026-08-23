"""N3, the analytics worker's cycle: turn tickets into facts. Phase 3 of the final architecture.

One tenant, one call. The loop that calls it lives in `services/workers/analytics_worker.py`, following
the split the Stage 2 queue already uses (`finalize_pending` + `log_stitch_worker`).

The cycle, per the plan:

    claim due tickets -> coalesce into disjoint runs -> advisory lock -> work_mem -> read the source
    range -> normalise -> read the stored range -> RANGE DIFF -> apply, ledger, quarantine -> update
    tenant state, publish the retention position, stamp tickets consumed

Four decisions here are load-bearing, and three of them are places where the obvious thing is wrong.

**The source read must never be truncated.** The usual rule in this codebase is that no query ships
without a `limit` (CLAUDE.md rule 3). Here a limit would be actively destructive: the diff treats
"stored, absent from the source" as *reverse it*, so a truncated source read would silently delete every
fact past the cut and take its contribution out of every total. The read is bounded instead by the RANGE
-- N1 splits tickets to at most one day, so the row count is bounded by one day of that tenant's traffic
-- and this runs in the background worker, not a web request. A run that reads a surprising number of
rows is logged rather than trimmed.

**Its own advisory-lock namespace.** `hashtext('analytics:' || customer_code)`, deliberately NOT the
stitcher's `hashtext(customer_code)`. Sharing it would make analytics folding block Stage 2 stitching
for the same tenant and vice versa, coupling a read-only consumer to the write path for no reason.

**One transaction per RUN, not per tenant.** The plan's step list reads as a single transaction per
tenant; per run is what makes N1's day-splitting mean anything. N1 splits a wide ticket into one per day
precisely so "each unit of work stays bounded and a poison day fails in isolation" -- which is only true
if a day is also a transaction boundary. A single transaction spanning a `regroup_all` ticket set would
read 60 days of transactions at once and let one bad day roll back 59 good ones. Invariant 4 is still
satisfied: each run's tickets are stamped consumed in the SAME transaction as that run's changes, so a
crash leaves them open and the work is redone rather than skipped. Identical to `finalize_pending`.

**The retention frontier is stored per tenant and published as a minimum.** See
`AnalyticsTenantState.source_write_frontier`; publishing each tenant's own frontier into the single
`consumer_cursors` row would let a tenant that is ahead speak for one that is behind.

A known imprecision, recorded rather than hidden
------------------------------------------------
F6 specifies the position as a `log_transactions.created_at` -- a WRITE time -- and that matches the
convention every other consumer follows (`NotificationRule.cursor_at` says so in its own comment). But
`log_partition_worker.periods_blocked_by_consumers` compares that position against a partition's
EVENT-TIME upper bound. Write times run ahead of event times, so the comparison releases partitions
slightly earlier than a strict reading would allow, and a transaction written long after the event it
describes is the case where that gap matters. This is a pre-existing property of the cursor convention,
not something introduced here, and deviating for one consumer would make the MIN across consumers a
comparison between two different units. Implemented as specified; flagged as a real finding for E4/F6.
"""

import logging
import uuid
from datetime import date as date_type, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from sqlalchemy import String, and_, cast, delete, distinct, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger, FactColumns
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence import partitioning as pt
from app.persistence.models.log_transaction import LogTransaction
from app.services import consumer_cursors
from app.services.analytics import diff as dd
from app.services.analytics import normalizer as n2
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
from app.settings import settings

logger = logging.getLogger(__name__)

#: F6: the name this consumer publishes its retention position under. THE single named constant, so a
#: deferred upstream move to update-in-place has exactly one place to change.
CONSUMER = "analytics:warehouse-v1"

#: The source column the frontier is measured on. Named here for the same reason as `CONSUMER`: if
#: `log_transactions` ever stops being delete-and-reinsert, this becomes `updated_at` and nothing else
#: in the module needs to know.
_FRONTIER_COLUMN = LogTransaction.created_at

#: Distinct from the stitcher's `hashtext(customer_code)`. Analytics is a read-only consumer of the
#: projection; making it contend with the write path would be a self-inflicted stall.
_LOCK_NAMESPACE = "analytics:"

#: Per transaction, not per session. The diff sorts and hashes a range's worth of rows on both sides,
#: and the web tier's default is tuned for many small concurrent queries rather than one batch.
_WORK_MEM = "64MB"

#: A run reading more than this is logged. NOT a limit -- see the module docstring: trimming the source
#: read would make the diff reverse everything past the cut.
_LOUD_RUN_ROWS = 100_000

#: Reasons written to the ledger, so a churning history can be explained rather than guessed at.
_REASON = {dd.Action.insert: "insert", dd.Action.update: "update", dd.Action.reverse: "reverse"}

#: Every column the fact table and its ledger share, taken from the mixin so the two cannot drift.
_FACT_COLUMNS: tuple[str, ...] = tuple(
    name for name in vars(FactColumns) if not name.startswith("_"))


#: The partitioned tables a run writes to. Its OWN destinations only -- deliberately not the log tables.
#: Analytics is a strict reader of the ingestion pipeline, and provisioning `log_entries` for a historic
#: range would hand retention new partitions to drop on tables this component has no business touching.
#:
#: `analytics_monthly_rollups` is absent because it is not partitioned at all.
_DESTINATION_TABLES: tuple[str, ...] = (
    "analytics_facts", "analytics_fact_ledger", "analytics_hourly_rollups",
    "analytics_daily_rollups", "analytics_quality_issues",
)

#: How far past the run's UTC range to provision, so the tenant-LOCAL `business_date` is covered.
#:
#: Real UTC offsets span -12 to +14, so a fact inside a UTC day can carry a business date one calendar
#: day either side of it. One day of slack costs nothing (the daily rollups are cut yearly, so it is
#: usually the same partition) and removes a boundary case that would otherwise appear once a year.
_BUSINESS_DAY_PAD = timedelta(days=1)


def _destination_days(lo: datetime, hi: datetime) -> list[date_type]:
    """Every day the run's destinations must have partitions for.

    Today is always included. `analytics_fact_ledger` and `analytics_quality_issues` are keyed on a
    WRITE time, so what they need is TODAY's partition -- which is not in the run's range at all when
    the run is folding history.
    """
    days = set(pt.days_between((lo - _BUSINESS_DAY_PAD).date(), (hi + _BUSINESS_DAY_PAD).date()))
    days.add(datetime.now(timezone.utc).date())
    return sorted(days)


async def _ensure_destination_partitions(customer_code: str, lo: datetime, hi: datetime) -> int:
    """Make sure every destination has a partition for this range, BEFORE the run reads anything.

    Why this exists. The partition runway is built forward only (`coverage_days(today, ahead=14)`),
    because ingestion only ever writes new data. But three paths write facts with an OLD `event_time`:
    `regroup_all` re-deriving a tenant's whole history, an ingested rotated log file
    (`eSmartServerLog.txt.40`) carrying weeks-old lines, and any late-arriving transaction. Measured on
    2026-08-22, `analytics_facts` partitions began at 2026-07-01 while source data reached back to
    2026-06-23 -- an eight-day window with nowhere to go.

    Without this, those facts land in `analytics_facts_default`, which is a ONE-WAY DOOR: PostgreSQL
    then refuses to create that period's real partition ("updated partition constraint for default
    partition would be violated by some row"), so it cannot be repaired by adding the partition later.
    The rows have to be moved out first. Hence: before the write, not after.

    Its OWN short transaction, not the run's. `CREATE TABLE ... PARTITION OF` takes ACCESS EXCLUSIVE on
    the parent; held to the end of the run's transaction it would block every other tenant's fold and
    the partition worker, whereas here it is held for milliseconds. An empty partition left behind by a
    run that then fails is harmless.
    """
    async with async_session() as db:
        created = await pt.ensure_coverage(db, days=_destination_days(lo, hi),
                                           tables=_DESTINATION_TABLES)
        if created:
            logger.info("Analytics: provisioned %d destination partition(s) for %s covering %s..%s - "
                        "this range predates the forward-only runway", created, customer_code,
                        lo.date(), hi.date())
        await db.commit()
    return created


def _lock(customer_code: str):
    return func.pg_advisory_xact_lock(func.hashtext(_LOCK_NAMESPACE + customer_code))


def _open_and_due():
    """The three exclusions that define claimable work, each load-bearing.

    `clock_timestamp()`, NOT `now()`: `now()` is `transaction_timestamp()`, so a session whose
    transaction began before a ticket was written would treat that ticket as permanently not-yet-due.
    """
    return (
        AnalyticsPendingWindow.consumed_at.is_(None),
        AnalyticsPendingWindow.abandoned_at.is_(None),
        AnalyticsPendingWindow.available_at <= func.clock_timestamp(),
    )


async def customers_with_due_work(limit: int | None = None) -> list[str]:
    """Tenants with at least one open, due ticket. Backed by ix_analytics_pending_due."""
    cap = limit if limit is not None else settings.analytics_max_customers_per_tick
    async with async_session() as db:
        return list((await db.execute(
            select(distinct(AnalyticsPendingWindow.customer_code))
            .where(*_open_and_due()).limit(cap))).scalars().all())


def _coalesce(tickets: Sequence[AnalyticsPendingWindow], gap: timedelta
              ) -> list[tuple[datetime, datetime, list[AnalyticsPendingWindow]]]:
    """Merge overlapping and near-adjacent ticket ranges into disjoint runs, each keeping its own rows.

    Keeping the rows attached is what lets one run be stamped consumed independently: a poison run fails
    without either blocking the others or wrongly consuming its own tickets. Same shape as Stage 2's
    `_coalesce_pending`, deliberately.

    Coalescing is not only an efficiency. N1 splits a wide range into per-day tickets, so a transaction
    whose rebuild moved it across midnight would otherwise be reversed by one day's ticket and inserted
    by the next. Merging adjacent tickets into one run puts both sides of that move in the same diff.
    """
    runs: list[list] = []
    for t in sorted(tickets, key=lambda r: r.range_start):
        if runs and t.range_start <= runs[-1][1] + gap:
            runs[-1][1] = max(runs[-1][1], t.range_end)
            runs[-1][2].append(t)
        else:
            runs.append([t.range_start, t.range_end, [t]])
    return [(lo, hi, rows) for lo, hi, rows in runs]


#: Source columns the normaliser needs, plus the two the cycle itself needs (`sealed` for F4's
#: settledness, `created_at` for F6's frontier). Specific columns rather than whole ORM objects: a
#: day's transactions as mapped instances would balloon the identity map for no benefit, since nothing
#: here mutates them.
_SOURCE_COLUMNS = (
    LogTransaction.id, LogTransaction.started_at, LogTransaction.duration_ms, LogTransaction.method,
    LogTransaction.transaction_name, LogTransaction.transaction_type, LogTransaction.status,
    LogTransaction.item_number, LogTransaction.order_number, LogTransaction.delivery_number,
    LogTransaction.warehouse, LogTransaction.warehouse_id, LogTransaction.user_name,
    LogTransaction.device_id, LogTransaction.device_name, LogTransaction.attributes,
    LogTransaction.sealed, LogTransaction.created_at,
)


async def _read_source(db: AsyncSession, customer_code: str, window: UtcWindow) -> list[dict]:
    """The projection's CURRENT truth for this range.

    `include_null=True` (A7): a transaction all of whose entries lack a parsable timestamp has a NULL
    `started_at` and lives in the DEFAULT partition. It still has to be diffed, and the stored side is
    read with the same predicate, so the two agree and such rows fold to `unchanged` on every pass.
    """
    rows = (await db.execute(
        select(*_SOURCE_COLUMNS).where(
            LogTransaction.customer_code == customer_code,
            window.covers(LogTransaction.started_at, include_null=True)))).mappings().all()
    if len(rows) >= _LOUD_RUN_ROWS:
        logger.warning("Analytics: run for %s read %d source rows for %s..%s - larger than expected "
                       "for a one-day ticket; NOT truncated, because a partial read would reverse "
                       "every fact past the cut", customer_code, len(rows), window.start, window.end)
    return [dict(r) for r in rows]


async def _read_stored(db: AsyncSession, customer_code: str, window: UtcWindow) -> list[dict]:
    """What analytics currently believes about the same range. Same predicate, necessarily.

    A wider stored read than source would reverse rows that are merely outside the window and still
    perfectly valid; a narrower one would never notice what left.
    """
    cols = (AnalyticsFact.id, AnalyticsFact.created_at,
            *(getattr(AnalyticsFact, name) for name in _FACT_COLUMNS))
    rows = (await db.execute(
        select(*cols).where(
            AnalyticsFact.customer_code == customer_code,
            window.covers(AnalyticsFact.event_time, include_null=True)))).mappings().all()
    return [dict(r) for r in rows]


def _key_predicate(customer_code: str, key: dd.Key):
    """Match one fact by its identity (F3). `event_time` needs `IS NULL` rather than `= NULL`."""
    txn_id, event_time = key
    return and_(
        AnalyticsFact.customer_code == customer_code,
        cast(AnalyticsFact.source_transaction_id, String) == str(txn_id),
        AnalyticsFact.event_time.is_(None) if event_time is None
        else AnalyticsFact.event_time == event_time)


def _fact_values(fact: Mapping[str, Any]) -> dict:
    return {name: fact.get(name) for name in _FACT_COLUMNS}


def _ledger_row(customer_code: str, values: Mapping[str, Any], *, revision: int, reason: str,
                recorded_at: datetime) -> dict:
    return {**_fact_values(values), "id": uuid.uuid4(), "customer_code": customer_code,
            "recorded_at": recorded_at, "revision": revision, "reason": reason}


async def _apply(db: AsyncSession, customer_code: str, outcomes: Sequence[dd.Outcome],
                 recorded_at: datetime) -> dict:
    """Write the diff's verdicts, and append every version it produced to the ledger.

    A reversal gets a ledger row too. Without one the history simply stops, and "what did the fact table
    hold at time T" -- the question the ledger exists to answer -- becomes unanswerable for exactly the
    rows a merge or a delete removed.
    """
    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "reversed": 0}
    ledger: list[dict] = []

    for o in outcomes:
        if o.action is dd.Action.unchanged:
            stats["unchanged"] += 1
            continue

        if o.action is dd.Action.reverse:
            await db.execute(delete(AnalyticsFact).where(_key_predicate(customer_code, o.key)))
            revision = int(o.stored.get("revision") or 1) + 1
            ledger.append(_ledger_row(customer_code, o.stored, revision=revision,
                                      reason=_REASON[o.action], recorded_at=recorded_at))
            stats["reversed"] += 1
            continue

        values = _fact_values(o.fact)
        if o.action is dd.Action.insert:
            values["revision"] = 1
            db.add(AnalyticsFact(id=uuid.uuid4(), customer_code=customer_code,
                                 created_at=recorded_at, **values))
            stats["inserted"] += 1
        else:
            # The stored row's revision decides the new one, so a fact's versions are consecutive even
            # across a worker restart. `created_at` is deliberately NOT touched: it is what F6's
            # frontier reads, and refreshing it on every rebuild would make the frontier meaningless.
            values["revision"] = int(o.stored.get("revision") or 1) + 1
            await db.execute(update(AnalyticsFact)
                             .where(_key_predicate(customer_code, o.key)).values(**values))
            stats["updated"] += 1
        ledger.append(_ledger_row(customer_code, values, revision=values["revision"],
                                  reason=_REASON[o.action], recorded_at=recorded_at))

    if ledger:
        await db.execute(pg_insert(AnalyticsFactLedger), ledger)
    return stats


async def _quarantine(db: AsyncSession, customer_code: str, issues: Sequence[Mapping[str, Any]],
                      detected_at: datetime) -> int:
    """Record rows that could not be normalised. A1: never halts the tenant.

    Not deduplicated against previous cycles, matching the table's own design note -- the same row
    failing for a different reason across rebuilds is a source getting worse, and collapsing those
    would hide it. Growth is bounded by tickets only being published when a range actually changed, and
    by the table's one-year retention.
    """
    if not issues:
        return 0
    await db.execute(pg_insert(AnalyticsQualityIssue), [
        {"id": uuid.uuid4(), "customer_code": customer_code, "detected_at": detected_at, **dict(i)}
        for i in issues])
    return len(issues)


async def _roll_up(db: AsyncSession, customer_code: str, outcomes: Sequence[dd.Outcome],
                   computed_at: datetime) -> dict:
    """N5: recompute every rollup bucket this diff dirtied, for every ACTIVE definition.

    In the SAME transaction as the facts, deliberately. A chart that disagrees with the fact table for
    the length of a gap between two commits is bad; one that disagrees forever because the second commit
    failed is the kind of thing nobody finds until someone questions a number.

    Driven by registry ROWS, not by `CONSUMPTION`. A metric invented from the interface is folded by this
    same call with nothing added, which is the property the whole user-configurable design rests on.

    The deltas decide only WHICH buckets are dirty; each one is then recomputed from scratch. Adding the
    deltas to the stored bucket would double-count on the first retry, and a retry is the normal
    consequence of any failure.
    """
    hours, dates = n5.dirty_buckets(outcomes)
    if not hours and not dates:
        # The 98.7% rebuild case, free all the way through rather than only as far as the fact table.
        return {"definitions": 0, "buckets": 0}

    await registry.ensure_seed(db, customer_code)
    stats = {"definitions": 0, "buckets": len(hours) + len(dates)}
    for definition_id, definition in await registry.active_definitions(db, customer_code):
        try:
            await n5.recompute(db, customer_code, definition_id, definition,
                               hours=hours, dates=dates, computed_at=computed_at)
            stats["definitions"] += 1
        except Exception:
            # Deliberately NOT swallowed beyond logging: this re-raises, failing the whole run. A
            # rollup that silently did not update is a chart that is wrong with nothing to say so,
            # which is worse than a ticket that stays open and retries. Contrast quarantine (A1),
            # where the alternative is halting a tenant over one unexplainable row.
            logger.exception("Analytics: rollup for definition %s (%r) failed for %s",
                             definition_id, definition.name, customer_code)
            raise
    return stats


def _settledness(source: Sequence[Mapping[str, Any]]) -> tuple[Decimal | None, datetime | None]:
    """F4: the share of this range's contributors still unsealed, and the oldest one's instant.

    Computed from rows already in hand rather than re-queried. A window with unsealed contributors is
    PROVISIONAL, not stale, and those are different words for the user -- so both numbers are stored
    instead of one being derived from the other.
    """
    if not source:
        return None, None
    unsealed = [r for r in source if not r.get("sealed")]
    share = (Decimal(len(unsealed)) / Decimal(len(source))).quantize(Decimal("0.00001"))
    oldest = min((r["started_at"] for r in unsealed if r.get("started_at")), default=None)
    return share, oldest


async def _source_watermark(db: AsyncSession, customer_code: str) -> datetime | None:
    """The newest `started_at` the PROJECTION holds for this tenant.

    F4's first number, and the half that was missing until it was noticed in production: the column
    existed, the API read it and `freshness()` divided by it, but nothing wrote it -- so `lag_seconds`
    was always null and `stale` could never fire. A pipeline hours behind would have reported itself as
    Provisional or Settled, which is the one thing F4 exists to prevent.

    Read inside the fold's own transaction, per the column's contract: "as observed at the same moment
    ... two reads would show a lag that is really just the gap between them". Observing it later would
    fold the worker's own scheduling delay into the reported lag.
    """
    return await db.scalar(select(func.max(LogTransaction.started_at)).where(
        LogTransaction.customer_code == customer_code))


async def _update_state(db: AsyncSession, customer_code: str, *, folded: dict, quarantined: int,
                        event_watermark: datetime | None, history_start: datetime | None,
                        source_watermark: datetime | None, frontier: datetime | None,
                        settledness: tuple[Decimal | None, datetime | None],
                        now: datetime) -> None:
    """F5: write everything the status card shows, so the polled endpoint is ONE indexed lookup.

    `facts_total` and `quarantined_rows` move INCREMENTALLY from the diff's own counters rather than
    being recounted. N3 is the only writer of both tables (the ownership table says so), so the
    increment is exact -- and a `COUNT(*)` per cycle over a table designed to reach 13M rows would get
    slower forever while answering a question the cycle already knows.

    Watermarks and the frontier only move FORWARD. A run over an older range is completely normal -- a
    late backfill produces exactly that -- and letting it drag the watermark back would make the card
    report a regression that never happened, and the frontier claim less than had truly been read.
    """
    share, oldest_unsealed = settledness
    net_facts = folded["inserted"] - folded["reversed"]

    values = {
        "id": uuid.uuid4(), "customer_code": customer_code,
        "analytics_watermark": event_watermark, "history_starts_at": history_start,
        "source_watermark": source_watermark,
        "source_write_frontier": frontier,
        "unsealed_share": share, "oldest_unsealed_at": oldest_unsealed,
        "facts_total": max(net_facts, 0), "quarantined_rows": quarantined,
        "revision": 1, "last_cycle_at": now, "last_error": None, "updated_at": now,
    }
    stmt = pg_insert(AnalyticsTenantState).values(**values)
    await db.execute(stmt.on_conflict_do_update(
        constraint="uq_analytics_tenant_state_customer",
        set_={
            "analytics_watermark": func.greatest(
                AnalyticsTenantState.analytics_watermark, stmt.excluded.analytics_watermark),
            # BACKWARD only, the mirror of the watermark above: folding an older range legitimately
            # extends history into the past, which is what a late backfill does. `least` ignores NULLs,
            # so a first fold sets it and later folds only ever widen the range.
            "history_starts_at": func.least(
                AnalyticsTenantState.history_starts_at, stmt.excluded.history_starts_at),
            # NOT forward-only, unlike the analytics watermark. This one describes the SOURCE, and the
            # source legitimately shrinks: a date-range delete or a partition drop lowers the newest
            # started_at. Clamping it forward would leave a permanent phantom lag that nothing could
            # clear.
            "source_watermark": stmt.excluded.source_watermark,
            "source_write_frontier": func.greatest(
                AnalyticsTenantState.source_write_frontier, stmt.excluded.source_write_frontier),
            "unsealed_share": stmt.excluded.unsealed_share,
            "oldest_unsealed_at": stmt.excluded.oldest_unsealed_at,
            "facts_total": AnalyticsTenantState.facts_total + net_facts,
            "quarantined_rows": AnalyticsTenantState.quarantined_rows + quarantined,
            # A5: one authoritative revision per tenant, bumped in the same commit as the work it
            # describes. Cache validation keys off it, so a revision that moved without the data
            # would serve a stale chart that looks fresh.
            "revision": AnalyticsTenantState.revision + 1,
            "last_cycle_at": now, "last_error": None, "updated_at": now,
        }))


async def _refresh_counts(db: AsyncSession, customer_code: str) -> None:
    """The two ticket counts the card shows. Cheap: both are indexed, and neither is derivable from the
    diff, since tickets can be created by any ingestion path between cycles."""
    open_n = await db.scalar(
        select(func.count()).select_from(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == customer_code,
            AnalyticsPendingWindow.consumed_at.is_(None),
            AnalyticsPendingWindow.abandoned_at.is_(None))) or 0
    dead_n = await db.scalar(
        select(func.count()).select_from(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == customer_code,
            AnalyticsPendingWindow.abandoned_at.isnot(None))) or 0
    await db.execute(update(AnalyticsTenantState)
                     .where(AnalyticsTenantState.customer_code == customer_code)
                     .values(open_tickets=open_n, abandoned_tickets=dead_n))


async def _tenant_timezone(db: AsyncSession, customer_code: str) -> str | None:
    """The tenant's IANA zone, or the configured default when it has none.

    NULL on the customer row means "not yet configured" rather than UTC, and the fallback is the same
    one the rest of the app uses -- so a business_date computed here matches the day the feed displays.
    """
    tz = await db.scalar(select(Customer.timezone).where(Customer.customer_code == customer_code))
    return tz or settings.display_timezone


async def publish_retention_position(db: AsyncSession) -> datetime | None:
    """F6. Publish the position, as the MINIMUM frontier across tenants.

    The minimum, not the maximum, and not per tenant: `consumer_cursors` holds one row for this whole
    consumer and retention is global, so the position has to be safe for the tenant that is FURTHEST
    BEHIND. Publishing a leader's frontier would let the partition worker drop source data a lagging
    tenant had never read -- and its cursor would then move past the gap without noticing.

    A tenant with no frontier yet (`NULL`) has processed nothing, so it cannot be spoken for at all:
    the explicit `isnot(None)` keeps SQL's MIN from skipping it into a claim that is too far ahead. No
    tenant state at all publishes nothing, rather than claiming everything.
    """
    position = await db.scalar(
        select(func.min(AnalyticsTenantState.source_write_frontier))
        .where(AnalyticsTenantState.source_write_frontier.isnot(None)))
    unstarted = await db.scalar(
        select(func.count()).select_from(AnalyticsTenantState).where(
            AnalyticsTenantState.source_write_frontier.is_(None)))
    if position is None or unstarted:
        return None
    await consumer_cursors.report(db, CONSUMER, position=position)
    return position


async def _consume_run(customer_code: str, lo: datetime, hi: datetime,
                       tickets: Sequence[AnalyticsPendingWindow], tz: str | None) -> dict:
    """One disjoint run, in its own transaction. Everything or nothing.

    The order inside matters: the tickets are stamped LAST, in this same transaction (invariant 4), so a
    crash anywhere above leaves them open and the range is redone rather than skipped.
    """
    window = UtcWindow(start=lo, end=hi)
    # Before the run's transaction opens, and before either read: the whole run is one transaction, so
    # a missing partition discovered at write time would roll back work already done.
    await _ensure_destination_partitions(customer_code, lo, hi)

    async with async_session() as db:
        await db.execute(select(_lock(customer_code)))
        await db.execute(text(f"SET LOCAL work_mem = '{_WORK_MEM}'"))

        source_rows = await _read_source(db, customer_code, window)
        facts, issues = [], []
        for row in source_rows:
            fact, issue = n2.normalise(row, tenant_timezone=tz)
            (facts if fact is not None else issues).append(fact if fact is not None else issue)

        stored = await _read_stored(db, customer_code, window)
        outcomes = dd.diff(stored, facts)

        now = datetime.now(timezone.utc)
        folded = await _apply(db, customer_code, outcomes, now)
        quarantined = await _quarantine(db, customer_code, issues, now)
        rolled = await _roll_up(db, customer_code, outcomes, now)

        await _update_state(
            db, customer_code, folded=folded, quarantined=quarantined,
            event_watermark=max((f["event_time"] for f in facts if f["event_time"]), default=None),
            history_start=min((f["event_time"] for f in facts if f["event_time"]), default=None),
            source_watermark=await _source_watermark(db, customer_code),
            frontier=max((r["created_at"] for r in source_rows if r.get("created_at")), default=None),
            settledness=_settledness(source_rows), now=now)
        # Stamped BEFORE the counts are refreshed, and the order is not cosmetic: `_refresh_counts`
        # counts open tickets, so counting first would always include the tickets this run is in the
        # act of consuming and the status card would never show a drained queue.
        await db.execute(update(AnalyticsPendingWindow)
                         .where(AnalyticsPendingWindow.id.in_([t.id for t in tickets]))
                         .values(consumed_at=now, last_attempt_at=now, last_error=None))
        await _refresh_counts(db, customer_code)
        await publish_retention_position(db)
        await db.commit()

    return {**folded, "quarantined": quarantined, "source_rows": len(source_rows),
            "consumed": len(tickets), "definitions_rolled": rolled["definitions"],
            "buckets_rolled": rolled["buckets"]}


async def _record_failure(customer_code: str, tickets: Sequence[AnalyticsPendingWindow],
                          error: BaseException) -> int:
    """Bump attempts, back the tickets off, and dead-letter at the cap. Its OWN transaction, because the
    run's transaction has already rolled back and its session cannot be reused.

    Never re-raises: one failed run must not stop the tenant's other runs, let alone other tenants.
    """
    from app.services.queueing import retry_policy

    abandoned = 0
    try:
        async with async_session() as db:
            for t in tickets:
                attempts = (t.attempts or 0) + 1
                give_up = attempts >= settings.analytics_max_attempts
                delay = retry_policy.backoff_seconds(
                    attempts, base=settings.analytics_backoff_base_seconds,
                    cap=settings.analytics_backoff_cap_seconds)
                await db.execute(
                    update(AnalyticsPendingWindow).where(AnalyticsPendingWindow.id == t.id).values(
                        attempts=attempts, last_error=str(error)[:2000],
                        last_attempt_at=func.clock_timestamp(),
                        available_at=func.clock_timestamp() + text(f"interval '{delay:.3f} seconds'"),
                        abandoned_at=func.clock_timestamp() if give_up else None))
                abandoned += 1 if give_up else 0
            await db.commit()
    except Exception:
        logger.exception("Analytics: could not record the failure for %s - its tickets stay open, "
                         "which retries the work rather than losing it", customer_code)
    return abandoned


async def consume_tenant(customer_code: str) -> dict:
    """Fold every due ticket for one tenant. Idempotent: with nothing pending, runs=0."""
    async with async_session() as db:
        tickets = list((await db.execute(
            select(AnalyticsPendingWindow).where(
                AnalyticsPendingWindow.customer_code == customer_code, *_open_and_due()))).scalars())
        tz = await _tenant_timezone(db, customer_code)

    stats = {"runs": 0, "inserted": 0, "updated": 0, "unchanged": 0, "reversed": 0,
             "quarantined": 0, "source_rows": 0, "consumed": 0, "failed": 0, "abandoned": 0,
             "definitions_rolled": 0, "buckets_rolled": 0}
    if not tickets:
        return stats

    # The same gap Stage 2 uses, for the same reason: two windows less than a pad apart describe
    # overlapping rebuilds, so diffing them separately would do the seam twice.
    from app.services.mnp_log_ingestion.pipeline.derive_transactions import _regroup_pad
    for lo, hi, rows in _coalesce(tickets, gap=2 * _regroup_pad()):
        stats["runs"] += 1
        try:
            for key, value in (await _consume_run(customer_code, lo, hi, rows, tz)).items():
                stats[key] += value
        except Exception as exc:
            stats["failed"] += 1
            stats["abandoned"] += await _record_failure(customer_code, rows, exc)
            logger.exception("Analytics: run %s..%s failed for %s - its tickets stay open for retry; "
                             "the tenant's other runs are unaffected", lo, hi, customer_code)
    return stats


async def drain_once() -> dict:
    """Fold every tenant with due work. Per-tenant failures are isolated (A1)."""
    stats = {"customers": 0}
    for cc in await customers_with_due_work():
        stats["customers"] += 1
        try:
            for key, value in (await consume_tenant(cc)).items():
                stats[key] = stats.get(key, 0) + value
        except Exception:
            stats["failed"] = stats.get("failed", 0) + 1
            logger.exception("Analytics: tenant %s failed entirely - others are unaffected", cc)
    return stats
