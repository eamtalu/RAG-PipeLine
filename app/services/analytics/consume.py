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
F6 specifies the position as a `log_transactions` WRITE time (`_FRONTIER_COLUMN`, `updated_at` since
S3 made rows update in place) -- and that matches the
convention every other consumer follows (`NotificationRule.cursor_at` says so in its own comment). But
`log_partition_worker.periods_blocked_by_consumers` compares that position against a partition's
EVENT-TIME upper bound. Write times run ahead of event times, so the comparison releases partitions
slightly earlier than a strict reading would allow, and a transaction written long after the event it
describes is the case where that gap matters. This is a pre-existing property of the cursor convention,
not something introduced here, and deviating for one consumer would make the MIN across consumers a
comparison between two different units. Implemented as specified; flagged as a real finding for E4/F6.
"""

import hashlib
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
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence import partitioning as pt
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.services import consumer_cursors
from app.services.analytics import diff as dd
from app.services.analytics import capture
from app.services.analytics import payload as pl
from app.services.analytics import normalizer as n2
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
from app.services.mnp_log_ingestion.pipeline.derive_transactions import _split_run
from app.services.mnp_log_ingestion.pipeline import maintenance
from app.settings import settings

logger = logging.getLogger(__name__)

#: F6: the name this consumer publishes its retention position under. THE single named constant, so a
#: deferred upstream move to update-in-place has exactly one place to change.
CONSUMER = "analytics:warehouse-v1"

#: The source column the frontier is measured on. Named here for the same reason as `CONSUMER`, and
#: the move the name existed for has happened: S3 made `log_transactions` UPDATE in place (2026-08-25),
#: so a row's latest WRITE is `updated_at` and `created_at` froze into "first written". A frontier
#: measured on `created_at` cannot see in-place rewrites - a rebuild-heavy tenant would report a
#: stalled frontier and hold source partitions forever (Flow F's watch, fired). Chunk 64 pins both
#: this binding and that the fold actually reads through it.
_FRONTIER_COLUMN = LogTransaction.updated_at

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
    # 18x: was missing, and the omission was the one-way door - a historic fold (a rotated file, a
    # regroup_all, a late transaction) sank record rows into the DEFAULT partition, after which that
    # period's real partition can never be created.
    "analytics_record_facts",
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
    """Tenants with at least one open, due ticket. Backed by ix_analytics_pending_due.

    Chunk 69: a tenant with a fresh RUNNING full rebuild is EXCLUDED - folding mid-rebuild states
    would restate facts against half-built transactions and then restate them again. Its tickets
    wait; a stale flag stops excluding and alarms (see `pipeline.maintenance`)."""
    cap = limit if limit is not None else settings.analytics_max_customers_per_tick
    async with async_session() as db:
        return list((await db.execute(
            select(distinct(AnalyticsPendingWindow.customer_code))
            .where(*_open_and_due(),
                   maintenance.not_under_maintenance(AnalyticsPendingWindow.customer_code))
            .limit(cap))).scalars().all())


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
        # `<=`, so ranges that touch at an instant merge too. That is right rather than lax: nothing
        # can fall strictly between them, so treating them as one range loses nothing - and the BOUND
        # now comes from `_split_run` below, not from refusing to merge.
        #
        # An earlier attempt at this fix used strict `<` to keep runs small. It was the wrong lever:
        # tickets are padded +/-900s (see `pending_windows`, which explains why - invariant 2), so
        # consecutive daily tickets GENUINELY overlap by 30 minutes and merge under any comparison.
        # Splitting after the merge is what bounds the work; strictness only broke a passing test.
        if runs and t.range_start <= runs[-1][1] + gap:
            runs[-1][1] = max(runs[-1][1], t.range_end)
            runs[-1][2].append(t)
        else:
            runs.append([t.range_start, t.range_end, [t]])
    return [(lo, hi, rows) for lo, hi, rows in runs]


#: Source columns the normaliser needs, plus the two the cycle itself needs (`sealed` for F4's
#: settledness, `_FRONTIER_COLUMN` for F6's frontier). Specific columns rather than whole ORM objects:
#: a day's transactions as mapped instances would balloon the identity map for no benefit, since
#: nothing here mutates them.
_SOURCE_COLUMNS = (
    LogTransaction.id, LogTransaction.started_at, LogTransaction.duration_ms, LogTransaction.method,
    LogTransaction.transaction_name, LogTransaction.transaction_type, LogTransaction.status,
    LogTransaction.item_number, LogTransaction.order_number, LogTransaction.delivery_number,
    LogTransaction.warehouse, LogTransaction.warehouse_id, LogTransaction.user_name,
    LogTransaction.device_id, LogTransaction.device_name, LogTransaction.attributes,
    LogTransaction.sealed, _FRONTIER_COLUMN,
    # S3's digests, read so the fold can tell "this transaction has not changed at all" without
    # touching its entries. That is what makes the response read (R3) skippable - see `_needs_entries`.
    LogTransaction.row_fingerprint, LogTransaction.members_fingerprint,
)


async def _read_source(db: AsyncSession, customer_code: str, window: UtcWindow,
                       suppressed: frozenset[str]) -> list[dict]:
    """The projection's CURRENT truth for this range.

    `include_null=True` (A7): a transaction all of whose entries lack a parsable timestamp has a NULL
    `started_at` and lives in the DEFAULT partition. It still has to be diffed, and the stored side is
    read with the same predicate, so the two agree and such rows fold to `unchanged` on every pass.

    R1: `suppressed` are the transaction names this tenant has turned CAPTURE off for. The same set is
    applied to `_read_stored` and to the auditor - see `capture` for why all three must agree, and why
    gating only this side would turn un-ticking a switch into a delete.

    REQUIRED, with no default. `None` defaulting to "gate nothing" would mean a caller that forgot the
    argument silently read every transaction including the suppressed ones, which is the failure this
    whole module exists to prevent. Pass `frozenset()` to mean "nothing suppressed" and say so.
    """
    gate = capture.source_predicate(suppressed)
    rows = (await db.execute(
        select(*_SOURCE_COLUMNS).where(
            LogTransaction.customer_code == customer_code,
            window.covers(LogTransaction.started_at, include_null=True),
            *([gate] if gate is not None else [])))).mappings().all()
    if len(rows) >= _LOUD_RUN_ROWS:
        logger.warning("Analytics: run for %s read %d source rows for %s..%s - larger than expected "
                       "for a one-day ticket; NOT truncated, because a partial read would reverse "
                       "every fact past the cut", customer_code, len(rows), window.start, window.end)
    return [dict(r) for r in rows]


#: Bumped when `normalise`, `payload.extract` or the approval rules change what a fact CONTAINS.
#:
#: The response-read skip below reuses a stored fact wholesale when its source has not changed. That is
#: only sound while the code that BUILT it is unchanged too - otherwise an edited derivation would never
#: reach a settled fact, silently, which is exactly the trap `_DERIVE_VERSION` exists to close on the
#: Stage 2 side. Same lesson, second place it applies.
_NORMALISE_VERSION = 1

#: Where the skip decision's inputs live on the stored fact. Prefixed `__` so they cannot be mistaken
#: for a WMS field, and kept in `attributes` rather than as new columns because they are bookkeeping for
#: this optimisation rather than anything a metric would measure.
_SRC_FP_KEY = "__src_fp"
_NORM_V_KEY = "__norm_v"


def _needs_entries(source_row, stored_fact) -> bool:
    """Whether this transaction's response entries have to be read at all.

    Measured: `_read_response_entries` was 22.8 s of a 23.7 s read - 96% - because it scales with every
    transaction in the window rather than with the ones that changed. That undoes S3's premise for
    exactly the runs already too large, which is how a 30 s timeout became unreachable.

    Skippable only when all three hold:
      - a fact already exists for this transaction
      - Stage 2's row digest is unchanged, so nothing about the transaction moved
      - the fact was built by this version of the normalisation

    The second is the load-bearing one. Stage 2's digest covers the row AND, via `members_fingerprint`,
    which entries it is made of - so an unchanged digest means the response entries are byte-identical.
    Without S3 there would be no cheap way to know that and this optimisation would not exist.
    """
    if stored_fact is None:
        return True
    src_fp = source_row.get("row_fingerprint")
    if src_fp is None:
        return True          # a pre-S3 row has no digest, so nothing can be proven about it
    prior = stored_fact.get("attributes") or {}
    return (prior.get(_SRC_FP_KEY) != src_fp
            or prior.get(_NORM_V_KEY) != _NORMALISE_VERSION)


async def _read_response_entries(db: AsyncSession, customer_code: str, window: UtcWindow,
                                 only: set | None = None
                                 ) -> dict[uuid.UUID, list[tuple[str, dict]]]:
    """R3. Each transaction's `response` and `mi_result` entries, keyed by transaction id.

    Stage 1 already parsed these in full and Stage 2 discards them, so this is the only place the
    response half of an exchange becomes measurable. Joined through `log_entry_assignment`, which is
    what maps an entry to its transaction.

    BOUNDED TWICE, because this is the one read in the cycle whose size is not proportional to the
    transaction count. Only two of the eight entry types are fetched - the other six carry nothing R3
    wants - and the window is the same one the source read uses, so the join prunes to the same
    partitions rather than opening all 94.

    Measured: 17.3 entries per transaction on average, of which the response half is a small fraction,
    so a one-day ticket is thousands of rows rather than the ~53,000 a full window read would be.
    """
    stmt = (select(LogEntryAssignment.transaction_id, LogEntry.entry_type, LogEntry.fields)
            .join(LogEntry, LogEntry.id == LogEntryAssignment.entry_id)
            .where(LogEntryAssignment.customer_code == customer_code,
                   window.covers(LogEntryAssignment.entry_ts, include_null=True),
                   LogEntry.entry_type.in_(("response", "mi_result")),
                   # R3 + S3: only the transactions whose facts could actually differ. On a settled
                   # window `only` is empty and this whole read is skipped, which is the 96% saving.
                   *([LogEntryAssignment.transaction_id.in_(sorted(only))] if only is not None else []))
            .order_by(LogEntryAssignment.transaction_id, LogEntryAssignment.seq))
    out: dict[uuid.UUID, list[tuple[str, dict]]] = {}
    for txn_id, entry_type, fields in (await db.execute(stmt)).all():
        et = entry_type.value if hasattr(entry_type, "value") else str(entry_type)
        out.setdefault(txn_id, []).append((et, fields))
    return out


async def _read_stored(db: AsyncSession, customer_code: str, window: UtcWindow,
                       suppressed: frozenset[str]) -> list[dict]:
    """What analytics currently believes about the same range. Same predicate, necessarily.

    A wider stored read than source would reverse rows that are merely outside the window and still
    perfectly valid; a narrower one would never notice what left.

    R1: that "necessarily" now also covers the capture gate. The diff reverses anything present here
    and absent from source, so gating source alone would make turning `capture` off DELETE every fact
    that transaction already has. Gating both sides makes them invisible to the diff instead - neither
    compared nor reversed, just left as they are.
    """
    gate = capture.fact_predicate(suppressed, AnalyticsFact)
    cols = (AnalyticsFact.id, AnalyticsFact.created_at,
            *(getattr(AnalyticsFact, name) for name in _FACT_COLUMNS))
    rows = (await db.execute(
        select(*cols).where(
            AnalyticsFact.customer_code == customer_code,
            window.covers(AnalyticsFact.event_time, include_null=True),
            *([gate] if gate is not None else [])))).mappings().all()
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
            # across a worker restart. `created_at` is deliberately NOT touched: it means "first
            # written", and the ledger's `recorded_at` is where every later version's instant lives.
            values["revision"] = int(o.stored.get("revision") or 1) + 1
            await db.execute(update(AnalyticsFact)
                             .where(_key_predicate(customer_code, o.key)).values(**values))
            stats["updated"] += 1
        ledger.append(_ledger_row(customer_code, values, revision=values["revision"],
                                  reason=_REASON[o.action], recorded_at=recorded_at))

    if ledger:
        await db.execute(pg_insert(AnalyticsFactLedger), ledger)
    return stats


#: Bookkeeping key inside each record row's `attributes` (18x): the expansion version the row was
#: written under. Underscore-prefixed like the fact grain's `__src_fp`, so it can never collide with
#: a `rec.*` field and readers can strip bookkeeping by prefix.
_EXP_V_KEY = "_exp_v"

#: Static half of the expansion version. Bump when `_expand_records` changes WHAT a row contains,
#: exactly as `_NORMALISE_VERSION` works for the fact grain - every stored record row goes stale at
#: once and the presence diff restates them through ordinary tickets.
_EXPAND_VERSION = 1


def _expansion_version(approved_record_fields: frozenset[str]) -> str:
    """The expansion version: code version + the tenant's approved `rec.*` set (18x).

    A stored row whose `_exp_v` differs was written under different rules - most commonly a field
    approved AFTER the row was written (rows are always written before anyone can approve their
    fields, because discovery precedes review) - and must be re-expanded to pick the change up.
    """
    digest = hashlib.sha256(
        (f"v{_EXPAND_VERSION}:" + ",".join(sorted(approved_record_fields))).encode()).hexdigest()
    return digest[:16]


def _predicts_records(stored_fact: Mapping | None) -> bool:
    """Whether the stored fact says its response carried records: `mi.record_count` > 0.

    This is what stops zero-record transactions re-expanding forever: `expand` covers a NAME, a name
    spans methods that legitimately return no records, and `_expand_records` writes nothing for them
    - so "no rows stored" alone would mean "expand again" on every fold. `mi.record_count` is on the
    seed list, so every captured fact carries it. Residual edge, accepted and pinned: a records list
    whose entries are all non-dicts counts here but expands to nothing, costing a bounded re-read
    per fold of that window - never observed live (every sampled record is a dict of scalars).
    """
    attrs = (stored_fact or {}).get("attributes") or {}
    try:
        return int(attrs.get("mi.record_count") or 0) > 0
    except (TypeError, ValueError):
        return False


async def _records_needing_expansion(db: AsyncSession, customer_code: str, window: UtcWindow,
                                     expanded: frozenset[str], stored_by_txn: dict,
                                     exp_v: str) -> set:
    """The record grain's own presence/staleness diff (18x): stored facts that predict records but
    whose record rows are MISSING or carry a stale `_exp_v`.

    This is what makes a late `expand` flip and a late field approval BACKFILL through ordinary
    tickets: the fact diff says `unchanged` for a settled window, but the record grain's stored
    state can still disagree with what the switches now demand. One bounded aggregate per window,
    pruned by the `event_time` predicate (record event_time is copied from the in-window parent),
    served by `ix_analytics_record_facts_customer_event`.
    """
    if not expanded:
        return set()
    candidates = {tid for tid, r in stored_by_txn.items()
                  if r.get("transaction_name") in expanded and _predicts_records(r)}
    if not candidates:
        return set()
    present = {row.txn: row.v for row in (await db.execute(
        select(AnalyticsRecordFact.source_transaction_id.label("txn"),
               func.min(AnalyticsRecordFact.attributes[_EXP_V_KEY].astext).label("v"))
        .where(AnalyticsRecordFact.customer_code == customer_code,
               window.covers(AnalyticsRecordFact.event_time, include_null=True),
               AnalyticsRecordFact.source_transaction_id.in_(sorted(candidates, key=str)))
        .group_by(AnalyticsRecordFact.source_transaction_id))).all()}
    return {tid for tid in candidates if present.get(tid) != exp_v}


async def _expand_records(db: AsyncSession, customer_code: str, *, outcomes,
                          by_txn: dict, expanded: frozenset[str], approved: frozenset[str],
                          discovered: dict, stored_by_txn: dict, refresh: set,
                          exp_v: str) -> dict:
    """R4/18x. Write one `analytics_record_facts` row per M3 record, for expanded transactions.

    Two drivers, one hygiene rule:

    - The DIFF's insert/update outcomes, as always - a transaction the diff reported `unchanged`
      has records that are also unchanged (S3's skip, one grain down).
    - The presence/staleness `refresh` set (18x) - settled transactions whose record rows are
      missing or stale because `expand` or a field approval arrived AFTER the window folded. Their
      facts did not change, so the row values come from `stored_by_txn`.
    - REVERSALS delete their record rows UNCONDITIONALLY - before and regardless of the `expanded`
      gate - because a parent fact that vanishes must take its records with it even when expand was
      turned off in between (invariant 5, one grain down; the alternative is permanent orphans in a
      KEEP_FOREVER table that a record rollup would silently count). An event-time move is
      reverse+insert at the diff (the key carries event_time), so this covers it too.

    Replace-per-transaction rather than upsert-per-record. A re-expansion may produce FEWER records
    than the last one, and an upsert keyed on `(transaction, index)` would leave the tail of the
    previous expansion behind forever.

    Approval is the same allowlist the scalar grain uses (`source = "record"`); every row is stamped
    with the expansion version it was written under (`_exp_v`), which is what lets the next fold
    prove it current without reading entries.
    """
    deleted = 0
    hours: set = set()
    dates: set = set()
    for o in outcomes:
        if o.action is not dd.Action.reverse or not o.stored:
            continue
        event_time = o.stored.get("event_time")
        result = await db.execute(delete(AnalyticsRecordFact).where(
            AnalyticsRecordFact.customer_code == customer_code,
            AnalyticsRecordFact.source_transaction_id == o.stored["source_transaction_id"],
            AnalyticsRecordFact.event_time.is_(None) if event_time is None
            else AnalyticsRecordFact.event_time == event_time))
        if result.rowcount:
            deleted += result.rowcount
            if event_time is not None:
                hours.add(event_time)
            if o.stored.get("business_date") is not None:
                dates.add(o.stored["business_date"])

    if not expanded:
        return {"records": 0, "transactions": 0, "deleted": deleted,
                "hours": hours, "dates": dates}

    changed_facts = [o.fact for o in outcomes
                     if o.action in (dd.Action.insert, dd.Action.update) and o.fact is not None]
    processed = {f["source_transaction_id"] for f in changed_facts}
    refresh_facts = [stored_by_txn[tid] for tid in refresh
                     if tid not in processed and tid in stored_by_txn]

    rows, touched = [], []
    for fact in (*changed_facts, *refresh_facts):
        if fact.get("transaction_name") not in expanded:
            continue
        txn_id = fact["source_transaction_id"]
        recs = pl.records(by_txn.get(txn_id, ()))
        if not recs:
            continue
        if len(recs) > pl.LOUD_EXPANSION:
            # A WARNING rather than a cap. Truncating would produce a record count that looks complete
            # and is not, which is worse than a large table somebody was told about.
            logger.warning("Analytics [%s]: transaction %s expanded to %d records, above the %d "
                           "threshold - check whether `expand` is ticked on the right transaction",
                           customer_code, txn_id, len(recs), pl.LOUD_EXPANSION)
        touched.append((txn_id, fact.get("event_time")))
        if fact.get("event_time") is not None:
            hours.add(fact["event_time"])
        if fact.get("business_date") is not None:
            dates.add(fact["business_date"])
        for index, rec in enumerate(recs):
            kept, _unknown = pl.select(rec["attributes"], approved)
            if rec["attributes"]:
                discovered.setdefault(fact.get("method"), set()).update(rec["attributes"])
            rows.append({
                "id": uuid.uuid4(), "customer_code": customer_code,
                "source_transaction_id": txn_id,
                "source_started_at": fact.get("source_started_at"),
                "record_index": index,
                "event_time": fact.get("event_time"), "business_date": fact.get("business_date"),
                "method": fact.get("method"), "transaction_name": fact.get("transaction_name"),
                "mi_program": rec["mi_program"], "mi_transaction": rec["mi_transaction"],
                "attributes": {**kept, _EXP_V_KEY: exp_v},
                "created_at": datetime.now(timezone.utc),
            })

    for txn_id, event_time in touched:
        replaced = await db.execute(delete(AnalyticsRecordFact).where(
            AnalyticsRecordFact.customer_code == customer_code,
            AnalyticsRecordFact.source_transaction_id == txn_id,
            AnalyticsRecordFact.event_time.is_(None) if event_time is None
            else AnalyticsRecordFact.event_time == event_time))
        # Chunk 82: counted, or every re-expansion double-counts its rows in
        # `record_facts_total` (live drift: 1,730,110 counted vs 1,641,626 actual).
        deleted += replaced.rowcount or 0
    if rows:
        await db.execute(pg_insert(AnalyticsRecordFact), rows)
    return {"records": len(rows), "transactions": len(touched), "deleted": deleted,
            "hours": hours, "dates": dates}


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
                   computed_at: datetime,
                   record_buckets: tuple[set, set] = (frozenset(), frozenset())) -> dict:
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
    # 18y: the record grain's dirty buckets are the UNION of the fact diff's and the expansion
    # driver's own (a backfill window's fact diff is all-unchanged, so without the union the
    # expand-on backfill would write record rows and never roll them up). Transaction rollups keep
    # using only the fact diff's buckets - a record-only refresh changes no transaction rollup.
    rec_hours = hours | record_buckets[0]
    rec_dates = dates | record_buckets[1]
    if not rec_hours and not rec_dates:
        # The 98.7% rebuild case, free all the way through rather than only as far as the fact table.
        return {"definitions": 0, "buckets": 0}

    await registry.ensure_seed(db, customer_code)
    # R2. The `show` switch, read once per run like the other two. Facts for a hidden transaction stay
    # exactly where they are; only the rollups exclude them, which is what makes the switch instant to
    # reverse - the next fold of the range refills complete history from facts that never left.
    hidden = await capture.hidden_names(db, customer_code)
    stats = {"definitions": 0, "buckets": len(rec_hours) + len(rec_dates)}
    for definition_id, definition in await registry.active_definitions(db, customer_code):
        d_hours, d_dates = ((rec_hours, rec_dates) if definition.source == "record"
                            else (hours, dates))
        fold = n5.recompute_records if definition.source == "record" else n5.recompute
        if not d_hours and not d_dates:
            continue
        try:
            await fold(db, customer_code, definition_id, definition,
                       hours=d_hours, dates=d_dates, computed_at=computed_at,
                       hidden=hidden)
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
                        records_net: int,
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
        "record_facts_total": max(records_net, 0),
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
            "record_facts_total": AnalyticsTenantState.record_facts_total + records_net,
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
        # The web tier's 30 s guard is wrong for a background fold, exactly as it was wrong for Stage
        # 1's bulk insert - which relaxes it for the same reason (CLAUDE.md rule 8 names that as a
        # deliberate exception). A fold of one day legitimately exceeds 30 s: measured 23.7 s of reads
        # alone on a 10,400-transaction day, before normalising, diffing or writing anything.
        #
        # 120 s rather than the 600 s first considered, and the difference is the BOUND. With
        # near-adjacent coalescing removed a run cannot exceed one ticket span, so the headroom needed
        # is one day's worth - knowable - instead of however large the next backlog happens to be. A
        # finite value keeps rule 8's safety net: a genuine runaway still aborts rather than holding a
        # transaction open across nine tables and pinning the vacuum horizon.
        await db.execute(text(
            f"SET LOCAL statement_timeout = {settings.analytics_fold_statement_timeout_ms}"))

        # R1. Read ONCE per run and passed to both reads, so the two halves of the diff cannot
        # disagree about what is captured even if somebody flips a switch mid-run. Reading it twice
        # would be a race whose symptom is a fact silently reversed.
        suppressed = await capture.suppressed_names(db, customer_code)

        source_rows = await _read_source(db, customer_code, window, suppressed)

        # R3. The response half, which Stage 2 parses and discards. Read here rather than in N2 so N2
        # keeps its no-database property: it is handed the already-approved scalars as a parameter.
        # Read stored FIRST now, because the response-read skip needs it to decide. Cheap - 134 ms
        # against the 22.8 s it saves.
        stored = await _read_stored(db, customer_code, window, suppressed)
        stored_by_txn = {r["source_transaction_id"]: r for r in stored}

        # R4/18x. MOVED above the entry read (it used to sit just before `_expand_records`), because
        # the record grain's presence diff decides which SETTLED transactions need their entries
        # re-read - and a switch is read once per run (the race rule at the top of this block).
        expanded = await capture.expanded_names(db, customer_code)
        exp_v = _expansion_version(await capture.approved_record_fields(db, customer_code))
        refresh = await _records_needing_expansion(db, customer_code, window, expanded,
                                                   stored_by_txn, exp_v)

        needs = {row["id"] for row in source_rows
                 if _needs_entries(row, stored_by_txn.get(row["id"]))} | refresh
        by_txn = await _read_response_entries(db, customer_code, window, only=needs)

        # Extract FIRST, register SECOND, read the approvals THIRD. The order is load-bearing and was
        # wrong on the first attempt.
        #
        # Reading `approved` before registering meant a seeded field was not yet approved on the run
        # that discovered it, so the fact was written WITHOUT it. That self-heals only if the window is
        # folded again - and tickets are published on change, so a window that never changes again
        # never is. The gap would have been permanent, which is the exact loss R3 exists to prevent.
        observed_by_row = [(row, pl.extract(by_txn.get(row["id"], ()))) for row in source_rows]
        discovered: dict[str, set[str]] = {}
        for row, observed in observed_by_row:
            if observed:
                # EVERY observed name is offered, approved or not: the registry is the review surface,
                # so a captured field still needs a row saying so and an unknown one needs a row saying
                # it exists. `observe_fields` seeds and de-duplicates.
                discovered.setdefault(row.get("method"), set()).update(observed)
        await capture.observe_fields(db, customer_code, discovered)

        # Now the seeded rows exist, so a field on the seed list is captured on the very run that
        # discovers it. Read ONCE for the tenant, like `suppressed`, so every transaction in the run is
        # judged against the same allowlist. An un-ticked field stays un-ticked: `observe_fields` uses
        # ON CONFLICT DO NOTHING, so it never resurrects a decision.
        approved = await capture.approved_attributes(db, customer_code)

        facts, issues = [], []
        for row, observed in observed_by_row:
            prior = stored_by_txn.get(row["id"])
            if row["id"] not in needs and prior is not None:
                # Nothing about this transaction moved and the normalisation is the same version, so the
                # stored fact is still exactly right. Carried through verbatim so the diff compares it
                # against itself and reports `unchanged` - no entry read, no normalise, no write.
                facts.append({k: v for k, v in prior.items() if k not in ("id", "created_at")})
                continue
            captured_attrs, _unknown = pl.select(observed, approved)
            fact, issue = n2.normalise(row, tenant_timezone=tz,
                                       response_attributes=captured_attrs)
            if fact is not None:
                # Stamp the skip inputs so the NEXT fold can prove this fact is current without
                # touching entries. Written into `attributes` after normalise, so they are inside the
                # fingerprint and a version bump therefore invalidates every stored fact by itself.
                fact["attributes"] = {**(fact.get("attributes") or {}),
                                      _SRC_FP_KEY: row.get("row_fingerprint"),
                                      _NORM_V_KEY: _NORMALISE_VERSION}
                fact["source_version_hash"] = n2._fingerprint(fact)
            (facts if fact is not None else issues).append(fact if fact is not None else issue)

        # Register any transaction name seen for the first time, at capture=on / show=off. Done from
        # the SOURCE rows rather than by a separate query: they are already in hand, and a name can
        # only be new if it appeared in a window somebody is folding.
        #
        # AFTER the read and inside the same transaction, so a name discovered here cannot suppress
        # itself on the run that discovered it - `observe_names` writes capture=true, but reading the
        # registry again mid-run is exactly the race the single read above avoids.
        await capture.observe_names(db, customer_code,
                                   {r.get("transaction_name") for r in source_rows})

        outcomes = dd.diff(stored, facts)

        now = datetime.now(timezone.utc)
        folded = await _apply(db, customer_code, outcomes, now)

        # R4. After the facts, because it is driven by their diff verdicts, and inside the same
        # transaction so a record set can never describe a fact that was rolled back. (`expanded`
        # itself was read once, above the entry read - the presence diff needed it there.)
        record_stats = await _expand_records(
            db, customer_code, outcomes=outcomes, by_txn=by_txn, expanded=expanded,
            approved=approved, discovered=discovered, stored_by_txn=stored_by_txn,
            refresh=refresh, exp_v=exp_v)
        # Discovery runs a second time because record fields were only just observed. Idempotent by
        # ON CONFLICT DO NOTHING, so the scalar names offered again cost nothing.
        await capture.observe_fields(db, customer_code, discovered, source="record")
        quarantined = await _quarantine(db, customer_code, issues, now)
        rolled = await _roll_up(db, customer_code, outcomes, now,
                                record_buckets=(record_stats["hours"], record_stats["dates"]))

        await _update_state(
            db, customer_code, folded=folded, quarantined=quarantined,
            records_net=record_stats["records"] - record_stats["deleted"],
            event_watermark=max((f["event_time"] for f in facts if f["event_time"]), default=None),
            history_start=min((f["event_time"] for f in facts if f["event_time"]), default=None),
            source_watermark=await _source_watermark(db, customer_code),
            frontier=max((r[_FRONTIER_COLUMN.key] for r in source_rows
                          if r.get(_FRONTIER_COLUMN.key)), default=None),
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
            "buckets_rolled": rolled["buckets"],
            "record_facts": record_stats["records"],
            "record_facts_deleted": record_stats["deleted"]}


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
             "definitions_rolled": 0, "buckets_rolled": 0,
             "record_facts": 0, "record_facts_deleted": 0}
    if not tickets:
        return stats

    # The same gap Stage 2 uses, for the same reason: two windows less than a pad apart describe
    # overlapping rebuilds, so diffing them separately would do the seam twice.
    from app.services.mnp_log_ingestion.pipeline.derive_transactions import _regroup_pad
    # Merge only tickets that GENUINELY OVERLAP - gap zero, not `2 * pad`.
    #
    # The old `2 * pad` gap merged merely-ADJACENT tickets, which is what Stage 2 does and what this
    # copied. Stage 2's reason does not transfer: there, an overlapping rebuild meant delete +
    # reinsert of the overlap, which is where its 22.4x write amplification came from. Here, since S3,
    # re-folding an overlap writes NOTHING - the diff reports `unchanged`.
    #
    # So the old gap paid a real price for a vanished benefit. Tickets are padded +/-900s, so adjacent
    # daily tickets overlap by 30 minutes: merging them saved re-reading 1.8% of an 8-day range, and
    # cost the `_MAX_TICKET_SPAN = 1 day` bound entirely. One coalesced run became 8 days of work in a
    # single transaction, which is precisely what exhausted the statement timeout and left 32,400 facts
    # unbuilt for five days.
    #
    # Zero gap still merges true overlaps, which is free and avoids folding the same instant twice in
    # one pass. It cannot produce a run wider than the widest single ticket plus its overlaps.
    for lo, hi, rows in _coalesce(tickets, gap=timedelta(0)):
        # MERGE, THEN SPLIT - exactly Stage 2's shape, which this only copied half of.
        #
        # Merging is load-bearing for correctness: a transaction whose rebuild moved its `started_at`
        # across a ticket boundary is reversed by one ticket and inserted by the next, and merging puts
        # both sides in one diff instead of leaving two facts transiently double-counted. Chunk 45
        # asserts that and it still holds.
        #
        # Splitting is load-bearing for bounded work. Because tickets are padded (invariant 2), merging
        # can turn eight bounded daily tickets into one eight-day run in a single transaction - which is
        # what exhausted the 30 s statement timeout and left 32,400 facts unbuilt for five days. The
        # ticket table already reasons this way for tickets (`_MAX_TICKET_SPAN`); coalescing undid it,
        # and this restores it one level up.
        #
        # Lossless for the same reason Stage 2's split is: consecutive sub-windows overlap at their seam
        # and the diff is idempotent, so a seam is folded twice and reported `unchanged` the second time.
        # `runs` keeps its original meaning - one COALESCED range, one unit of correctness - because
        # that is what chunk 45 asserts and the property is unchanged. `slices` is the new number: how
        # many bounded jobs that range was executed as. Reporting both means the split is observable
        # without redefining a figure other tests and the status card already read.
        stats["runs"] += 1
        slices = list(_split_run(lo, hi, settings.analytics_max_window_seconds))
        for index, (sub_lo, sub_hi) in enumerate(slices):
            stats["slices"] = stats.get("slices", 0) + 1
            # The tickets go to the LAST slice only. Every slice was stamping all of them, so two
            # tickets split into two slices reported four consumed - caught by chunk 45.
            #
            # The last slice rather than the first, because a ticket claims its whole range: consuming
            # it before the range is folded would let a crash mid-run leave the remainder with nothing
            # to retry it. This way an earlier slice's work commits without its ticket, so a retry
            # re-folds a range that is already correct and the diff reports `unchanged` - at-least-once,
            # never at-most-once, which is the direction that cannot lose data.
            claim = rows if index == len(slices) - 1 else []
            try:
                for key, value in (await _consume_run(
                        customer_code, sub_lo, sub_hi, claim, tz)).items():
                    stats[key] += value
            except Exception as exc:
                # Per SUB-WINDOW, so one poison six-hour slice fails in isolation instead of taking the
                # whole coalesced run with it. The tickets are attached to the run, so they stay open and
                # the next tick retries only what failed.
                stats["failed"] += 1
                stats["abandoned"] += await _record_failure(customer_code, rows, exc)
                logger.exception("Analytics: run %s..%s failed for %s - its tickets stay open for "
                                 "retry; the tenant's other runs are unaffected",
                                 sub_lo, sub_hi, customer_code)
                break   # the rest of this run's slices would very likely fail the same way
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
