"""Phase 4: reconciliation. The only component whose job is to catch the design being wrong.

Every invariant in this platform fails SILENTLY - the doc's own list says so, item by item, each one
"producing a plausible wrong number rather than an error". Nothing else here detects that. The range diff
keeps totals correct given a ticket; the ticket publisher covers every delete path it knows about; the
fingerprint absorbs rechecks. All of them are careful, and none of them notices when one of the others is
wrong. That is what this is for.

Three checks, and each catches a class the other two cannot see.

**1. facts vs transactions.** Does every transaction in the window have a fact, or a recorded reason it
does not? This is the ONLY check that can see a missed ticket, because it is the only one that reads the
projection rather than the warehouse. A recount from facts agrees with itself and reports nothing.

**2. rollups vs a fresh fold of the facts.** Reuses N5's own fold, deliberately. That looks like cheating
and is not: the bug class here is a bucket that was never recomputed - a dirty bucket the diff failed to
identify - so the fact table is right and the chart is wrong. Staleness is invisible to a second
implementation of the arithmetic, while re-folding catches it exactly. A hand-written second fold would
only ever test whether two pieces of arithmetic agree, which is not a failure anyone has.

**3. entries vs assignments.** F2 is explicit about why a recount is not enough:

    Recomputing from `log_transactions` cannot detect the roughly 1,000 orphaned entries, because both
    sides read the same incomplete projection and agree.

Measured on the live server: 847 to 1,079 entries with no assignment row, all past the abandon window,
from two named files. Grouped by source file so a cause can be found, because whatever caused it recurs.

Two rules about how it behaves
------------------------------
**Windowed** (A4). A full recount grows with all retained history and becomes a job nobody runs. Checks 1
and 2 take a window; check 3 does not, because an orphan is defined against the tenant's watermark rather
than against a range, and the orphan nobody has noticed is precisely the old one.

**Report-only by default** (Phase 7: "then report-only reconciliation"). A checker that silently fixes
things cannot be trusted to have found what it says it found, and the first weeks of findings are how you
learn whether the check itself is right.

**A repair never invents a number.** It re-runs whichever component should have produced the right one,
and which component that is depends on the check - a distinction an earlier version of this module got
wrong by answering both repairable checks with a ticket:

  facts_vs_transactions   publish a TICKET. A missing fact is a change, and only the range diff derives
                          a fact, so this sends the correction through the normal path.
  rollups_vs_facts        RE-FOLD the bucket. A ticket achieves nothing here: the facts are unchanged, so
                          the diff correctly reports every one `unchanged`, no bucket is marked dirty, and
                          N5 never recomputes. The corruption would survive its own repair, silently.
  entries_vs_assignments  NEITHER. The entry was never stitched into a transaction, so re-diffing reads
                          the same projection and re-folding aggregates the same facts. It needs a Stage 2
                          regroup, which is an operator action.

Neither repair is a second way to change a total: one calls the diff, the other calls the same folder the
writer calls, and both are idempotent. Nothing here writes a fact or a rollup value of its own.
"""

import logging
from dataclasses import dataclass, field as dc_field
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import String, and_, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (DIMENSION_SLOTS, AnalyticsDailyRollup,
                                                     AnalyticsHourlyRollup)
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import capture
from app.services.analytics import definition as d
from app.services.analytics import pending_windows as n1
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
from app.settings import settings

logger = logging.getLogger(__name__)

#: Named in one place so the worker can report per check, and an operator can see WHICH one is firing
#: rather than a single healthy/unhealthy bit.
CHECKS: tuple[str, ...] = ("facts_vs_transactions", "rollups_vs_facts", "entries_vs_assignments",
                           "record_rollups_vs_record_facts", "records_vs_facts")

#: Cap on the rows any single finding enumerates. A window that is wholly unfolded would otherwise
#: produce a report the size of the window, and the first hundred identify the cause just as well as ten
#: thousand do. The COUNT is always exact; only the sample is capped.
_SAMPLE = 100

#: Below this, two rollup values are the same number. Both sides are NUMERIC and the comparison is
#: exact, so this exists only to stop a scale difference (`10` against `10.000000`) reading as drift.
_EPSILON = Decimal("0.000001")

#: The two repairable checks need DIFFERENT repairs, and discovering that took running the thing end to
#: end. An earlier version answered both with a ticket, and for a stale rollup a ticket achieves NOTHING:
#: the facts are unchanged, so the range diff correctly reports every one of them `unchanged`, no bucket
#: is marked dirty, and N5 never recomputes. The corruption survives its own repair, silently.
#:
#: A MISSING FACT is a change, and only the diff can derive a fact, so a ticket is exactly right.
_TICKET_REPAIRABLE: frozenset[str] = frozenset({"facts_vs_transactions",
                                                # 18y: missing record rows ARE repairable by ticket
                                                # since 18x - the presence diff re-expands them.
                                                "records_vs_facts"})

#: A DRIFTED BUCKET is not a change to any fact, so the repair is to re-fold that bucket. That is not a
#: second way to change a total: it calls the same folder the writer calls, and recompute-and-replace is
#: idempotent by construction, so repairing twice is indistinguishable from repairing once.
_REFOLD_REPAIRABLE: frozenset[str] = frozenset({"rollups_vs_facts",
                                                "record_rollups_vs_record_facts"})

#: Neither repair helps an orphaned entry. It was never stitched into a transaction, so re-diffing the
#: range reads the same projection and re-folding aggregates the same facts. It needs a Stage 2 regroup,
#: which is an operator action, so the finding is reported and deliberately left alone.


@dataclass(frozen=True)
class Finding:
    """One thing that is wrong, and enough detail to act on it without re-running anything."""

    check: str
    summary: str
    detail: dict = dc_field(default_factory=dict)
    #: The event-time range a repair ticket should cover, when a ticket can help.
    repair_range: tuple[datetime, datetime] | None = None
    #: (grain, bucket) to re-fold, when re-folding is what helps instead.
    refold: tuple[str, Any] | None = None


async def facts_vs_transactions(db: AsyncSession, customer_code: str, *,
                                window: UtcWindow) -> list[Finding]:
    """Transactions in the window with no fact and no recorded reason for not having one.

    The missed-ticket detector. `include_null=True` (A7) because a range test is false for NULL and a
    transaction whose entries all lack a parsable timestamp still has to be folded - a check that forgot
    it would never notice the DEFAULT partition going unfolded.

    A quarantined transaction is NOT reported. It has no fact on purpose and the reason is recorded, so
    reporting it would make the check cry wolf on every unusable row, which is how a check gets switched
    off.
    """
    has_fact = select(AnalyticsFact.id).where(
        AnalyticsFact.customer_code == customer_code,
        cast(AnalyticsFact.source_transaction_id, String) == cast(LogTransaction.id, String),
        or_(and_(AnalyticsFact.event_time.is_(None), LogTransaction.started_at.is_(None)),
            AnalyticsFact.event_time == LogTransaction.started_at))
    has_reason = select(AnalyticsQualityIssue.id).where(
        AnalyticsQualityIssue.customer_code == customer_code,
        cast(AnalyticsQualityIssue.source_transaction_id, String) == cast(LogTransaction.id, String))

    # R1: a transaction whose capture is switched off has no fact ON PURPOSE, exactly like a
    # quarantined one. Without this clause the auditor reports it as a missing fact on every single
    # run, forever - and a permanently red check is worse than no check, because it teaches everyone
    # to ignore the one thing that would have caught a real divergence. The predicate is imported
    # rather than rewritten so it cannot drift from the one the fold uses.
    gate = capture.source_predicate(await capture.suppressed_names(db, customer_code))
    stmt = select(LogTransaction.id, LogTransaction.started_at).where(
        LogTransaction.customer_code == customer_code,
        window.covers(LogTransaction.started_at, include_null=True),
        ~has_fact.exists(), ~has_reason.exists(),
        *([gate] if gate is not None else []))

    rows = (await db.execute(stmt)).all()
    if not rows:
        return []

    instants = [r.started_at for r in rows if r.started_at is not None]
    return [Finding(
        check="facts_vs_transactions",
        summary=(f"{len(rows)} transaction(s) in this window have neither a fact nor a recorded reason "
                 f"for not having one, which is what a missed ticket looks like"),
        detail={"transactions_without_facts": len(rows),
                "sample_ids": [str(r.id) for r in rows[:_SAMPLE]],
                "earliest": min(instants).isoformat() if instants else None,
                "latest": max(instants).isoformat() if instants else None,
                "with_no_start_instant": sum(1 for r in rows if r.started_at is None)},
        repair_range=(min(instants), max(instants)) if instants else None)]


def _stored_key(row) -> tuple:
    return (row.measure_name,
            tuple(getattr(row, f"dim{i + 1}") for i in range(DIMENSION_SLOTS)))


async def _stored_rollups(db: AsyncSession, customer_code: str, model, definition_id,
                          bucket_column: str, window: UtcWindow, *, is_date: bool) -> dict:
    column = getattr(model, bucket_column)
    predicate = (column.between(window.start.date(), window.end.date())
                 if is_date and window.start and window.end
                 else window.covers(column, include_null=False))
    rows = (await db.execute(select(model).where(
        model.customer_code == customer_code, model.definition_id == definition_id,
        predicate))).scalars().all()
    return {(getattr(r, bucket_column), *_stored_key(r)): r for r in rows}


async def rollups_vs_facts(db: AsyncSession, customer_code: str, *,
                           window: UtcWindow) -> list[Finding]:
    """Re-fold the window's facts and compare with what the rollup tables hold.

    Catches both directions of staleness: a bucket whose stored value no longer matches its facts, and a
    bucket whose facts are all gone but whose rollup row survived. The second is the exact shape N5's
    "recompute to nothing means DELETE" rule prevents, so this has to be able to see it if that rule ever
    regresses.
    """
    facts = [{c.name: getattr(f, c.name) for c in AnalyticsFact.__table__.columns}
             for f in (await db.execute(select(AnalyticsFact).where(
                 AnalyticsFact.customer_code == customer_code,
                 window.covers(AnalyticsFact.event_time, include_null=False)))).scalars().all()]
    # 18y: only TRANSACTION-source definitions. A record definition folded against transaction facts
    # produces non-empty expectations (record metrics carry no status/classification filters, so
    # every fact "contributes") while its real rollup rows surface as orphaned - permanently red,
    # and under repair=true the refold would REPLACE record rollups with transaction-fact folds.
    definitions = [(i, dfn) for i, dfn in await registry.active_definitions(db, customer_code)
                   if dfn.source == "transaction"]
    return await _compare_rollups(db, customer_code, window=window, facts=facts,
                                  definitions=definitions, check="rollups_vs_facts")


async def record_rollups_vs_record_facts(db: AsyncSession, customer_code: str, *,
                                         window: UtcWindow) -> list[Finding]:
    """18y: the record grain's own rollup audit - the same comparator, fed record rows and
    record-source definitions, so the two grains cannot drift in what "audited" means."""
    facts = [{c.name: getattr(f, c.name) for c in AnalyticsRecordFact.__table__.columns}
             for f in (await db.execute(select(AnalyticsRecordFact).where(
                 AnalyticsRecordFact.customer_code == customer_code,
                 window.covers(AnalyticsRecordFact.event_time, include_null=False)))).scalars().all()]
    definitions = [(i, dfn) for i, dfn in await registry.active_definitions(db, customer_code)
                   if dfn.source == "record"]
    return await _compare_rollups(db, customer_code, window=window, facts=facts,
                                  definitions=definitions,
                                  check="record_rollups_vs_record_facts")


async def _compare_rollups(db: AsyncSession, customer_code: str, *, window: UtcWindow,
                           facts: list[dict], definitions, check: str) -> list[Finding]:

    # Chunk 66. Only buckets the window covers WHOLE are comparable: the stored row was folded from
    # the entire bucket, the recount above only from the window's slice, so a bucket straddling a
    # window edge disagrees BY CONSTRUCTION. On the live tenant that construction produced 127-463
    # findings per hourly pass (every one of the three kinds, all boundary clothes), which is the
    # "always red" failure the worker's own docstring warns about. The daily bucket is the tenant's
    # LOCAL day, so its UTC range needs the tenant timezone - and needs the window to be at least
    # 48 h, or no local day is ever whole inside it (settings.analytics_reconcile_window_hours).
    tenant_tz = ZoneInfo(await get_customer_timezone(db, customer_code))

    def _covered(grain: str, bucket) -> bool:
        if window.start is None or window.end is None:
            return True   # an open window is the explicit full-history audit: everything is whole
        if bucket is None:
            return False  # no instant, no range, nothing to clip a comparison to
        if grain == "hourly":
            return bucket >= window.start and bucket + timedelta(hours=1) <= window.end
        day = datetime.combine(bucket, time.min, tzinfo=tenant_tz)
        nxt = datetime.combine(bucket + timedelta(days=1), time.min, tzinfo=tenant_tz)
        return day >= window.start and nxt <= window.end

    findings: list[Finding] = []
    for definition_id, definition in definitions:
        for grain, model, column, bucket_of, is_date in (
            ("hourly", AnalyticsHourlyRollup, "bucket_start",
             lambda r: n5.hour_of(r["event_time"]) if r.get("event_time") else None, False),
            ("daily", AnalyticsDailyRollup, "business_date",
             lambda r: r.get("business_date"), True),
        ):
            if grain not in definition.grains:
                continue
            # The SAME fold the writer uses. See the module docstring: the bug class is a bucket that
            # was never recomputed, not arithmetic that disagrees with itself.
            expected = n5.group_fold(facts, definition, bucket_of)
            stored = await _stored_rollups(db, customer_code, model, definition_id, column, window,
                                           is_date=is_date)
            # Both sides clipped to whole buckets, or a partially covered stored row would surface as
            # a false "orphaned" the moment its expected twin was skipped.
            expected = {k: v for k, v in expected.items() if _covered(grain, k[0])}
            stored = {k: v for k, v in stored.items() if _covered(grain, k[0])}

            for (bucket, dims), measures in expected.items():
                for measure_name, roles in measures.items():
                    if n5._is_empty(roles):
                        continue
                    row = stored.pop((bucket, measure_name, dims), None)
                    want = roles.get(d.Role.sum_value)
                    got = getattr(row, "sum_value", None) if row is not None else None
                    if row is None:
                        findings.append(_drift(grain, definition, measure_name, bucket, dims,
                                               None, want, "missing", check=check))
                    elif want is not None and abs(Decimal(str(got or 0)) - want) > _EPSILON:
                        findings.append(_drift(grain, definition, measure_name, bucket, dims,
                                               got, want, "differs", check=check))
            # Anything left in `stored` has no facts behind it at all.
            for (bucket, measure_name, dims), row in list(stored.items())[:_SAMPLE]:
                findings.append(_drift(grain, definition, measure_name, bucket, dims,
                                        row.sum_value, None, "orphaned", check=check))
    return findings


async def records_vs_facts(db: AsyncSession, customer_code: str, *,
                           window: UtcWindow) -> list[Finding]:
    """18y: every expanded fact that PREDICTS records (`mi.record_count` > 0 - the same gate the
    18x presence diff uses, without which every no-record response would be permanently red) must
    have record rows. The one check that can catch a destructive re-expansion that dropped rows.

    Repairable by ticket since 18x: the presence diff re-expands the range - PROVIDED the raw
    entries still exist. Beyond entry retention the rows are unrecoverable and the finding says so
    instead of promising a repair that would do nothing.
    """
    expanded = await capture.expanded_names(db, customer_code)
    if not expanded:
        return []
    facts = (await db.execute(
        select(AnalyticsFact.source_transaction_id, AnalyticsFact.event_time,
               AnalyticsFact.attributes, AnalyticsFact.transaction_name)
        .where(AnalyticsFact.customer_code == customer_code,
               window.covers(AnalyticsFact.event_time, include_null=False),
               AnalyticsFact.transaction_name.in_(sorted(expanded))))).all()
    predicted = {}
    for f in facts:
        count = (f.attributes or {}).get("mi.record_count")
        try:
            if int(count or 0) > 0:
                predicted[f.source_transaction_id] = f
        except (TypeError, ValueError):
            continue
    if not predicted:
        return []
    present = set((await db.execute(
        select(AnalyticsRecordFact.source_transaction_id).distinct()
        .where(AnalyticsRecordFact.customer_code == customer_code,
               window.covers(AnalyticsRecordFact.event_time, include_null=False),
               AnalyticsRecordFact.source_transaction_id.in_(
                   sorted(predicted, key=str))))).scalars().all())
    missing = [predicted[t] for t in predicted if t not in present]
    if not missing:
        return []
    retention_floor = datetime.now(timezone.utc) - timedelta(
        days=settings.log_partition_retention_days)
    findings = []
    for f in missing[:_SAMPLE]:
        recoverable = f.event_time is not None and f.event_time >= retention_floor
        findings.append(Finding(
            check="records_vs_facts",
            summary=(f"expanded transaction {f.source_transaction_id} ({f.transaction_name}) "
                     f"predicts records (mi.record_count) but has no record rows"
                     + ("" if recoverable else " - beyond entry retention, unrecoverable")),
            detail={"transaction_name": f.transaction_name,
                    "source_transaction_id": str(f.source_transaction_id),
                    "recoverable": recoverable},
            repair_range=((f.event_time, f.event_time + timedelta(seconds=1))
                          if recoverable else None)))
    return findings


def _drift(grain, definition, measure_name, bucket, dims, stored, recomputed, kind,
           check: str = "rollups_vs_facts") -> Finding:
    return Finding(
        check=check,
        summary=(f"{grain} rollup for {definition.name}/{measure_name} at {bucket} is {kind}: "
                 f"stored {stored}, recomputed {recomputed}"),
        detail={"grain": grain, "definition": definition.name, "measure": measure_name,
                "bucket": str(bucket), "dimensions": list(dims), "kind": kind,
                "stored": None if stored is None else str(stored),
                "recomputed": None if recomputed is None else str(recomputed)},
        # No repair_range: a ticket cannot fix this, see _REFOLD_REPAIRABLE.
        refold=(grain, bucket))


def _bucket_range(grain: str, bucket) -> tuple[datetime, datetime] | None:
    """The event-time range a repair ticket must cover to re-derive this bucket."""
    from datetime import date as date_type, timezone
    if isinstance(bucket, datetime):
        return (bucket, bucket + timedelta(hours=1))
    if isinstance(bucket, date_type):
        start = datetime.combine(bucket, datetime.min.time(), tzinfo=timezone.utc)
        # Padded a day either side: `business_date` is the tenant-LOCAL day, so the UTC instants that
        # produced it can sit outside the UTC day of the same name.
        return (start - timedelta(days=1), start + timedelta(days=2))
    return None


async def orphaned_entries(db: AsyncSession, customer_code: str) -> list[Finding]:
    """Entries past the abandon window with no assignment row, grouped by source file.

    Deliberately NOT windowed, unlike the other two checks: an orphan is defined against the tenant's
    watermark rather than against a range, and the orphan nobody has noticed is precisely the old one.

    The cutoff follows the tenant's own DATA watermark, exactly as `_cutoffs` in Stage 2 does, not wall
    clock. A quiet tenant whose newest log line is a week old has lost nothing, and a wall-clock cutoff
    would report every one of its entries as orphaned - which is the kind of false alarm that gets a
    check disabled.
    """
    max_ts = await db.scalar(select(func.max(LogEntry.timestamp)).where(
        LogEntry.customer_code == customer_code))
    if max_ts is None:
        return []
    cutoff = max_ts - timedelta(seconds=settings.log_abandon_window_seconds)

    assigned = select(LogEntryAssignment.entry_id).where(
        LogEntryAssignment.customer_code == customer_code,
        LogEntryAssignment.entry_id == LogEntry.id)
    rows = (await db.execute(
        select(LogEntry.source_file, func.count().label("n"),
               func.min(LogEntry.timestamp), func.max(LogEntry.timestamp))
        .where(LogEntry.customer_code == customer_code,
               LogEntry.timestamp < cutoff, ~assigned.exists())
        .group_by(LogEntry.source_file).order_by(func.count().desc()))).all()

    return [Finding(
        check="entries_vs_assignments",
        summary=(f"{n} entry/entries in {source_file} are past the abandon window with no assignment "
                 f"row, so they belong to no transaction and are invisible to any recount"),
        detail={"source_file": source_file, "orphaned": n,
                "earliest": lo.isoformat() if lo else None,
                "latest": hi.isoformat() if hi else None,
                "cutoff": cutoff.isoformat(),
                "cutoff_basis": "tenant data watermark, not wall clock"},
        # No repair range: a ticket cannot fix this. See _REPAIRABLE.
        repair_range=None) for source_file, n, lo, hi in rows]


async def _repair_by_ticket(db: AsyncSession, customer_code: str, findings) -> int:
    """Publish a ticket per finding whose cause is "this range was never diffed".

    Sends the correction through the same range diff as everything else, so the fact is derived exactly
    as it would have been on the first pass.
    """
    published = 0
    for f in findings:
        if f.check in _TICKET_REPAIRABLE and f.repair_range:
            lo, hi = f.repair_range
            published += await n1.publish(db, customer_code, lo=lo, hi=hi)
    return published


async def _repair_by_refold(db: AsyncSession, customer_code: str, findings) -> int:
    """Re-fold every drifted bucket, using the same folder the writer uses.

    Collected across findings and applied once per definition rather than per finding: N5 replaces whole
    buckets, so re-folding the same hour three times because three measures drifted would do identical
    work three times.
    """
    def _buckets(check: str, grain: str) -> set:
        return {f.refold[1] for f in findings
                if f.check == check and f.refold and f.refold[0] == grain}

    txn_hours, txn_dates = _buckets("rollups_vs_facts", "hourly"), _buckets("rollups_vs_facts", "daily")
    rec_hours = _buckets("record_rollups_vs_record_facts", "hourly")
    rec_dates = _buckets("record_rollups_vs_record_facts", "daily")
    if not (txn_hours or txn_dates or rec_hours or rec_dates):
        return 0
    # A drifted hour implies its local day may be drifted too, and monthly folds from daily -- so the
    # day is included rather than left to the next pass to notice.
    txn_dates |= {h.date() for h in txn_hours}
    rec_dates |= {h.date() for h in rec_hours}
    # 18y: routed by the definition's source. Un-routed, the repair would DELETE a record
    # definition's rollup buckets and REPLACE them with transaction-fact folds - corruption through
    # the repair path, which is worse than the drift it was fixing.
    for definition_id, definition in await registry.active_definitions(db, customer_code):
        if definition.source == "record":
            if rec_hours or rec_dates:
                await n5.recompute_records(db, customer_code, definition_id, definition,
                                           hours=rec_hours, dates=rec_dates)
        elif txn_hours or txn_dates:
            await n5.recompute(db, customer_code, definition_id, definition,
                               hours=txn_hours, dates=txn_dates)
    return len(txn_hours | rec_hours) + len(txn_dates | rec_dates)


async def reconcile_tenant(db: AsyncSession, customer_code: str, *, window: UtcWindow,
                           repair: bool = False) -> dict:
    """Run all three checks for one tenant. Report-only unless `repair` is set. Does not commit.

    `repair` publishes a ticket per repairable finding, which sends the correction through the same range
    diff as everything else. It never writes a fact or a rollup: that would be a second way to change a
    total, and an untested one.
    """
    findings: list[Finding] = []
    findings += await facts_vs_transactions(db, customer_code, window=window)
    findings += await rollups_vs_facts(db, customer_code, window=window)
    findings += await record_rollups_vs_record_facts(db, customer_code, window=window)
    findings += await records_vs_facts(db, customer_code, window=window)
    findings += await orphaned_entries(db, customer_code)

    published, recomputed = 0, 0
    if repair:
        published = await _repair_by_ticket(db, customer_code, findings)
        recomputed = await _repair_by_refold(db, customer_code, findings)
        if published or recomputed:
            logger.warning("Reconciliation repaired %s: %d ticket(s) published, %d bucket(s) re-folded, "
                           "from %d finding(s)", customer_code, published, recomputed, len(findings))

    by_check = {c: sum(1 for f in findings if f.check == c) for c in CHECKS}
    if findings:
        logger.error("Reconciliation for %s found %d problem(s): %s", customer_code, len(findings),
                     ", ".join(f"{k}={v}" for k, v in by_check.items() if v))
    return {"customer_code": customer_code, "findings": findings, "by_check": by_check,
            "healthy": not findings, "tickets_published": published,
            "buckets_recomputed": recomputed,
            "window": {"start": window.start.isoformat() if window.start else None,
                       "end": window.end.isoformat() if window.end else None}}
