"""N5, the rollup folder: maintain the grain cascade for every active definition.

Phase 3c of docs/analytics-ml-architecture/final_architecture.md.

    Maintains the grain cascade per active definition: `facts -> hourly -> daily -> monthly`.
    Each level reads only the level below, so the fact table is read once per cycle.
    **Every write is recompute-and-replace, never increment.** An additive upsert double-counts on the
    first retry.

Recompute-and-replace is the decision everything else follows from, and it is worth being precise about
what it changes. N3 hands over diff OUTCOMES, each carrying the old and the new version of a fact, and
the tempting thing is to subtract the old and add the new to the stored bucket. That is wrong for a
reason no test of the happy path would catch: a cycle that fails after writing the rollup but before
committing its tickets is retried, and the retry adjusts the same bucket again. So the outcomes are used
ONLY to decide *which buckets are dirty*; each dirty bucket is then recomputed from scratch and its rows
replaced. Applying the same range twice is then indistinguishable from applying it
once, which is the only property that makes a retry safe.

A consequence that is easy to miss: **a bucket that recomputes to nothing must be deleted, not skipped.**
When the last fact in an hour is reversed, "replace the rows for this bucket" has to mean removing them.
Leaving them would strand the old total in every chart with nothing to indicate it was stale -- the same
silent-wrongness the range diff exists to prevent, reintroduced one level up.

The cascade, and the zone guard (chunk 91)
-------------------------------------------
Each level reads only the level below: dirty HOURS are folded from facts (read once per run for every
active definition), each dirty local DAY is merged from its hourly rows, and each month from its days.
Hourly buckets are UTC hours and daily buckets are the tenant-LOCAL `business_date`, so the middle step
is exact only when the local day is a whole set of hourly buckets - which is true whenever both of the
day's local midnights fall on whole UTC hours. Every whole-hour zone qualifies, Europe/London on both
sides of a clock change included (23 or 25 buckets, still whole). A zone at +05:30 does not: one UTC
hour per day straddles two local dates. `local_day_span` is that guard, and a day it refuses, a day
older than the hourly retention horizon (its hourly rows may already be dropped), or a definition with
no hourly grain falls back to the fact read by date that this module always did.

Distinct counts cascade too since chunk 88: the sketch unions register-wise like every other role.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import date as date_type, datetime, time as time_type, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (DIMENSION_SLOTS, AnalyticsDailyRollup,
                                                     AnalyticsHourlyRollup, AnalyticsMonthlyRollup)
from app.services.analytics import contract as c
from app.services.analytics import definition as d
from app.services.analytics import diff as dd

logger = logging.getLogger(__name__)

#: Role -> the column holding it. The role names ARE the column names, which is the whole point of
#: naming them for their additive role: this mapping is an identity, and a new role is a column rather
#: than a branch.
_ROLE_COLUMN: dict[d.Role, str] = {r: r.value for r in d.Role}

#: A bucket's dimension values, positional, interpreted through the definition's `dimensions` list.
DimKey = tuple[str | None, ...]

#: How long hourly rows are kept, mirrored from `log_partition_worker.RETENTION_DAYS` and pinned equal
#: by a test. A day older than this may have lost its hourly rows to retention, so its daily bucket
#: must come from facts: deriving it from a half-dropped hourly level would DELETE the day's history.
HOURLY_RETENTION_DAYS = 90
#: Days of slack before the horizon, because retention runs hourly and drops whole partitions.
DERIVE_MARGIN_DAYS = 2


def _zone(tz: str | None):
    """The tenant zone, resolved EXACTLY as `normalizer._local_date` resolves it: None and an unusable
    name both mean UTC. The guard and `business_date` must agree, or a derived day would not be the day
    the facts were filed under."""
    if not tz:
        return timezone.utc
    try:
        return ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        return timezone.utc


def local_day_span(day: date_type, tz: str | None) -> tuple[datetime, datetime] | None:
    """The UTC instants of `day`'s local midnight and the next, or None when the day is NOT a whole set
    of hourly buckets.

    Both midnights must fall on whole UTC hours. For a whole-hour zone that always holds, including
    across a DST change (a 23- or 25-hour day is still whole hours). For a half-hour zone it never
    does, and the daily bucket has to be folded from facts instead.
    """
    zone = _zone(tz)
    lo = datetime.combine(day, time_type.min, tzinfo=zone).astimezone(timezone.utc)
    hi = datetime.combine(day + timedelta(days=1), time_type.min, tzinfo=zone).astimezone(timezone.utc)
    if lo != hour_of(lo) or hi != hour_of(hi):
        return None
    return lo, hi


def daily_derivable(day: date_type, tz: str | None, *, today: date_type) -> bool:
    """Whether `day`'s daily bucket may be merged from hourly rows rather than folded from facts."""
    if day < today - timedelta(days=HOURLY_RETENTION_DAYS - DERIVE_MARGIN_DAYS):
        return False
    return local_day_span(day, tz) is not None


def hours_in(lo: datetime, hi: datetime) -> set[datetime]:
    """Every hourly bucket the closed range `[lo, hi]` touches."""
    out: set[datetime] = set()
    cursor = hour_of(lo)
    last = hour_of(hi)
    while cursor <= last:
        out.add(cursor)
        cursor += timedelta(hours=1)
    return out


def local_dates_in(lo: datetime, hi: datetime, tz: str | None) -> set[date_type]:
    """Every tenant-local day the closed range `[lo, hi]` touches."""
    zone = _zone(tz)
    first = lo.astimezone(zone).date()
    last = hi.astimezone(zone).date()
    return {first + timedelta(days=i) for i in range((last - first).days + 1)}


@dataclass(frozen=True)
class Changed:
    """What one run's writing outcomes touched, from BOTH sides of every outcome."""

    methods: frozenset
    names: frozenset


def changed_of(outcomes: Iterable[dd.Outcome]) -> Changed:
    methods: set = set()
    names: set = set()
    for o in outcomes:
        if not o.writes:
            continue
        for side in (o.stored, o.fact):
            if side is None:
                continue
            methods.add(side.get("method"))
            names.add(side.get("transaction_name"))
    return Changed(frozenset(methods), frozenset(names))


def concerns(definition: d.MetricDefinition, changed: Changed) -> bool:
    """Whether any changed fact can be inside `definition`'s filter. False means the run cannot have
    altered any of its buckets, so folding it would rewrite identical rows for nothing."""
    if definition.method_filter and not (set(definition.method_filter) & changed.methods):
        return False
    if definition.transaction_filter and not (set(definition.transaction_filter) & changed.names):
        return False
    return True


def hour_of(moment: datetime) -> datetime:
    """The UTC hour `moment` falls in. Always UTC: the hourly grain is a machine-time axis, and the
    tenant-local axis is the daily grain's job."""
    utc = moment.astimezone(timezone.utc)
    return utc.replace(minute=0, second=0, microsecond=0)


def month_of(day: date_type) -> date_type:
    """The first of `day`'s month. A pure function of the business date, which is what makes monthly
    foldable from daily exactly."""
    return day.replace(day=1)


def _month_end(month_start: date_type) -> date_type:
    """The last day of `month_start`'s month. Computed via the next month's first day rather than by a
    length table, so December needs no special case."""
    nxt = date_type(month_start.year + month_start.month // 12,
                    month_start.month % 12 + 1, 1)
    return nxt - timedelta(days=1)


def dirty_buckets(outcomes: Iterable[dd.Outcome]) -> tuple[set[datetime], set[date_type]]:
    """The hours and business dates the diff touched.

    BOTH sides of every outcome contribute. A rebuild can move a transaction's `event_time`, and when it
    does, the bucket it left is just as dirty as the one it arrived in -- recomputing only the new one
    would leave the old bucket holding a contribution that no longer exists anywhere.

    `unchanged` outcomes contribute nothing, which is what keeps the 98.7% rebuild case free all the way
    through to the rollups rather than only as far as the fact table.
    """
    hours: set[datetime] = set()
    dates: set[date_type] = set()
    for o in outcomes:
        if not o.writes:
            continue
        for side in (o.stored, o.fact):
            if side is None:
                continue
            # A fact with no event_time cannot be placed in an hour or a local day, and both bucket
            # columns are NOT NULL. Such a row is excluded from every grain rather than bucketed into a
            # DEFAULT partition retention could never reclaim; the read layer reaches it by scanning
            # facts directly.
            when = side.get("event_time")
            if when is not None:
                hours.add(hour_of(when))
            day = side.get("business_date")
            if day is not None:
                dates.add(day)
    return hours, dates


def _dim_key(row: Mapping[str, Any], definition: d.MetricDefinition) -> DimKey:
    """`row`'s dimension values, padded to the fixed number of slots.

    Padded rather than truncated-to-length so the tuple length is stable across definitions, which is
    what lets one insert path serve them all.

    R1b: resolved through `contract.resolve_field`, so a dimension may name a key inside `attributes`
    as `attr:resp.BaseUoM`. Normalised through `contract.dimension_value`, which is the SAME helper the
    promoted-column path uses - if the two differed by so much as trimming, one item's total would
    split across two buckets once a field was promoted (decision C, section 18e).
    """
    values = [c.dimension_value(c.resolve_field(row, name))
              for name in definition.dimensions[:DIMENSION_SLOTS]]
    values += [None] * (DIMENSION_SLOTS - len(values))
    return tuple(values)


def group_fold(rows: Iterable[Mapping[str, Any]], definition: d.MetricDefinition, bucket_of
               ) -> dict[tuple[Any, DimKey], dict]:
    """Fold `rows` into `{(bucket, dimensions): {measure: {role: value}}}`.

    `bucket_of` returns a row's bucket, or None to exclude it. Delegates the arithmetic entirely to
    `definition.fold`, so every filter -- method, classification and status -- is applied in exactly one
    place and this function never asks what it is measuring.
    """
    grouped: dict[tuple[Any, DimKey], list] = {}
    for row in rows:
        bucket = bucket_of(row)
        if bucket is None:
            continue
        grouped.setdefault((bucket, _dim_key(row, definition)), []).append(row)
    return {key: d.fold(group, definition) for key, group in grouped.items()}


def _is_empty(roles: Mapping[d.Role, Any]) -> bool:
    """Whether a measure's bucket carries no observations at all.

    Keyed on the count and on min/max rather than on the sum, because a sum of zero is a real and
    important answer: a bucket of nothing but zero-unit picks sums to 0 and must still be stored, or the
    zero-pick rate loses exactly the rows it is about.
    """
    if roles.get(d.Role.count_value):
        return False
    if roles.get(d.Role.min_value) is not None or roles.get(d.Role.max_value) is not None:
        return False
    return True


def _rows_for(customer_code: str, definition_id: uuid.UUID, definition: d.MetricDefinition,
              folded: Mapping[tuple[Any, DimKey], dict], *, bucket_column: str,
              computed_at: datetime) -> list[dict]:
    """Rollup rows for one grain: one per (bucket, dimensions, MEASURE).

    Per measure, per correction log C5: a definition needing one sum and two counts cannot share one set
    of role columns, so consumption emits three rows per bucket rather than one.
    """
    out: list[dict] = []
    for (bucket, dims), measures in folded.items():
        for measure_name, roles in measures.items():
            if _is_empty(roles):
                continue
            row = {"id": uuid.uuid4(), "customer_code": customer_code,
                   "definition_id": definition_id, "measure_name": measure_name,
                   bucket_column: bucket, "computed_at": computed_at}
            row.update({f"dim{i + 1}": dims[i] for i in range(DIMENSION_SLOTS)})
            row.update({_ROLE_COLUMN[role]: value for role, value in roles.items()
                        if role is not d.Role.histogram})
            # Chunk 92: a JSONB array of band counts, NULL when nothing was counted so an unused
            # column costs nothing and `_is_empty` keeps deciding on the count.
            hist = roles.get(d.Role.histogram)
            row["histogram"] = [int(x) for x in hist] if hist is not None and any(hist) else None
            out.append(row)
    return out


async def _replace(db: AsyncSession, model, customer_code: str, definition_id: uuid.UUID, *,
                   bucket_column: str, buckets: Sequence[Any], rows: list[dict]) -> dict:
    """Delete every row for these buckets, then insert the recomputed ones.

    The delete is unconditional over the buckets rather than keyed to what was recomputed, and that is
    the point: a dimension combination that disappeared, or a bucket that now folds to nothing, is
    removed by the same statement. Keying the delete to the new rows would leave both behind.
    """
    if not buckets:
        return {"deleted": 0, "inserted": 0}
    column = getattr(model, bucket_column)
    result = await db.execute(delete(model).where(
        model.customer_code == customer_code, model.definition_id == definition_id,
        column.in_(list(buckets))))
    if rows:
        await db.execute(pg_insert(model), rows)
    return {"deleted": result.rowcount or 0, "inserted": len(rows)}


def _since_gate(model, since: datetime | None) -> list:
    """Chunk 86: the metric's `rollups_from`, as a predicate. Inclusive at the instant itself, and an
    empty list when unbounded so the query plan of a pre-builder metric is byte-identical to before."""
    return [model.event_time >= since] if since is not None else []


async def _read_dirty_facts(db: AsyncSession, customer_code: str, hours: set[datetime],
                            dates: set[date_type],
                            hidden: frozenset[str] = frozenset(),
                            since: datetime | None = None) -> list[dict]:
    """The facts feeding every dirty bucket, read ONCE and folded into both grains.

    Two predicates OR-ed rather than one: an hour and a business date are different axes, and a fact can
    be in a dirty hour without being on a dirty date (or the reverse) after a rebuild moved it. Read as
    a contiguous range per axis and filtered to the exact dirty set in Python -- a hundred-term OR would
    defeat the planner, while a range that spans a gap merely reads a few rows that are then ignored.

    `since` is the definition's `rollups_from` (chunk 86). A dirty bucket entirely before it reads no
    facts, folds to nothing, and `_replace` deletes whatever the bucket held - which is what a metric
    that starts at an instant means.
    """
    conditions = []
    if hours:
        conditions.append(and_(AnalyticsFact.event_time >= min(hours),
                              AnalyticsFact.event_time < max(hours) + timedelta(hours=1)))
    if dates:
        conditions.append(and_(AnalyticsFact.business_date >= min(dates),
                               AnalyticsFact.business_date <= max(dates)))
    if not conditions:
        return []
    # R2: `hidden` transactions are excluded from every rollup. A NULL `transaction_name` always
    # passes - the unnamed rows are the connectivity probes, and their rule is fixed in code: always
    # captured, never shown, which is expressed by the registry never holding a row for them and this
    # clause never matching them. `x NOT IN (...)` is NULL for a NULL x and a row is kept only when the
    # predicate is TRUE, so without the explicit IS NULL they would be silently dropped instead.
    gate = ([AnalyticsFact.transaction_name.is_(None)
             | AnalyticsFact.transaction_name.notin_(sorted(hidden))] if hidden else [])
    rows = (await db.execute(
        select(AnalyticsFact).where(AnalyticsFact.customer_code == customer_code,
                                    or_(*conditions), *gate,
                                    *_since_gate(AnalyticsFact, since)))).scalars().all()
    return [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]


async def _read_dirty_record_facts(db: AsyncSession, customer_code: str, hours: set[datetime],
                                   dates: set[date_type],
                                   hidden: frozenset[str] = frozenset(),
                                   since: datetime | None = None) -> list[dict]:
    """18y: the record grain's own reader - `_read_dirty_facts`' mirror, and deliberately a PARALLEL
    function rather than a parameterised one. The 18n structural guarantee ("an existing metric
    cannot see a record row even if somebody forgets a filter") is held by each reader naming
    exactly one table, and it is pinned by source inspection in both directions.

    Same shape end to end: two OR-ed range predicates (an hour and a business date are different
    axes), exact filtering in Python, and the same `show` gate - a hidden transaction vanishes from
    BOTH grains, because records still charting while their transaction is hidden would be a silent
    inconsistency."""
    conditions = []
    if hours:
        conditions.append(and_(AnalyticsRecordFact.event_time >= min(hours),
                               AnalyticsRecordFact.event_time < max(hours) + timedelta(hours=1)))
    if dates:
        conditions.append(and_(AnalyticsRecordFact.business_date >= min(dates),
                               AnalyticsRecordFact.business_date <= max(dates)))
    if not conditions:
        return []
    gate = ([AnalyticsRecordFact.transaction_name.is_(None)
             | AnalyticsRecordFact.transaction_name.notin_(sorted(hidden))] if hidden else [])
    rows = (await db.execute(
        select(AnalyticsRecordFact).where(AnalyticsRecordFact.customer_code == customer_code,
                                          or_(*conditions), *gate,
                                          *_since_gate(AnalyticsRecordFact, since)))).scalars().all()
    return [{c.name: getattr(r, c.name) for c in AnalyticsRecordFact.__table__.columns}
            for r in rows]


def _since(facts: list[dict], since: datetime | None) -> list[dict]:
    """The definition's `rollups_from`, applied in Python to a once-read list. Same predicate as
    `_since_gate`: inclusive at the instant, and a fact with no event_time is excluded."""
    if since is None:
        return facts
    return [f for f in facts if f.get("event_time") is not None and f["event_time"] >= since]


async def _fold_daily_from_hourly(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                                  definition: d.MetricDefinition, dates: set[date_type], *,
                                  tz: str | None, computed_at: datetime) -> dict:
    """Chunk 91: each local day merged from its hourly rows, exactly as monthly is merged from daily.

    Only for days `local_day_span` accepts. Reads the definition's hourly rows across the whole span
    of the requested days in one query and assigns each row to the day whose span holds it, then
    `add_roles` merges per (day, dimensions, measure): sums add, counts add, mins and maxes take the
    extreme, histograms add element-wise, sketches union. Must run AFTER the hourly level has been
    replaced in the same transaction, or a day would be merged from stale hours.
    """
    if not dates:
        return {"deleted": 0, "inserted": 0}
    spans = {day: local_day_span(day, tz) for day in dates}
    assert all(spans.values()), "caller must route non-derivable days to the fact fold"
    lo = min(span[0] for span in spans.values())
    hi = max(span[1] for span in spans.values())
    hourly = (await db.execute(
        select(AnalyticsHourlyRollup).where(
            AnalyticsHourlyRollup.customer_code == customer_code,
            AnalyticsHourlyRollup.definition_id == definition_id,
            AnalyticsHourlyRollup.bucket_start >= lo,
            AnalyticsHourlyRollup.bucket_start < hi))).scalars().all()

    folded: dict[tuple[Any, DimKey], dict] = {}
    for row in hourly:
        day = next((day for day, (s_lo, s_hi) in spans.items() if s_lo <= row.bucket_start < s_hi), None)
        if day is None:
            continue          # inside the outer range but in a gap between two requested days
        key = (day, tuple(getattr(row, f"dim{i + 1}") for i in range(DIMENSION_SLOTS)))
        roles = {role: getattr(row, column) for role, column in _ROLE_COLUMN.items()
                 if getattr(row, column, None) is not None}
        bucket = folded.setdefault(key, {})
        bucket[row.measure_name] = d.add_roles(bucket.get(row.measure_name, {}), roles)

    rows = _rows_for(customer_code, definition_id, definition, folded,
                     bucket_column="business_date", computed_at=computed_at)
    return await _replace(db, AnalyticsDailyRollup, customer_code, definition_id,
                          bucket_column="business_date", buckets=sorted(dates), rows=rows)


async def _fold_monthly(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                        definition: d.MetricDefinition, months: set[date_type],
                        computed_at: datetime) -> dict:
    """Monthly from DAILY, the one level that really does read only the level below.

    Exact, unlike folding daily from hourly: a month start is a pure function of a business date, so no
    daily bucket can straddle two months. Uses `add_roles`, which is uniform across measures -- sums
    add, counts add, mins take the min, histograms add element-wise -- and never asks what it is looking
    at, which is the "registry, not an if-chain" requirement met structurally.
    """
    if not months:
        return {"deleted": 0, "inserted": 0}
    # Read by DATE RANGE and select the exact months in Python, rather than `date_trunc(...) IN (...)`.
    # A function on the column cannot use `ix_analytics_daily_read`, so the SQL version would scan the
    # tenant's whole history to answer a question about one month.
    first = min(months)
    last = _month_end(max(months))
    dailies = (await db.execute(
        select(AnalyticsDailyRollup).where(
            AnalyticsDailyRollup.customer_code == customer_code,
            AnalyticsDailyRollup.definition_id == definition_id,
            AnalyticsDailyRollup.business_date >= first,
            AnalyticsDailyRollup.business_date <= last))).scalars().all()

    folded: dict[tuple[Any, DimKey], dict] = {}
    for row in dailies:
        if month_of(row.business_date) not in months:
            continue
        key = (month_of(row.business_date),
               tuple(getattr(row, f"dim{i + 1}") for i in range(DIMENSION_SLOTS)))
        roles = {role: getattr(row, column) for role, column in _ROLE_COLUMN.items()
                 if getattr(row, column, None) is not None}
        bucket = folded.setdefault(key, {})
        bucket[row.measure_name] = d.add_roles(bucket.get(row.measure_name, {}), roles)

    rows = _rows_for(customer_code, definition_id, definition, folded,
                     bucket_column="month_start", computed_at=computed_at)
    return await _replace(db, AnalyticsMonthlyRollup, customer_code, definition_id,
                          bucket_column="month_start", buckets=sorted(months), rows=rows)


def _merge(a: dict, b: dict) -> dict:
    return {"deleted": a.get("deleted", 0) + b.get("deleted", 0),
            "inserted": a.get("inserted", 0) + b.get("inserted", 0)}


async def recompute(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                    definition: d.MetricDefinition, *, hours: set[datetime],
                    dates: set[date_type], computed_at: datetime | None = None,
                    hidden: frozenset[str] = frozenset(), tz: str | None) -> dict:
    """Rebuild every dirty bucket of ONE transaction-source definition, at every grain it declares.

    The single-definition entry, kept for reconcile's drifted-bucket repair and for tests. The worker
    uses `fold_all`, which reads the facts once for every definition. Does NOT commit: the caller owns
    the boundary, so the rollups land in the same transaction as the facts they summarise.

    `tz` is REQUIRED, with no default. It decides which hourly buckets make up a local day, and a
    caller that forgot it would silently derive a London tenant's day on UTC midnights. Pass None only
    to mean "this tenant's business dates are UTC", which is what `normalizer._local_date` does with
    None too.
    """
    if not hours and not dates:
        return {"hourly": {}, "daily": {}, "monthly": {}}
    now = computed_at or datetime.now(timezone.utc)
    derivable, by_fact = _route_dates(definition, dates, tz, today=now.date())
    facts = await _read_dirty_facts(db, customer_code, hours, by_fact, hidden,
                                    since=definition.rollups_from)
    return await _fold_grains(db, customer_code, definition_id, definition, facts,
                              hours=hours, dates=dates, derivable=derivable, tz=tz, now=now)


async def recompute_records(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                            definition: d.MetricDefinition, *, hours: set[datetime],
                            dates: set[date_type], computed_at: datetime | None = None,
                            hidden: frozenset[str] = frozenset(), tz: str | None) -> dict:
    """18y: `recompute`'s record-grain twin - reads `analytics_record_facts` through the parallel
    reader and folds through the SAME grain cascade into the same definition-keyed rollup tables."""
    if not hours and not dates:
        return {"hourly": {}, "daily": {}, "monthly": {}}
    now = computed_at or datetime.now(timezone.utc)
    derivable, by_fact = _route_dates(definition, dates, tz, today=now.date())
    facts = await _read_dirty_record_facts(db, customer_code, hours, by_fact, hidden,
                                           since=definition.rollups_from)
    return await _fold_grains(db, customer_code, definition_id, definition, facts,
                              hours=hours, dates=dates, derivable=derivable, tz=tz, now=now)


def _route_dates(definition: d.MetricDefinition, dates: set[date_type], tz: str | None, *,
                 today: date_type) -> tuple[set[date_type], set[date_type]]:
    """Split dirty days into those merged from hourly rows and those folded from facts.

    A definition without an hourly grain has no hourly rows to merge, so every day goes to facts.
    """
    if "hourly" not in definition.grains:
        return set(), set(dates)
    derivable = {day for day in dates if daily_derivable(day, tz, today=today)}
    return derivable, set(dates) - derivable


async def fold_all(db: AsyncSession, customer_code: str,
                   definitions: Sequence[tuple[uuid.UUID, d.MetricDefinition]], *,
                   hours: set[datetime], dates: set[date_type],
                   rec_hours: set[datetime], rec_dates: set[date_type],
                   hidden: frozenset[str], tz: str | None, now: datetime,
                   changed: Changed | None = None) -> dict:
    """Chunk 91: fold every active definition from ONE fact read per source.

    Facts for the dirty hours (plus any day that cannot derive from hourly) are read once, then each
    definition is folded from that list with its own `rollups_from` applied in Python. `changed` is
    what the run's diff touched; a transaction definition whose filter cannot match any of it is
    skipped, because none of its buckets can have moved. None means a refold: skip nothing.
    """
    stats = {"definitions": 0, "skipped": 0, "rows_written": 0,
             "buckets": len(hours | rec_hours) + len(dates | rec_dates)}
    today = now.date()

    async def _one_source(reader, model_defs, d_hours, d_dates):
        # The days that cannot derive for ANY definition decide the read's date predicate. A
        # definition with no hourly grain needs facts for every day; read those lazily, once.
        by_fact_all: set[date_type] = set()
        routed = {}
        for definition_id, definition in model_defs:
            derivable, by_fact = _route_dates(definition, d_dates, tz, today=today)
            routed[definition_id] = (derivable, by_fact)
            by_fact_all |= by_fact
        if not model_defs or (not d_hours and not d_dates):
            return
        facts = await reader(db, customer_code, d_hours, by_fact_all, hidden)
        for definition_id, definition in model_defs:
            derivable, _by_fact = routed[definition_id]
            out = await _fold_grains(db, customer_code, definition_id, definition,
                                     _since(facts, definition.rollups_from),
                                     hours=d_hours, dates=d_dates, derivable=derivable, tz=tz, now=now)
            stats["definitions"] += 1
            for grain in out.values():
                stats["rows_written"] += grain.get("inserted", 0)

    transaction_defs, record_defs = [], []
    for definition_id, definition in definitions:
        if definition.source == "record":
            record_defs.append((definition_id, definition))
        elif changed is not None and not concerns(definition, changed):
            stats["skipped"] += 1
        else:
            transaction_defs.append((definition_id, definition))

    await _one_source(_read_dirty_facts, transaction_defs, hours, dates)
    await _one_source(_read_dirty_record_facts, record_defs, rec_hours, rec_dates)
    return stats


async def _fold_grains(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                       definition: d.MetricDefinition, facts: list[dict], *,
                       hours: set[datetime], dates: set[date_type], now: datetime,
                       derivable: set[date_type] | None = None, tz: str | None = None) -> dict:
    """One definition through the cascade. `derivable` are the dirty days merged from hourly rows;
    the rest of `dates` are folded from `facts`. Hourly is always replaced first, because the daily
    merge reads it."""
    stats: dict[str, dict] = {}
    derivable = set(derivable or ())
    by_fact = set(dates) - derivable

    if "hourly" in definition.grains:
        folded = group_fold(
            facts, definition,
            lambda r: hour_of(r["event_time"])
            if r.get("event_time") and hour_of(r["event_time"]) in hours else None)
        stats["hourly"] = await _replace(
            db, AnalyticsHourlyRollup, customer_code, definition_id, bucket_column="bucket_start",
            buckets=sorted(hours),
            rows=_rows_for(customer_code, definition_id, definition, folded,
                           bucket_column="bucket_start", computed_at=now))

    if "daily" in definition.grains:
        daily = {"deleted": 0, "inserted": 0}
        if by_fact:
            folded = group_fold(
                facts, definition,
                lambda r: r["business_date"] if r.get("business_date") in by_fact else None)
            daily = _merge(daily, await _replace(
                db, AnalyticsDailyRollup, customer_code, definition_id, bucket_column="business_date",
                buckets=sorted(by_fact),
                rows=_rows_for(customer_code, definition_id, definition, folded,
                               bucket_column="business_date", computed_at=now)))
        if derivable:
            daily = _merge(daily, await _fold_daily_from_hourly(
                db, customer_code, definition_id, definition, derivable, tz=tz, computed_at=now))
        stats["daily"] = daily

    if "monthly" in definition.grains:
        # AFTER daily has been replaced, necessarily: monthly reads the level below, so folding it from
        # a stale daily level would produce a month that disagrees with its own days.
        stats["monthly"] = await _fold_monthly(
            db, customer_code, definition_id, definition, {month_of(x) for x in dates}, now)

    return stats
