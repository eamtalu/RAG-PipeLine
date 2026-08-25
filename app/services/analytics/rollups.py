"""N5, the rollup folder: maintain the grain cascade for every active definition.

Phase 3c of docs/analytics-ml-architecture/final_architecture.md.

    Maintains the grain cascade per active definition: `facts -> hourly -> daily -> monthly`.
    Each level reads only the level below, so the fact table is read once per cycle.
    **Every write is recompute-and-replace, never increment.** An additive upsert double-counts on the
    first retry.

Recompute-and-replace is the decision everything else follows from, and it is worth being precise about
what it changes. N3 hands over signed deltas, and the tempting thing is to add them to the stored bucket.
That is wrong for a reason no test of the happy path would catch: a cycle that fails after writing the
rollup but before committing its tickets is retried, and the retry adds the same delta again. So the
deltas are used ONLY to decide *which buckets are dirty*; each dirty bucket is then recomputed from
scratch and its rows replaced. Applying the same range twice is then indistinguishable from applying it
once, which is the only property that makes a retry safe.

A consequence that is easy to miss: **a bucket that recomputes to nothing must be deleted, not skipped.**
When the last fact in an hour is reversed, "replace the rows for this bucket" has to mean removing them.
Leaving them would strand the old total in every chart with nothing to indicate it was stale -- the same
silent-wrongness the range diff exists to prevent, reintroduced one level up.

Where this deviates from the plan, and why
------------------------------------------
The plan says each level reads only the level below. Hourly buckets are UTC hours; daily buckets are the
tenant-LOCAL `business_date`. Folding daily from hourly is therefore exact only when the tenant's UTC
offset is a whole number of hours. For a zone at +05:30 one UTC hour per day straddles two local dates,
and that hour's rows would be attributed entirely to one of them -- wrong by up to half a day's traffic,
silently, and only for some tenants.

So hourly AND daily are both folded from the facts, and monthly from daily (which is exact, because a
month start is a pure function of a business date). The plan's actual concern -- "the fact table is read
once per cycle" -- is preserved exactly: the dirty facts are read ONCE and folded into both grains in a
single pass.

Distinct counts are deliberately absent, per the plan: they do not cascade, and are computed per period
from the fact table by the read layer.
"""

import logging
import uuid
from datetime import date as date_type, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
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
            hist = roles.get(d.Role.histogram)
            row["histogram"] = dict(hist) if isinstance(hist, Mapping) else None
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


async def _read_dirty_facts(db: AsyncSession, customer_code: str, hours: set[datetime],
                            dates: set[date_type],
                            hidden: frozenset[str] = frozenset()) -> list[dict]:
    """The facts feeding every dirty bucket, read ONCE and folded into both grains.

    Two predicates OR-ed rather than one: an hour and a business date are different axes, and a fact can
    be in a dirty hour without being on a dirty date (or the reverse) after a rebuild moved it. Read as
    a contiguous range per axis and filtered to the exact dirty set in Python -- a hundred-term OR would
    defeat the planner, while a range that spans a gap merely reads a few rows that are then ignored.
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
                                    or_(*conditions), *gate))).scalars().all()
    return [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]


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


async def recompute(db: AsyncSession, customer_code: str, definition_id: uuid.UUID,
                    definition: d.MetricDefinition, *, hours: set[datetime],
                    dates: set[date_type], computed_at: datetime | None = None,
                    hidden: frozenset[str] = frozenset()) -> dict:
    """Rebuild every dirty bucket of one definition, at every grain it declares.

    Does NOT commit: the caller owns the boundary, so the rollups land in the same transaction as the
    facts they summarise. Committing separately would let a chart disagree with the fact table for as
    long as the gap between the two commits, and forever if the second one failed.
    """
    if not hours and not dates:
        return {"hourly": {}, "daily": {}, "monthly": {}}
    now = computed_at or datetime.now(timezone.utc)
    # R2: `hidden` are the transactions whose `show` switch is off. Excluded HERE rather than at read
    # time because `transaction_name` is only reliably available at this point - a metric whose
    # dimensions omit it could not be filtered from a pre-aggregated bucket later.
    #
    # Recomputed from scratch every time, so flipping `show` back on refills complete history on the
    # next fold of the range. That is the "one recompute" the switch promises.
    facts = await _read_dirty_facts(db, customer_code, hours, dates, hidden)
    stats: dict[str, dict] = {}

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
        folded = group_fold(
            facts, definition,
            lambda r: r["business_date"] if r.get("business_date") in dates else None)
        stats["daily"] = await _replace(
            db, AnalyticsDailyRollup, customer_code, definition_id, bucket_column="business_date",
            buckets=sorted(dates),
            rows=_rows_for(customer_code, definition_id, definition, folded,
                           bucket_column="business_date", computed_at=now))

    if "monthly" in definition.grains:
        # AFTER daily has been replaced, necessarily: monthly reads the level below, so folding it from
        # a stale daily level would produce a month that disagrees with its own days.
        stats["monthly"] = await _fold_monthly(
            db, customer_code, definition_id, definition, {month_of(x) for x in dates}, now)

    return stats
