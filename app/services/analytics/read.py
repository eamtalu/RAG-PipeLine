"""N6, the read layer: answers every question, and is the only component that does.

Phase 5 of docs/analytics-ml-architecture/final_architecture.md.

This module is the planning half, and it is PURE: no database, no clock. Given a window, a grain and one
persisted watermark it decides what to read and from where. The execution half issues the queries. The
split exists because every interesting failure here is a planning failure, and a plan is something a test
can inspect without a database.

**Grain selection, and it has TWO constraints.** Cost is the obvious one: under 100,000 rows scanned.
Resolution is the one the budget cannot express, and it is what the plan's own example turns on -- 365
daily buckets at a realistic ~20 rows each is 7,300 rows, comfortably inside the budget, so cost alone
would happily serve a year at daily. "The coarsest grain covering the window" is the plan saying that 365
points for a year chart is servable and useless. So the answer is the coarsest grain that still resolves
the window, and cost can only push it coarser, never finer.

**Two-tier read, and the silent failure it prevents.** Pre-aggregated rollups for settled ranges, unioned
with a bounded live scan of the recent tail. Both halves derive from ONE boundary value, and the reason is
that computing it twice fails in two opposite directions, both of which produce a plausible number rather
than an error: if the worker folds a bucket between the two reads, the same rows appear in both halves (a
double count); if it moves the other way, rows appear in neither (a gap). So `plan_read` takes the
watermark as an argument and cannot fetch it - a caller doing two reads would otherwise get two
boundaries.

**Bucket alignment, the second trap.** A request for 09:00 to 09:30 cannot use the 09:00 hourly bucket:
that bucket holds the whole hour, so counting it would include thirty minutes nobody asked for. Whole
buckets come from the rollups; the ragged edges come from a bounded fact scan.

**Ad-hoc fallback.** A query no definition covers falls back to a bounded fact-table scan, and the
response marks itself as such so the interface can show it rather than silently running slow. A group-by
on a field that is not on the fact row at all is an ERROR, not a fallback: a fallback would scan and
return nothing, which reads as "no data" rather than "that field does not exist".
"""

import logging
from dataclasses import dataclass
from datetime import date as date_type, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from app.services.analytics import contract
from app.services.analytics import definition as d
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

logger = logging.getLogger(__name__)

#: The plan's target: "targeting under 100,000 rows scanned". Expressed as a budget rather than a span
#: table so a tenant with high dimension cardinality resolves coarser for the same window, which is what
#: actually determines whether a request is servable.
ROW_BUDGET = 100_000

#: The other half of the rule, and the half the row budget cannot express.
#:
#: The plan's one concrete requirement is "a twelve-month request resolves to monthly, never daily". The
#: budget alone does NOT produce that: 365 daily buckets at a realistic ~20 rows each is 7,300 rows, well
#: inside 100,000. So grain selection is not only about cost -- it is also about RESOLUTION. Returning 365
#: points for a year chart is servable and useless, and "the coarsest grain covering the window" is the
#: plan saying exactly that.
#:
#: Sixty is chosen to make the plan's own cases land where it says they should: a year gives 12 monthly
#: buckets, a month gives 30 daily ones, a shift gives its hours.
MAX_BUCKETS = 60

#: Hard cap on an ad-hoc fact scan. The fact table is designed to reach 13M rows, so an unbounded
#: fallback is one careless request away from the outage CLAUDE.md rule 3 exists to prevent.
AD_HOC_MAX_ROWS = 200_000

#: Coarsest first. `choose_grain` walks this order and takes the first that fits, which is what makes
#: "the coarsest grain that covers the window" a property of the data rather than a comment.
_COARSEST_FIRST: tuple[str, ...] = ("monthly", "daily", "hourly")

#: Rough bucket lengths, used only to count buckets in a window. A month is approximated because the
#: count feeds a budget comparison, not a boundary - alignment is done exactly, in `plan_read`.
_BUCKET_LENGTH: dict[str, timedelta] = {
    "hourly": timedelta(hours=1),
    "daily": timedelta(days=1),
    "monthly": timedelta(days=30),
}


def buckets_in(window: UtcWindow, grain: str) -> int:
    """How many buckets of `grain` a window spans. At least one, because a request inside a single
    bucket still reads that bucket."""
    if window.start is None or window.end is None:
        return 10 ** 9          # unbounded: treat as unservable at any fine grain
    span = window.end - window.start
    return max(1, -(-int(span.total_seconds()) // int(_BUCKET_LENGTH[grain].total_seconds())))


def choose_grain(window: UtcWindow, *, available: tuple[str, ...] = _COARSEST_FIRST,
                 rows_per_bucket: int = 20) -> str:
    """The coarsest grain that both covers the window and fits the row budget.

    `available` is the definition's own grain list. Choosing a grain it never folded would return an
    empty chart that looks like zero activity, which is the failure mode the whole plan is written
    against, so a grain outside this list is never selected.

    Walks coarsest first and returns the FIRST that fits, then keeps refining while the budget allows.
    An unbounded window therefore lands on the coarsest available rather than defaulting to the finest.
    """
    ordered = [g for g in _COARSEST_FIRST if g in available]
    if not ordered:
        raise ValueError(f"no usable grain in {available!r}")
    # Two constraints, and both are required. MAX_BUCKETS keeps the answer readable; ROW_BUDGET keeps it
    # servable. Walking finest-first and taking the first that satisfies both yields the coarsest grain
    # that still resolves the window, because anything finer has already been rejected.
    for grain in reversed(ordered):
        buckets = buckets_in(window, grain)
        if buckets <= MAX_BUCKETS and buckets * max(1, rows_per_bucket) <= ROW_BUDGET:
            return grain
    # Nothing satisfies both -- an unbounded window, or a cardinality no grain can serve. The coarsest
    # available is the least bad, and the caller is told which grain it actually got.
    return ordered[0]


@dataclass(frozen=True)
class ReadPlan:
    """What to read, and from where. Two halves that together cover the request exactly once."""

    grain: str
    #: The instant range whose WHOLE buckets can be served from the hourly rollups. Both sides None when
    #: no whole bucket fits.
    rollup_window: UtcWindow
    #: For the daily grain, the inclusive span of whole local dates instead. None when none fit.
    rollup_dates: tuple[date_type, date_type] | None
    #: Ragged edges and anything past the watermark: read from the fact table, bounded.
    live_windows: list[tuple[datetime, datetime]]
    #: Why the plan looks the way it does, when it is not the obvious shape.
    reason: str | None = None


def _ceil_hour(moment: datetime) -> datetime:
    floor = moment.replace(minute=0, second=0, microsecond=0)
    return floor if floor == moment else floor + timedelta(hours=1)


def _floor_hour(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


def _midnight(day: date_type) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)


def plan_read(window: UtcWindow, grain: str, *, watermark: datetime | None) -> ReadPlan:
    """Split a request into a rollup half and a live half. PURE: no database, no clock.

    `watermark` is `analytics_tenant_state.analytics_watermark`, read ONCE by the caller. Everything at
    or before it has been folded, so the rollups are authoritative there; beyond it the rollups would
    report zero for a range that has data, so it has to be read live.

    Taking it as an argument rather than fetching it is the plan's requirement made structural: a caller
    doing two reads with two freshly-computed boundaries gets a double count or a gap, and both look
    like real numbers.
    """
    if window.start is None or window.end is None:
        return ReadPlan(grain, UtcWindow(None, None), None,
                        [], reason="unbounded window: the caller must bound it before reading")

    settled_end = window.end if watermark is None else min(window.end, watermark)
    reason = None
    if watermark is None:
        # Nothing folded: the rollups hold nothing, so serving from them would report zero everywhere.
        return ReadPlan(grain, UtcWindow(None, None), None, [(window.start, window.end)],
                        reason="no analytics watermark yet, so nothing has been folded: read live")
    if settled_end < window.end:
        reason = "part of the window is past the analytics watermark and is read live"

    if grain == "daily":
        # Daily buckets are keyed on the tenant-LOCAL business_date, so alignment is a DATE operation.
        # Treating it as a UTC-midnight instant would mis-align every tenant that is not on UTC.
        first = window.start.date() if window.start == _midnight(window.start.date()) \
            else window.start.date() + timedelta(days=1)
        last_exclusive = min(settled_end, window.end)
        last = last_exclusive.date() - timedelta(days=1) \
            if last_exclusive != _midnight(last_exclusive.date()) else last_exclusive.date()
        if first > last:
            return ReadPlan(grain, UtcWindow(None, None), None,
                            [(window.start, window.end)],
                            reason=reason or "no whole local day fits inside the request")
        live = _edges(window, _midnight(first), _midnight(last) + timedelta(days=1))
        return ReadPlan(grain, UtcWindow(_midnight(first), _midnight(last) + timedelta(days=1)),
                        (first, last), live, reason=reason)

    lo = _ceil_hour(window.start)
    hi = _floor_hour(settled_end)
    if hi <= lo:
        return ReadPlan(grain, UtcWindow(None, None), None, [(window.start, window.end)],
                        reason=reason or "no whole bucket fits inside the request")
    return ReadPlan(grain, UtcWindow(lo, hi), None, _edges(window, lo, hi), reason=reason)


def _edges(window: UtcWindow, lo: datetime, hi: datetime) -> list[tuple[datetime, datetime]]:
    """The parts of the request the rollups do not cover. Together with [lo, hi) these tile the request
    exactly once: no overlap, no gap."""
    out = []
    if window.start < lo:
        out.append((window.start, lo))
    if hi < window.end:
        out.append((hi, window.end))
    return out


@dataclass(frozen=True)
class Resolution:
    """Whether a request can be served from the rollups, or needs a fact scan."""

    ad_hoc: bool
    reason: str
    #: The dimensions to group by, in rollup slot order when not ad hoc.
    group_by: tuple[str, ...]


def resolve(definition: d.MetricDefinition, *, group_by: tuple[str, ...]) -> Resolution:
    """Whether `definition`'s rollups can answer a group-by, or the fact table must be scanned.

    A field that is not on the fact row at all raises. A fallback there would scan and return nothing,
    which reads as "no data" rather than "you asked for a field that does not exist" -- and the second is
    the only one anybody can act on.
    """
    # 18y/chunk 80: source-aware, and `attr:` paths are legal group-bys. An attr path of the WRONG
    # namespace for the source is refused exactly as validate() refuses it on the definition - it
    # can only ever be silently empty. Approval of ad-hoc attr paths is the API layer's check (it
    # has the registry); this stays pure.
    record_grain = definition.source == "record"
    fields = contract.RECORD_FIELDS if record_grain else contract.FACT_FIELDS
    row_name = "record row" if record_grain else "fact row"
    for g in group_by:
        if contract.is_attr_path(g):
            key = contract.attr_key(g)
            if record_grain and not key.startswith("rec."):
                raise ValueError(f"{g!r} is not a record attribute; a record metric's attributes "
                                 f"are all rec.-prefixed")
            if not record_grain and key.startswith("rec."):
                raise ValueError(f"{g!r} is a record attribute, which never appears on a "
                                 f"transaction fact row")
        elif g not in fields:
            raise ValueError(f"{g!r} is not a field on the {row_name}, so no query can group "
                             f"by it; the {row_name}'s fields are fixed in analytics.contract")
    if len(group_by) > 4:
        raise ValueError("group_by is capped at 4 fields (the rollup dimension slots)")

    outside = [g for g in group_by if g not in definition.dimensions]
    if outside:
        return Resolution(True, f"{', '.join(outside)} is not a dimension of {definition.name!r}, so "
                                f"no rollup is keyed by it: falling back to a bounded fact scan",
                          tuple(group_by))
    return Resolution(False, f"served from {definition.name!r} rollups", tuple(group_by))


def freshness(*, analytics_watermark: datetime | None, source_watermark: datetime | None,
              unsealed_share: Decimal | None, oldest_unsealed_at: datetime | None,
              stale_after_seconds: int = 300) -> dict:
    """F4's TWO numbers, because one cannot say what the user needs to know.

    Copy freshness answers "am I behind"; settledness answers "is what I have still going to move". A
    screen can truthfully say "updated 2 seconds ago" about a number that is still due to change, which
    is why a window with unsealed contributors reads as PROVISIONAL rather than stale. Those are
    different words for the user and different actions for an operator.

    A NULL analytics watermark is reported as `never_folded`, not as zero lag. Reporting zero would put a
    green light over an empty chart.
    """
    never = analytics_watermark is None
    lag = None
    if not never and source_watermark is not None:
        lag = max(0, int((source_watermark - analytics_watermark).total_seconds()))
    share = Decimal(unsealed_share) if unsealed_share is not None else None
    return {
        "never_folded": never,
        "analytics_watermark": analytics_watermark,
        "source_watermark": source_watermark,
        "lag_seconds": lag,
        "stale": bool(lag is not None and lag > stale_after_seconds),
        "unsealed_share": share,
        "oldest_unsealed_at": oldest_unsealed_at,
        "provisional": bool(share is not None and share > 0),
    }


# ============================================================== execution
#
# Everything above is pure. Below issues the queries, and the only reason it is in the same module is
# that a plan is worthless separated from the one thing allowed to execute it.

_ROLE_COLUMN = {role: role.value for role in d.Role}


async def _rollup_points(db, model, customer_code: str, definition_id, *, bucket_column: str,
                         lo, hi, measure: str, group_by: tuple[str, ...],
                         dimensions: tuple[str, ...]) -> dict:
    """Whole buckets, straight from the pre-aggregated table.

    Parameterised throughout, so asyncpg prepares the statement -- worth roughly 100 ms per call on this
    server, which is the difference between a chart that feels live and one that does not.
    """
    from sqlalchemy import select

    column = getattr(model, bucket_column)
    slots = [dimensions.index(g) for g in group_by if g in dimensions]
    dim_cols = [getattr(model, f"dim{i + 1}") for i in slots]

    stmt = select(column, *dim_cols,
                  *(getattr(model, _ROLE_COLUMN[r]) for r in
                    (d.Role.sum_value, d.Role.count_value))).where(
        model.customer_code == customer_code, model.definition_id == definition_id,
        model.measure_name == measure, column >= lo, column < hi)

    out: dict = {}
    for row in (await db.execute(stmt)).all():
        bucket, *rest = row
        dims = tuple(rest[:len(dim_cols)])
        total, count = rest[len(dim_cols):]
        roles = {}
        if total is not None:
            roles[d.Role.sum_value] = Decimal(total)
        if count is not None:
            roles[d.Role.count_value] = count
        key = (bucket, dims)
        out[key] = d.add_roles(out.get(key, {}), roles)
    return out


async def _live_points(db, customer_code: str, definition: d.MetricDefinition,
                       spans: list[tuple[datetime, datetime]], *, grain: str,
                       measure: str, group_by: tuple[str, ...]) -> dict:
    """The ragged edges and the unfolded tail, folded on the fly from the fact table.

    Bounded by AD_HOC_MAX_ROWS. Note this is the ONE place a total is computed at read time, and it uses
    the same `definition.fold` the writer uses -- so a live edge and a settled bucket cannot disagree
    about what the metric means, only about how fresh they are.
    """
    import dataclasses

    from sqlalchemy import or_, and_, select

    from app.persistence.models.analytics_fact import AnalyticsFact
    from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
    from app.services.analytics import rollups as n5

    if not spans:
        return {}
    # 18y: an explicit two-entry map, mirroring the fold's parallel readers - a record definition
    # scans the record table and nothing else.
    model = AnalyticsRecordFact if definition.source == "record" else AnalyticsFact
    conditions = [and_(model.event_time >= lo, model.event_time < hi)
                  for lo, hi in spans]
    rows = (await db.execute(select(model).where(
        model.customer_code == customer_code, or_(*conditions))
        .limit(AD_HOC_MAX_ROWS))).scalars().all()
    facts = [{c.name: getattr(r, c.name) for c in model.__table__.columns} for r in rows]

    bucket_of = ((lambda r: n5.hour_of(r["event_time"]) if r.get("event_time") else None)
                 if grain == "hourly" else (lambda r: r.get("business_date")))
    # Chunk 80: fold by the REQUESTED group-by, not the definition's dimensions. The old shape
    # folded by the definition's dimensions and then narrowed to the requested ones - any requested
    # field that was not already a dimension was silently DISCARDED from the key, which is how an
    # ad-hoc breakdown lumped every row into one unlabeled group (verified live). `resolve_field`
    # reads plain columns and attr: paths alike, so the same line serves both grains and both kinds
    # of field; measures and filters still come from the real definition.
    folded = n5.group_fold(facts, dataclasses.replace(definition, dimensions=tuple(group_by)),
                           bucket_of)

    # ACCUMULATED, not comprehended. `group_fold` keys by the definition's FULL dimension
    # tuple, and the requested `group_by` is usually narrower -- with no grouping at all it
    # is empty, so every combo in a bucket reduces to the same key. A dict comprehension
    # keeps the LAST of those and silently discards the rest.
    #
    # That is not hypothetical: it shipped. One production hour held eight distinct
    # (method, transaction_name) combos of which one contributed 681 units; the last combo
    # iterated was a non-quantity method, so the bucket reported ZERO and the units tile
    # read 681 short of the breakdown table directly beneath it. It only showed with no
    # grouping, because a grouping keeps the keys distinct -- which is why every earlier
    # test passed.
    out: dict = {}
    for (bucket, dims), measures in folded.items():
        if measure not in measures:
            continue
        # `_dim_key` pads to the four rollup slots; trim to the requested arity so a live key and a
        # rollup key for the same group are identical and merge instead of doubling the bucket.
        key = (bucket, tuple(dims[:len(group_by)]))
        out[key] = d.add_roles(out.get(key, {}), measures[measure])
    return out


async def series(db, customer_code: str, definition_id, definition: d.MetricDefinition, *,
                 window: UtcWindow, measure: str, group_by: tuple[str, ...] = (),
                 watermark: datetime | None, rows_per_bucket: int = 20,
                 ad_hoc: bool = False) -> dict:
    """One measure over time, two-tier. The watermark is passed in, never fetched here (see plan_read).

    Returns additive ROLE values per bucket, never a finished answer: the caller divides a sum by a count
    if it wants an average. Invariant 8 does not stop at the rollup table.
    """
    from app.persistence.models.analytics_rollup import AnalyticsDailyRollup, AnalyticsHourlyRollup

    grain = choose_grain(window, available=definition.grains, rows_per_bucket=rows_per_bucket)
    plan = plan_read(window, grain, watermark=watermark)

    # Chunk 80: an ad-hoc group-by cannot be answered by rollups AT ALL - no rollup is keyed by the
    # requested field, so the rollup tier would contribute points keyed by () while the live tier
    # groups properly, and the merge lumps the settled majority into one unlabeled row. Verified
    # live before this chunk: /breakdown by item_number returned ONE null row holding the tenant's
    # whole sum. Ad-hoc is therefore served entirely from the bounded live scan.
    if ad_hoc:
        live = ([(window.start, window.end)] if window.start and window.end else plan.live_windows)
        points = dict(await _live_points(db, customer_code, definition, live,
                                         grain=grain, measure=measure, group_by=group_by))
        return {
            "grain": grain, "measure": measure, "group_by": list(group_by),
            "from_rollups": False,
            "live_spans": [[s0.isoformat(), e0.isoformat()] for s0, e0 in live],
            "reason": "ad-hoc group-by: served entirely from a bounded fact scan",
            "points": [{"bucket": str(bucket), "dimensions": list(dims),
                        "roles": {r.value: (str(v) if isinstance(v, Decimal) else v)
                                  for r, v in roles.items()}}
                       for (bucket, dims), roles in sorted(points.items(),
                                                           key=lambda kv: str(kv[0][0]))],
        }

    points: dict = {}
    if plan.rollup_window.start is not None:
        model, column = ((AnalyticsHourlyRollup, "bucket_start") if grain == "hourly"
                         else (AnalyticsDailyRollup, "business_date"))
        lo, hi = plan.rollup_window.start, plan.rollup_window.end
        if grain == "daily":
            lo, hi = plan.rollup_dates[0], plan.rollup_dates[1] + timedelta(days=1)
        points = await _rollup_points(db, model, customer_code, definition_id,
                                      bucket_column=column, lo=lo, hi=hi, measure=measure,
                                      group_by=group_by, dimensions=definition.dimensions)

    for key, roles in (await _live_points(db, customer_code, definition, plan.live_windows,
                                          grain=grain, measure=measure,
                                          group_by=group_by)).items():
        points[key] = d.add_roles(points.get(key, {}), roles)

    return {
        "grain": grain,
        "measure": measure,
        "group_by": list(group_by),
        "from_rollups": plan.rollup_window.start is not None,
        "live_spans": [[s.isoformat(), e.isoformat()] for s, e in plan.live_windows],
        "reason": plan.reason,
        "points": [{"bucket": str(bucket), "dimensions": list(dims),
                    "roles": {r.value: (str(v) if isinstance(v, Decimal) else v)
                              for r, v in roles.items()}}
                   for (bucket, dims), roles in sorted(points.items(), key=lambda kv: str(kv[0][0]))],
    }
