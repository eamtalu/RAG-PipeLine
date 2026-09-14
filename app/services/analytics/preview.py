"""The preview: a dry run of a metric definition over real facts, before anyone activates it (chunk 85).

Why this exists
---------------
`definition.validate` says whether a definition is LEGAL. It cannot say whether the definition is
USEFUL: whether anything matches its filters, whether the field it aggregates is actually present on
those rows, how many rollup rows it will produce, or what the chart will look like. Those are questions
about data, and the person building a metric needs them answered before the definition goes live and
starts writing rollups that are then expensive to reshape.

What is pinned
--------------
    zero matches is a refusal       a chart of nothing is indistinguishable from a chart of zero
    sparse fields WARN and allow    decided 2026-09-12: the person sees the percentage and decides;
                                    a quantity over a method that carries none is still refused by
                                    `validate`, so the dangerous case is covered either way
    the sample uses the writer's    `rollups.group_fold` and `definition.fold`, so the preview cannot
    own fold                        show a number the rollup would not later hold
    nothing is written              `assess` is pure; `load_rows` reads a bounded window and nothing else

The budget warning threshold is the read planner's `rows_per_bucket` assumption (`read.choose_grain`):
past roughly 20 distinct dimension combinations per hour, a year of this metric resolves coarser than
the person expects.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.services.analytics import contract
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import rollups as n5

#: Above this many distinct dimension combinations in one hourly bucket the preview warns. It is the
#: `rows_per_bucket` the read planner assumes when it picks a grain for a window; a metric that
#: routinely exceeds it pushes every long-range chart to a coarser grain than the person expects.
BUDGET_COMBOS_PER_HOUR = 20

#: Hard cap on the rows a preview reads. The same cap the ad-hoc read path uses, for the same reason:
#: the fact table is designed to reach millions of rows and a preview is a dry run, not an audit.
MAX_ROWS = n6.AD_HOC_MAX_ROWS


# ---------------------------------------------------------------------------------------- pure

@dataclass(frozen=True)
class Matching:
    rows: Sequence[Mapping[str, Any]]
    total: int
    per_method: Mapping[str, int]
    per_transaction: Mapping[str, int]


def _passes_filter(row: Mapping[str, Any], definition: d.MetricDefinition) -> bool:
    """The definition's row filter alone: methods and transaction names. Per-measure filters on
    classification and status belong to the fold, not to "does this row match the metric"."""
    if definition.method_filter and row.get("method") not in definition.method_filter:
        return False
    if definition.transaction_filter and row.get("transaction_name") not in definition.transaction_filter:
        return False
    return True


def matching(rows: Iterable[Mapping[str, Any]], definition: d.MetricDefinition) -> Matching:
    """The rows behind the filter, and what they are made of."""
    kept = [r for r in rows if _passes_filter(r, definition)]
    per_method = Counter(str(r.get("method")) for r in kept)
    per_txn = Counter(str(r.get("transaction_name")) for r in kept)
    return Matching(rows=kept, total=len(kept),
                    per_method=dict(sorted(per_method.items())),
                    per_transaction=dict(sorted(per_txn.items())))


def _percent(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 1) if whole else 0.0


def _one_field_coverage(rows: Sequence[Mapping[str, Any]], measure: d.Measure,
                        field: str) -> dict:
    present = numeric = 0
    for row in rows:
        value = contract.resolve_field(row, field)
        if value is not None:
            present += 1
            if contract.numeric_or_none(value) is not None:
                numeric += 1
    total = len(rows)
    return {"measure": measure.name, "field": field, "total": total,
            "present": present, "numeric": numeric,
            "percent_present": _percent(present, total),
            "percent_numeric": _percent(numeric, total)}


def field_coverage(rows: Sequence[Mapping[str, Any]], measure: d.Measure) -> list[dict]:
    """How many matching rows carry the measure's field, and how many carry a NUMBER.

    `present` is any non-null value, which is what a count-like aggregation needs; `numeric` is what
    a sum needs. Both are reported because a field can be present on every row and numeric on none,
    and the two failures call for different fixes.

    Chunk 101: a list, because a measure can now name two fields. Reporting only the first would let
    somebody build a pick shortfall whose expected quantity is missing everywhere, see a confident
    100% and get an answer of nothing at all - and "no shortfall" is the most dangerous wrong answer
    this measure could give.
    """
    return [_one_field_coverage(rows, measure, field)
            for field in (measure.field, measure.minus) if field]


def dimension_coverage(rows: Sequence[Mapping[str, Any]], dimensions: Sequence[str]) -> list[dict]:
    """Per dimension, the share of matching rows that have a value. A null dimension lands the row in
    an empty bucket on every chart grouped by it, which is worth knowing before that chart exists."""
    out = []
    for name in dimensions:
        present = sum(1 for r in rows
                      if contract.dimension_value(contract.resolve_field(r, name)) is not None)
        out.append({"name": name, "total": len(rows), "present": present,
                    "percent_present": _percent(present, len(rows))})
    return out


def _combo(row: Mapping[str, Any], dimensions: Sequence[str]) -> tuple:
    return tuple(contract.dimension_value(contract.resolve_field(row, name)) for name in dimensions)


def budget(rows: Sequence[Mapping[str, Any]], definition: d.MetricDefinition) -> dict:
    """Distinct dimension combinations per hourly bucket, and the rollup rows per day they imply.

    Projected rows per day = mean combinations per observed hour x 24 x the number of measures,
    because a rollup row is keyed per (definition, measure, bucket, dimensions).
    """
    by_hour: dict[datetime, set] = {}
    for row in rows:
        when = row.get("event_time")
        if when is None:
            continue
        by_hour.setdefault(n5.hour_of(when), set()).add(_combo(row, definition.dimensions))
    counts = sorted(len(s) for s in by_hour.values())
    if not counts:
        return {"hours_observed": 0, "combos_per_hour_max": 0, "combos_per_hour_mean": 0.0,
                "combos_per_hour_p95": 0, "projected_rollup_rows_per_day": 0.0,
                "threshold": BUDGET_COMBOS_PER_HOUR, "warning": False}
    mean = sum(counts) / len(counts)
    p95 = counts[min(len(counts) - 1, int(round(0.95 * (len(counts) - 1))))]
    return {"hours_observed": len(counts), "combos_per_hour_max": counts[-1],
            "combos_per_hour_mean": mean, "combos_per_hour_p95": p95,
            "projected_rollup_rows_per_day": mean * 24 * max(1, len(definition.measures)),
            "threshold": BUDGET_COMBOS_PER_HOUR,
            "warning": counts[-1] > BUDGET_COMBOS_PER_HOUR}


def sample(rows: Sequence[Mapping[str, Any]], definition: d.MetricDefinition, *,
           measure: str) -> list[dict]:
    """A daily series of the first measure, folded exactly as the writer would fold it.

    `rollups.group_fold` keyed by business_date, then only the requested measure's roles per point,
    serialised the way `/series` serialises them so the wizard can draw both with one component.
    """
    folded = n5.group_fold(rows, definition, lambda r: r.get("business_date"))
    points = []
    for (bucket, dims), measures in sorted(folded.items(), key=lambda kv: str(kv[0][0])):
        roles = measures.get(measure)
        if not roles:
            continue
        points.append({"bucket": str(bucket),
                       "dimensions": list(dims[:len(definition.dimensions)]),
                       # Chunk 98: the shared plain formatter, not a local one. The local copy
                       # called `Decimal.normalize()`, which turns 40 into 4E+1.
                       "roles": {k: v for k, v in d.public_roles(roles).items()
                                 if v not in (None, ())}})
    return points


def assess(rows: Iterable[Mapping[str, Any]], definition: d.MetricDefinition, *,
           problems: Sequence[str], truncated: bool = False) -> dict:
    """Everything the wizard shows, from rows already in memory. Pure.

    `ok` is false for a legal-shape problem or for zero matches. Coverage and budget only ever add to
    `warnings`; the decision to proceed stays with the person, who now has the numbers.
    """
    m = matching(rows, definition)
    refusals = list(problems)
    warnings: list[str] = []
    if m.total == 0:
        refusals.append("no facts match the filter in the preview window: "
                        f"methods={list(definition.method_filter) or 'any'}, "
                        f"transactions={list(definition.transaction_filter) or 'any'}")
    coverage = [c for ms in definition.measures for c in field_coverage(m.rows, ms)]
    for c in coverage:
        if m.total and c["percent_present"] < 100.0:
            warnings.append(f"measure {c['measure']!r}: field {c['field']!r} is present on "
                            f"{c['percent_present']}% of matching facts ({c['present']} of {c['total']}); "
                            f"the metric will be built from those rows only")
    dims = dimension_coverage(m.rows, definition.dimensions)
    for x in dims:
        if m.total and x["percent_present"] < 100.0:
            warnings.append(f"dimension {x['name']!r} is empty on {100.0 - x['percent_present']:.1f}% "
                            f"of matching facts; those land in a blank bucket")
    b = budget(m.rows, definition)
    if b["warning"]:
        warnings.append(f"up to {b['combos_per_hour_max']} distinct dimension combinations in one hour, "
                        f"above the {BUDGET_COMBOS_PER_HOUR} the read planner assumes; long-range charts "
                        f"will resolve coarser than expected")
    if truncated:
        warnings.append(f"the preview read was truncated at {MAX_ROWS} rows; counts and the sample "
                        f"describe that subset")
    first_measure = definition.measures[0].name if definition.measures else None
    return {
        "ok": not refusals,
        "problems": list(problems),
        "refusals": [r for r in refusals if r not in problems],
        "warnings": warnings,
        "matches": {"total": m.total, "per_method": m.per_method,
                    "per_transaction": m.per_transaction},
        "field_coverage": coverage,
        "dimension_coverage": dims,
        "budget": b,
        "sample": {"grain": "daily", "measure": first_measure, "truncated": truncated,
                   "points": sample(m.rows, definition, measure=first_measure) if first_measure else []},
    }


# ---------------------------------------------------------------------------------------- loading

async def load_rows(db: AsyncSession, customer_code: str, definition: d.MetricDefinition, *,
                    since: datetime, until: datetime, limit: int = MAX_ROWS
                    ) -> tuple[list[dict], bool]:
    """Facts (or record facts) in the window, as plain dicts, plus whether the read hit the cap.

    Reads one row more than the cap so truncation is a fact rather than a guess. The filter is applied
    in Python by `matching`, deliberately: the per-method counts the wizard shows must include the
    methods the filter would EXCLUDE only when the filter is empty, and the definition's own filter is
    the thing being previewed.
    """
    model = AnalyticsRecordFact if definition.source == "record" else AnalyticsFact
    stmt = (select(model)
            .where(model.customer_code == customer_code,
                   and_(model.event_time >= since, model.event_time < until))
            .order_by(model.event_time)
            .limit(limit + 1))
    rows = (await db.execute(stmt)).scalars().all()
    truncated = len(rows) > limit
    columns = [c.name for c in model.__table__.columns]
    return [{c: getattr(r, c) for c in columns} for r in rows[:limit]], truncated


async def run(db: AsyncSession, customer_code: str, definition: d.MetricDefinition, *,
              problems: Sequence[str], window_hours: int, now: datetime | None = None) -> dict:
    """`load_rows` then `assess`, plus the window it looked at. Writes nothing."""
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(hours=window_hours)
    rows, truncated = await load_rows(db, customer_code, definition, since=since, until=now)
    out = assess(rows, definition, problems=problems, truncated=truncated)
    out["window"] = {"hours": window_hours, "start": since.isoformat(), "end": now.isoformat(),
                     "rows_read": len(rows)}
    out["written"] = False
    return out
