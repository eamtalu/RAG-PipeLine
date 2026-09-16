"""The semantic catalog: what this tenant's analytics MEAN, as one read (chunk 84, metric builder part 0).

Two readers, one document
-------------------------
The metric wizard needs to offer dimensions, fields and transaction names with human meaning. A chat
agent needs to decide which metric answers "how many units did Brighton pick yesterday" and to say
what unit the answer is in. Both questions are the same question, so they get the same answer: this
document. Neither reader ever sees raw SQL or a table name.

Three properties, each pinned by a test
---------------------------------------
    active only        drafts and inactive definitions are absent, exactly as `registry.active_definitions`
                       hides them from the fold. An agent must never chart what nobody activated.
    domains are recent the values a dimension takes are read from the HOURLY rollups over the last
    and capped         `domain_days`, at most `domain_cap` per dimension, and a capped list says
                       `truncated: true`. A silently shortened list would read as complete.
    shape is pure      `shape` turns rows already in memory into the body. The database is touched only
                       in `load`, so ordering and field selection are testable without one.

Units come from the field registry, once. A measure over `attr:<field>` reports that field's `unit`;
a measure over a typed column reports the unit stored on the measure itself, if any (part 5 of the
builder writes it). One source of truth for a unit, so a field described as "units" cannot be charted
as "kg" by a metric that forgot to say.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_field_meaning import AnalyticsFieldMeaning
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_rollup import DIMENSION_SLOTS, AnalyticsHourlyRollup
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.services.analytics import contract
from app.services.analytics import definition as d

#: How far back the hourly rollups are read for a dimension's observed values, and how many values
#: are returned per dimension. Both are request parameters with these defaults; the cap exists because
#: a dimension such as item_number has thousands of values and the catalog is a picker, not a dump.
DEFAULT_DOMAIN_DAYS = 7
DEFAULT_DOMAIN_CAP = 200

#: Aggregations whose stored roles are an estimate rather than an exact figure. `distinct` is a
#: HyperLogLog sketch (chunk 88, about 1.6 percent error); `percentile` is read out of a 20-band log
#: histogram (chunk 92, a band is a factor of two wide). Everything else is exact arithmetic.
_APPROXIMATE_AGGREGATIONS: frozenset[str] = frozenset({d.Aggregation.distinct.value,
                                                       d.Aggregation.percentile.value})


def aggregations() -> list[dict]:
    """Every aggregation the builder may pick, with what the wizard needs to offer it correctly: whether
    it needs a field, and whether its answer is an estimate. Derived from the definition module, so a
    new aggregation appears here without a second list to keep in step."""
    return [{
        "name": a.value,
        "needs_field": a is not d.Aggregation.count,
        "approximate": a.value in _APPROXIMATE_AGGREGATIONS,
        # Chunks 109 and 110: which KINDS of field may not be read this way. Derived from
        # `definition.REFUSED_BY_KIND`, so a screen greys out exactly what the server refuses and
        # keeps no second copy of the rule to drift out of step.
        "refuses_kinds": list(d.refused_kinds(a)),
        "roles": sorted(r.value for r in d.roles_for(a)),
    } for a in d.Aggregation]


# ---------------------------------------------------------------------------------------- row shapes

@dataclass(frozen=True)
class MetricRow:
    id: uuid.UUID
    name: str
    description: str | None
    source: str
    dimensions: Sequence[str]
    measures: Sequence[Mapping[str, Any]]
    filter: Mapping[str, Any]
    grains: Sequence[str]
    rollups_from: datetime | None
    backfilled_through: date | None


@dataclass(frozen=True)
class FieldRow:
    field: str
    source: str
    description: str | None
    unit: str | None
    methods: Sequence[str]
    #: Chunk 109. `measure`, `level`, `slice`, `noise`, or None when nobody has decided. Carried so an
    #: agent reading this catalogue knows a stock reading from a quantity picked: that difference is
    #: 18,248 units in stock against 340,206, and nothing in the values supplies it.
    kind: str | None = None


@dataclass(frozen=True)
class TransactionRow:
    transaction_name: str
    description: str | None
    capture: bool
    show: bool
    expand: bool
    mi: bool = False


@dataclass(frozen=True)
class Domain:
    values: Sequence[str]
    truncated: bool


@dataclass(frozen=True)
class Rows:
    metrics: Sequence[MetricRow]
    #: {(definition id, dimension name) -> Domain}
    domains: Mapping[tuple[uuid.UUID, str], Domain] = field(default_factory=dict)
    fields: Sequence[FieldRow] = ()
    transactions: Sequence[TransactionRow] = ()


# ---------------------------------------------------------------------------------------- pure shape

def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _unit_for(measure: Mapping[str, Any], units_by_field: Mapping[str, str | None]) -> str | None:
    """The unit a measure is in: the field registry's for an `attr:` field, else the measure's own."""
    name = measure.get("field")
    if isinstance(name, str) and contract.is_attr_path(name):
        return units_by_field.get(contract.attr_key(name))
    return measure.get("unit")


def _level_for(measure: Mapping[str, Any], kinds_by_field: Mapping[str, str | None]) -> bool:
    """Whether a measure reads a LEVEL: how much there is at a moment rather than how much happened.

    Mirrors `_unit_for`, including its `attr:` handling. Published per MEASURE rather than left for a
    reader to join back to the field list, because the screen that must not print a summed level under
    the word "total" has the measure in hand and not the field catalogue.

    A measure built from two levels subtracted is NOT one: a stock minus a stock is a change, and a
    change is an ordinary amount that adds.
    """
    name = measure.get("field")
    if not isinstance(name, str) or not contract.is_attr_path(name):
        return False
    if kinds_by_field.get(contract.attr_key(name)) != "level":
        return False
    other = measure.get("minus")
    if isinstance(other, str) and contract.is_attr_path(other):
        if kinds_by_field.get(contract.attr_key(other)) == "level":
            return False
    return True


def shape(customer_code: str, rows: Rows) -> dict:
    """The catalog body from rows already in memory. No database, no clock."""
    units_by_field = {f.field: f.unit for f in rows.fields}
    kinds_by_field = {f.field: f.kind for f in rows.fields}
    metrics = []
    for m in sorted(rows.metrics, key=lambda r: r.name):
        dims = []
        for name in m.dimensions:
            domain = rows.domains.get((m.id, name), Domain(values=(), truncated=False))
            dims.append({"name": name, "values": list(domain.values), "truncated": domain.truncated})
        measures = [{
            "name": ms.get("name"),
            "aggregation": ms.get("aggregation"),
            "field": ms.get("field"),
            # Chunk 101: present only for a measure built from two fields, so the interface can say
            # "picked minus expected" rather than just "picked".
            "minus": ms.get("minus"),
            "unit": _unit_for(ms, units_by_field),
            "approximate": ms.get("aggregation") in _APPROXIMATE_AGGREGATIONS,
            # Chunk 109: true when this measure reads a stock level. The reader needs it to stop
            # labelling the stored `sum_value` component "total" on a screen.
            "level": _level_for(ms, kinds_by_field),
        } for ms in m.measures]
        metrics.append({
            "id": str(m.id), "name": m.name, "description": m.description, "source": m.source,
            "dimensions": dims, "measures": measures, "filter": dict(m.filter), "grains": list(m.grains),
            "rollups_from": _iso(m.rollups_from), "backfilled_through": _iso(m.backfilled_through),
        })
    fields = [{
        "field": f.field, "source": f.source, "description": f.description, "unit": f.unit,
        "kind": f.kind, "methods": sorted(f.methods),
    } for f in sorted(rows.fields, key=lambda r: r.field)]
    transactions = [{
        "transaction_name": t.transaction_name, "description": t.description,
        "capture": t.capture, "show": t.show, "expand": t.expand, "mi": t.mi,
    } for t in sorted(rows.transactions, key=lambda r: r.transaction_name)]
    return {"customer_code": customer_code, "metrics": metrics, "fields": fields,
            "transactions": transactions, "aggregations": aggregations()}


def etag(body: Mapping[str, Any]) -> str:
    """A quoted digest of the body, so an unchanged catalog is a 304 rather than a re-read by the
    caller. Keyed on content because no single revision covers three tables and a rollup read."""
    payload = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return '"' + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32] + '"'


# ---------------------------------------------------------------------------------------- loading

async def _active_metrics(db: AsyncSession, customer_code: str) -> list[MetricRow]:
    rows = (await db.execute(select(AnalyticsMetric).where(
        AnalyticsMetric.customer_code == customer_code,
        AnalyticsMetric.status == d.Status.active.value))).scalars().all()
    return [MetricRow(
        id=r.id, name=r.name, description=r.description,
        source=getattr(r, "source", None) or "transaction",
        dimensions=tuple(r.dimensions or ()), measures=tuple(r.measures or ()),
        filter=dict(r.filter or {}), grains=tuple(r.grains or ()),
        # Part 2 adds the column; getattr keeps this total over pre-migration rows.
        rollups_from=getattr(r, "rollups_from", None),
        backfilled_through=r.backfilled_through,
    ) for r in rows]


async def _domains(db: AsyncSession, customer_code: str, metrics: Sequence[MetricRow], *,
                   now: datetime, domain_days: int, domain_cap: int
                   ) -> dict[tuple[uuid.UUID, str], Domain]:
    """Distinct values per (definition, dimension) from the hourly rollups in the window.

    One query per dimension slot in use, bounded by `domain_cap + 1` so truncation is detected without
    reading the whole domain. Hourly rather than daily because it is the finest grain that exists for
    every definition, and the window is short enough that the partition pruning keeps it cheap.
    """
    since = now - timedelta(days=domain_days)
    out: dict[tuple[uuid.UUID, str], Domain] = {}
    for m in metrics:
        for slot, name in enumerate(m.dimensions[:DIMENSION_SLOTS]):
            column = getattr(AnalyticsHourlyRollup, f"dim{slot + 1}")
            values = (await db.execute(
                select(column).distinct().where(
                    AnalyticsHourlyRollup.customer_code == customer_code,
                    AnalyticsHourlyRollup.definition_id == m.id,
                    AnalyticsHourlyRollup.bucket_start >= since,
                    column.isnot(None))
                .order_by(column).limit(domain_cap + 1))).scalars().all()
            truncated = len(values) > domain_cap
            out[(m.id, name)] = Domain(values=tuple(values[:domain_cap]), truncated=truncated)
    return out


async def _fields(db: AsyncSession, customer_code: str) -> list[FieldRow]:
    """Approved fields only, one entry per field NAME with the methods it is approved on.

    Collapsed by name because that is how approval is enforced today (`capture.approved_attributes`
    returns names) and how a metric addresses a field (`attr:<name>`). Description and unit are taken
    from the first described row for the name, so a person needs to describe a field once.
    """
    rows = (await db.execute(select(AnalyticsFieldRegistry).where(
        AnalyticsFieldRegistry.customer_code == customer_code,
        AnalyticsFieldRegistry.captured.is_(True))
        .order_by(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.method))).scalars().all()
    # Chunk 108: meaning comes from the NAME, in one read, rather than from whichever per-method
    # row happened to carry it first. `EmployeeName` is on 44 methods live and means one thing.
    meanings = {m.field: m for m in (await db.execute(select(AnalyticsFieldMeaning).where(
        AnalyticsFieldMeaning.customer_code == customer_code))).scalars().all()}
    by_name: dict[str, dict] = {}
    for r in rows:
        entry = by_name.setdefault(r.field, {"source": r.source, "methods": []})
        entry["methods"].append(r.method)
    out = []
    for name, e in by_name.items():
        m = meanings.get(name)
        out.append(FieldRow(field=name, source=e["source"],
                            description=m.description if m else None,
                            unit=m.unit if m else None, methods=tuple(e["methods"]),
                            # Chunk 109. Loaded and then thrown away until now, which is why nothing
                            # downstream could tell a stock reading from a quantity picked.
                            kind=m.kind if m else None))
    return out


async def _transactions(db: AsyncSession, customer_code: str) -> list[TransactionRow]:
    rows = (await db.execute(select(AnalyticsTransactionRegistry).where(
        AnalyticsTransactionRegistry.customer_code == customer_code)
        .order_by(AnalyticsTransactionRegistry.transaction_name))).scalars().all()
    return [TransactionRow(transaction_name=r.transaction_name, description=r.description,
                           capture=r.capture, show=r.show, expand=r.expand,
                           mi=getattr(r, "mi", False)) for r in rows]


async def load(db: AsyncSession, customer_code: str, *, now: datetime | None = None,
               domain_days: int = DEFAULT_DOMAIN_DAYS, domain_cap: int = DEFAULT_DOMAIN_CAP) -> Rows:
    """Every row the catalog needs, in a handful of bounded reads."""
    now = now or datetime.now(timezone.utc)
    metrics = await _active_metrics(db, customer_code)
    return Rows(
        metrics=metrics,
        domains=await _domains(db, customer_code, metrics, now=now,
                               domain_days=domain_days, domain_cap=domain_cap),
        fields=await _fields(db, customer_code),
        transactions=await _transactions(db, customer_code),
    )


async def build(db: AsyncSession, customer_code: str, *, now: datetime | None = None,
                domain_days: int = DEFAULT_DOMAIN_DAYS, domain_cap: int = DEFAULT_DOMAIN_CAP) -> dict:
    """`load` then `shape`: the catalog body for one tenant."""
    return shape(customer_code, await load(db, customer_code, now=now,
                                           domain_days=domain_days, domain_cap=domain_cap))
