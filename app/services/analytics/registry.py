"""The metric registry as stored rows: `analytics_metrics` <-> `MetricDefinition`.

Phase 3c. This is the narrow slice N5 needs -- serialisation, and "which definitions are active for this
tenant" -- not the whole of N4. The API that creates and validates definitions, and the backfill that
must run before one goes active, are still N4's and Phase 4's.

The point of the file is that folding is driven by ROWS. `CONSUMPTION` exists in code only as a seed: the
worker reads whatever the table holds, so a metric invented from the interface is folded by the same code
path with nothing added. A `_DISPATCH` keyed on metric name would have been the if-chain the plan rules
out, and it would have made consumption the only metric the system could ever have.

Round-tripping is asserted rather than assumed. A definition that serialises lossily is worse than one
that fails to serialise: the fold would quietly use a different filter from the one the user saved, and
the chart would be confidently wrong.
"""

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_metric import AnalyticsMetric
from app.services.analytics import capture
from app.services.analytics import contract as c
from app.services.analytics import definition as d

logger = logging.getLogger(__name__)


def measure_to_json(measure: d.Measure) -> dict:
    """One measure as a JSONB object. Sets are stored SORTED, so a definition's stored form is stable
    across processes -- an unordered dump would make two identical definitions compare unequal."""
    out = {
        "name": measure.name,
        "aggregation": measure.aggregation.value,
        "field": measure.field,
        "only": sorted(x.value if hasattr(x, "value") else str(x) for x in measure.only),
        "statuses": sorted(measure.statuses),
    }
    if measure.unit:
        out["unit"] = measure.unit      # chunk 89: absent when unset, so older stored forms are unchanged
    if measure.minus:
        out["minus"] = measure.minus    # chunk 101, same rule: no key at all when there is no difference
    return out


def measure_from_json(raw: dict) -> d.Measure:
    return d.Measure(
        name=raw["name"],
        aggregation=d.Aggregation(raw["aggregation"]),
        field=raw.get("field"),
        only=frozenset(c.Classification(x) for x in raw.get("only") or ()),
        statuses=frozenset(raw.get("statuses") or ()),
        unit=(str(raw["unit"]).strip() or None) if raw.get("unit") is not None else None,
        minus=(str(raw["minus"]).strip() or None) if raw.get("minus") is not None else None,
    )


def to_row(definition: d.MetricDefinition, *, customer_code: str,
           created_by: str | None = None) -> dict:
    """A definition as `analytics_metrics` column values."""
    return {
        "customer_code": customer_code,
        "name": definition.name,
        "dimensions": list(definition.dimensions),
        "measures": [measure_to_json(m) for m in definition.measures],
        # The method allow-list lives under `filter`, which is where the model says row filters go.
        # Per-measure status filters stay ON the measure, because one definition can hold a total and
        # an error count that differ only by status.
        #
        # R1 adds `transactions` BESIDE `methods` rather than replacing it. Both are needed: a metric
        # may want every "Full Stock Count" regardless of method, or every `ConfirmPickLine`
        # regardless of transaction, or the intersection.
        "filter": {"methods": list(definition.method_filter),
                   "transactions": list(definition.transaction_filter)},
        "grains": list(definition.grains),
        "source": definition.source,
        "status": definition.status.value,
        # Chunk 86: None round-trips as NULL, which is "unbounded" for every pre-builder metric.
        "rollups_from": definition.rollups_from,
        "created_by": created_by,
    }


def from_row(row: AnalyticsMetric) -> d.MetricDefinition:
    return d.MetricDefinition(
        name=row.name,
        dimensions=tuple(row.dimensions or ()),
        measures=tuple(measure_from_json(m) for m in (row.measures or ())),
        grains=tuple(row.grains or ()),
        # getattr keeps from_row total over fakes and pre-migration rows alike.
        source=getattr(row, "source", None) or "transaction",
        method_filter=tuple((row.filter or {}).get("methods") or ()),
        transaction_filter=tuple((row.filter or {}).get("transactions") or ()),
        status=d.Status(row.status),
        # Chunk 86: absent on fakes and pre-migration rows, and absent means unbounded.
        rollups_from=getattr(row, "rollups_from", None),
    )


async def active_definitions(db: AsyncSession, customer_code: str
                             ) -> list[tuple[uuid.UUID, d.MetricDefinition]]:
    """Every active definition for this tenant, with its id.

    The id is returned alongside rather than folded into `MetricDefinition` on purpose: the definition is
    a pure value that Phase 0 tests without a database, and giving it a database identity would end that.

    A definition that fails to validate is SKIPPED and logged rather than raised on. A1's reasoning
    applies to definitions as much as to rows: one malformed registry row must not stop every other
    metric for that tenant from folding.
    """
    rows = (await db.execute(
        select(AnalyticsMetric).where(
            AnalyticsMetric.customer_code == customer_code,
            AnalyticsMetric.status == d.Status.active.value))).scalars().all()

    # R1b. Read ONCE for the whole tenant rather than per definition: it is a small indexed read, but
    # per definition it would be N queries on the hot fold path for an answer that cannot differ
    # between them. A definition naming an attribute nobody has approved is skipped and logged by the
    # same path that skips a malformed one - it does not stop the tenant's other metrics folding.
    known = await capture.approved_attributes(db, customer_code)

    out: list[tuple[uuid.UUID, d.MetricDefinition]] = []
    for row in rows:
        try:
            definition = from_row(row)
        except (KeyError, ValueError) as exc:
            logger.error("Analytics: registry row %s (%r) for %s could not be read (%s) - skipped; "
                         "the tenant's other metrics still fold", row.id, row.name, customer_code, exc)
            continue
        # Chunk 109: `level_fields` is deliberately NOT passed. This is the fold, and a fold that
        # stopped folding a metric somebody has been reading for months - because a colleague ticked
        # a box on a describe form this morning - is a far worse failure than a wrong label on a
        # chart. A failed definition here is SKIPPED, three lines below, so handing it the level set
        # would silently switch a live metric off. The refusal belongs at the moment somebody chooses
        # the measure, which is preview, create, a reshape and a draft first going live.
        problems = d.validate(definition, known_attributes=known)
        if problems:
            logger.error("Analytics: definition %r for %s is invalid and will not be folded: %s",
                         row.name, customer_code, "; ".join(problems))
            continue
        out.append((row.id, definition))
    return out


async def advance_backfilled_through(db: AsyncSession, customer_code: str, *,
                                     range_end: datetime) -> int:
    """Chunk 87: record how far a fold has built each active definition's history. Does NOT commit.

    A run that folded `[lo, range_end]` has fully covered every day before `range_end`'s own, so
    `backfilled_through` becomes the day before, never the partial day itself. Forward only, with
    GREATEST: a backfill of an older range must not drag the marker back. Definitions whose
    `rollups_from` is after the range end have nothing to gain from this run and are left alone.

    A Date rather than an instant because that is what the column has always been and what the
    interface reads; a gap in the middle of a backfill is not representable, which is the same limit
    the column carried before.
    """
    through = range_end.date() - timedelta(days=1)
    result = await db.execute(
        update(AnalyticsMetric)
        .where(AnalyticsMetric.customer_code == customer_code,
               AnalyticsMetric.status == d.Status.active.value,
               or_(AnalyticsMetric.rollups_from.is_(None),
                   AnalyticsMetric.rollups_from <= range_end))
        .values(backfilled_through=func.greatest(
            func.coalesce(AnalyticsMetric.backfilled_through, through), through))
        .execution_options(synchronize_session=False))
    return result.rowcount or 0


async def ensure_seed(db: AsyncSession, customer_code: str) -> uuid.UUID:
    """Make sure this tenant has the consumption definition, and return its id. Does not commit.

    Seeded as ACTIVE, which is a deliberate exception to N4's "a definition cannot go active until its
    backfill has run". That rule protects a USER-created definition, whose chart would otherwise show a
    false start date. This is the seed definition that the Phase 4 backfill itself targets, so there is
    nothing for it to wait on. `backfilled_through` stays NULL, which is what the interface reads to say
    "no history before now" -- so the honest signal is preserved without blocking the fold.
    """
    existing = await db.scalar(
        select(AnalyticsMetric.id).where(AnalyticsMetric.customer_code == customer_code,
                                         AnalyticsMetric.name == d.CONSUMPTION.name))
    if existing is not None:
        return existing
    seed = d.MetricDefinition(**{**d.CONSUMPTION.__dict__, "status": d.Status.active})
    row = AnalyticsMetric(**to_row(seed, customer_code=customer_code, created_by="system:seed"))
    db.add(row)
    await db.flush()
    return row.id
