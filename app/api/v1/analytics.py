"""N7: the analytics read API. Phase 5.

Every endpoint takes the tenant through `get_current_customer` and the session through `get_session`, so
a typo'd tenant is a clean 404 and never a silent cross-tenant query. That is the existing convention and
the reason it exists applies here more than anywhere: a chart that quietly answered for the wrong tenant
would look entirely plausible.

Three things here are shaped by measurements rather than taste.

**`/status` reads exactly ONE row** (F5). The browser polls it every 2 seconds per tab across four
gunicorn workers. The original design computed counts over several tables per poll; the worker now writes
every field this endpoint needs into `analytics_tenant_state`, so this is one indexed lookup plus an
ETag. There is a test asserting the query count, because the natural way to add a field to this response
is to join another table.

**`/series` never returns a finished answer.** It returns additive role values per bucket - sums and
counts - and the caller divides. Invariant 8 does not stop at the rollup table: an endpoint that returned
an average would be the one place twelve monthly averages could get averaged into a year.

**There is no `POST /analytics/backfill`.** The plan lists one; correction log D8 cancelled it. An
endpoint that 202s and then folds nothing would be worse than its absence, and an endpoint that DID
backfill would contradict a decision taken deliberately. `POST /analytics/reconcile` remains, because
that is the half of Phase 4 that survived.
"""

import hashlib
import json
import logging
from decimal import Decimal
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Response
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.api.deps import get_current_customer
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.config.database import get_session
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_field_meaning import KINDS, AnalyticsFieldMeaning
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.services.analytics import capture
from app.services.analytics import contract
from app.services.analytics import catalog as n8
from app.services.analytics import preview as n9
from app.services.analytics import payload as pl
from app.services.analytics import pending_windows
from app.services.analytics import definition as d
from app.services.analytics import lookup as lookup_model
from app.services.analytics import lookup_store
from app.services.analytics import settle as settle_model
from app.services.analytics import settle_store
from app.services.analytics import read as n6
from app.services.analytics import reconcile as rc
from app.services.analytics import registry
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analytics", tags=["analytics"])

#: Hard cap on a breakdown's top-N. Keyset pagination is not meaningful for a ranked aggregate, so the
#: bound is the cap itself, and it is small because a chart with 500 bars is not a chart.
_MAX_TOP_N = 200

#: Default series window when the caller gives none. A day, not "everything": an unbounded default is how
#: a read endpoint becomes an outage on the first curious click.
_DEFAULT_SPAN = timedelta(days=1)


def _window(start: datetime | None, end: datetime | None) -> UtcWindow:
    end = end or datetime.now(timezone.utc)
    start = start or (end - _DEFAULT_SPAN)
    if start >= end:
        raise HTTPException(400, detail="`start` must be before `end`.")
    return UtcWindow(start=start, end=end)


async def _state(db: AsyncSession, customer: str) -> AnalyticsTenantState | None:
    """The ONE row. Every field the status card shows lives here by design (F5)."""
    return (await db.execute(select(AnalyticsTenantState).where(
        AnalyticsTenantState.customer_code == customer))).scalar_one_or_none()


@router.get("/status")
async def analytics_status(response: Response,
                           customer: str = Depends(get_current_customer),
                           db: AsyncSession = Depends(get_session)):
    """Freshness and health for one tenant. EXACTLY one row read, plus an ETag.

    Both freshness numbers (F4), because one cannot say what the user needs to know: `lag_seconds`
    answers "am I behind", and `unsealed_share` answers "is what I have still going to move". A window
    with unsealed contributors is PROVISIONAL, not stale - different words for the user and different
    actions for an operator.

    The ETag is the tenant revision (A5), which the worker bumps in the same commit as the work it
    describes. Keying it off anything computed here would let a 304 be served over changed data.
    """
    state = await _state(db, customer)
    if state is None:
        # Not an error: the worker ships disabled, so this is the normal state until it is switched on.
        # Saying so explicitly beats zeros, which would render as a healthy, empty chart.
        body = {"customer_code": customer, "configured": False,
                "detail": "analytics has not folded anything for this tenant yet",
                "freshness": n6.freshness(analytics_watermark=None, source_watermark=None,
                                          unsealed_share=None, oldest_unsealed_at=None)}
        response.headers["ETag"] = '"unconfigured"'
        return body

    freshness = n6.freshness(analytics_watermark=state.analytics_watermark,
                            source_watermark=state.source_watermark,
                            unsealed_share=state.unsealed_share,
                            oldest_unsealed_at=state.oldest_unsealed_at)
    response.headers["ETag"] = f'"{state.revision}"'
    return {
        "customer_code": customer,
        "configured": True,
        "revision": state.revision,
        "freshness": {**freshness,
                      "analytics_watermark": _iso(freshness["analytics_watermark"]),
                      "source_watermark": _iso(freshness["source_watermark"]),
                      "oldest_unsealed_at": _iso(freshness["oldest_unsealed_at"]),
                      "unsealed_share": (None if freshness["unsealed_share"] is None
                                         else str(freshness["unsealed_share"]))},
        "queue": {"open_tickets": state.open_tickets,
                  "abandoned_tickets": state.abandoned_tickets},
        "volume": {"facts_total": state.facts_total,
                   "record_facts_total": state.record_facts_total,
                   "quarantined_rows": state.quarantined_rows},
        "last_cycle_at": _iso(state.last_cycle_at),
        "last_error": state.last_error,
        # D8: there is no backfill, so the interface must say "no history before here" rather than draw
        # an empty chart, which reads as zero activity.
        #
        # This used to report `analytics_watermark`, the NEWEST folded instant, as the point history
        # STARTS at -- so the notice claimed there was no history before a moment the chart was already
        # plotting data at. It now reads the earliest folded instant, which is what the sentence means.
        "history_starts_at": _iso(state.history_starts_at),
        "backfilled": False,
    }


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


@router.get("/metrics")
async def list_metrics(customer: str = Depends(get_current_customer),
                       db: AsyncSession = Depends(get_session),
                       limit: int = Query(default=50, ge=1, le=200)):
    """This tenant's metric definitions. Bounded, and no default COUNT(*).

    Reads the ROWS rather than reporting `CONSUMPTION`: the whole point of the registry is that the code
    does not know which metrics exist.
    """
    rows = (await db.execute(select(AnalyticsMetric).where(
        AnalyticsMetric.customer_code == customer)
        .order_by(AnalyticsMetric.name).limit(limit))).scalars().all()
    return {"metrics": [{
        "id": str(r.id), "name": r.name, "status": r.status, "description": r.description,
        "dimensions": r.dimensions, "grains": r.grains, "source": r.source,
        "measures": [m.get("name") for m in (r.measures or [])],
        "filter": r.filter,
        # Chunk 87: the day through which this metric's history has been built, and the instant it
        # starts from. NULL start = unbounded (every pre-builder metric); NULL through = not yet folded.
        "backfilled_through": _iso(r.backfilled_through),
        "rollups_from": _iso(getattr(r, "rollups_from", None)),
        "created_by": r.created_by,
    } for r in rows]}


_SHAPE_KEYS = ("name", "dimensions", "measures", "filter", "grains", "source")
_TRANSITIONS = {("draft", "active"), ("active", "inactive"), ("inactive", "active")}


def _parse_instant(value, *, field: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, detail=f"{field!r} is not an ISO-8601 instant: {value!r}") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@router.patch("/metrics/{metric_id}")
async def update_metric(metric_id: str, payload: dict = Body(...),
                        customer: str = Depends(get_current_customer),
                        db: AsyncSession = Depends(get_session)):
    """Edit a metric's shape while it is a draft, describe it any time, and move it through its
    lifecycle: draft -> active, active -> inactive, inactive -> active (chunk 87).

    Activation is the one step with a side effect beyond the row. It fixes `rollups_from` - the
    instant given, or now - and if that instant is before what analytics has already folded, it
    publishes tickets for exactly that range so the ordinary fold builds the history. The same queue
    field approval uses; no job table, no second worker. A start at or after the watermark publishes
    nothing, because there is no history to build yet.

    Shape edits on an active metric are refused rather than applied: rollup rows are keyed by the
    definition id, and changing the key under them would leave rows nothing can explain. Deactivate,
    copy, edit, activate.
    """
    row = await db.scalar(select(AnalyticsMetric).where(
        AnalyticsMetric.customer_code == customer, AnalyticsMetric.id == metric_id))
    if row is None:
        raise HTTPException(404, detail="no such metric for this logspace")
    allowed = set(_SHAPE_KEYS) | {"status", "rollups_from", "description"}
    if not any(k in payload for k in allowed):
        raise HTTPException(400, detail=f"body must contain at least one of {sorted(allowed)}")

    changed: list[str] = []
    shape_edits = [k for k in _SHAPE_KEYS if k in payload]
    if shape_edits and row.status != d.Status.draft.value:
        raise HTTPException(409, detail=f"the metric is {row.status}; its shape ({', '.join(shape_edits)}) "
                                        f"can only change while it is a draft. Deactivate, copy, edit, activate.")
    if "description" in payload:
        row.description = (str(payload["description"]).strip() or None
                           if payload["description"] is not None else None)
        changed.append("description")

    # The definition as it would be after this patch, validated as a whole - a dimension edit can
    # invalidate a measure, so the parts are never checked in isolation.
    merged = {"name": row.name, "dimensions": row.dimensions, "measures": row.measures,
              "filter": row.filter, "grains": row.grains, "source": row.source, "status": row.status}
    merged.update({k: payload[k] for k in shape_edits})
    definition = _definition_from_payload(merged)
    if shape_edits:
        if not definition.name:
            raise HTTPException(400, detail="`name` is required.")
        if not definition.measures:
            raise HTTPException(400, detail="at least one measure is required.")

    target = payload.get("status")
    if target is not None:
        try:
            target = d.Status(target).value
        except ValueError:
            raise HTTPException(400, detail=f"status must be one of "
                                            f"{[s.value for s in d.Status]}, got {target!r}") from None
        if target != row.status and (row.status, target) not in _TRANSITIONS:
            raise HTTPException(409, detail=f"cannot move a {row.status} metric to {target}; allowed "
                                            f"from {row.status}: "
                                            f"{sorted(t for f, t in _TRANSITIONS if f == row.status) or 'nothing'}")
    activating = target == d.Status.active.value and row.status != d.Status.active.value

    if shape_edits or activating:
        # Chunk 109. The level refusal is a gate on the way IN, so it applies when somebody is
        # CHOOSING the measure: a reshaped metric, or a draft going live for the first time. It is
        # deliberately NOT applied when an inactive metric is reactivated - that metric already ran,
        # `_TRANSITIONS` allows a pause and a resume, and refusing it now would strand it paused for
        # ever over a decision taken long before anybody described the field.
        choosing = bool(shape_edits) or row.status == d.Status.draft.value
        problems = d.validate(
            definition,
            known_attributes=await capture.approved_attributes(db, customer),
            field_kinds=await capture.field_kinds(db, customer) if choosing else None)
        if problems:
            raise HTTPException(400, detail=problems)
    for k in shape_edits:
        setattr(row, k, registry.to_row(definition, customer_code=customer)[k])
        changed.append(k)

    published = 0
    backfill = None
    if "rollups_from" in payload and row.status != d.Status.draft.value:
        # The bound is fixed at first activation: rollup rows already built under it would be
        # unexplained by a different one. Refused rather than silently ignored.
        raise HTTPException(409, detail=f"the metric is {row.status}; `rollups_from` was fixed when it "
                                        f"first went active and cannot change. Copy the metric as a "
                                        f"new draft to start from a different instant.")
    if "rollups_from" in payload:
        row.rollups_from = _parse_instant(payload.get("rollups_from"), field="rollups_from")
        changed.append("rollups_from")
    if activating and row.status == d.Status.draft.value:
        if row.rollups_from is None:
            row.rollups_from = datetime.now(timezone.utc)
            changed.append("rollups_from")
        state = await _state(db, customer)
        watermark = state.source_watermark if state else None
        if watermark is not None and row.rollups_from < watermark:
            # Same transaction as the status change (invariant 3): an active metric with tickets
            # that never committed would sit with a start date and no history, forever.
            # Only from DRAFT: an inactive metric's history was built when it first went active, and
            # re-publishing that range would only re-fold rows that already exist.
            # Chunk 91: refold, because these facts have already been folded and will diff as
            # unchanged; without the flag the new metric would get no rollup rows at all.
            published = await pending_windows.publish(db, customer, lo=row.rollups_from, hi=watermark,
                                                      refold=True)
            backfill = {"from": row.rollups_from.isoformat(), "to": watermark.isoformat()}

    if target is not None and target != row.status:
        row.status = target
        changed.append("status")
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"id": str(row.id), "name": row.name, "status": row.status, "description": row.description,
            "dimensions": row.dimensions, "measures": [m.get("name") for m in (row.measures or [])],
            "filter": row.filter, "grains": row.grains, "source": row.source,
            "rollups_from": _iso(row.rollups_from), "backfilled_through": _iso(row.backfilled_through),
            "changed": changed, "tickets_published": published, "backfill": backfill,
            "detail": ("history will be built by the worker from the tickets just published"
                       if published else "no re-fold needed for this change")}


@router.get("/catalog")
async def analytics_catalog(response: Response,
                            if_none_match: str | None = Header(default=None),
                            domain_days: int = Query(default=n8.DEFAULT_DOMAIN_DAYS, ge=1, le=90),
                            domain_cap: int = Query(default=n8.DEFAULT_DOMAIN_CAP, ge=1, le=1000),
                            customer: str = Depends(get_current_customer),
                            db: AsyncSession = Depends(get_session)):
    """What this tenant's analytics MEAN, as one read (chunk 84).

    Active metrics with their descriptions, dimensions and the values those dimensions have taken
    recently, measures with units, plus the approved fields and the transaction names with their
    descriptions. The metric wizard's pickers and the chat agent's `list_metrics` tool both read this
    and nothing else, so a description written once on the review screen reaches both.

    ETag is a digest of the body: three tables and a rollup read have no single revision to key on,
    and the interface polls pickers rarely enough that computing the body to compare is the cheaper
    honest option.
    """
    body = await n8.build(db, customer, domain_days=domain_days, domain_cap=domain_cap)
    tag = n8.etag(body)
    if if_none_match and if_none_match.strip() == tag:
        return Response(status_code=304, headers={"ETag": tag})
    response.headers["ETag"] = tag
    return body


def _definition_from_payload(payload: dict) -> d.MetricDefinition:
    """The request body as a definition, shared by create and preview so the two cannot drift on what
    a body means. A malformed measure is a 400 naming the problem, as create always did."""
    try:
        measures = tuple(registry.measure_from_json(m) for m in (payload.get("measures") or ()))
        status = d.Status(payload.get("status") or d.Status.draft.value)
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(400, detail=f"malformed measure: {exc}") from None
    return d.MetricDefinition(
        name=(payload.get("name") or "").strip(),
        dimensions=tuple(payload.get("dimensions") or ()),
        measures=measures,
        grains=tuple(payload.get("grains") or ("hourly", "daily", "monthly")),
        method_filter=tuple((payload.get("filter") or {}).get("methods") or ()),
        # R1: accepted here so the interface can write a per-transaction metric without a deploy.
        transaction_filter=tuple((payload.get("filter") or {}).get("transactions") or ()),
        # 18y: which fact table the metric folds and reads. Validate() is source-aware, so a wrong
        # value or a cross-grain field mix is a 400 with the reasons listed, never a silent chart.
        source=payload.get("source") or "transaction",
        status=status,
    )


@router.post("/metrics/preview")
async def preview_metric(payload: dict = Body(...),
                         window_hours: int = Query(default=None, ge=1, le=24 * 60),
                         customer: str = Depends(get_current_customer),
                         db: AsyncSession = Depends(get_session)):
    """A dry run of a definition over the last `window_hours` of real facts (chunk 85). Writes nothing.

    Same body as create. Returns every finding at once: the shape problems `validate` would raise,
    whether anything matches the filter and which methods sit behind it, how much of the matching
    data carries the aggregated field, how many rollup rows the dimensions imply, and a sample
    series folded with the writer's own functions. `ok` is false only for a shape problem or zero
    matches; a sparse field is a warning with its percentage, and the decision stays with the person.
    """
    definition = _definition_from_payload(payload)
    if not definition.measures:
        raise HTTPException(400, detail="at least one measure is required.")
    problems = d.validate(definition,
                          known_attributes=await capture.approved_attributes(db, customer),
                          field_kinds=await capture.field_kinds(db, customer))
    hours = window_hours or settings.analytics_preview_window_hours
    return await n9.run(db, customer, definition, problems=problems, window_hours=hours)


@router.post("/metrics", status_code=201)
async def create_metric(payload: dict = Body(...),
                        customer: str = Depends(get_current_customer),
                        db: AsyncSession = Depends(get_session)):
    """Register a metric. The registry is the whole point: this writes a ROW, not code.

    Validated through `definition.validate()` -- the SAME function the worker applies before folding, so
    a definition that would produce a silently empty chart is rejected here rather than accepted and
    then skipped at fold time. Every problem is returned at once, because a half-valid definition should
    not be reported one error per save.

    201, not the plan's 202: the 202 existed because creating a metric started a backfill job, and
    correction D8 cancelled the backfill. Returning 202 with nothing running behind it would promise
    work that never happens. History for a new metric comes from re-folding a range instead, which is an
    explicit operator action -- `POST /analytics/reconcile?repair=true`.
    """
    definition = _definition_from_payload(payload)
    if not definition.name:
        raise HTTPException(400, detail="`name` is required.")
    if not definition.measures:
        raise HTTPException(400, detail="at least one measure is required.")

    # R1b. The field registry decides which `attr:` paths are usable, so a metric naming an
    # unapproved or misspelled attribute is refused HERE, at save time, with a message naming the
    # field - rather than being accepted and producing a silently empty chart.
    #
    # Chunk 109: the same gate refuses adding up a LEVEL. A brand new metric is the one moment where
    # somebody is choosing the measure, so it is the right and only place to say no.
    problems = d.validate(definition,
                          known_attributes=await capture.approved_attributes(db, customer),
                          field_kinds=await capture.field_kinds(db, customer))
    if problems:
        raise HTTPException(400, detail=problems)

    existing = await db.scalar(select(AnalyticsMetric.id).where(
        AnalyticsMetric.customer_code == customer, AnalyticsMetric.name == definition.name))
    if existing is not None:
        raise HTTPException(409, detail=f"a metric named {definition.name!r} already exists here.")

    row = AnalyticsMetric(**registry.to_row(definition, customer_code=customer,
                                            created_by=payload.get("created_by") or "api"))
    # Chunk 89: the meaning, as data. The wizard refuses an empty one; the API stays lenient so a
    # scripted create keeps working, and a blank is NULL rather than "" so the catalog can tell.
    if payload.get("description") is not None:
        row.description = str(payload["description"]).strip() or None
    db.add(row)
    await db.commit()
    return {"id": str(row.id), "name": row.name, "status": row.status, "source": row.source,
            "description": row.description,
            "dimensions": row.dimensions, "grains": row.grains,
            "measures": [m.get("name") for m in (row.measures or [])],
            # D8 again: no history exists for it until a range is re-folded.
            "backfilled_through": None,
            "detail": ("Registered. It has no history yet -- re-fold a range with "
                       "POST /analytics/reconcile?repair=true to populate it.")}


async def _definition(db: AsyncSession, customer: str, name: str):
    for definition_id, definition in await registry.active_definitions(db, customer):
        if definition.name == name:
            return definition_id, definition
    raise HTTPException(404, detail=f"No ACTIVE metric named {name!r} for this tenant. "
                                    f"GET /analytics/metrics lists what exists.")


async def _refuse_unapproved_ad_hoc_attrs(db, customer: str, definition, dims: tuple):
    """Chunk 80: an ad-hoc `attr:` group-by must name an APPROVED field. The definition's own
    dimensions were checked at creation; an ad-hoc path bypasses that, and an unapproved (or
    typo'd) one would scan and return nothing - "no data" instead of the 400 someone can act on."""
    from app.services.analytics import contract as c
    from app.services.analytics import lookup as lk
    # Chunk 100: a `lookup:` path names no attribute of its own - its key field does, and `lk.plan`
    # has already substituted that by the time this runs on the STORED grouping. Passing a raw path
    # through `attr_key` would refuse a perfectly valid grouping.
    dims = tuple(g for g in dims if not lk.is_lookup_path(g))
    ad_hoc_attrs = [g for g in dims if c.is_attr_path(g) and g not in definition.dimensions]
    if not ad_hoc_attrs:
        return
    known = await capture.approved_attributes(db, customer)
    for g in ad_hoc_attrs:
        if c.attr_key(g) not in known:
            raise HTTPException(400, detail=f"{g!r} names an attribute that is not approved for "
                                            f"capture, so grouping by it would be silently empty")

@router.get("/series")
async def analytics_series(customer: str = Depends(get_current_customer),
                          db: AsyncSession = Depends(get_session),
                          metric: str = Query(default="consumption"),
                          measure: str = Query(default="quantity"),
                          start: datetime | None = Query(default=None),
                          end: datetime | None = Query(default=None),
                          group_by: str | None = Query(default=None)):
    """One measure over time, two-tier.

    Returns additive ROLES per bucket, never a finished answer. The response states which grain it chose
    and which spans were read live, so a caller can tell a settled number from a provisional one instead
    of having to trust that they are the same.
    """
    window = _window(start, end)
    definition_id, definition = await _definition(db, customer, metric)
    if measure not in {m.name for m in definition.measures}:
        raise HTTPException(400, detail=f"{metric!r} has no measure {measure!r}; it has "
                                        f"{sorted(m.name for m in definition.measures)}.")
    dims = tuple(x.strip() for x in (group_by or "").split(",") if x.strip())
    # Read once per request, like the registry switches: a declaration consulted per row would be a
    # query per row, and a set read twice in one request could disagree with itself.
    lookups = await lookup_store.load(db, customer)
    try:
        decision = n6.resolve(definition, group_by=dims, lookups=lookups)
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None
    await _refuse_unapproved_ad_hoc_attrs(db, customer, definition, dims)

    state = await _state(db, customer)
    out = await n6.series(db, customer, definition_id, definition, window=window, measure=measure,
                          group_by=dims, ad_hoc=decision.ad_hoc, tz=await get_customer_timezone(db, customer),
                          watermark=state.analytics_watermark if state else None, lookups=lookups)
    return {**out, "metric": metric, "ad_hoc": decision.ad_hoc, "resolution": decision.reason,
            "window": {"start": window.start.isoformat(), "end": window.end.isoformat()}}


def _measure_reads_a_level(measure: d.Measure, kinds: dict[str, str]) -> bool:
    """Chunk 109. Whether this measure's answer is a stock level rather than an amount.

    False for a `sum`, which the level rule refuses on the way in and deliberately leaves alone for a
    metric that was already running: for one of those the total genuinely is the answer it was built
    to give, and relabelling it now would only make an old chart unreadable.

    False for a difference of two levels: a stock minus a stock is a change, and changes add.
    """
    def kind_of(name):
        if not name or not contract.is_attr_path(name):
            return None
        return kinds.get(contract.attr_key(name))

    if measure.aggregation is d.Aggregation.sum:
        return False
    if kind_of(measure.field) != "level":
        return False
    return kind_of(measure.minus) != "level"


@router.get("/breakdown")
async def analytics_breakdown(customer: str = Depends(get_current_customer),
                             db: AsyncSession = Depends(get_session),
                             metric: str = Query(default="consumption"),
                             measure: str = Query(default="quantity"),
                             dimension: str = Query(...),
                             start: datetime | None = Query(default=None),
                             end: datetime | None = Query(default=None),
                             top: int = Query(default=10, ge=1, le=_MAX_TOP_N)):
    """Top-N by one dimension for a window. Bounded by `top`, capped at _MAX_TOP_N."""
    window = _window(start, end)
    definition_id, definition = await _definition(db, customer, metric)
    if measure not in {m.name for m in definition.measures}:
        # Chunk 80: /series always refused a wrong measure; /breakdown silently returned empty rows.
        raise HTTPException(400, detail=f"{metric!r} has no measure {measure!r}; it has "
                                        f"{sorted(m.name for m in definition.measures)}.")
    lookups = await lookup_store.load(db, customer)
    try:
        decision = n6.resolve(definition, group_by=(dimension,), lookups=lookups)
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None
    await _refuse_unapproved_ad_hoc_attrs(db, customer, definition, (dimension,))

    state = await _state(db, customer)
    out = await n6.series(db, customer, definition_id, definition, window=window, measure=measure,
                          group_by=(dimension,), ad_hoc=decision.ad_hoc, tz=await get_customer_timezone(db, customer),
                          watermark=state.analytics_watermark if state else None, lookups=lookups)

    totals: dict = {}
    for point in out["points"]:
        key = point["dimensions"][0] if point["dimensions"] else None
        roles = point["roles"]
        bucket = totals.setdefault(key, {"sum_value": 0, "count_value": 0})
        from decimal import Decimal as _D
        bucket["sum_value"] = str(_D(str(bucket["sum_value"])) + _D(roles.get("sum_value", "0")))
        bucket["count_value"] += roles.get("count_value", 0) or 0

    # Chunk 109. `sum_value` is still summed and still returned, because the reader divides it by
    # `count_value` to get the mean at whatever grouping was asked for. What changes for a measure
    # that reads a LEVEL is the RANKING: ordering warehouses by the sum of their stock readings puts
    # the most frequently scanned one on top rather than the fullest, and the order of a top-N list
    # is the whole answer. The typical level is the honest weight, so the fullest leads.
    chosen = next(m for m in definition.measures if m.name == measure)
    is_level = _measure_reads_a_level(chosen, await capture.field_kinds(db, customer))

    from decimal import Decimal as _D

    def _weight(bucket: dict) -> _D:
        total = _D(str(bucket["sum_value"]))
        if not is_level:
            return abs(total)
        readings = bucket["count_value"] or 0
        return abs(total / _D(readings)) if readings else _D(0)

    ranked = sorted(totals.items(), key=lambda kv: -_weight(kv[1]))[:top]
    return {"metric": metric, "measure": measure, "dimension": dimension, "grain": out["grain"],
            "ad_hoc": decision.ad_hoc, "resolution": decision.reason, "level": is_level,
            "window": {"start": window.start.isoformat(), "end": window.end.isoformat()},
            "rows": [{"value": k, **v} for k, v in ranked]}


# 200, not the original 202 (chunk 68): the checks run inline and the COMPLETE report is in this
# very response - a 202 promises a poll that does not exist.
@router.post("/reconcile", status_code=200)
async def trigger_reconcile(customer: str = Depends(get_current_customer),
                           db: AsyncSession = Depends(get_session),
                           start: datetime | None = Query(default=None),
                           end: datetime | None = Query(default=None),
                           repair: bool = Query(default=False)):
    """Run the three reconciliation checks now, for one tenant. 202 with the report.

    `repair` defaults to FALSE, matching the worker and Phase 7's sequencing. A repair never invents a
    number: a missing fact publishes a ticket, a drifted bucket is re-folded, and an orphaned entry gets
    neither because it needs a Stage 2 regroup.
    """
    window = _window(start, end)
    report = await rc.reconcile_tenant(db, customer, window=window, repair=repair)
    if repair:
        await db.commit()
    return {
        "customer_code": customer,
        "window": report["window"],
        "healthy": report["healthy"],
        "by_check": report["by_check"],
        "tickets_published": report["tickets_published"],
        "buckets_recomputed": report["buckets_recomputed"],
        "findings": [{"check": f.check, "summary": f.summary, "detail": f.detail}
                     for f in report["findings"]],
    }


# ============================================================== R2: the registry (what analytics may do)
@router.get("/registry/transactions")
async def list_transaction_registry(customer: str = Depends(get_current_customer),
                                    db: AsyncSession = Depends(get_session)):
    """Every transaction analytics has seen for this tenant, with its three switches.

    Rows are CREATED by the fold, never here: discovery is what knows a transaction exists. So an empty
    list means analytics has not folded anything yet, not that nothing is configured - which is worth
    distinguishing, because the two look identical on a screen.

    `needs_review` is `reviewed_at IS NULL`, i.e. nobody has touched the switches. Surfaced rather than
    enforced by hiding data: the defaults are both ON, so an unreviewed transaction is counted and
    flagged, not silently dropped.
    """
    rows = (await db.execute(
        select(AnalyticsTransactionRegistry)
        .where(AnalyticsTransactionRegistry.customer_code == customer)
        .order_by(AnalyticsTransactionRegistry.transaction_name))).scalars().all()
    return {"transactions": [{
        "transaction_name": r.transaction_name,
        "capture": r.capture, "show": r.show, "expand": r.expand, "mi": r.mi,
        "first_seen_at": _iso(r.first_seen_at),
        "reviewed_at": _iso(r.reviewed_at), "reviewed_by": r.reviewed_by,
        "needs_review": r.reviewed_at is None,
    } for r in rows]}


@router.patch("/registry/transactions/{transaction_name}")
async def set_transaction_switches(transaction_name: str, payload: dict = Body(...),
                                   customer: str = Depends(get_current_customer),
                                   db: AsyncSession = Depends(get_session)):
    """Set one transaction's switches. Only the keys present in the body are changed.

    A PATCH rather than a PUT because the three switches are independent decisions with very different
    consequences, and a PUT would make "I toggled show" silently also reassert capture and expand from
    whatever the client last read.

    TURNING `capture` OFF PUBLISHES NO TICKET, and turning it ON DOES. That asymmetry is the point:
    capture-on needs the retention range re-examined so the newly captured transaction gets facts, while
    capture-off needs nothing re-examined at all - the existing facts are deliberately left alone (see
    `capture`), so there is nothing for a fold to change.

    `show` publishes a ticket in both directions, because it gates the ROLLUPS and those genuinely have
    to be recomputed either way. That is the "one recompute" the switch costs, and it is why `show` is
    the reversible one.
    """
    row = await db.scalar(
        select(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.customer_code == customer,
            AnalyticsTransactionRegistry.transaction_name == transaction_name))
    if row is None:
        # 404 rather than an upsert: a name analytics has never seen is almost always a typo, and
        # creating a row for it would silently accept the typo and then do nothing measurable.
        raise HTTPException(404, f"analytics has not seen a transaction named {transaction_name!r} "
                                 f"for this logspace, so there is nothing to configure")

    before = (row.capture, row.show, row.expand, row.mi)
    switched = [f for f in ("capture", "show", "expand", "mi") if f in payload]
    for field in switched:
        setattr(row, field, bool(payload[field]))
    if "description" in payload:
        # Chunk 84: metadata for the catalog, not a review decision. Describing a transaction must
        # not stamp `reviewed_at`, or the "needs review" list would empty itself as people document.
        row.description = (str(payload["description"]).strip() or None
                           if payload["description"] is not None else None)
    if not switched and "description" not in payload:
        raise HTTPException(400, "body must contain at least one of capture, show, expand, mi, description")
    if switched:
        row.reviewed_at = datetime.now(timezone.utc)
        row.reviewed_by = payload.get("reviewed_by") or "api"
    row.updated_at = datetime.now(timezone.utc)

    # A ticket only when a switch that changes stored data actually moved, and only in the direction
    # that needs work. `expand` ON needs the retention range re-examined so the record grain
    # BACKFILLS (18x: the presence diff expands settled windows through ordinary tickets); OFF
    # publishes nothing - existing record rows are deliberately kept, capture-off semantics.
    published = 0
    turned_capture_on = (not before[0]) and row.capture
    show_changed = before[1] != row.show
    turned_expand_on = (not before[2]) and row.expand
    # Chunk 94: MI on changes what the fact CONTAINS, so the range diff finds every affected fact by
    # fingerprint - ordinary tickets, no refold flag. Off publishes nothing: the keys fade when each
    # range is next restated, and nothing reads them meanwhile.
    turned_mi_on = (not before[3]) and row.mi
    if turned_capture_on or show_changed or turned_expand_on or turned_mi_on:
        frontier = await db.scalar(
            select(AnalyticsTenantState.source_watermark).where(
                AnalyticsTenantState.customer_code == customer))
        history = await db.scalar(
            select(AnalyticsTenantState.history_starts_at).where(
                AnalyticsTenantState.customer_code == customer))
        if frontier is not None:
            # In the SAME transaction as the switch, which is invariant 3 applied to a registry write:
            # row first, commit, then publish would leave a switch flipped with nothing to act on it,
            # and it would stay that way until some unrelated rebuild happened to touch those windows.
            #
            # Chunk 91: a `show` flip changes no fact, so its tickets must ask for a REFOLD or the
            # rollups stay exactly as they were (verified: they did, before this flag existed).
            # Capture-on and expand-on change facts and record rows, which dirty buckets by themselves.
            published = await pending_windows.publish(
                db, customer,
                lo=history or (frontier - timedelta(days=settings.log_partition_retention_days)),
                hi=frontier, refold=show_changed)
    await db.commit()

    return {"transaction_name": transaction_name, "capture": row.capture, "show": row.show,
            "expand": row.expand, "mi": row.mi, "description": row.description,
            "tickets_published": published,
            "detail": ("the retention range will be re-examined on the next worker tick"
                       if published else "no re-fold needed for this change")}


@router.get("/registry/fields")
async def list_field_registry(only_unreviewed: bool = Query(False),
                              limit: int = Query(500, ge=1, le=2000),
                              customer: str = Depends(get_current_customer),
                              db: AsyncSession = Depends(get_session)):
    """Every response field analytics has observed, and whether its VALUE is being kept.

    This is the review surface for the allowlist. A field with `captured = false` has had its NAME
    recorded and nothing else - there is no column in that table a value could live in - so this
    endpoint cannot leak one even if a field is a credential.

    `unreviewed first` by default in the ordering, because the whole point of the list is the tail of
    things nobody has looked at yet.
    """
    stmt = (select(AnalyticsFieldRegistry)
            .where(AnalyticsFieldRegistry.customer_code == customer)
            .order_by(AnalyticsFieldRegistry.captured,
                      AnalyticsFieldRegistry.field)
            .limit(limit))
    if only_unreviewed:
        stmt = stmt.where(AnalyticsFieldRegistry.reviewed_at.is_(None))
    rows = (await db.execute(stmt)).scalars().all()
    return {"fields": [{
        "id": str(r.id), "method": r.method, "source": r.source, "field": r.field,
        "captured": r.captured,
        # Reported so the interface can warn before somebody ticks a credential by hand. It is advice,
        # not a block: a person is allowed to decide, which is exactly what the veto reserves for them.
        "credential_shaped": pl.never_auto_approve(r.field),
        "seeded": pl.seeded(r.field),
        "description": r.description, "unit": r.unit,
        "first_seen_at": _iso(r.first_seen_at), "last_seen_at": _iso(r.last_seen_at),
        "reviewed_at": _iso(r.reviewed_at), "reviewed_by": r.reviewed_by,
        "needs_review": r.reviewed_at is None,
    } for r in rows]}


@router.patch("/registry/fields/{field_id}")
async def set_field_capture(field_id: str, payload: dict = Body(...),
                            customer: str = Depends(get_current_customer),
                            db: AsyncSession = Depends(get_session)):
    """Approve or un-approve one observed field.

    Approving publishes a ticket: the field's values are not in any existing fact, so the retention
    range has to be re-folded for them to appear. Un-approving publishes one too, because the values
    ARE in existing facts and removing them is also a change - and unlike `capture`, this one really
    does remove data, which is why it is the caller's explicit action rather than a side effect.
    """
    row = await db.scalar(
        select(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == customer,
            AnalyticsFieldRegistry.id == field_id))
    if row is None:
        raise HTTPException(404, "no such observed field for this logspace")
    if not any(k in payload for k in ("captured", "description", "unit")):
        raise HTTPException(400, "body must contain at least one of captured, description, unit")

    was = row.captured
    if "captured" in payload:
        row.captured = bool(payload["captured"])
        row.reviewed_at = datetime.now(timezone.utc)
        row.reviewed_by = payload.get("reviewed_by") or "api"
    # Chunk 84: meaning for the catalog. Metadata, so it neither stamps a review nor publishes a
    # ticket; only `captured` moving changes what a fold stores.
    #
    # Chunk 108: written THROUGH to the per-name meanings table, which is now the one source of
    # truth. Writing it onto this per-method row as well would leave two places to disagree, and
    # the row's own columns are no longer read by anything.
    if "description" in payload or "unit" in payload:
        meaning = await db.scalar(select(AnalyticsFieldMeaning).where(
            AnalyticsFieldMeaning.customer_code == customer,
            AnalyticsFieldMeaning.field == row.field))
        if meaning is None:
            meaning = AnalyticsFieldMeaning(customer_code=customer, field=row.field)
            db.add(meaning)
        if "description" in payload:
            meaning.description = (str(payload["description"]).strip() or None
                                   if payload["description"] is not None else None)
        if "unit" in payload:
            meaning.unit = (str(payload["unit"]).strip()[:32] or None
                            if payload["unit"] is not None else None)
        meaning.reviewed_at = datetime.now(timezone.utc)
        meaning.reviewed_by = payload.get("reviewed_by") or "api"
        row.description, row.unit = meaning.description, meaning.unit
    row.updated_at = datetime.now(timezone.utc)

    published = 0
    if was != row.captured:
        frontier = await db.scalar(
            select(AnalyticsTenantState.source_watermark).where(
                AnalyticsTenantState.customer_code == customer))
        history = await db.scalar(
            select(AnalyticsTenantState.history_starts_at).where(
                AnalyticsTenantState.customer_code == customer))
        if frontier is not None:
            published = await pending_windows.publish(
                db, customer,
                lo=history or (frontier - timedelta(days=settings.log_partition_retention_days)),
                hi=frontier)
    await db.commit()
    return {"id": str(row.id), "field": row.field, "captured": row.captured,
            "description": row.description, "unit": row.unit,
            "tickets_published": published}


# ============================================================== chunk 100: lookups
#
# A fact records one exchange, and that is often not enough to answer the question somebody has. A pick
# carries its delivery number on every one of 1,343 live records and the customer name on none of them.
# These endpoints declare where the missing half comes from; the value is resolved when somebody reads,
# never copied onto the fact. The reasoning, with measurements, is at the top of `analytics/lookup.py`.


def _lookup_from_payload(body: dict) -> lookup_model.Lookup:
    """A request body as the pure value. Raises ValueError on a shape that cannot be a lookup."""
    attributes = []
    for raw in (body.get("attributes") or []):
        if not isinstance(raw, dict) or not raw.get("name"):
            raise ValueError("each attribute needs a name")
        sources = []
        for src in (raw.get("sources") or []):
            missing = [k for k in ("method", "key_field", "value_field") if not src.get(k)]
            if missing:
                raise ValueError(f"source of {raw['name']!r} is missing {', '.join(missing)}; both "
                                 f"field names are explicit because the spelling differs per method")
            sources.append(lookup_model.Source(method=str(src["method"]),
                                               key_field=str(src["key_field"]),
                                               value_field=str(src["value_field"])))
        attributes.append(lookup_model.Attribute(
            name=str(raw["name"]).strip(), sources=tuple(sources),
            stable=bool(raw.get("stable", True)),
            on_conflict=str(raw.get("on_conflict") or "first_wins")))
    return lookup_model.Lookup(name=str(body.get("name") or "").strip(),
                               key_field=str(body.get("key_field") or "").strip(),
                               attributes=tuple(attributes))


def _lookup_json(row) -> dict:
    return {"id": str(row.id), "name": row.name, "description": row.description,
            "key_field": row.key_field, "attributes": row.attributes or [],
            "enabled": row.enabled, "created_by": row.created_by,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None}


@router.get("/lookups")
async def list_lookups(customer: str = Depends(get_current_customer),
                       db: AsyncSession = Depends(get_session)):
    """Every declared lookup, enabled or not, with how many values each has harvested."""
    rows = (await db.execute(
        select(AnalyticsLookup).where(AnalyticsLookup.customer_code == customer)
        .order_by(AnalyticsLookup.name))).scalars().all()
    counts = dict((await db.execute(
        select(AnalyticsLookupValue.lookup, func.count())
        .where(AnalyticsLookupValue.customer_code == customer)
        .group_by(AnalyticsLookupValue.lookup))).all())
    return {"lookups": [{**_lookup_json(r), "values": counts.get(r.name, 0)} for r in rows]}


@router.post("/lookups", status_code=201)
async def create_lookup(body: dict = Body(...),
                        customer: str = Depends(get_current_customer),
                        db: AsyncSession = Depends(get_session)):
    """Declare a lookup. Validated against the fact contract, so a typo is refused rather than
    silently keying on nothing."""
    try:
        declared = _lookup_from_payload(body)
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None
    known = await capture.approved_attributes(db, customer)
    problems = lookup_model.validate(declared, fact_fields=contract.FACT_FIELDS,
                                     known_attributes=known)
    if problems:
        raise HTTPException(400, detail="; ".join(problems))
    exists = await db.scalar(select(AnalyticsLookup.id).where(
        AnalyticsLookup.customer_code == customer, AnalyticsLookup.name == declared.name))
    if exists:
        raise HTTPException(409, detail=f"a lookup called {declared.name!r} already exists")
    row = AnalyticsLookup(customer_code=customer, name=declared.name,
                          description=(str(body.get("description") or "").strip() or None),
                          key_field=declared.key_field,
                          attributes=lookup_store.to_row(declared),
                          enabled=bool(body.get("enabled", True)),
                          created_by=str(body.get("created_by") or "api"))
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _lookup_json(row)


@router.patch("/lookups/{name}")
async def update_lookup(name: str, body: dict = Body(...),
                        customer: str = Depends(get_current_customer),
                        db: AsyncSession = Depends(get_session)):
    """Change a declaration. Harvested values are never deleted by an edit: they are history, and a
    source removed today says nothing about what was true yesterday."""
    row = await db.scalar(select(AnalyticsLookup).where(
        AnalyticsLookup.customer_code == customer, AnalyticsLookup.name == name))
    if row is None:
        raise HTTPException(404, detail=f"no lookup called {name!r} for this logspace")
    if "attributes" in body or "key_field" in body:
        try:
            declared = _lookup_from_payload({"name": name,
                                             "key_field": body.get("key_field", row.key_field),
                                             "attributes": body.get("attributes", row.attributes)})
        except ValueError as exc:
            raise HTTPException(400, detail=str(exc)) from None
        known = await capture.approved_attributes(db, customer)
        problems = lookup_model.validate(declared, fact_fields=contract.FACT_FIELDS,
                                         known_attributes=known)
        if problems:
            raise HTTPException(400, detail="; ".join(problems))
        row.key_field = declared.key_field
        row.attributes = lookup_store.to_row(declared)
    if "enabled" in body:
        row.enabled = bool(body["enabled"])
    if "description" in body:
        row.description = (str(body["description"]).strip() or None
                           if body["description"] is not None else None)
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(row)
    return _lookup_json(row)


@router.get("/lookups/suggest")
async def suggest_lookup_sources(
        key_field: str = Query(..., description="The fact field holding the key, e.g. delivery_number"),
        days: int = Query(14, ge=1, le=365),
        customer: str = Depends(get_current_customer),
        db: AsyncSession = Depends(get_session)):
    """Propose where a key's attributes could be harvested from, by looking at the data.

    The scan itself is `lookup_store.suggest_sources`; this validates the key against the fact contract
    first, because the field name reaches SQL and because a typo should be a 400 rather than an empty
    answer somebody reads as "nothing to find".
    """
    if key_field not in contract.FACT_FIELDS:
        raise HTTPException(400, detail=f"{key_field!r} is not a field on the fact row; the fields "
                                        f"are fixed in analytics.contract")
    return await lookup_store.suggest_sources(db, customer, key_field=key_field, days=days)


@router.post("/lookups/{name}/backfill")
async def backfill_lookup(name: str,
                          days: int = Query(60, ge=1, le=3650),
                          customer: str = Depends(get_current_customer),
                          db: AsyncSession = Depends(get_session)):
    """Fill a newly declared lookup from the facts already stored. Rewrites no fact."""
    declared = (await lookup_store.load(db, customer, enabled_only=False)).get(name)
    if declared is None:
        raise HTTPException(404, detail=f"no lookup called {name!r} for this logspace")
    if not declared.sources_by_method():
        raise HTTPException(400, detail=f"lookup {name!r} declares no sources, so there is nothing "
                                        f"to harvest")
    report = await lookup_store.backfill(db, customer, declared, days=days)
    await db.commit()
    return {"lookup": name, "days": days, **report}


@router.get("/lookups/{name}/values")
async def list_lookup_values(name: str,
                             key: str | None = Query(default=None),
                             limit: int = Query(50, ge=1, le=500),
                             customer: str = Depends(get_current_customer),
                             db: AsyncSession = Depends(get_session)):
    """What a lookup currently knows. The screen shows it so a wrong value can be traced to its source."""
    stmt = select(AnalyticsLookupValue).where(AnalyticsLookupValue.customer_code == customer,
                                              AnalyticsLookupValue.lookup == name)
    if key:
        stmt = stmt.where(AnalyticsLookupValue.key == key)
    rows = (await db.execute(stmt.order_by(AnalyticsLookupValue.key,
                                           AnalyticsLookupValue.attribute,
                                           AnalyticsLookupValue.valid_from)
                             .limit(limit))).scalars().all()
    return {"lookup": name, "values": [
        {"key": r.key, "attribute": r.attribute, "value": r.value,
         "valid_from": r.valid_from.isoformat(), "valid_to": r.valid_to.isoformat() if r.valid_to else None,
         "origin": r.origin, "source_method": r.source_method, "observations": r.observations}
        for r in rows]}


@router.post("/registry/fields/prune")
async def prune_field_registry(dry_run: bool = Query(True, description="Report only. Pass false to delete."),
                               days: int = Query(60, ge=1, le=365,
                                                 description="A field carried by any fact of its method "
                                                             "within this many days is kept."),
                               customer: str = Depends(get_current_customer),
                               db: AsyncSession = Depends(get_session)):
    """Chunk 96: delete response-field registry rows that no fact of their method carries.

    Dry run by default. Rows a person decided on and rows a metric names are never deleted and are
    listed with the reason. Meant to be run once after a Stage 2 history rebuild (18ac) has restated
    the facts, not on a schedule: discovery itself never deletes.
    """
    out = await capture.prune_unseen_fields(db, customer, days=days, dry_run=dry_run)
    if not dry_run:
        await db.commit()
    return out


# ============================================================== chunk 108: what a field MEANS
#
# Meaning lived on the field registry, whose rows are per field PER METHOD. That is right for a
# DECISION - `EmployeeName` may be ticked on picking and not on counting - and wrong for a MEANING,
# because the name means the same thing on all 44 methods that carry it. Describing it meant writing
# the same sentence 44 times, so nobody did: 1,492 rows on the live tenant, 0 described.
#
# `kind` is the part no amount of looking at the data can supply. Discovery over the live facts
# classified `ItemNumber`, `DeliveryNumber`, `LotNumber`, `UserID` and `DeviceID` as measures,
# because they are numeric and they repeat exactly as a quantity does. A delivery number is a name
# spelled with digits, and only a person can say so.


async def _meanings_for(db, customer: str) -> dict[str, AnalyticsFieldMeaning]:
    """Every recorded meaning for this tenant, by field name. Read once per request."""
    rows = (await db.execute(select(AnalyticsFieldMeaning).where(
        AnalyticsFieldMeaning.customer_code == customer))).scalars().all()
    return {r.field: r for r in rows}


@router.get("/registry/meanings")
async def list_field_meanings(customer: str = Depends(get_current_customer),
                              db: AsyncSession = Depends(get_session)):
    """Every registered field NAME, described or not.

    Undescribed names are listed too, and that is the point: a name absent from the list cannot be
    filled in, and the gap is what somebody is here to close.
    """
    registered = (await db.execute(
        select(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.source,
               func.count().label("methods"), func.sum(AnalyticsFieldRegistry.seen_count),
               func.bool_or(AnalyticsFieldRegistry.captured))
        .where(AnalyticsFieldRegistry.customer_code == customer)
        .group_by(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.source))).all()
    meanings = await _meanings_for(db, customer)

    out = []
    for field, source, methods, seen, captured in registered:
        m = meanings.get(field)
        out.append({
            "field": field, "source": source, "methods": int(methods),
            "seen": int(seen or 0), "captured": bool(captured),
            "description": m.description if m else None,
            "unit": m.unit if m else None,
            "kind": m.kind if m else None,
            "reviewed_by": m.reviewed_by if m else None,
            "reviewed_at": m.reviewed_at.isoformat() if m and m.reviewed_at else None,
        })
    out.sort(key=lambda e: (-e["seen"], e["field"]))
    return {"meanings": out, "total": len(out),
            "described": sum(1 for e in out if e["description"]),
            "kinds": list(KINDS)}


@router.patch("/registry/meanings")
async def set_field_meaning(body: dict = Body(...),
                            customer: str = Depends(get_current_customer),
                            db: AsyncSession = Depends(get_session)):
    """Record what one field name means. Writes nothing else and starts no storage.

    The field is taken in the BODY rather than the path: a name can contain a dot (`resp.value`) and
    a colon, and a path segment is the wrong place to carry one.
    """
    field = str(body.get("field") or "").strip()
    if not field:
        raise HTTPException(400, detail="body must name a field")
    if not any(k in body for k in ("description", "unit", "kind")):
        raise HTTPException(400, detail="body must contain at least one of description, unit, kind")

    kind = body.get("kind")
    if "kind" in body and kind is not None and kind not in KINDS:
        raise HTTPException(400, detail=f"kind {kind!r} is not one of {', '.join(KINDS)}")

    # Fails closed, like every other name in this system. A typo would otherwise sit in the
    # catalogue describing a field nothing produces.
    known = await db.scalar(select(AnalyticsFieldRegistry.id).where(
        AnalyticsFieldRegistry.customer_code == customer,
        AnalyticsFieldRegistry.field == field).limit(1))
    if known is None:
        raise HTTPException(404, detail=f"nothing has ever produced a field called {field!r}, so "
                                        f"there is nothing to describe")

    row = await db.scalar(select(AnalyticsFieldMeaning).where(
        AnalyticsFieldMeaning.customer_code == customer, AnalyticsFieldMeaning.field == field))
    if row is None:
        row = AnalyticsFieldMeaning(customer_code=customer, field=field)
        db.add(row)

    def _text_or_none(value, limit=None):
        if value is None:
            return None
        text_value = str(value).strip()
        return (text_value[:limit] if limit else text_value) or None

    if "description" in body:
        row.description = _text_or_none(body["description"])
    if "unit" in body:
        row.unit = _text_or_none(body["unit"], 32)
    if "kind" in body:
        row.kind = kind
    row.reviewed_at = datetime.now(timezone.utc)
    row.reviewed_by = str(body.get("reviewed_by") or "api")[:128]
    row.updated_at = row.reviewed_at
    await db.commit()
    await db.refresh(row)
    return {"field": row.field, "description": row.description, "unit": row.unit,
            "kind": row.kind, "reviewed_by": row.reviewed_by,
            "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None}


@router.get("/registry/summary")
async def registry_summary(customer: str = Depends(get_current_customer),
                           db: AsyncSession = Depends(get_session)):
    """Counts at a glance for the registry console (chunk 77, section 18w).

    Three blocks, each a handful of single-table indexed counts over small tenant-scoped tables
    (the transaction registry holds tens of rows, the field registry ~1,000, metrics a handful) -
    nothing here can grow with fact volume, so counting at request time is fine where it would not
    be on the fact tables.
    """
    def _count(model, *conds):
        return db.scalar(select(func.count()).select_from(model)
                         .where(model.customer_code == customer, *conds))

    t = AnalyticsTransactionRegistry
    f = AnalyticsFieldRegistry
    m = AnalyticsMetric
    return {
        "transactions": {
            "total": await _count(t) or 0,
            "capture_on": await _count(t, t.capture.is_(True)) or 0,
            "show_on": await _count(t, t.show.is_(True)) or 0,
            "expand_on": await _count(t, t.expand.is_(True)) or 0,
            "needs_review": await _count(t, t.reviewed_at.is_(None)) or 0,
        },
        "fields": {
            "total": await _count(f) or 0,
            "captured": await _count(f, f.captured.is_(True)) or 0,
            "needs_review": await _count(f, f.reviewed_at.is_(None)) or 0,
        },
        "metrics": {
            "total": await _count(m) or 0,
            "active": await _count(m, m.status == "active") or 0,
        },
    }


@router.get("/registry/transactions/{transaction_name}")
async def transaction_registry_detail(transaction_name: str,
                                      customer: str = Depends(get_current_customer),
                                      db: AsyncSession = Depends(get_session)):
    """Everything the registry knows about ONE transaction (chunk 77, section 18w).

    Fields are registered per M3 METHOD while transactions are registered per NAME, and the two are
    many-to-many - so the detail first resolves the name to the methods that actually served it
    (from `log_transactions`, which carries both columns), then lists those methods' fields.

    The fact count is a real `count(*)`, and that is fine HERE for the reason logs.py's day counts
    are fine: one name at a time on a detail page nobody polls, index-only since
    `ix_analytics_facts_customer_txn_event` - not a list endpoint multiplying the cost per row.

    Metrics are read whole (a tenant has a handful; the list endpoint caps at 200) and split in
    Python, matching how metric filters are read everywhere else: `referencing` names this
    transaction in `filter -> transactions`; `apply_to_all` counts metrics whose transaction filter
    is EMPTY, which by the fold's gate means they cover this transaction too. Reported separately
    because "mentions this name" and "covers everything anyway" answer different questions.
    """
    from app.persistence.models.analytics_fact import AnalyticsFact
    from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
    from app.persistence.models.log_transaction import LogTransaction

    row = await db.scalar(
        select(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.customer_code == customer,
            AnalyticsTransactionRegistry.transaction_name == transaction_name))
    if row is None:
        # same rule as the PATCH: a name analytics has never seen is almost always a typo
        raise HTTPException(404, f"analytics has not seen a transaction named "
                                 f"{transaction_name!r} for this logspace")

    methods = list((await db.execute(
        select(LogTransaction.method).distinct()
        .where(LogTransaction.customer_code == customer,
               LogTransaction.transaction_name == transaction_name,
               LogTransaction.method.is_not(None))
        .limit(50))).scalars().all())

    fields = []
    if methods:
        fields = (await db.execute(
            select(AnalyticsFieldRegistry)
            .where(AnalyticsFieldRegistry.customer_code == customer,
                   AnalyticsFieldRegistry.method.in_(methods))
            .order_by(AnalyticsFieldRegistry.method, AnalyticsFieldRegistry.field)
            .limit(500))).scalars().all()

    facts = (await db.execute(
        select(func.count(), func.min(AnalyticsFact.event_time),
               func.max(AnalyticsFact.event_time))
        .where(AnalyticsFact.customer_code == customer,
               AnalyticsFact.transaction_name == transaction_name))).one()

    # 18x: the record grain's volume for this name - what `expand` has actually produced. Same
    # index-only justification as the fact count (ix_analytics_record_facts_customer_txn_event).
    record_count = await db.scalar(
        select(func.count()).select_from(AnalyticsRecordFact)
        .where(AnalyticsRecordFact.customer_code == customer,
               AnalyticsRecordFact.transaction_name == transaction_name)) or 0

    metric_rows = (await db.execute(
        select(AnalyticsMetric).where(AnalyticsMetric.customer_code == customer)
        .order_by(AnalyticsMetric.name).limit(200))).scalars().all()
    referencing = [{"id": str(mr.id), "name": mr.name, "status": mr.status}
                   for mr in metric_rows
                   if transaction_name in ((mr.filter or {}).get("transactions") or [])]
    apply_to_all = sum(1 for mr in metric_rows
                       if not ((mr.filter or {}).get("transactions") or []))

    return {
        "transaction_name": row.transaction_name,
        "capture": row.capture, "show": row.show, "expand": row.expand, "mi": row.mi,
        "first_seen_at": _iso(row.first_seen_at),
        "reviewed_at": _iso(row.reviewed_at), "reviewed_by": row.reviewed_by,
        "needs_review": row.reviewed_at is None,
        "methods": methods,
        "fields": [{
            "id": str(r.id), "method": r.method, "source": r.source, "field": r.field,
            "captured": r.captured,
            "credential_shaped": pl.never_auto_approve(r.field),
            "seeded": pl.seeded(r.field),
            "description": r.description, "unit": r.unit,
            "first_seen_at": _iso(r.first_seen_at), "last_seen_at": _iso(r.last_seen_at),
            "reviewed_at": _iso(r.reviewed_at), "reviewed_by": r.reviewed_by,
            "needs_review": r.reviewed_at is None,
        } for r in fields],
        "field_count": len(fields),
        "facts": {"count": facts[0] or 0,
                  "first_event_at": _iso(facts[1]), "last_event_at": _iso(facts[2])},
        "records": {"count": record_count},
        "metrics": {"referencing": referencing, "apply_to_all": apply_to_all},
    }


async def _looked_up_fields(db, customer: str, transaction_name: str, since) -> list[dict]:
    """Chunk 105: the fifth source. What this transaction can reach through a declared lookup.

    A fact records one exchange, and the composition card could only ever describe that. A pick
    carries its delivery number and no customer name, and the name is one hop away - which is the
    exact limit lookups exist to lift, so this is where somebody realises they want one.

    Reported with how far it actually reaches, per attribute. Measured live, the customer name is
    available on 1,528 of 1,530 picks; the two that are not are real, and a card claiming full
    coverage would be the more useful-looking lie.

    A lookup is not DECLARED here. One delivery lookup serves picking, packing and routing, so it
    cannot belong to any one of them; this says what is reachable and the section manages them once.
    """
    lookups = await lookup_store.load(db, customer)
    if not lookups:
        return []
    out: list[dict] = []
    for lookup in sorted(lookups.values(), key=lambda x: x.name):
        key = lookup.key_field
        # The key is either an `attr:` path or a typed column, and a typed column name reaches SQL,
        # so it is checked against the contract rather than interpolated on trust.
        if contract.is_attr_path(key):
            column, params = "f.attributes ->> :attr", {"attr": contract.attr_key(key)}
        elif key in contract.FACT_FIELDS:
            column, params = f"f.{key}", {}
        else:
            continue          # a declaration naming something the fact row has not got
        for attribute in sorted(lookup.attributes, key=lambda a: a.name):
            row = (await db.execute(text(f"""
                SELECT count(*) FILTER (WHERE k IS NOT NULL) AS with_key,
                       count(*) FILTER (WHERE v.value IS NOT NULL) AS resolved
                FROM (SELECT {column} AS k FROM analytics_facts f
                      WHERE f.customer_code = :c AND f.transaction_name = :n
                        AND f.event_time >= :since) s
                LEFT JOIN analytics_lookup_values v
                  ON v.customer_code = :c AND v.lookup = :lookup AND v.attribute = :attribute
                 AND v.key = s.k"""),
                {"c": customer, "n": transaction_name, "since": since, "lookup": lookup.name,
                 "attribute": attribute.name, **params})).one()
            with_key, resolved = int(row[0] or 0), int(row[1] or 0)
            if not with_key:
                continue      # not reachable from this transaction at all
            out.append({
                "field": lookup_model.path(lookup.name, attribute.name),
                "lookup": lookup.name,
                "attribute": attribute.name,
                "key_field": lookup.key_field,
                "facts_with_key": with_key,
                "facts_resolved": resolved,
                "percent_resolved": round(100.0 * resolved / with_key, 1),
                "stable": attribute.stable,
            })
    return out


#: Registry `source` to the card it belongs on. Explicit rather than inferred from the name's prefix:
#: a bare name means "sent by the handheld" and inferring it as a response is the chunk 104 defect.
_SOURCE_GROUP = {"request": "request", "response": "response", "mi_result": "mi", "record": "record"}


#: The fewest facts that can tell a slice from an identifier.
#:
#: With two records a field holding one value looks constant and a field holding two looks unique,
#: and neither reading is earned. Three is the fewest that separates them, because two values over
#: three records means one of them genuinely repeats. Found on the live tenant, where a transaction
#: with exactly two facts had all thirty of its fields marked useless.
_MIN_FACTS_TO_JUDGE = 3

#: How close to one value per record a field may get before it is called an identifier rather than
#: a slice. Not 1.0: a handful of accidental repeats does not turn a request id into a category.
_IDENTIFIER_SHARE = 0.95


def _worth_grouping_by(facts: int, different: int) -> bool | None:
    """Whether a field is worth offering as a slice, or None when there is not enough to say.

    Two ways to be useless and they look nothing alike. One value on every record groups everything
    into a single row; a different value on every record groups nothing at all, which is the
    pathological case the summaries exist to avoid. Both are common: of the 77 request fields on a
    live pick, 45 are one or the other.

    None rather than False in the two cases where the answer is not earned: no recent fact carries
    the field, or too few do to tell the two failure modes apart. Absent is not zero, and calling a
    field useless on the strength of no evidence is the same mistake in a different coat.
    """
    if facts < _MIN_FACTS_TO_JUDGE:
        return None
    if different <= 1:
        return False
    # NEARLY one per record is one per record. "Strictly fewer" was the first rule and it was too
    # tight: `ReqId` held 9,292 different values over 9,293 live facts, one accidental repeat in
    # nine thousand, and duly read as worth slicing by at the top of the card it exists to push
    # down. The same threshold the lookup check uses, for the same reason.
    return different < facts * _IDENTIFIER_SHARE


@router.get("/registry/transactions/{transaction_name}/composition")
async def transaction_composition(transaction_name: str,
                                  customer: str = Depends(get_current_customer),
                                  db: AsyncSession = Depends(get_session)):
    """What a fact of this transaction is MADE OF (chunk 94): the newest fact's attributes grouped into
    request, response and MI; the kinds of M3 call the newest transaction made, read from its own
    timeline so they are visible before anyone switches MI on; the four switches; the field approvals
    per source; and how many facts carry a bare response value or MI detail.

    Bounded by construction: one fact, one transaction's entries through the assignment index, three
    counts on the tenant+name index. Bookkeeping `__` keys are never shown.
    """
    from sqlalchemy import String, cast
    from app.persistence.models.analytics_fact import AnalyticsFact
    from app.persistence.models.log_entry import LogEntry, LogEntryType
    from app.persistence.models.log_entry_assignment import LogEntryAssignment
    from app.persistence.models.log_transaction import LogTransaction
    from app.services.analytics import payload as pl
    row = await db.scalar(
        select(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.customer_code == customer,
            AnalyticsTransactionRegistry.transaction_name == transaction_name))
    if row is None:
        raise HTTPException(404, f"analytics has not seen a transaction named "
                                 f"{transaction_name!r} for this logspace")

    newest = (await db.execute(
        select(AnalyticsFact).where(AnalyticsFact.customer_code == customer,
                                    AnalyticsFact.transaction_name == transaction_name)
        .order_by(AnalyticsFact.event_time.desc().nulls_last()).limit(1))).scalar_one_or_none()
    sample = None
    if newest is not None:
        request, response, mi = {}, {}, {}
        for key, value in sorted((newest.attributes or {}).items()):
            if key.startswith("__"):
                continue
            if key.startswith(pl.RESPONSE_PREFIX):
                response[key] = value
            elif key.startswith(pl.MI_PREFIX):
                parts = key.split(".")
                if len(parts) == 4:
                    mi.setdefault(f"{parts[1]}.{parts[2]}", {})[parts[3]] = value
                else:
                    mi.setdefault("_legacy", {})[key] = value
            else:
                request[key] = value
        sample = {"fact_id": str(newest.id), "event_time": _iso(newest.event_time),
                  "method": newest.method, "status": newest.status,
                  "quantity": None if newest.quantity is None else str(newest.quantity),
                  "request": request, "response": response, "mi": mi}

    # The kinds of MI call, from the NEWEST transaction's own timeline: what the transaction does,
    # independent of whether the fact records it. One transaction, through the assignment index.
    mi_kinds: list[dict] = []
    latest_txn = (await db.execute(
        select(LogTransaction.id).where(LogTransaction.customer_code == customer,
                                        LogTransaction.transaction_name == transaction_name)
        .order_by(LogTransaction.started_at.desc().nulls_last()).limit(1))).scalar_one_or_none()
    if latest_txn is not None:
        entries = (await db.execute(
            select(LogEntry.fields).join(LogEntryAssignment, LogEntryAssignment.entry_id == LogEntry.id)
            .where(LogEntryAssignment.transaction_id == latest_txn,
                   LogEntry.entry_type == LogEntryType.mi_result)
            .order_by(LogEntryAssignment.seq))).scalars().all()
        seen: dict[tuple, dict] = {}
        for fields in entries:
            if not isinstance(fields, dict):
                continue
            key = (fields.get("program") or "_", fields.get("transaction") or "_")
            k = seen.setdefault(key, {"program": key[0], "transaction": key[1], "calls": 0,
                                      "records": 0, "errors": 0})
            k["calls"] += 1
            records = fields.get("records")
            k["records"] += len(records) if isinstance(records, list) else 0
            if fields.get("result") not in (None, "OK"):
                k["errors"] += 1
        mi_kinds = list(seen.values())

    total, with_value, with_mi = (await db.execute(
        select(func.count(),
               func.count().filter(AnalyticsFact.attributes.has_key(pl.BARE_RESPONSE_FIELD)),
               func.count().filter(cast(AnalyticsFact.attributes, String).like('%"mi.%')))
        .where(AnalyticsFact.customer_code == customer,
               AnalyticsFact.transaction_name == transaction_name))).one()

    # How often each response or MI key actually appears on RECENT facts of this name. The registry
    # says a field exists for a method; only the facts say whether it is regular or a one-off. On
    # tmp-live 47 of 48 response fields under ConfirmPickLine had been seen exactly once, from a
    # foreign response object stitched into a pick, and the card showed them as if they were normal.
    # One aggregate over the name's last two weeks of facts, on the tenant+name+event index.
    recent_days = 14
    since = datetime.now(timezone.utc) - timedelta(days=recent_days)
    from sqlalchemy import text as _text
    # Per METHOD and key, not per name: a name is served by many methods and a field regular on one
    # is a one-off on another. Under ConfirmPickLine, "resp.ItemNumber on 19,198 of 43,020" was the
    # delivery lookups' count dressed as the picks'; the picks carried it on 67.
    # Chunk 104: every key, not only the `resp.` and `mi.` ones. The request half is on the fact too
    # and now has a card of its own, so it needs the same frequency the other cards already had.
    #
    # `different` comes back beside the count because it is the one signal that separates a useful
    # slice from protocol noise. Measured live: of the 46 request fields on a pick, 24 hold the SAME
    # value on every record of the tenant (the server address, the port, the company, the division)
    # and a few hold a different one on every single record (`ReqId`, `StartDateTime`). Both are
    # useless to group by, and a card that merely listed them would bury the dozen that are not.
    # GROUPING SETS so ONE scan answers both questions, which are not the same question.
    #
    # Frequency is per METHOD: a field regular on the delivery lookup can be a one-off on the pick,
    # and only the per-method figure means anything under a method group.
    #
    # Variety is per TRANSACTION, and the first version got this wrong in a way that defeated the
    # whole ranking. It counted distinct values per method and ADDED them up, so `ApiPort`, holding
    # one value on each of a dozen methods, summed to twelve and read as worth slicing by. Checked
    # against the live tenant, that put the server address, the port, the company, the division and
    # the locale at the top of every card - exactly the noise the ranking exists to push down.
    recent = (await db.execute(_text("""
        SELECT f.method, kv.key, count(*), count(DISTINCT kv.value)
        FROM analytics_facts f, jsonb_each(f.attributes) kv
        WHERE f.customer_code = :c AND f.transaction_name = :n AND f.event_time >= :since
          AND left(kv.key, 2) <> :bookkeeping
        GROUP BY GROUPING SETS ((f.method, kv.key), (kv.key))"""),
        {"c": customer, "n": transaction_name, "since": since,
         "bookkeeping": pl.BOOKKEEPING_PREFIX})).all()
    recent_by_key: dict[str, dict[str, int]] = {}
    #: Per field name over the whole transaction: how many facts carried it, and how many DIFFERENT
    #: values it held. The row with a NULL method is the transaction-wide one.
    variety: dict[str, tuple[int, int]] = {}
    for method, k, n, different in recent:
        if method is None:
            variety[k] = (n, different)
        else:
            recent_by_key.setdefault(k, {})[method] = n
    recent_by_method = {(m or "(none)"): n for m, n in (await db.execute(
        select(AnalyticsFact.method, func.count())
        .where(AnalyticsFact.customer_code == customer,
               AnalyticsFact.transaction_name == transaction_name,
               AnalyticsFact.event_time >= since)
        .group_by(AnalyticsFact.method))).all()}
    recent_total = sum(recent_by_method.values())

    methods = list((await db.execute(
        select(LogTransaction.method).distinct()
        .where(LogTransaction.customer_code == customer,
               LogTransaction.transaction_name == transaction_name,
               LogTransaction.method.is_not(None)).limit(50))).scalars().all())
    # One entry per FIELD NAME, not per registry row. Rows are per M3 method, and a name served by
    # eleven methods has the same response field registered eleven times - which is what the first
    # version of these cards showed (`resp.AccessToken` x11 on Brighton Stock Pick). Approval is by
    # name across methods (`capture.approved_attributes`), so `captured` is true if ANY row is, and
    # every row id is carried so the screen can flip them all together.
    fields: dict[str, list] = {"request": [], "response": [], "mi": [], "record": [],
                              "looked_up": []}
    meanings = await _meanings_for(db, customer)
    if methods:
        grouped: dict[tuple[str, str], dict] = {}
        for r in (await db.execute(
                select(AnalyticsFieldRegistry)
                .where(AnalyticsFieldRegistry.customer_code == customer,
                       AnalyticsFieldRegistry.method.in_(methods))
                .order_by(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.method)
                .limit(2000))).scalars().all():
            # Chunk 104: read off the stored source rather than inferred. The old spelling asked
            # "record? MI? otherwise response", which was right until chunk 99 began registering the
            # request half - those rows then fell through the last branch, and 77 of them landed
            # among 36 genuine response names on one card of the live tenant.
            group = _SOURCE_GROUP.get(r.source, "response")
            meaning = meanings.get(r.field)
            entry = grouped.setdefault((group, r.field), {
                "id": str(r.id), "ids": [], "methods": [], "field": r.field, "captured": False,
                # Chunk 108: read from the NAME, not from this method's row, so one sentence
                # covers every method that carries it.
                "description": meaning.description if meaning else None,
                "unit": meaning.unit if meaning else None,
                "kind": meaning.kind if meaning else None,
                "source": r.source,
                # What the handheld sent is on the fact whatever anybody ticks, so the tick decides
                # whether the field may be REPORTED on rather than whether it is stored. The two are
                # the same box with two meanings and the screen has to be able to say which.
                "stored_regardless": group == "request",
                # Credential-shaped names are recorded by name and never approved by default; the
                # screen says so instead of offering them as ordinary fields.
                "credential": pl.never_auto_approve(r.field),
                # Registry sightings per method. A fact count only measures APPROVED fields (an
                # unapproved one is never stored), so for the rest this is the truthful number: how
                # many responses showed the name and when it was last seen.
                "seen_by_method": {}})
            entry["ids"].append(str(r.id))
            entry["methods"].append(r.method)
            entry["seen_by_method"][r.method] = {
                "count": int(r.seen_count or 0),
                "last_seen_at": r.last_seen_at.isoformat() if r.last_seen_at else None}
            entry["captured"] = entry["captured"] or bool(r.captured)
        for (group, field), entry in grouped.items():
            # record fields live on record rows, not facts; their frequency is not measured here
            per_method = {} if group == "record" else recent_by_key.get(field, {})
            entry["recent_by_method"] = per_method
            entry["recent_facts"] = None if group == "record" else sum(per_method.values())
            facts_seen, different = variety.get(field, (0, 0)) if group != "record" else (0, 0)
            entry["distinct_values"] = different or None
            entry["useful"] = _worth_grouping_by(facts_seen, different)
            fields[group].append(entry)

    fields["looked_up"] = await _looked_up_fields(db, customer, transaction_name, since)

    return {
        "transaction_name": transaction_name,
        "switches": {"capture": row.capture, "show": row.show, "expand": row.expand, "mi": row.mi},
        "methods": methods,
        "sample": sample,
        "mi_kinds": mi_kinds,
        "counts": {"facts": total, "with_resp_value": with_value, "with_mi": with_mi,
                   "recent_facts": recent_total, "recent_days": recent_days,
                   "recent_by_method": recent_by_method},
        "fields": fields,
    }

# ============================================================== chunk 117: settlements
#
# A settlement turns the many call rows that share a key into one row, by rules a person writes. It
# exists because a pick-list release is confirmed in several calls and its expected quantity is
# stamped on every one: release 540551 picked 9 and summed to 17, and across 6,160 releases the
# shortfall read -5,576 summed every call and -4,344 settled. The rules and the measurements are at
# the top of `app/services/analytics/settle.py`. Nothing about picking is coded here.

def _settlement_json(row, rows_count: int | None = None) -> dict:
    out = {"id": str(row.id), "name": row.name, "description": row.description,
           "enabled": row.enabled, "created_by": row.created_by,
           "updated_at": row.updated_at.isoformat() if row.updated_at else None,
           **(row.definition or {})}
    if rows_count is not None:
        out["rows"] = rows_count
    return out


def _settlement_from_payload(name: str, body: dict) -> settle_model.Settlement:
    try:
        return settle_store.from_json(name, body)
    except (ValueError, TypeError, KeyError) as exc:
        raise HTTPException(400, detail=str(exc)) from None


async def _settlement_row(db, customer: str, name: str) -> AnalyticsSettlement:
    row = await db.scalar(select(AnalyticsSettlement).where(
        AnalyticsSettlement.customer_code == customer, AnalyticsSettlement.name == name))
    if row is None:
        raise HTTPException(404, detail=f"no settlement called {name!r} for this logspace")
    return row


async def _validate_settlement(db, customer: str, declared: settle_model.Settlement) -> None:
    """The pure rules, plus the one thing they cannot know: which attributes this tenant approved."""
    problems = settle_model.validate(declared)
    known = await capture.approved_attributes(db, customer)
    named = list(declared.key) + list(declared.carry) + [v.field for v in declared.values if v.field]
    for field in named:
        if contract.is_attr_path(field) and contract.attr_key(field) not in known:
            problems.append(f"{field!r} is not an approved attribute: tick it in fact composition first")
        elif not contract.is_attr_path(field) and field not in contract.FACT_FIELDS \
                and field not in ("event_time", "business_date"):
            problems.append(f"{field!r} is not a field on the fact row")
    if problems:
        raise HTTPException(400, detail=problems)


@router.get("/settlements")
async def list_settlements(customer: str = Depends(get_current_customer),
                           db: AsyncSession = Depends(get_session)):
    """Every declared settlement, enabled or not, with how many rows each holds."""
    rows = (await db.execute(select(AnalyticsSettlement).where(
        AnalyticsSettlement.customer_code == customer).order_by(AnalyticsSettlement.name))).scalars().all()
    counts = dict((await db.execute(
        select(AnalyticsSettledRow.settlement, func.count())
        .where(AnalyticsSettledRow.customer_code == customer)
        .group_by(AnalyticsSettledRow.settlement))).all())
    return {"settlements": [_settlement_json(r, counts.get(r.name, 0)) for r in rows],
            "rules": [r.value for r in settle_model.Rule]}


@router.post("/settlements", status_code=201)
async def create_settlement(body: dict = Body(...),
                            backfill: bool = Query(True, description="Settle every existing key now"),
                            customer: str = Depends(get_current_customer),
                            db: AsyncSession = Depends(get_session)):
    """Declare a settlement and, by default, settle every key the facts already hold, so the rows
    exist the moment the declaration does rather than trickling in with the next fold."""
    name = str(body.get("name") or "").strip()
    if not name or ":" in name or "." in name:
        raise HTTPException(400, detail="a settlement needs a name without ':' or '.'")
    declared = _settlement_from_payload(name, body)
    await _validate_settlement(db, customer, declared)
    exists = await db.scalar(select(AnalyticsSettlement.id).where(
        AnalyticsSettlement.customer_code == customer, AnalyticsSettlement.name == name))
    if exists:
        raise HTTPException(409, detail=f"a settlement called {name!r} already exists")
    row = AnalyticsSettlement(customer_code=customer, name=name,
                              description=(str(body.get("description") or "").strip() or None),
                              definition=settle_store.to_json(declared),
                              enabled=bool(body.get("enabled", True)),
                              created_by=str(body.get("created_by") or "api"))
    db.add(row)
    written = 0
    if backfill and row.enabled:
        written = await settle_store.resettle_all(db, customer, declared)
    await db.commit()
    await db.refresh(row)
    return {**_settlement_json(row, written), "detail": f"settled {written} key(s)"}


@router.patch("/settlements/{name}")
async def update_settlement(name: str, body: dict = Body(...),
                            customer: str = Depends(get_current_customer),
                            db: AsyncSession = Depends(get_session)):
    """Change a declaration. A changed rule set makes every existing row wrong, so the rows are
    rebuilt from scratch under the new rules; a change to the description or the switch is not."""
    row = await _settlement_row(db, customer, name)
    rebuilt = None
    shape = {k: body[k] for k in ("reads", "key", "carry", "values") if k in body}
    if shape:
        declared = _settlement_from_payload(name, {**(row.definition or {}), **shape})
        await _validate_settlement(db, customer, declared)
        row.definition = settle_store.to_json(declared)
        await db.execute(delete(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == customer, AnalyticsSettledRow.settlement == name))
        rebuilt = await settle_store.resettle_all(db, customer, declared) if row.enabled else 0
    if "enabled" in body:
        row.enabled = bool(body["enabled"])
    if "description" in body:
        row.description = (str(body["description"]).strip() or None
                           if body["description"] is not None else None)
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(row)
    out = _settlement_json(row)
    if rebuilt is not None:
        out["detail"] = f"rules changed; rebuilt {rebuilt} row(s)"
    return out


@router.get("/settlements/{name}/preview")
async def preview_settlement_key(name: str, key: list[str] = Query(...),
                                 customer: str = Depends(get_current_customer),
                                 db: AsyncSession = Depends(get_session)):
    """One key: its calls on one side and the row they settle to on the other, computed live from
    the calls so it is right before any fold has run. This is how somebody checks a rule."""
    row = await _settlement_row(db, customer, name)
    declared = settle_store.from_json(name, row.definition or {})
    if len(key) != len(declared.key):
        raise HTTPException(400, detail=f"this settlement's key has {len(declared.key)} part(s): "
                                        f"{list(declared.key)}")
    calls, settled = await settle_store.read_key(db, customer, declared, tuple(key))
    shown = []
    for c in calls:
        entry = {"event_time": c["event_time"].isoformat() if c.get("event_time") else None,
                 "status": c.get("status"), "classification": c.get("quantity_classification")}
        for v in declared.values:
            if v.field and contract.is_attr_path(v.field):
                entry[contract.attr_key(v.field)] = (c.get("attributes") or {}).get(contract.attr_key(v.field))
        shown.append(entry)
    return {"settlement": name, "key": list(key), "calls": shown,
            "settled": None if settled is None else {
                "event_time": settled.event_time.isoformat() if settled.event_time else None,
                "carried": {k: settle_store._stringify(v) for k, v in settled.carried.items()},
                "values": {k: settle_store._stringify(v) for k, v in settled.values.items()},
                "calls": settled.calls}}


@router.get("/settlements/{name}/list")
async def list_settlement_rows(name: str,
                               start: datetime | None = Query(default=None),
                               end: datetime | None = Query(default=None),
                               search: str | None = Query(default=None),
                               limit: int = Query(100, ge=1, le=1000),
                               offset: int = Query(0, ge=0),
                               customer: str = Depends(get_current_customer),
                               db: AsyncSession = Depends(get_session)):
    """The settled rows themselves, newest first. A grouped read answers "how much"; this answers
    "show me the rows", which is the other half of seeing the data."""
    row = await _settlement_row(db, customer, name)
    declared = settle_store.from_json(name, row.definition or {})
    rows, total = await settle_store.list_rows(db, customer, declared, since=start, until=end,
                                               search=search, limit=limit, offset=offset)
    return {"settlement": name, "key": list(declared.key), "carry": list(declared.carry),
            "values": [v.name for v in declared.values],
            "rows": rows, "total": total, "limit": limit, "offset": offset}


@router.get("/settlements/{name}/rows")
async def read_settlement_rows(name: str,
                               group_by: list[str] = Query(default=[]),
                               start: datetime | None = Query(default=None),
                               end: datetime | None = Query(default=None),
                               limit: int = Query(500, ge=1, le=50000),
                               customer: str = Depends(get_current_customer),
                               db: AsyncSession = Depends(get_session)):
    """Settled rows grouped and summed on request, with `lookup:` paths resolved exactly as they are
    for a metric: the rows are grouped by the lookup's KEY and re-labelled afterwards. No roll-up
    stands between the reader and the rows; one row per release is already the aggregation."""
    row = await _settlement_row(db, customer, name)
    declared = settle_store.from_json(name, row.definition or {})
    lookups = await lookup_store.load(db, customer)
    try:
        translation = lookup_model.plan(tuple(group_by), lookups)
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None
    grouped = await settle_store.read_grouped(db, customer, declared,
                                              group_by=translation.stored_group_by,
                                              since=start, until=end, limit=limit)
    numeric = [k for k in (grouped[0].keys() if grouped else []) if k not in ("dimensions",)]
    # `translate` re-keys `(instant, dims)` pairs and reads each lookup as at that instant, because
    # a metric's points are time buckets. A grouped read over settled rows has one instant: the end
    # of the window, or now, so a customer's name is the one it has as things stand.
    as_at = end or datetime.now(timezone.utc)
    points = {(as_at, tuple(g["dimensions"])): {k: g[k] for k in numeric} for g in grouped}
    if translation.translates and points:
        resolver = await lookup_store.resolver(
            db, customer, translation.keys_needed(points),
            tuple(step for step in translation.steps if step is not None))

        def _merge(a: dict, b: dict) -> dict:
            """Two groups that resolve to one label: counts add as ints, settled values add as the
            strings they are stored as, and an absent side contributes nothing."""
            out = {}
            for k in set(a) | set(b):
                x, y = a.get(k), b.get(k)
                if x is None or y is None:
                    out[k] = x if y is None else y
                elif isinstance(x, int) and isinstance(y, int):
                    out[k] = x + y
                else:
                    out[k] = format((Decimal(str(x)) + Decimal(str(y))).normalize(), "f")
            return out
        points = lookup_model.translate(points, translation, resolver, merge=_merge)
    # Grouped by its own key a settlement has as many groups as rows - 6,252 on the live tenant -
    # and a silent cap would drop whole releases from the bottom of a drill-down. So the cap is
    # high, and hitting it is reported rather than hidden.
    return {"settlement": name, "group_by": list(group_by),
            "values": [v.name for v in declared.values],
            "truncated": len(grouped) >= limit,
            "rows": [{"dimensions": [d.replace(settle_store.KEY_SEP, " · ") if isinstance(d, str) else d
                                     for d in dims], **v} for (_at, dims), v in points.items()]}
