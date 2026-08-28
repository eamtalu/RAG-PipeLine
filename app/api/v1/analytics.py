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
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.api.deps import get_current_customer
from app.config.database import get_session
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.services.analytics import capture
from app.services.analytics import payload as pl
from app.services.analytics import pending_windows
from app.services.analytics import definition as d
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
        "id": str(r.id), "name": r.name, "status": r.status,
        "dimensions": r.dimensions, "grains": r.grains,
        "measures": [m.get("name") for m in (r.measures or [])],
        "filter": r.filter,
        # NULL means no history has been built, which after D8 is the permanent state for every metric.
        "backfilled_through": _iso(r.backfilled_through),
        "created_by": r.created_by,
    } for r in rows]}


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
    try:
        measures = tuple(registry.measure_from_json(m) for m in (payload.get("measures") or ()))
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(400, detail=f"malformed measure: {exc}") from None

    definition = d.MetricDefinition(
        name=(payload.get("name") or "").strip(),
        dimensions=tuple(payload.get("dimensions") or ()),
        measures=measures,
        grains=tuple(payload.get("grains") or ("hourly", "daily", "monthly")),
        method_filter=tuple((payload.get("filter") or {}).get("methods") or ()),
        # R1: accepted here so the interface can write a per-transaction metric without a deploy.
        transaction_filter=tuple((payload.get("filter") or {}).get("transactions") or ()),
        status=d.Status(payload.get("status") or d.Status.draft.value),
    )
    if not definition.name:
        raise HTTPException(400, detail="`name` is required.")
    if not definition.measures:
        raise HTTPException(400, detail="at least one measure is required.")

    # R1b. The field registry decides which `attr:` paths are usable, so a metric naming an
    # unapproved or misspelled attribute is refused HERE, at save time, with a message naming the
    # field - rather than being accepted and producing a silently empty chart.
    problems = d.validate(definition,
                          known_attributes=await capture.approved_attributes(db, customer))
    if problems:
        raise HTTPException(400, detail=problems)

    existing = await db.scalar(select(AnalyticsMetric.id).where(
        AnalyticsMetric.customer_code == customer, AnalyticsMetric.name == definition.name))
    if existing is not None:
        raise HTTPException(409, detail=f"a metric named {definition.name!r} already exists here.")

    row = AnalyticsMetric(**registry.to_row(definition, customer_code=customer,
                                            created_by=payload.get("created_by") or "api"))
    db.add(row)
    await db.commit()
    return {"id": str(row.id), "name": row.name, "status": row.status,
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
    try:
        decision = n6.resolve(definition, group_by=dims)
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None

    state = await _state(db, customer)
    out = await n6.series(db, customer, definition_id, definition, window=window, measure=measure,
                          group_by=dims,
                          watermark=state.analytics_watermark if state else None)
    return {**out, "metric": metric, "ad_hoc": decision.ad_hoc, "resolution": decision.reason,
            "window": {"start": window.start.isoformat(), "end": window.end.isoformat()}}


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
    try:
        decision = n6.resolve(definition, group_by=(dimension,))
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from None

    state = await _state(db, customer)
    out = await n6.series(db, customer, definition_id, definition, window=window, measure=measure,
                          group_by=(dimension,),
                          watermark=state.analytics_watermark if state else None)

    totals: dict = {}
    for point in out["points"]:
        key = point["dimensions"][0] if point["dimensions"] else None
        roles = point["roles"]
        bucket = totals.setdefault(key, {"sum_value": 0, "count_value": 0})
        from decimal import Decimal as _D
        bucket["sum_value"] = str(_D(str(bucket["sum_value"])) + _D(roles.get("sum_value", "0")))
        bucket["count_value"] += roles.get("count_value", 0) or 0

    from decimal import Decimal as _D
    ranked = sorted(totals.items(), key=lambda kv: -abs(_D(str(kv[1]["sum_value"]))))[:top]
    return {"metric": metric, "measure": measure, "dimension": dimension, "grain": out["grain"],
            "ad_hoc": decision.ad_hoc, "resolution": decision.reason,
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
        "capture": r.capture, "show": r.show, "expand": r.expand,
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

    before = (row.capture, row.show, row.expand)
    for field in ("capture", "show", "expand"):
        if field in payload:
            setattr(row, field, bool(payload[field]))
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
    if turned_capture_on or show_changed or turned_expand_on:
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
            published = await pending_windows.publish(
                db, customer,
                lo=history or (frontier - timedelta(days=settings.log_partition_retention_days)),
                hi=frontier)
    await db.commit()

    return {"transaction_name": transaction_name, "capture": row.capture, "show": row.show,
            "expand": row.expand, "tickets_published": published,
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
    if "captured" not in payload:
        raise HTTPException(400, "body must contain 'captured'")

    was = row.captured
    row.captured = bool(payload["captured"])
    row.reviewed_at = datetime.now(timezone.utc)
    row.reviewed_by = payload.get("reviewed_by") or "api"
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
            "tickets_published": published}


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
        "capture": row.capture, "show": row.show, "expand": row.expand,
        "first_seen_at": _iso(row.first_seen_at),
        "reviewed_at": _iso(row.reviewed_at), "reviewed_by": row.reviewed_by,
        "needs_review": row.reviewed_at is None,
        "methods": methods,
        "fields": [{
            "id": str(r.id), "method": r.method, "source": r.source, "field": r.field,
            "captured": r.captured,
            "credential_shaped": pl.never_auto_approve(r.field),
            "seeded": pl.seeded(r.field),
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
