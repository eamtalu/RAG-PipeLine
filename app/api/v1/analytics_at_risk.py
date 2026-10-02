"""Deliveries at risk as a screen sees it (chunk 148).

    GET    /analytics/at-risk/board                       the open deliveries for today, tomorrow or both, worst first
    POST   /analytics/at-risk/deliveries/{n}/check        a person's word that someone has looked at it
    DELETE /analytics/at-risk/deliveries/{n}/check        take that word back
    GET    /analytics/at-risk/history                     closed rows between two departure dates, keyset paged
    GET    /analytics/at-risk/checks                      the acknowledgement ledger, newest first
    GET    /analytics/at-risk/accuracy                    flags scored against outcomes: precision and recall
    GET    /analytics/at-risk/settings  PUT               the tenant's floors and knobs
    GET    /analytics/at-risk/routes                      what each route's history taught, with the effective threshold
    GET    /analytics/at-risk/status                      one row for a page header, with readiness flags
    POST   /analytics/at-risk/evaluate                    run one pass now (sub-second; no 202 needed)

Every read is bounded and reads STORED rows only: the worker computes, the API shows. Measures are
decimal strings as the settled reads return them, counts are integers, instants are ISO 8601 with
their offset.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_customer
from app.config.database import get_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery, OUTCOMES, TIERS
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_settlement import AnalyticsSettlement
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import lookup_store
from app.services.analytics_at_risk import (RULE_VERSION, check_store, delivery_store, model, profile_store, runner,
                                            settings_store, state_store)
from app.settings import settings

router = APIRouter(prefix="/analytics/at-risk", tags=["analytics-at-risk"])

WINDOWS = ("today", "tomorrow", "both")
BOARD_MAX = 1000
HISTORY_MAX = 500
HISTORY_SPAN_MAX = timedelta(days=180)
ACCURACY_DAYS_MAX = 90
#: The board is stale once the worker has missed this many polls.
STALE_POLLS = 3
DEPARTURE_FIELDS = ("resp.DeparatureDate", "resp.DeparatureTime")
ROUTING_METHOD = "GetNextDeliveryByRoute"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _s(value) -> str | None:
    return None if value is None else format(Decimal(str(value)).normalize(), "f")


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _check(name: str, value: str | None, allowed: tuple[str, ...]) -> str | None:
    if value is not None and value not in allowed:
        raise HTTPException(422, detail=f"{name} must be one of {', '.join(allowed)}, not {value!r}")
    return value


def _date(name: str, value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(422, detail=f"{name} must be YYYY-MM-DD")


async def _tz(db: AsyncSession, cc: str) -> ZoneInfo:
    return ZoneInfo(await get_customer_timezone(db, cc))


def _row_json(row: AnalyticsAtRiskDelivery, now: datetime) -> dict:
    check = None
    if row.checked_at is not None:
        check = {"checked_at": _iso(row.checked_at), "checked_by": row.checked_by, "note": row.check_note,
                 "tier": row.checked_tier, "reopened_count": int(row.reopened_count or 0)}
    reopened = check is not None and model.TIER_RANK[model.Tier(row.tier)] > model.TIER_RANK[model.Tier(row.checked_tier or "none")]
    return {
        "delivery_number": row.delivery_number, "route": row.route, "customer_name": row.customer_name,
        "customer_number": row.customer_number, "departure_at": _iso(row.departure_at),
        "departure_date": row.departure_date.isoformat(),
        "minutes_to_departure": _s(model.minutes_to_departure(row.departure_at, now).quantize(Decimal("0.01"))),
        "tier": row.tier, "max_tier": row.max_tier, "first_flagged_at": _iso(row.first_flagged_at),
        "first_flagged_tier": row.first_flagged_tier,
        "threshold": {"load_min": _s(row.load_threshold_min), "load_source": row.load_threshold_source,
                      "pick_min": _s(row.pick_threshold_min), "pick_source": row.pick_threshold_source},
        "lines": {"expected": row.lines_expected, "confirmed": int(row.lines_confirmed or 0),
                  "picked": int(row.lines_picked or 0), "short": int(row.lines_short or 0)},
        "packages": {"created": int(row.packages_created or 0), "loaded": int(row.packages_loaded or 0)},
        "last_pick_at": _iso(row.last_pick_at), "last_load_at": _iso(row.last_load_at), "route_loaded_at": _iso(row.route_loaded_at),
        "loading_expected": True if row.loading_expected is None else bool(row.loading_expected),
        "transaction_names": list(row.transaction_names or []),
        "category": model.category_for(outcome=row.outcome, max_tier=row.max_tier, lines_expected=row.lines_expected,
                                       lines_picked=int(row.lines_picked or 0)),
        "status": row.status, "closed_at": _iso(row.closed_at), "outcome": row.outcome,
        "outcome_lead_min": _s(row.outcome_lead_min), "check": check, "reopened": reopened,
        "tier_history": list(row.tier_history or []), "rule_version": row.rule_version,
    }


# ============================================================== board

@router.get("/board")
async def read_board(window: str = "both", tier: str | None = None, include_closed: bool = False, limit: int = 500,
                     customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """The open deliveries departing today, tomorrow or both, worst tier first then nearest departure.
    `stale` says the worker has stopped writing, so a quiet board is not mistaken for a calm one."""
    _check("window", window, WINDOWS)
    _check("tier", tier, TIERS)
    tz = await _tz(db, customer)
    now = _now()
    today = now.astimezone(tz).date()
    dates = {"today": [today], "tomorrow": [today + timedelta(days=1)], "both": [today, today + timedelta(days=1)]}[window]
    if window != "tomorrow":
        dates = [today - timedelta(days=1), *dates]  # yesterday's departure still open is the late one
    rows = await delivery_store.board_rows(db, customer, dates=dates, tiers=[tier] if tier else None,
                                           include_closed=include_closed, limit=min(max(limit, 1), BOARD_MAX))
    cfg = await settings_store.effective(db, customer)
    state = await state_store.get(db, customer)
    evaluated_at = state.last_evaluated_at if state else None
    stale = evaluated_at is None or (now - evaluated_at) > timedelta(seconds=settings.analytics_at_risk_poll_seconds * STALE_POLLS)
    open_rows = [r for r in rows if r.status == "open"]
    counts = {"open": len(open_rows), "watch": sum(r.tier == "watch" for r in open_rows),
              "at_risk": sum(r.tier == "at_risk" for r in open_rows), "late": sum(r.tier == "late" for r in open_rows),
              "checked": sum(r.checked_at is not None for r in open_rows)}
    return {"timezone": tz.key, "now": now.isoformat(), "evaluated_at": _iso(evaluated_at), "stale": stale,
            "settings": {"load_floor_min": cfg.load_floor_min, "pick_floor_min": cfg.pick_floor_min},
            "counts": counts, "deliveries": [_row_json(r, now) for r in rows]}


# ============================================================== checks

def _actor(body: dict | None) -> str:
    return str((body or {}).get("checked_by") or check_store.DEFAULT_ACTOR).strip()[:128] or check_store.DEFAULT_ACTOR


async def _acknowledge(db: AsyncSession, cc: str, delivery_number: str, body: dict | None, *, undo: bool) -> dict:
    body = body or {}
    departure_date = _date("departure_date", body.get("departure_date"))
    now = _now()
    try:
        if undo:
            row = await check_store.uncheck(db, cc, delivery_number, actor=_actor(body), now=now, departure_date=departure_date)
        else:
            note = body.get("note")
            row = await check_store.check(db, cc, delivery_number, actor=_actor(body), note=str(note) if note else None,
                                          now=now, departure_date=departure_date)
    except check_store.NoOpenDelivery as exc:
        raise HTTPException(404, detail=str(exc))
    except check_store.AmbiguousDelivery as exc:
        raise HTTPException(409, detail=str(exc))
    await db.commit()
    return _row_json(row, now)


@router.post("/deliveries/{delivery_number}/check")
async def check_delivery(delivery_number: str, body: dict = Body(default={}), customer: str = Depends(get_current_customer),
                         db: AsyncSession = Depends(get_session)):
    """Mark the delivery as checked. Body: `checked_by` (defaults to "api"), `note`, and `departure_date`
    when the delivery has more than one row. Returns the delivery in the board shape."""
    return await _acknowledge(db, customer, delivery_number, body, undo=False)


@router.delete("/deliveries/{delivery_number}/check")
async def uncheck_delivery(delivery_number: str, body: dict = Body(default={}), customer: str = Depends(get_current_customer),
                           db: AsyncSession = Depends(get_session)):
    return await _acknowledge(db, customer, delivery_number, body, undo=True)


@router.get("/checks")
async def read_checks(start: str | None = None, end: str | None = None, limit: int = 200, after: str | None = None,
                      customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """The acknowledgement ledger, newest first. `start` and `end` are tenant-local dates over the
    action time; `after` is the previous page's `next_after`."""
    tz = await _tz(db, customer)
    start_d, end_d = _date("start", start), _date("end", end)
    since = datetime.combine(start_d, datetime.min.time(), tzinfo=tz) if start_d else None
    until = datetime.combine(end_d + timedelta(days=1), datetime.min.time(), tzinfo=tz) if end_d else None
    cursor = None
    if after:
        try:
            at_text, ident = after.rsplit("|", 1)
            import uuid as _uuid
            cursor = (datetime.fromisoformat(at_text), _uuid.UUID(ident))
        except ValueError:
            raise HTTPException(422, detail="after must be a value this endpoint returned")
    rows, truncated = await check_store.list_checks(db, customer, start=since, end=until, limit=min(max(limit, 1), HISTORY_MAX),
                                                    after=cursor)
    return {"checks": [{"delivery_number": c.delivery_number, "departure_date": c.departure_date.isoformat(), "action": c.action,
                        "tier": c.tier, "actor": c.actor, "note": c.note, "at": _iso(c.at)} for c in rows],
            "truncated": truncated, "next_after": f"{rows[-1].at.isoformat()}|{rows[-1].id}" if rows and truncated else None}


# ============================================================== history and accuracy

@router.get("/history")
async def read_history(start: str, end: str, tier: str | None = None, checked: bool | None = None, route: str | None = None,
                       outcome: str | None = None, category: str | None = None, transaction: str | None = None,
                       delivery: str | None = None, limit: int = 200, after: str | None = None,
                       customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """Closed rows departing between `start` and `end` (tenant-local dates, at most 180 days apart),
    newest first, keyset paged. `tier` filters on the highest tier the row reached; `category` is a
    comma list of the plain words (missed, delayed, fine, unknown); `transaction` a picking screen the
    delivery went through; `delivery` the start of a delivery number. `counts` and `transactions`
    describe the whole range under every filter except `category`, for the pie and the pills."""
    _check("tier", tier, TIERS)
    _check("outcome", outcome, OUTCOMES)
    categories = [c.strip() for c in (category or "").split(",") if c.strip()]
    for c in categories:
        _check("category", c, model.CATEGORIES)
    start_d, end_d = _date("start", start), _date("end", end)
    if end_d < start_d:
        raise HTTPException(422, detail="end must not be before start")
    if end_d - start_d > HISTORY_SPAN_MAX:
        raise HTTPException(422, detail=f"start and end may be at most {HISTORY_SPAN_MAX.days} days apart")
    cursor = None
    if after:
        try:
            at_text, number = after.rsplit("|", 1)
            cursor = (datetime.fromisoformat(at_text), number)
        except ValueError:
            raise HTTPException(422, detail="after must be a value this endpoint returned")
    filters = dict(tier=tier, checked=checked, route=route, outcome=outcome, transaction=transaction or None, delivery=delivery or None)
    rows, truncated = await delivery_store.history_rows(db, customer, start=start_d, end=end_d, categories=categories or None,
                                                        limit=min(max(limit, 1), HISTORY_MAX), after=cursor, **filters)
    summary = await delivery_store.history_counts(db, customer, start=start_d, end=end_d, **filters) if cursor is None else None
    now = _now()
    return {"start": start_d.isoformat(), "end": end_d.isoformat(), "rows": [_row_json(r, now) for r in rows],
            "truncated": truncated,
            "next_after": f"{rows[-1].departure_at.isoformat()}|{rows[-1].delivery_number}" if rows and truncated else None,
            "counts": summary["counts"] if summary else None, "transactions": summary["transactions"] if summary else None}


def _ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return _s((Decimal(numerator) / Decimal(denominator)).quantize(Decimal("0.0001")))


def _scored(counts: dict) -> dict:
    flagged, flagged_late, late_not_flagged = counts["flagged"], counts["flagged_late"], counts["late_not_flagged"]
    return {**counts, "precision": _ratio(flagged_late, flagged), "recall": _ratio(flagged_late, flagged_late + late_not_flagged),
            "outcomes": {o: counts.pop(o) for o in delivery_store.SCORED_OUTCOMES}}


@router.get("/accuracy")
async def read_accuracy(days: int = 28, customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """Flags scored against outcomes over the last `days` of closed departures, per route and in total.
    Precision is the share of flagged deliveries that really ended late; recall is the share of late
    deliveries that had been flagged. Both are null, not zero, when nothing is scorable."""
    days = min(max(days, 1), ACCURACY_DAYS_MAX)
    tz = await _tz(db, customer)
    end = _now().astimezone(tz).date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    agg = await delivery_store.accuracy(db, customer, start=start, end=end)
    keys = ("departures", "flagged", "flagged_late", "late_not_flagged", "flagged_not_late", *delivery_store.SCORED_OUTCOMES)
    total = {k: sum(r[k] for r in agg["routes"].values()) for k in keys}
    return {"window": {"start": start.isoformat(), "end": end.isoformat(), "days": days},
            "total": {**_scored(dict(total)), "by_tier": agg["by_tier"]},
            "routes": [{"route": route, **_scored(dict(counts))} for route, counts in agg["routes"].items()]}


# ============================================================== settings, routes, status

def _settings_json(s: settings_store.Settings) -> dict:
    return {"enabled": s.enabled, "load_floor_min": s.load_floor_min, "pick_floor_min": s.pick_floor_min,
            "min_sample": s.min_sample, "window_days": s.window_days, "close_grace_min": s.close_grace_min,
            "coverage": _s(s.coverage), "defaulted": s.defaulted, "updated_by": s.updated_by, "updated_at": _iso(s.updated_at)}


@router.get("/settings")
async def read_settings(customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    return _settings_json(await settings_store.effective(db, customer))


@router.put("/settings")
async def put_settings(body: dict = Body(...), customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """Change any of the floors and knobs. Ranges are checked and every problem is reported at once."""
    changes = {k: v for k, v in (body or {}).items()}
    problems = settings_store.validate(changes)
    if problems:
        raise HTTPException(422, detail=problems)
    out = await settings_store.put(db, customer, **changes)
    await db.commit()
    return _settings_json(out)


@router.get("/routes")
async def read_routes(customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """The latest profile per route with the threshold the board is judged against."""
    cfg = await settings_store.effective(db, customer)
    profiles = await profile_store.latest(db, customer)
    for_route = profile_store.thresholds_for(profiles, cfg)
    out = []
    for route, p in sorted(profiles.items()):
        th = for_route(route)
        out.append({"route": route, "as_of_date": p.as_of_date.isoformat(), "window_days": p.window_days, "sample": p.sample,
                    "loaded_sample": p.loaded_sample, "learned_load_min": _s(p.learned_load_min), "effective_load_min": _s(th.load_min),
                    "load_source": th.load_source, "learned_pick_min": _s(p.learned_pick_min), "effective_pick_min": _s(th.pick_min),
                    "pick_source": th.pick_source, "load_lead_p50": _s(p.load_lead_p50), "load_lead_min": _s(p.load_lead_min),
                    "pick_lead_p50": _s(p.pick_lead_p50), "pick_lead_min": _s(p.pick_lead_min),
                    "departure_time_mode": p.departure_time_mode, "coverage": _s(p.coverage), "computed_at": _iso(p.computed_at)})
    return {"routes": out, "floors": {"load_floor_min": cfg.load_floor_min, "pick_floor_min": cfg.pick_floor_min},
            "min_sample": cfg.min_sample}


async def _readiness(db: AsyncSession, cc: str) -> dict:
    declared = await db.scalar(select(AnalyticsSettlement.id).where(
        AnalyticsSettlement.customer_code == cc, AnalyticsSettlement.name == settings.analytics_at_risk_settlement,
        AnalyticsSettlement.enabled.is_(True)))
    captured = (await db.execute(select(AnalyticsFieldRegistry.field).where(
        AnalyticsFieldRegistry.customer_code == cc, AnalyticsFieldRegistry.method == ROUTING_METHOD,
        AnalyticsFieldRegistry.field.in_(DEPARTURE_FIELDS), AnalyticsFieldRegistry.captured.is_(True)))).scalars().all()
    lookups = await lookup_store.load(db, cc)
    pick_line = lookups.get(settings.analytics_at_risk_pick_line_lookup)
    return {"settlement_declared": declared is not None,
            "departure_fields_captured": set(captured) == set(DEPARTURE_FIELDS),
            "pick_line_has_delivery": pick_line is not None and pick_line.attribute("DeliveryNumber") is not None}


@router.get("/status")
async def read_status(customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """One cheap row for a page header: how the loop is configured, when it last wrote, and whether the
    tenant's configuration is complete."""
    state = await state_store.get(db, customer)
    return {"worker_enabled": settings.analytics_at_risk_worker_enabled, "poll_seconds": settings.analytics_at_risk_poll_seconds,
            "profile_hour_local": settings.analytics_at_risk_profile_hour_local, "rule_version": RULE_VERSION,
            "settlement": settings.analytics_at_risk_settlement,
            "last_evaluated_at": _iso(state.last_evaluated_at) if state else None,
            "last_profiled_date": state.last_profiled_date.isoformat() if state and state.last_profiled_date else None,
            "open_rows": int(state.open_rows) if state else 0, "last_error": state.last_error if state else None,
            "readiness": await _readiness(db, customer)}


@router.post("/evaluate")
async def evaluate_now(customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """One pass for this tenant, inline. A pass is a few bounded reads and sub-second on the live volume,
    so there is no run to poll; the result is the pass's counts."""
    return await runner.evaluate_tenant(customer, now=_now())
