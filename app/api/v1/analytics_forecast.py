"""The demand forecast as a screen sees it (chunk 140).

    GET  /analytics/forecast/series     three grains in one call, actuals merged from the model's own read
    GET  /analytics/forecast/accuracy   rolling out-of-sample scores per horizon, or why there are none yet
    GET  /analytics/forecast/heatmap    eight days by twenty-four hours: lines and pickers needed
    GET  /analytics/forecast/items      items by recent volume, keyset paged, with their next bucket
    POST /analytics/forecast/runs       202 + poll: run now for this tenant
    GET  /analytics/forecast/runs[/id]  the run ledger
    GET  /analytics/forecast/status     one row for a page header

Every read is bounded (a window of a few hundred buckets at most), every number is a string as the
settled reads return them, and nothing here computes an actual itself: `history_store` does, the
same way it does for training.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_customer
from app.config.database import get_session
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics.settle_reads import UnknownSettlement
from app.services.analytics_forecast import (MODEL_VERSION, accuracy_store, history_store, prediction_store,
                                             run_store, runner, series)
from app.settings import settings

router = APIRouter(prefix="/analytics/forecast", tags=["analytics-forecast"])

METRICS = ("lines", "units", "pickers")
SUBJECT_KINDS = ("total", "warehouse", "transaction_name", "item_number")
GRAINS = series.GRAINS
MAX_BACK = {"day": 365, "week": 52, "month": 24, "hour": 336}
HOUR_METRICS = ("lines", "pickers")
ITEMS_MAX = 500
RUNS_MAX = 100

#: In-flight manual runs in THIS process (per-process by design, like logs.py's `_fetch_tasks`); the
#: run row in Postgres is the shared truth.
_run_tasks: dict[str, asyncio.Task] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _s(value) -> str | None:
    return None if value is None else format(Decimal(str(value)).normalize(), "f")


def _check(name: str, value: str, allowed: tuple[str, ...]) -> str:
    if value not in allowed:
        raise HTTPException(422, detail=f"{name} must be one of {', '.join(allowed)}, not {value!r}")
    return value


async def _tz(db: AsyncSession, cc: str) -> ZoneInfo:
    return ZoneInfo(await get_customer_timezone(db, cc))


async def _as_of(db: AsyncSession, cc: str, tz: ZoneInfo) -> tuple[date, object | None]:
    """The last day the latest completed run learned from, and that run. Without a run: yesterday."""
    run = await run_store.latest_completed(db, cc)
    if run is not None:
        return run.as_of_date, run
    return _now().astimezone(tz).date() - timedelta(days=1), None


def _run_json(run) -> dict:
    return {"run_id": str(run.id), "status": run.status, "trigger": run.trigger, "as_of_date": run.as_of_date.isoformat(),
            "model_version": run.model_version, "created_at": run.created_at.isoformat(),
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "points_written": run.points_written, "scored": run.scored, "error": run.error, "detail": run.detail or {}}


# ============================================================== series

def _window(grain: str, back: int, as_of: date, horizon_n: int) -> tuple[date, date]:
    """First and last bucket start shown: `back` buckets before the one in progress, then the targets."""
    today = as_of + timedelta(days=1)
    first, _ = series.bucket_bounds(today, grain)
    if grain == "day":
        return first - timedelta(days=back), as_of + timedelta(days=horizon_n)
    if grain == "week":
        return first - timedelta(weeks=back), first + timedelta(weeks=horizon_n - 1)
    return series.add_months(first, -back), series.add_months(first, horizon_n - 1)


def _bucket_points(grain: str, first: date, last: date, daily: dict[date, float], as_of: date, today_so_far: date,
                   preds: dict[datetime, object], tz: ZoneInfo) -> list[dict]:
    out = []
    start = first
    while start <= last:
        s, e = series.bucket_bounds(start, grain)
        at = series.local_midnight(s, tz)
        p = preds.get(at)
        complete = e <= as_of
        in_progress = s <= today_so_far and not complete
        total = sum(daily.get(s + timedelta(days=i), 0.0) for i in range((min(e, today_so_far) - s).days + 1)) \
            if s <= today_so_far else None
        out.append({
            "target": s.isoformat(), "start": s.isoformat(), "end": e.isoformat(),
            "actual": _s(total) if complete else None,
            "actual_to_date": _s(total) if in_progress else None,
            "partial": in_progress,
            "p10": _s(p.p10) if p else None, "p50": _s(p.value) if p else None, "p90": _s(p.p90) if p else None,
            "horizon": p.horizon if p else None, "model": (p.detail or {}).get("model") if p else None,
            "predicted_at": p.predicted_at.isoformat() if p else None,
            "scored": bool(p and p.scored_at)})
        start = s + timedelta(days=1) if grain == "day" else (s + timedelta(weeks=1) if grain == "week"
                                                               else series.add_months(s, 1))
    return out


def _hour_points(first_local: datetime, count: int, actual: dict[datetime, float], now_local: datetime,
                 preds: dict[datetime, object], tz: ZoneInfo) -> list[dict]:
    """One point per local hour from `first_local`. Hours before the one in progress carry the actual
    (0 when the read had nothing for them), the hour in progress is partial, later hours are ahead."""
    out = []
    now_hour = now_local.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    for i in range(count):
        start = first_local + timedelta(hours=i)
        at = start.replace(tzinfo=tz).astimezone(timezone.utc)
        p = preds.get(at)
        closed = start < now_hour
        in_progress = start == now_hour
        value = actual.get(start, 0.0) if start <= now_hour else None
        out.append({
            "target": start.strftime("%Y-%m-%dT%H:%M"), "start": start.strftime("%Y-%m-%dT%H:%M"),
            "end": (start + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M"),
            "actual": _s(value) if closed else None,
            "actual_to_date": _s(value) if in_progress else None,
            "partial": in_progress,
            "p10": _s(p.p10) if p else None, "p50": _s(p.value) if p else None, "p90": _s(p.p90) if p else None,
            "horizon": p.horizon if p else None, "model": (p.detail or {}).get("model") if p else None,
            "predicted_at": p.predicted_at.isoformat() if p else None,
            "scored": bool(p and p.scored_at)})
    return out


async def _hour_grain(db: AsyncSession, customer: str, *, metric: str, hours_back: int, as_of: date, today: date,
                      now_local: datetime, tz: ZoneInfo, settlement: str, horizon: str) -> dict:
    """The hour grain: `hours_back` hours before today, then every hour the heatmap forecasts ahead."""
    first_local = datetime.combine(today, datetime.min.time()) - timedelta(hours=hours_back)
    last_day = as_of + timedelta(days=settings.analytics_forecast_heatmap_days)
    end_local = datetime.combine(last_day + timedelta(days=1), datetime.min.time())
    count = int((end_local - first_local).total_seconds() // 3600)
    since = first_local.replace(tzinfo=tz).astimezone(timezone.utc)
    until = end_local.replace(tzinfo=tz).astimezone(timezone.utc)
    rows = await history_store.read_hourly(db, customer, settlement, since=since,
                                           until=min(until, now_local.astimezone(timezone.utc) + timedelta(hours=1)), tz=tz)
    actual = {r.start: (r.lines if metric == "lines" else float(r.pickers)) for r in rows}
    preds = {p.target_at: p for p in await prediction_store.latest_per_target(
        db, customer, metric=metric, grain="hour", subject_kind="total", subject="total", start=since, end=until,
        horizon=None if horizon == "latest" else horizon)}
    return {"points": _hour_points(first_local, count, actual, now_local, preds, tz)}


@router.get("/series")
async def read_series(metric: str = "lines", subject_kind: str = "total", subject: str = "total",
                      grains: str = "day,week,month", days_back: int = 28, weeks_back: int = 8, months_back: int = 6,
                      hours_back: int = 72, horizon: str = "latest", customer: str = Depends(get_current_customer),
                      db: AsyncSession = Depends(get_session)):
    """Actuals and forecast for one series at up to four grains, in one call.

    Actuals come from the settled rows through the forecast's own history read. A bucket that is
    still running (this hour, today, this week, this month) carries `actual_to_date` and
    `partial: true`; a closed one carries `actual`. Forecast points are the newest prediction per
    target, or the one made at `horizon` when pinned ("what did we say a week out"). The `hour`
    grain exists for the total only, for lines and pickers: it is what the heatmap is made of."""
    _check("metric", metric, METRICS)
    _check("subject_kind", subject_kind, SUBJECT_KINDS)
    wanted = tuple(g.strip() for g in grains.split(",") if g.strip())
    for g in wanted:
        _check("grains", g, GRAINS + ("hour",))
    if "hour" in wanted and (subject_kind != "total" or metric not in HOUR_METRICS):
        raise HTTPException(422, detail="the hour grain exists for the total only, for lines and pickers")
    back = {"day": min(max(days_back, 1), MAX_BACK["day"]), "week": min(max(weeks_back, 1), MAX_BACK["week"]),
            "month": min(max(months_back, 1), MAX_BACK["month"])}
    horizon_n = {"day": settings.analytics_forecast_horizon_days, "week": settings.analytics_forecast_horizon_weeks,
                 "month": settings.analytics_forecast_horizon_months}
    tz = await _tz(db, customer)
    as_of, run = await _as_of(db, customer, tz)
    now_local = _now().astimezone(tz)
    today = now_local.date()
    cfg = runner.config_from_settings(customer)
    calendar_grains = tuple(g for g in wanted if g != "hour")
    windows = {g: _window(g, back[g], as_of, horizon_n[g]) for g in calendar_grains}
    daily: dict[date, float] = {}
    out_grains = {}
    try:
        if windows:
            first_day = min(w[0] for w in windows.values())
            rows = await history_store.read_daily_for(
                db, customer, cfg.settlement, since=series.local_midnight(first_day, tz),
                until=series.local_midnight(today + timedelta(days=1), tz), tz=tz, units_value=cfg.units_value,
                subject_kind=subject_kind, subject=subject)
            daily = {d: float(lines if metric == "lines" else units) for d, lines, units in rows}
        if "hour" in wanted:
            out_grains["hour"] = await _hour_grain(
                db, customer, metric=metric, hours_back=min(max(hours_back, 1), MAX_BACK["hour"]), as_of=as_of,
                today=today, now_local=now_local, tz=tz, settlement=cfg.settlement, horizon=horizon)
    except UnknownSettlement as exc:
        raise HTTPException(404, detail=str(exc))
    for g in calendar_grains:
        first, last = windows[g]
        preds = {p.target_at: p for p in await prediction_store.latest_per_target(
            db, customer, metric=metric, grain=g, subject_kind=subject_kind, subject=subject,
            start=series.local_midnight(first, tz), end=series.local_midnight(last + timedelta(days=1), tz),
            horizon=None if horizon == "latest" else horizon)}
        out_grains[g] = {"points": _bucket_points(g, first, last, daily, as_of, today, preds, tz)}
    history = (run.detail or {}).get("history", {}) if run else {}
    for g in out_grains.values():
        g["history_from"] = history.get("start")
        g["steady_from"] = history.get("steady_from")
    return {"metric": metric, "subject_kind": subject_kind, "subject": subject, "timezone": tz.key,
            "as_of_date": as_of.isoformat(), "today": today.isoformat(),
            "latest_run": {"run_id": str(run.id), "as_of_date": run.as_of_date.isoformat(),
                           "predicted_at": run.finished_at.isoformat() if run.finished_at else None,
                           "model_version": run.model_version, "warnings": (run.detail or {}).get("warnings", [])}
            if run else None,
            "grains": out_grains}


# ============================================================== accuracy

@router.get("/accuracy")
async def read_accuracy(metric: str = "lines", subject_kind: str = "total", subject: str = "total", grain: str = "day",
                        customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """Rolling out-of-sample scores for one series, per horizon. Says plainly when nothing has been
    scorable yet, and when the first bucket will be; the backtest score is reported separately so
    a screen never shows it as if it were live accuracy."""
    _check("metric", metric, METRICS)
    _check("subject_kind", subject_kind, SUBJECT_KINDS)
    _check("grain", grain, GRAINS)
    tz = await _tz(db, customer)
    as_of, _run = await _as_of(db, customer, tz)
    rows = await accuracy_store.read_series(db, customer, metric=metric, grain=grain, subject_kind=subject_kind,
                                            subject=subject, model_version=MODEL_VERSION)
    summary = await prediction_store.series_one(db, customer, metric=metric, grain="day", subject_kind=subject_kind,
                                                subject=subject)
    first = await prediction_store.first_target(db, customer, metric=metric, grain=grain, subject_kind=subject_kind,
                                                subject=subject)
    window_days = settings.analytics_forecast_accuracy_window_days
    return {
        "metric": metric, "grain": grain, "subject_kind": subject_kind, "subject": subject, "model_version": MODEL_VERSION,
        "window": {"start": (as_of - timedelta(days=window_days)).isoformat(), "end": as_of.isoformat(), "days": window_days},
        "horizons": [{"horizon": a.horizon, "n": a.n, "mae": _s(a.mae), "wape": _s(a.wape), "mape": _s(a.mape),
                      "mape_n": a.mape_n, "bias": _s(a.bias), "computed_at": a.computed_at.isoformat()} for a in rows],
        "backtest": {"model": summary.model, "wape": _s(summary.backtest_wape), "classification": summary.classification,
                     "history_days": summary.history_days} if summary else None,
        "status": "ok" if rows else "no_out_of_sample_yet",
        "first_scorable_on": first.astimezone(tz).date().isoformat() if first else None}


# ============================================================== heatmap

@router.get("/heatmap")
async def read_heatmap(week: str = "next", customer: str = Depends(get_current_customer),
                       db: AsyncSession = Depends(get_session)):
    """Eight days by twenty-four hours of forecast lines and pickers needed. `week=next` starts
    tomorrow; `week=last` covers the eight days ending yesterday and overlays what happened."""
    _check("week", week, ("next", "last"))
    tz = await _tz(db, customer)
    as_of, run = await _as_of(db, customer, tz)
    days = settings.analytics_forecast_heatmap_days
    first = as_of + timedelta(days=1) if week == "next" else as_of - timedelta(days=days - 1)
    last = first + timedelta(days=days - 1)
    start, end = series.local_midnight(first, tz), series.local_midnight(last + timedelta(days=1), tz)
    lines = {p.target_at: p for p in await prediction_store.latest_per_target(
        db, customer, metric="lines", grain="hour", subject_kind="total", subject="total", start=start, end=end)}
    pickers = {p.target_at: p for p in await prediction_store.latest_per_target(
        db, customer, metric="pickers", grain="hour", subject_kind="total", subject="total", start=start, end=end)}
    actual = {}
    if week == "last":
        cfg = runner.config_from_settings(customer)
        try:
            for r in await history_store.read_hourly(db, customer, cfg.settlement, since=start, until=end, tz=tz):
                actual[r.start] = r
        except UnknownSettlement:
            actual = {}
    out_days = []
    for i in range(days):
        day = first + timedelta(days=i)
        hours = []
        for hour in range(24):
            at_local = datetime(day.year, day.month, day.day, hour, tzinfo=tz)
            at = at_local.astimezone(timezone.utc)
            ln, pk, act = lines.get(at), pickers.get(at), actual.get(at_local.replace(tzinfo=None))
            hours.append({
                "hour": hour, "lines_p10": _s(ln.p10) if ln else None, "lines_p50": _s(ln.value) if ln else None,
                "lines_p90": _s(ln.p90) if ln else None,
                "pickers_p10": int(pk.p10) if pk and pk.p10 is not None else None,
                "pickers_p50": int(pk.value) if pk and pk.value is not None else None,
                "pickers_p90": int(pk.p90) if pk and pk.p90 is not None else None,
                "actual_lines": _s(act.lines) if act else None, "actual_pickers": act.pickers if act else None})
        out_days.append({"date": day.isoformat(), "dow": day.weekday(), "hours": hours})
    staffing = ((run.detail or {}).get("history", {}).get("staffing", {}) if run else {}) or {}
    return {"timezone": tz.key, "week": {"start": first.isoformat(), "end": last.isoformat(), "which": week},
            "throughput": {"lines_per_picker_hour": _s(staffing.get("lines_per_picker_hour")),
                           "method": "median lines per distinct picker-hour over the last 28 days",
                           "buffer_pct": settings.analytics_forecast_staffing_buffer_pct},
            "days": out_days}


# ============================================================== items

def _parse_after(after: str | None) -> tuple[Decimal, str] | None:
    if not after:
        return None
    try:
        volume, subject = after.split("|", 1)
        return Decimal(volume), subject
    except (ValueError, ArithmeticError):
        raise HTTPException(422, detail="after must be '<volume>|<item_number>' as returned in next_after")


def _grain_of(horizon: str) -> str:
    suffix = horizon[-1:] if horizon else ""
    grain = {"d": "day", "w": "week", "m": "month"}.get(suffix)
    if grain is None or not horizon[:-1].isdigit():
        raise HTTPException(422, detail="horizon must look like 1d, 1w or 0m")
    return grain


@router.get("/items")
async def list_items(metric: str = "units", horizon: str = "1w", sort: str = "volume", limit: int = 50,
                     after: str | None = None, q: str | None = None, customer: str = Depends(get_current_customer),
                     db: AsyncSession = Depends(get_session)):
    """Items by recent volume, biggest first, keyset paged. Each carries its demand class, the model
    it got, its backtest score, its live accuracy at `horizon` when scored, and its next bucket."""
    _check("metric", metric, ("lines", "units"))
    _check("sort", sort, ("volume", "wape"))
    grain = _grain_of(horizon)
    limit = min(max(limit, 1), ITEMS_MAX)
    tz = await _tz(db, customer)
    as_of, _run = await _as_of(db, customer, tz)
    page = await prediction_store.series_page(db, customer, metric=metric, grain="day", subject_kind="item_number",
                                              after=_parse_after(after), limit=limit, q=q)
    truncated = len(page) > limit
    page = page[:limit]
    subjects = [s.subject for s in page]
    acc = await accuracy_store.read_for_subjects(db, customer, metric=metric, grain=grain, subject_kind="item_number",
                                                 horizon=horizon, subjects=subjects, model_version=MODEL_VERSION)
    nxt = {}
    for p in await prediction_store.latest_for_subjects(
            db, customer, metric=metric, grain=grain, subject_kind="item_number", subjects=subjects,
            start=series.local_midnight(as_of - timedelta(days=31), tz),
            end=series.local_midnight(as_of + timedelta(days=130), tz), horizon=horizon):
        nxt.setdefault(p.subject, p)  # ordered by target: the first is the soonest
    threshold = settings.analytics_forecast_item_daily_min_active_days
    items = []
    for s in page:
        a, p = acc.get(s.subject), nxt.get(s.subject)
        items.append({
            "item_number": s.subject, "classification": s.classification, "model": s.model,
            "backtest_wape": _s(s.backtest_wape), "history_days": s.history_days, "active_days_28d": s.active_days_28d,
            "volume_28d": _s(s.volume_28d),
            "grains": list(GRAINS) if s.active_days_28d >= threshold else ["week", "month"],
            "accuracy": {"n": a.n, "wape": _s(a.wape), "mae": _s(a.mae), "bias": _s(a.bias)} if a else None,
            "next": {"horizon": p.horizon, "target": p.target_at.astimezone(tz).date().isoformat(),
                     "p10": _s(p.p10), "p50": _s(p.value), "p90": _s(p.p90)} if p else None})
    if sort == "wape":
        items.sort(key=lambda i: (i["accuracy"] is None, Decimal(i["accuracy"]["wape"]) if i["accuracy"] and i["accuracy"]["wape"]
                                  else Decimal("Infinity")))
    last = page[-1] if page else None
    return {"metric": metric, "grain": grain, "horizon": horizon, "sort": sort, "limit": limit, "items": items,
            "truncated": truncated, "next_after": f"{_s(last.volume_28d)}|{last.subject}" if truncated and last else None}


# ============================================================== subjects

@router.get("/subjects")
async def list_subjects(subject_kind: str = "transaction_name", metric: str = "lines", limit: int = 100,
                        customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """What has been forecast for one subject kind: the warehouses or transaction names a screen can
    ask `/series` about, with the model each got. Item numbers have `/items`."""
    _check("subject_kind", subject_kind, ("total", "warehouse", "transaction_name"))
    _check("metric", metric, METRICS)
    page = await prediction_store.series_page(db, customer, metric=metric, grain="day", subject_kind=subject_kind,
                                              after=None, limit=min(max(limit, 1), ITEMS_MAX))
    return {"subject_kind": subject_kind, "metric": metric,
            "subjects": [{"subject": s.subject, "model": s.model, "classification": s.classification,
                          "volume_28d": _s(s.volume_28d)} for s in page]}


# ============================================================== runs

@router.post("/runs", status_code=202)
async def trigger_run(body: dict = Body(default={}), customer: str = Depends(get_current_customer),
                      db: AsyncSession = Depends(get_session)):
    """Run the forecast for this tenant now. 202 + a poll URL; 409 while one is already in progress."""
    tz = await _tz(db, customer)
    as_of_text = (body or {}).get("as_of_date")
    try:
        as_of = date.fromisoformat(as_of_text) if as_of_text else _now().astimezone(tz).date() - timedelta(days=1)
    except ValueError:
        raise HTTPException(422, detail="as_of_date must be YYYY-MM-DD")
    existing = await run_store.running(db, customer)
    if existing is not None:
        raise HTTPException(409, detail={"message": "A forecast run is already in progress for this logspace",
                                         "run_id": str(existing.id)})
    run = await run_store.create(db, customer, as_of_date=as_of, trigger="manual", model_version=MODEL_VERSION)
    await db.commit()
    task = asyncio.create_task(runner.run_tenant(customer, as_of_date=as_of, trigger="manual", run_id=run.id))
    _run_tasks[str(run.id)] = task
    task.add_done_callback(lambda t, rid=str(run.id): _run_tasks.pop(rid, None))
    return {"run_id": str(run.id), "status": "queued", "as_of_date": as_of.isoformat(),
            "poll": f"/api/v1/analytics/forecast/runs/{run.id}"}


@router.get("/runs/{run_id}")
async def get_run(run_id: str, customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    try:
        rid = uuid.UUID(run_id)
    except ValueError:
        raise HTTPException(404, detail="no such run")
    run = await run_store.get(db, customer, rid)
    if run is None:
        raise HTTPException(404, detail="no such run")
    return _run_json(run)


@router.get("/runs")
async def list_runs(limit: int = 20, customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    runs = await run_store.list_runs(db, customer, limit=min(max(limit, 1), RUNS_MAX))
    return {"runs": [_run_json(r) for r in runs]}


@router.get("/status")
async def read_status(customer: str = Depends(get_current_customer), db: AsyncSession = Depends(get_session)):
    """One cheap row for a page header: the latest run and how the loop is configured."""
    tz = await _tz(db, customer)
    latest = await run_store.latest(db, customer)
    return {"worker_enabled": settings.analytics_forecast_worker_enabled,
            "run_hour_local": settings.analytics_forecast_run_hour_local,
            "settlement": settings.analytics_forecast_settlement, "model_version": MODEL_VERSION,
            "min_history_days": settings.analytics_forecast_min_history_days,
            "next_as_of_date": (_now().astimezone(tz).date() - timedelta(days=1)).isoformat(),
            "latest_run": _run_json(latest) if latest else None}
