"""Chunk 142: the forecast block of the Teams Home snapshot.

The Forecast tab of the web app reads the forecast tables through `/analytics/forecast/*`. The
Teams tab cannot reach this server, so the same tables are read here, once a minute, into one
small block the edge only draws: four tiles, fifteen days of actual against forecast, the next
seven shifts' peak pickers, and the confidence note. Every value a person reads is text decided
here; the raw numbers ride alongside only so the edge can scale a chart.

The honesty rules are the web page's, so the two never disagree: a forecast line is drawn only
where a prediction was stored before the fact; the accuracy tile shows the live out-of-sample WAPE
once seven days are scored and the backtest, labelled backtest, until then; the note names the
limit in plain words.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_forecast import MODEL_VERSION, accuracy_store, history_store, prediction_store, run_store, runner, series

DAYS_BACK = 7
DAYS_AHEAD = 7
SHIFTS = 7
SHIFT_START_HOUR = 14
SHIFT_END_HOUR = 8
MIN_SCORED_DAYS = 7
NO_RUN_NOTE = "no forecast run yet · the model runs nightly after the pick window closes"
CAPTION = "last 7 days against the forecast made before each · then 7 days ahead"


# ============================================================== text
def _int(n) -> str:
    return f"{int(round(float(n))):,}"


def _pct(x) -> str:
    return "–" if x is None else f"{float(x) * 100:.1f}%"


def _day(d: date) -> str:
    return d.strftime("%a %-d")


def _day_month(d: date) -> str:
    return d.strftime("%a %-d %b")


def _hh(hour: int) -> str:
    return f"{hour:02d}:00"


def _err(actual, p50) -> str:
    if actual is None or p50 is None or float(actual) == 0:
        return "–"
    e = (float(p50) - float(actual)) / float(actual)
    v = f"{abs(e) * 100:.1f}%"
    return f"+{v} over" if e > 0 else f"−{v} under" if e < 0 else "spot on"


def confidence(history_days: int | None, scored: int) -> str:
    h = history_days or 0
    if h < 28 or scored < MIN_SCORED_DAYS:
        return "low"
    return "medium" if h < 56 or scored < 2 * MIN_SCORED_DAYS else "high"


def note(history_days: int | None, scored: int) -> str:
    tone = confidence(history_days, scored)
    head = "indicative only" if tone == "low" else "settling in" if tone == "medium" else "forecast"
    return f"{head} · {history_days or 0} days of steady history · scored on {scored} of the {MIN_SCORED_DAYS} days it needs"


def axis(peak: float) -> dict:
    """A nice whole-step axis: the web page's `axisStep`, so the two charts round alike."""
    raw = max(1.0, peak) / 4
    mag = 10 ** math.floor(math.log10(raw))
    step = next((c * mag for c in (1, 2, 2.5, 5, 10) if raw / mag <= c and c * mag >= 1 and float(c * mag).is_integer()),
                max(1, round(10 * mag)))
    top = int(math.ceil(max(1.0, peak) / step) * step)
    return {"top": top, "ticks": [{"value": v, "text": f"{v:,}"} for v in range(0, top + 1, int(step))]}


# ============================================================== build
def build_days(today: date, daily: dict[date, float], preds: dict[date, object]) -> list[dict]:
    out = []
    for k in range(-DAYS_BACK, DAYS_AHEAD + 1):
        d = today + timedelta(days=k)
        kind = "past" if k < 0 else "today" if k == 0 else "ahead"
        p = preds.get(d)
        actual = daily.get(d) if k < 0 else None
        to_date = daily.get(d, 0.0) if k == 0 else None
        p50 = float(p.value) if p is not None and p.value is not None else None
        out.append({
            "label": _day(d), "kind": kind,
            "actual": actual, "actual_text": _int(actual) if actual is not None else "–",
            "actual_to_date": to_date, "actual_to_date_text": f"{_int(to_date)} so far" if to_date is not None else None,
            "p50": p50, "p50_text": _int(p50) if p50 is not None else "–",
            "p10": float(p.p10) if p is not None and p.p10 is not None else None,
            "p90": float(p.p90) if p is not None and p.p90 is not None else None,
            "range_text": f"{_int(p.p10)} – {_int(p.p90)}" if p is not None and p.p10 is not None and p.p90 is not None else "–",
            "error_text": _err(actual, p50) if kind == "past" else "–",
            "horizon": p.horizon if p is not None else None,
        })
    return out


def build_shifts(today: date, hours: dict[datetime, tuple[int, float]]) -> list[dict]:
    """Seven shifts from today's: 14:00 → 08:00 next day, dated by the start. `hours` maps a local
    naive hour start to (pickers, lines)."""
    out = []
    for k in range(SHIFTS):
        d = today + timedelta(days=k)
        cells = []
        for hour in [*range(SHIFT_START_HOUR, 24), *range(0, SHIFT_END_HOUR + 1)]:
            day = d if hour >= SHIFT_START_HOUR else d + timedelta(days=1)
            at = datetime(day.year, day.month, day.day, hour)
            pickers, lines = hours.get(at, (0, 0.0))
            cells.append((hour, pickers, lines))
        peak = max(cells, key=lambda c: (c[1], c[2]), default=None)
        picker_hours = sum(c[1] for c in cells)
        quiet = peak is None or peak[1] <= 0
        out.append({
            "label": _day(d), "quiet": quiet,
            "peak_pickers": 0 if quiet else peak[1], "peak_text": "quiet" if quiet else f"{peak[1]:,} pickers",
            "peak_hour": "" if quiet else _hh(peak[0]), "peak_lines_text": "" if quiet else f"{_int(peak[2])} lines",
            "picker_hours": picker_hours, "picker_hours_text": f"{picker_hours:,} picker-hours",
        })
    return out


def tiles(days: list[dict], live, backtest, shifts: list[dict], scored: int) -> list[dict]:
    tomorrow = next((d for d in days if d["kind"] == "ahead"), None)
    ahead = [d for d in days if d["kind"] == "ahead" and d["p50"] is not None]
    week = sum(d["p50"] for d in ahead) if ahead else None
    week_lo = sum(d["p10"] or 0 for d in ahead) if ahead else None
    week_hi = sum(d["p90"] or 0 for d in ahead) if ahead else None
    if live is not None and scored >= MIN_SCORED_DAYS:
        acc_value, acc_caption = _pct(live.wape), f"live WAPE · {scored} scored days · 1 day ahead"
    elif backtest is not None and backtest.backtest_wape is not None:
        acc_value = _pct(backtest.backtest_wape)
        acc_caption = f"backtest · not enough data yet · {scored} of {MIN_SCORED_DAYS} scored days"
    else:
        acc_value, acc_caption = "–", "no scored days yet"
    tonight = shifts[0] if shifts else None
    return [
        {"id": "tomorrow", "label": "Lines tomorrow", "value": tomorrow["p50_text"] if tomorrow else "–",
         "caption": f"p10 – p90 {tomorrow['range_text']}" if tomorrow and tomorrow["p50"] is not None else "no forecast yet"},
        {"id": "week", "label": "Lines next 7 days", "value": _int(week) if week is not None else "–",
         "caption": f"p10 – p90 {_int(week_lo)} – {_int(week_hi)} · {len(ahead)} days" if week is not None else "no forecast yet"},
        {"id": "accuracy", "label": "Accuracy · 1 day ahead", "value": acc_value, "caption": acc_caption},
        {"id": "tonight", "label": "Pickers · tonight's peak",
         "value": str(tonight["peak_pickers"]) if tonight and not tonight["quiet"] else "–",
         "caption": (f"{tonight['peak_hour']} – {_hh((int(tonight['peak_hour'][:2]) + 1) % 24)} · {tonight['peak_lines_text']}"
                     if tonight and not tonight["quiet"] else "no picking forecast tonight")},
    ]


# ============================================================== the reads
async def compute(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> dict:
    """The block for one customer, or the two-field "no run yet" block."""
    run = await run_store.latest_completed(db, customer_code)
    if run is None:
        return {"available": False, "note": NO_RUN_NOTE}
    cfg = runner.config_from_settings(customer_code)
    today = now.astimezone(tz).date()
    as_of = run.as_of_date
    first = today - timedelta(days=DAYS_BACK)
    last = today + timedelta(days=DAYS_AHEAD)
    rows = await history_store.read_daily_for(
        db, customer_code, cfg.settlement, since=series.local_midnight(first, tz),
        until=series.local_midnight(today + timedelta(days=1), tz), tz=tz, units_value=cfg.units_value,
        subject_kind="total", subject="total")
    daily = {d: float(lines) for d, lines, _units in rows}
    preds = {p.target_at.astimezone(tz).date(): p for p in await prediction_store.latest_per_target(
        db, customer_code, metric="lines", grain="day", subject_kind="total", subject="total",
        start=series.local_midnight(first, tz), end=series.local_midnight(last + timedelta(days=1), tz))}
    days = build_days(today, daily, preds)

    hour_rows = {}
    span_end = series.local_midnight(today + timedelta(days=SHIFTS + 1), tz)
    lines_by_hour = {p.target_at: float(p.value or 0) for p in await prediction_store.latest_per_target(
        db, customer_code, metric="lines", grain="hour", subject_kind="total", subject="total",
        start=series.local_midnight(today, tz), end=span_end)}
    for p in await prediction_store.latest_per_target(
            db, customer_code, metric="pickers", grain="hour", subject_kind="total", subject="total",
            start=series.local_midnight(today, tz), end=span_end):
        local = p.target_at.astimezone(tz).replace(tzinfo=None)
        hour_rows[local] = (int(p.value or 0), lines_by_hour.get(p.target_at, 0.0))
    shifts = build_shifts(today, hour_rows)

    acc = await accuracy_store.read_series(db, customer_code, metric="lines", grain="day", subject_kind="total",
                                           subject="total", model_version=MODEL_VERSION)
    live = next((a for a in acc if a.horizon == "1d"), None)
    scored = live.n if live is not None else 0
    summary = await prediction_store.series_one(db, customer_code, metric="lines", grain="day", subject_kind="total",
                                                subject="total")
    history_days = ((run.detail or {}).get("history") or {}).get("days") or (summary.history_days if summary else None)
    peak = max([d["p90"] or 0 for d in days] + [d["actual"] or 0 for d in days] + [d["actual_to_date"] or 0 for d in days], default=0)
    return {
        "available": True,
        "as_of_text": f"as of {_day_month(as_of)}",
        "model": (summary.model if summary else None) or ((run.detail or {}).get("history") or {}).get("model") or "–",
        "confidence": confidence(history_days, scored),
        "note": note(history_days, scored),
        "tiles": tiles(days, live, summary, shifts, scored),
        "axis": axis(peak),
        "days": days,
        "shifts": shifts,
        "caption": CAPTION,
    }
