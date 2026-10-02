"""Chunk 150: the deliveries-at-risk block of the Teams Home snapshot.

The web tab reads the board through `/analytics/at-risk/*`. The Teams tab cannot reach this server,
so the same rows are read here, once a minute, into one small block the edge only draws: a summary
line, up to twelve deliveries worst first, and the accuracy line. Every value a person reads is text
decided here; `tier` and `minutes_to_departure` ride alongside only so the edge can colour and order.

The honesty rules are the web page's: a check is shown as the person's word and re-opened when the
tier rises past it; the accuracy line appears only once enough departures have closed; and a board
the worker has stopped writing says `stale`, never `calm`.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.services.analytics_at_risk import delivery_store, model, settings_store, state_store
from app.settings import settings

MAX_ROWS = 12
MIN_SCORED_DEPARTURES = 20
STALE_POLLS = 3
NO_BOARD_NOTE = "no delivery board yet · the worker needs a delivery_route settlement to read"
STALE_NOTE = "the board has not been refreshed for a while · the figures below may be behind"
TIER_TEXT = {"none": "fine", "watch": "Watch", "at_risk": "At risk", "late": "Late"}
TIER_ORDER = {"late": 3, "at_risk": 2, "watch": 1, "none": 0}


# ============================================================== text
def _hhmm(at: datetime, tz: ZoneInfo) -> str:
    return at.astimezone(tz).strftime("%H:%M")


def _day_month(d: date) -> str:
    return d.strftime("%a %-d %b")


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def minutes_text(minutes: Decimal) -> str:
    m = int(minutes.to_integral_value(rounding="ROUND_HALF_UP"))
    if m < 0:
        return f"left {abs(m)} min ago"
    if m == 0:
        return "due now"
    if m >= 120:
        return f"{m // 60} h {m % 60:02d} to go"
    return f"{m} min to go"


def progress_text(row: AnalyticsAtRiskDelivery) -> str:
    expected = row.lines_expected
    lines = f"{row.lines_picked} of {expected} lines" if expected is not None else f"{row.lines_picked} lines picked"
    if row.loading_expected is False:
        packages = "no loading step on this route"
    elif row.packages_created:
        packages = f"{row.packages_loaded} of {row.packages_created} packages loaded"
    elif row.packages_loaded:
        packages = f"{row.packages_loaded} loaded"
    else:
        packages = "no package yet"
    return f"{lines} · {packages}"


def last_text(row: AnalyticsAtRiskDelivery, tz: ZoneInfo) -> str:
    pick = f"last pick {_hhmm(row.last_pick_at, tz)}" if row.last_pick_at else "no pick yet"
    load = f"last load {_hhmm(row.last_load_at, tz)}" if row.last_load_at else "no load yet"
    return f"{pick} · {load}"


def threshold_text(row: AnalyticsAtRiskDelivery) -> str:
    if row.tier in ("at_risk", "late"):
        minutes, source = row.load_threshold_min, row.load_threshold_source
        verb = "needs loading"
    else:
        minutes, source = row.pick_threshold_min, row.pick_threshold_source
        verb = "needs picking"
    if minutes is None:
        return ""
    m = int(Decimal(str(minutes)).to_integral_value(rounding="ROUND_HALF_UP"))
    origin = "learned from this route's history" if source == "learned" else "the configured floor"
    return f"{verb} {m} min before departure · {origin}"


def summary_text(counts: dict) -> str:
    parts = []
    if counts["late"]:
        parts.append(f"{counts['late']} late")
    if counts["at_risk"]:
        parts.append(f"{counts['at_risk']} at risk")
    if counts["watch"]:
        parts.append(f"{counts['watch']} to watch")
    fine = counts["open"] - counts["late"] - counts["at_risk"] - counts["watch"]
    parts.append(f"{fine} fine")
    if counts["checked"]:
        parts.append(f"{counts['checked']} checked")
    return " · ".join(parts)


def empty_text(today_closed: list[AnalyticsAtRiskDelivery], next_departure: datetime | None, tz: ZoneInfo) -> str:
    nxt = f" · next departure {_day_month(next_departure.astimezone(tz).date())} {_hhmm(next_departure, tz)}" if next_departure else ""
    if not today_closed:
        return f"No departures today{nxt}"
    late = sum(r.outcome in delivery_store.LATE_OUTCOMES for r in today_closed)
    n = len(today_closed)
    if late == 0:
        return f"All {_plural(n, 'departure')} today left on time{nxt}"
    return f"{_plural(n, 'departure')} today · {late} left late · {n - late} on time{nxt}"


def accuracy_text(agg: dict, *, days: int) -> str:
    totals = {k: sum(r[k] for r in agg["routes"].values()) for k in ("departures", "flagged", "flagged_late", "late_not_flagged")}
    if totals["departures"] < MIN_SCORED_DEPARTURES:
        return f"accuracy · not enough closed departures yet · {totals['departures']} of {MIN_SCORED_DEPARTURES} needed"
    precision = totals["flagged_late"] / totals["flagged"] if totals["flagged"] else None
    late = totals["flagged_late"] + totals["late_not_flagged"]
    recall = totals["flagged_late"] / late if late else None
    p = "–" if precision is None else f"{precision * 100:.0f}%"
    r = "–" if recall is None else f"{recall * 100:.0f}%"
    return (f"last {days} days · flagged {totals['flagged']} · actually late {late} · "
            f"precision {p} · recall {r}")


# ============================================================== build
def build_rows(rows: list[AnalyticsAtRiskDelivery], now: datetime, tz: ZoneInfo) -> list[dict]:
    ordered = sorted(rows, key=lambda r: (-TIER_ORDER.get(r.tier, 0), r.departure_at, r.delivery_number))
    out = []
    for row in ordered[:MAX_ROWS]:
        minutes = model.minutes_to_departure(row.departure_at, now)
        checked = None
        reopened = False
        if row.checked_at is not None:
            checked = {"by": row.checked_by or "someone", "at_text": _hhmm(row.checked_at, tz), "note": row.check_note}
            reopened = TIER_ORDER.get(row.tier, 0) > TIER_ORDER.get(row.checked_tier or "none", 0)
        out.append({
            "delivery_number": row.delivery_number, "route": row.route or "–", "departure_at": row.departure_at.isoformat(),
            "departure_text": _hhmm(row.departure_at, tz), "minutes_to_departure": int(minutes.to_integral_value(rounding="ROUND_HALF_UP")),
            "minutes_text": minutes_text(minutes), "tier": row.tier, "tier_text": TIER_TEXT.get(row.tier, row.tier),
            "customer_name": row.customer_name, "progress_text": progress_text(row), "last_text": last_text(row, tz),
            "threshold_text": threshold_text(row) if row.tier != "none" else "",
            "checked": checked, "reopened_count": int(row.reopened_count or 0),
            "reopened_text": f"checked at {TIER_TEXT.get(row.checked_tier or 'none')} · now {TIER_TEXT.get(row.tier)}" if reopened else "",
        })
    return out


# ============================================================== the reads
async def compute(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> dict:
    """The block for one customer, or the two-field "no board yet" block."""
    state = await state_store.get(db, customer_code)
    if state is None or state.last_evaluated_at is None:
        return {"available": False, "note": NO_BOARD_NOTE}
    today = now.astimezone(tz).date()
    cfg = await settings_store.effective(db, customer_code)
    open_rows = await delivery_store.board_rows(db, customer_code, dates=[today - timedelta(days=1), today, today + timedelta(days=1)])
    stale = (now - state.last_evaluated_at) > timedelta(seconds=settings.analytics_at_risk_poll_seconds * STALE_POLLS)
    counts = {"open": len(open_rows), "late": sum(r.tier == "late" for r in open_rows),
              "at_risk": sum(r.tier == "at_risk" for r in open_rows), "watch": sum(r.tier == "watch" for r in open_rows),
              "checked": sum(r.checked_at is not None for r in open_rows)}
    flagged = [r for r in open_rows if r.tier != "none"]
    empty = None
    if not open_rows:
        closed_today = [r for r in await delivery_store.board_rows(db, customer_code, dates=[today], include_closed=True)
                        if r.status == "closed"]
        upcoming = await delivery_store.board_rows(db, customer_code, dates=[today + timedelta(days=1), today + timedelta(days=2)])
        empty = empty_text(closed_today, min((r.departure_at for r in upcoming), default=None), tz)
    agg = await delivery_store.accuracy(db, customer_code, start=today - timedelta(days=14), end=today - timedelta(days=1))
    rows = build_rows(flagged or open_rows, now, tz)
    more = len(flagged or open_rows) - len(rows)
    return {
        "available": True,
        "stale": stale,
        "note": STALE_NOTE if stale else "",
        "as_of_text": f"as of {_hhmm(state.last_evaluated_at, tz)}",
        "caption": (f"departures today and tomorrow · thresholds learned per route · "
                    f"floor {cfg.load_floor_min} min load, {cfg.pick_floor_min} min pick"),
        "summary": {**counts, "text": summary_text(counts)},
        "empty_text": empty,
        "deliveries": rows,
        "more_text": f"and {more} more" if more > 0 else "",
        "accuracy_text": accuracy_text(agg, days=14),
    }
