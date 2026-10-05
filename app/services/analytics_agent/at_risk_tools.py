"""The assistant's tools over deliveries at risk (chunk 152).

Five tools, the same stores the HTTP endpoints and the Teams card read, so the assistant's figures are
the page's figures:

- describe_at_risk: the vocabulary (the van is the clock; watch, at risk, left behind; missed, held
  the van, fine), the tenant's windows, each route's usual van time, and recipes for common questions;
- at_risk_history: closed deliveries over a day or a range of dates, filtered by word, route, customer,
  picking kind or delivery number, with the counts per word;
- at_risk_board: the open deliveries today or tomorrow, by tier, with the van clock and why each is flagged;
- at_risk_vans: one row per route per day: when the van was ready, when it usually is, how late, and what
  it carried, held or went without;
- explain_at_risk_delivery: one delivery's story: its clocks, the tiers it went through and who checked it.

Every tool takes the tenant from the closure, never from the model; every read is bounded; dates are
resolved on the warehouse's clock so the model never does date arithmetic; and each list-shaped result
carries a `table` the evidence layer draws from, so the card and the prose never disagree.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_at_risk import check_store, delivery_store, model, profile_store, settings_store
from app.services.teams import home_at_risk

LIST_LIMIT = 100
RANGE_DAYS_MAX = 92
DEFAULT_RANGE_DAYS = 7

WORDS = {
    "missed": "the van went without it: a package was never loaded, or lines were never confirmed",
    "held": "it was on the van, but only after the van's usual ready time plus the tenant's allowance, or after the WMS departure: "
            "the van ran noticeably late and this delivery was one of those still going on",
    "fine": "on the van before the van's usual ready time",
    "unknown": "the board lost sight of it before it closed",
}
TIERS = {
    "watch": "picking is not finished and the van is usually ready within the warning window",
    "at_risk": "packages are still off the van and the van is usually ready within the window, or the usual time has passed",
    "left_behind": "the usual time has passed and the dock has been quiet for the gone window: the van is taken as gone and the delivery is not on it",
}
HOW_TO_READ = (
    "The clock is the VAN, not the WMS departure time. The WMS departure (11:30 weekdays, 12:00 Saturdays) is a planning "
    "time; every van is loaded and ready four to five hours before it. Each route learns the time of day its van is usually "
    "ready (the dock's last scan on nine days in ten); deliveries are judged against that time on their departure day. "
    "A route with too few days of history has no rhythm yet and the WMS departure stands in (clock_source wms_departure). "
    "Routes that never scan a load (the BRILA runs) are judged on their last pick of the day. 'van ready' is the last "
    "package scanned onto the route's van that day; 'van late by' is that against the usual time; 'on van' is when the "
    "delivery's own last package went on; 'before van' is how long before the van was ready it went on. Rows marked "
    "reconstructed were written from the logs after the fact by the backfill, not watched live. Counts per word come with "
    "every history call: quote them, never add rows up yourself."
)
RECIPES = [
    {"question": "how many deliveries were missed this week", "call": {"tool": "at_risk_history", "start": "<7 days ago>", "end": "<today>", "category": ["missed"]},
     "read": "counts.missed is the answer; the rows are the deliveries"},
    {"question": "which deliveries held the van yesterday", "call": {"tool": "at_risk_history", "day": "yesterday", "category": ["held"]}},
    {"question": "missed deliveries for Hilton this month", "call": {"tool": "at_risk_history", "start": "<first of month>", "end": "<today>", "category": ["missed"], "customer_name": "Hilton"}},
    {"question": "what is at risk right now", "call": {"tool": "at_risk_board", "window": "today"}, "read": "only flagged rows are returned; summary has the counts"},
    {"question": "is anything left behind", "call": {"tool": "at_risk_board", "window": "today", "tier": "left_behind"}},
    {"question": "when is the BRI06 van usually ready", "call": {"tool": "describe_at_risk"}, "read": "routes[].van_usually_ready"},
    {"question": "which vans ran late this week and by how much", "call": {"tool": "at_risk_vans", "start": "<7 days ago>", "end": "<today>"}, "read": "late_min per van; positive is late"},
    {"question": "how late was BRI04 on 1 October", "call": {"tool": "at_risk_vans", "day": "2026-10-01", "route": "BRI04"}},
    {"question": "what happened to delivery 29467", "call": {"tool": "explain_at_risk_delivery", "delivery_number": "29467"}},
    {"question": "why is 29237 at risk", "call": {"tool": "explain_at_risk_delivery", "delivery_number": "29237"}, "read": "why and the tier story"},
]

AT_RISK_TOOLS: list[dict] = [
    {
        "name": "describe_at_risk",
        "description": (
            "What the deliveries-at-risk data holds for this logspace: the vocabulary (the van is the clock; the tiers "
            "watch, at risk, left behind; the words missed, held the van, fine), the tenant's windows, when each route's "
            "van is usually ready, and recipes for common questions. Call it once before the other at_risk tools, and "
            "answer 'when is the X van usually ready' from it."
        ),
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "at_risk_history",
        "description": (
            "Closed deliveries over a day or a range of departure dates, each with its word (missed, held, fine), its "
            "van clocks and its picking kinds, plus counts per word over the whole range. Use for 'how many were missed "
            "this week', 'which deliveries held the van yesterday', 'missed deliveries for a customer this month'. "
            "Quote counts from `counts`; never add rows up."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "day": {"type": "string", "description": "One departure day on the warehouse's clock: 'today', 'yesterday' or YYYY-MM-DD. Leave start and end out."},
                "start": {"type": "string", "description": "First departure date, YYYY-MM-DD. Default: 6 days before end."},
                "end": {"type": "string", "description": "Last departure date, YYYY-MM-DD, inclusive. Default: today."},
                "category": {"type": "array", "items": {"type": "string", "enum": ["missed", "held", "fine", "unknown"]},
                             "description": "Words to keep. Default: every word."},
                "route": {"type": "string", "description": "One route, e.g. BRI04."},
                "customer_name": {"type": "string", "description": "Part of a customer name, e.g. Hilton."},
                "transaction": {"type": "string", "description": "A picking kind: stock, jit, milk or freezer, or the full transaction name."},
                "delivery": {"type": "string", "description": "A delivery number or its start."},
                "limit": {"type": "integer", "description": f"Max rows (default 20, max {LIST_LIMIT}). Counts cover the whole range regardless."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "at_risk_board",
        "description": (
            "The live board: the open deliveries departing today or tomorrow that are flagged, worst first, each with "
            "its tier, when its van is usually ready, how long until then, lines and packages, and why it is flagged. "
            "Use for 'what is at risk right now', 'is anything left behind', 'how is BRI06 doing'. The summary carries "
            "the counts, fine ones included; the rows are the flagged ones only unless include_fine is true."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "window": {"type": "string", "enum": ["today", "tomorrow", "both"], "description": "Default today."},
                "tier": {"type": "string", "enum": ["watch", "at_risk", "left_behind"], "description": "Only this tier."},
                "route": {"type": "string", "description": "One route, e.g. BRI04."},
                "include_fine": {"type": "boolean", "description": "Also list the deliveries that are on course. Default false."},
                "limit": {"type": "integer", "description": f"Max rows (default 20, max {LIST_LIMIT})."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "at_risk_vans",
        "description": (
            "The vans themselves, one row per route per departure day over the closed rows: when loading started, when "
            "the van was ready, when it is usually ready, how late it ran (late_min, positive is late), and how many "
            "deliveries it carried, held or went without. Use for 'which vans ran late this week', 'how late was BRI04 "
            "on 1 October'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "day": {"type": "string", "description": "One departure day: 'today', 'yesterday' or YYYY-MM-DD."},
                "start": {"type": "string", "description": "First departure date, YYYY-MM-DD. Default: 6 days before end."},
                "end": {"type": "string", "description": "Last departure date, YYYY-MM-DD, inclusive. Default: today."},
                "route": {"type": "string", "description": "One route, e.g. BRI04."},
                "late_only": {"type": "boolean", "description": "Only vans that ran past their usual time. Default false."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "explain_at_risk_delivery",
        "description": (
            "One delivery's story on the at-risk board: its route and customer, its van clocks, lines and packages, the "
            "tiers it went through and when, its word once closed, and who marked it checked. Use for 'what happened to "
            "29467' or 'why is 29237 at risk'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "delivery_number": {"type": "string", "description": "The delivery number, e.g. 29467."},
                "departure_date": {"type": "string", "description": "YYYY-MM-DD, when the delivery has rows on more than one day. Default: the latest."},
            },
            "required": ["delivery_number"],
            "additionalProperties": False,
        },
    },
]

PICK_KINDS = {"stock": "Brighton Stock Pick", "jit": "JIT and Shorts Pick (Brighton)", "milk": "Milk Pick (Brighton)", "freezer": "Freezer Pick (Brighton)"}


def _json(value: Any) -> str:
    return json.dumps(value, default=str)


def _hhmm(at: datetime | None, tz: ZoneInfo) -> str | None:
    return None if at is None else at.astimezone(tz).strftime("%H:%M")


def _minutes(a: datetime | None, b: datetime | None) -> int | None:
    return None if a is None or b is None else int(model.minutes_to_departure(a, b).to_integral_value(rounding="ROUND_HALF_UP"))


def _clamp(value, default: int, hi: int) -> int:
    try:
        n = int(value) if value is not None else default
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, hi))


async def _tz(db: AsyncSession, cc: str) -> ZoneInfo:
    return ZoneInfo(await get_customer_timezone(db, cc))


def _one_day(raw: str, today: date) -> date:
    text = str(raw).strip().lower()
    if text == "today":
        return today
    if text == "yesterday":
        return today - timedelta(days=1)
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"`day` must be today, yesterday or YYYY-MM-DD, not {raw!r}")


def _range(args: dict, today: date) -> tuple[date, date, list[str]]:
    """The departure dates asked for, on the warehouse's clock, bounded."""
    notes: list[str] = []
    if args.get("day"):
        d = _one_day(args["day"], today)
        return d, d, [f"day {args['day']} is {d.isoformat()} on the warehouse's clock"]
    try:
        end = date.fromisoformat(str(args["end"])) if args.get("end") else today
        start = date.fromisoformat(str(args["start"])) if args.get("start") else end - timedelta(days=DEFAULT_RANGE_DAYS - 1)
    except ValueError:
        raise ValueError("`start` and `end` must be YYYY-MM-DD")
    if start > end:
        raise ValueError("`start` must not be after `end`")
    if (end - start).days >= RANGE_DAYS_MAX:
        start = end - timedelta(days=RANGE_DAYS_MAX - 1)
        notes.append(f"range clamped to the {RANGE_DAYS_MAX} days ending {end.isoformat()}")
    if not args.get("start") and not args.get("end"):
        notes.append(f"no dates given: the last {DEFAULT_RANGE_DAYS} days, {start.isoformat()} to {end.isoformat()}")
    return start, end, notes


def _word(row: AnalyticsAtRiskDelivery, held_after: timedelta) -> str:
    loading = True if row.loading_expected is None else bool(row.loading_expected)
    last_at = row.last_load_at if loading else row.last_pick_at
    return model.category_for(outcome=row.outcome, last_at=last_at, usual_ready_at=row.usual_ready_at,
                              lines_expected=row.lines_expected, lines_confirmed=int(row.lines_confirmed or 0), held_after=held_after)


def _row(row: AnalyticsAtRiskDelivery, tz: ZoneInfo, held_after: timedelta, now: datetime) -> dict:
    loading = True if row.loading_expected is None else bool(row.loading_expected)
    last_at = row.last_load_at if loading else row.last_pick_at
    usual = row.usual_ready_at or row.departure_at
    out = {
        "delivery": row.delivery_number, "route": row.route, "customer": row.customer_name, "day": row.departure_date.isoformat(),
        "status": row.status, "word": _word(row, held_after) if row.status == "closed" else "open",
        "tier": row.tier, "highest_tier": row.max_tier, "first_flagged": _hhmm(row.first_flagged_at, tz),
        "van_usually_ready": _hhmm(usual, tz), "clock_source": row.usual_ready_source or "wms_departure",
        "van_ready": _hhmm(row.route_loaded_at, tz), "van_late_min": _minutes(row.route_loaded_at, row.usual_ready_at),
        "loading_from": _hhmm(row.route_loading_from, tz),
        "on_van": _hhmm(last_at, tz), "before_van_min": _minutes(row.route_loaded_at, last_at),
        "lines": home_at_risk.lines_text(row), "packages": home_at_risk.packages_text(row),
        "last": home_at_risk.last_text(row, tz), "picking": list(row.transaction_names or []),
        "wms_departure": _hhmm(row.departure_at, tz), "reconstructed": bool(row.reconstructed),
        "checked": None if row.checked_at is None else {"by": row.checked_by, "at": _hhmm(row.checked_at, tz), "note": row.check_note,
                                                         "re_opened": int(row.reopened_count or 0)},
    }
    if row.status == "open":
        out["until_van"] = home_at_risk.minutes_text(model.minutes_to_departure(usual, now))
        out["why"] = home_at_risk.threshold_text(row, tz) if row.tier != "none" else ""
    return out


def _table(columns: list[str], rows: list[list], title: str, facts: dict) -> dict:
    return {"title": title, "columns": columns, "rows": [[("" if c is None else str(c)) for c in r] for r in rows], "facts": facts}


# ============================================================== the tools

async def describe_at_risk(db: AsyncSession, args: dict, cc: str) -> dict:
    tz = await _tz(db, cc)
    cfg = await settings_store.effective(db, cc)
    profiles = await profile_store.latest(db, cc)
    routes = []
    for route, p in sorted(profiles.items()):
        loading = (p.loaded_sample or 0) > 0
        usual = profile_store.usual_minutes(p, loading_expected=loading)
        hhmm = None if usual is None else f"{int(usual) // 60:02d}:{int(usual) % 60:02d}"
        routes.append({"route": route, "loading_step": loading, "van_usually_ready": hhmm,
                       "clock_source": "learned" if hhmm else "wms_departure", "days_learned_from": p.van_days if loading else p.pick_days,
                       "as_of": p.as_of_date.isoformat()})
    return {
        "how_to_read": HOW_TO_READ,
        "words": WORDS, "tiers": TIERS,
        "settings": {"warn_before_min": cfg.warn_before_min, "gone_after_min": cfg.gone_after_min, "min_days": cfg.min_days,
                     "held_after_min": cfg.held_after_min, "window_days": cfg.window_days, "coverage": str(cfg.coverage)},
        "routes": routes,
        "timezone": tz.key,
        "recipes": RECIPES,
    }


def _transaction(raw) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    return PICK_KINDS.get(text.lower(), text)


async def at_risk_history(db: AsyncSession, args: dict, cc: str) -> dict:
    tz = await _tz(db, cc)
    now = datetime.now(timezone.utc)
    today = now.astimezone(tz).date()
    start, end, notes = _range(args, today)
    cfg = await settings_store.effective(db, cc)
    held_after = timedelta(minutes=cfg.held_after_min)
    categories = [str(c) for c in (args.get("category") or []) if str(c) in model.CATEGORIES] or None
    filters = dict(route=(str(args["route"]).strip().upper() if args.get("route") else None), customer=(str(args["customer_name"]).strip() if args.get("customer_name") else None),
                   transaction=_transaction(args.get("transaction")), delivery=(str(args["delivery"]).strip() if args.get("delivery") else None),
                   held_after=held_after)
    limit = _clamp(args.get("limit"), 20, LIST_LIMIT)
    rows, truncated = await delivery_store.history_rows(db, cc, start=start, end=end, categories=categories, limit=limit, **filters)
    summary = await delivery_store.history_counts(db, cc, start=start, end=end, **filters)
    shaped = [_row(r, tz, held_after, now) for r in rows]
    table = _table(["day", "delivery", "customer", "route", "word", "van ready", "usually by", "van late", "on van", "before van"],
                   [[r["day"], r["delivery"], r["customer"] or "", r["route"] or "", "held the van" if r["word"] == "held" else r["word"],
                     r["van_ready"] or "–", r["van_usually_ready"] or "–", _late_word(r["van_late_min"]), r["on_van"] or "never",
                     _before_word(r["before_van_min"])] for r in shaped],
                   f"Closed deliveries {start.isoformat()} to {end.isoformat()}" + (f", {', '.join(categories)}" if categories else ""),
                   {"counts over the range": ", ".join(f"{k} {v}" for k, v in summary["counts"].items()), "rows shown": f"{len(shaped)}{' of more' if truncated else ''}"})
    return {"range": {"start": start.isoformat(), "end": end.isoformat()}, "counts": summary["counts"], "picking_kinds_seen": summary["transactions"],
            "rows": shaped, "truncated": truncated, "grain": "one row = one delivery on one departure day; counts are over the whole range under every filter except category",
            "held_after_min": cfg.held_after_min, "table": table, **({"notes": notes} if notes else {})}


def _late_word(minutes: int | None) -> str:
    if minutes is None:
        return "–"
    if minutes <= 0:
        return "on time" if minutes > -5 else f"{-minutes} min early"
    return f"{minutes} min late"


def _before_word(minutes: int | None) -> str:
    if minutes is None:
        return "–"
    return "last one on" if minutes == 0 else f"{minutes} min before"


async def at_risk_board(db: AsyncSession, args: dict, cc: str) -> dict:
    tz = await _tz(db, cc)
    now = datetime.now(timezone.utc)
    today = now.astimezone(tz).date()
    window = str(args.get("window") or "today")
    dates = {"today": [today - timedelta(days=1), today], "tomorrow": [today + timedelta(days=1)],
             "both": [today - timedelta(days=1), today, today + timedelta(days=1)]}.get(window)
    if dates is None:
        raise ValueError("`window` must be today, tomorrow or both")
    tier = args.get("tier")
    if tier and tier not in TIERS:
        raise ValueError("`tier` must be watch, at_risk or left_behind")
    cfg = await settings_store.effective(db, cc)
    held_after = timedelta(minutes=cfg.held_after_min)
    rows = await delivery_store.board_rows(db, cc, dates=dates, tiers=[tier] if tier else None, limit=1000)
    route = str(args["route"]).strip().upper() if args.get("route") else None
    if route:
        rows = [r for r in rows if r.route == route]
    summary = {"open": len(rows), "left_behind": sum(r.tier == "left_behind" for r in rows), "at_risk": sum(r.tier == "at_risk" for r in rows),
               "watch": sum(r.tier == "watch" for r in rows), "fine": sum(r.tier == "none" for r in rows), "checked": sum(r.checked_at is not None for r in rows)}
    listed = rows if args.get("include_fine") else [r for r in rows if r.tier != "none"]
    limit = _clamp(args.get("limit"), 20, LIST_LIMIT)
    shaped = [_row(r, tz, held_after, now) for r in listed[:limit]]
    table = _table(["tier", "delivery", "route", "customer", "van usually ready", "until van", "lines", "packages", "why"],
                   [[home_at_risk.TIER_TEXT.get(r["tier"], r["tier"]), r["delivery"], r["route"] or "", r["customer"] or "",
                     (r["van_usually_ready"] or "") + (" (WMS)" if r["clock_source"] == "wms_departure" else ""), r.get("until_van", ""),
                     r["lines"], r["packages"], r.get("why", "")] for r in shaped],
                   f"Board · {window}", {"summary": home_at_risk.summary_text({**summary, "open": summary["open"]}), "as of": _hhmm(now, tz) or ""})
    return {"window": window, "dates": [d.isoformat() for d in dates], "summary": summary, "rows": shaped,
            "truncated": len(listed) > limit, "grain": "one row = one open delivery; fine ones are counted in summary and listed only with include_fine",
            "table": table}


async def at_risk_vans(db: AsyncSession, args: dict, cc: str) -> dict:
    tz = await _tz(db, cc)
    today = datetime.now(tz).date()
    start, end, notes = _range(args, today)
    cfg = await settings_store.effective(db, cc)
    vans = await delivery_store.vans(db, cc, start=start, end=end, held_after=timedelta(minutes=cfg.held_after_min))
    route = str(args["route"]).strip().upper() if args.get("route") else None
    out = []
    for v in vans:
        if route and v["route"] != route:
            continue
        late = None if v["late_min"] is None else int(Decimal(v["late_min"]).to_integral_value(rounding="ROUND_HALF_UP"))
        if args.get("late_only") and not (late is not None and late > 0):
            continue
        out.append({"day": v["departure_date"].isoformat(), "route": v["route"], "loading_step": v["loading_expected"],
                    "loading_from": _hhmm(v["loading_from"], tz), "van_ready": _hhmm(v["ready_at"], tz), "van_usually_ready": _hhmm(v["usual_ready_at"], tz),
                    "clock_source": v["usual_ready_source"], "late_min": late, "deliveries": v["deliveries"], "held": v["held"], "missed": v["missed"],
                    "fine": v["fine"], "reconstructed": v["reconstructed"]})
    table = _table(["day", "route", "loading from", "van ready", "usually by", "late by", "deliveries", "held", "missed"],
                   [[r["day"], r["route"], r["loading_from"] or ("no loading step" if not r["loading_step"] else "–"), r["van_ready"] or "–",
                     (r["van_usually_ready"] or "–") + (" (WMS)" if r["clock_source"] == "wms_departure" else ""), _late_word(r["late_min"]),
                     r["deliveries"], r["held"] or "", r["missed"] or ""] for r in out],
                   f"Vans {start.isoformat()} to {end.isoformat()}" + (f" · {route}" if route else ""),
                   {"vans": str(len(out)), "ran late": str(sum(1 for r in out if r["late_min"] is not None and r["late_min"] > 0))})
    return {"range": {"start": start.isoformat(), "end": end.isoformat()}, "vans": out, "grain": "one row = one route's van on one day",
            "table": table, **({"notes": notes} if notes else {})}


async def explain_at_risk_delivery(db: AsyncSession, args: dict, cc: str) -> dict:
    number = str(args.get("delivery_number") or "").strip()
    if not number:
        raise ValueError("`delivery_number` is required")
    tz = await _tz(db, cc)
    now = datetime.now(timezone.utc)
    cfg = await settings_store.effective(db, cc)
    rows = (await delivery_store.rows_for(db, cc, [number])).get(number, [])
    if not rows:
        return {"delivery": number, "found": False, "note": "no row on the at-risk board for this delivery: it was never named by a routing call in the window the board keeps"}
    if args.get("departure_date"):
        wanted = date.fromisoformat(str(args["departure_date"]))
        rows = [r for r in rows if r.departure_date == wanted] or rows
    row = sorted(rows, key=lambda r: r.departure_date)[-1]
    story = []
    for h in row.tier_history or []:
        at = datetime.fromisoformat(h["at"]).astimezone(tz).strftime("%H:%M") if h.get("at") else None
        story.append({"tier": home_at_risk.TIER_TEXT.get(h.get("tier"), h.get("tier")), "at": at,
                      "minutes_to_usual_ready": h.get("minutes_to_usual_ready"), "clock_source": h.get("source")})
    ledger, _ = await check_store.list_checks(db, cc, start=None, end=None, limit=200, after=None)
    checks = [{"action": c.action, "tier": c.tier, "by": c.actor, "note": c.note, "at": _hhmm(c.at, tz), "day": c.departure_date.isoformat()}
              for c in ledger if c.delivery_number == number]
    shaped = _row(row, tz, timedelta(minutes=cfg.held_after_min), now)
    if row.status == "closed":
        shaped["why"] = home_at_risk.threshold_text(row, tz) if row.max_tier != "none" else "never flagged"
    return {"delivery": number, "found": True, "other_days": [r.departure_date.isoformat() for r in rows if r is not row], **shaped,
            "tier_story": story, "checks": checks, "outcome": row.outcome, "rule_version": row.rule_version}


AT_RISK_DISPATCH = {
    "describe_at_risk": describe_at_risk,
    "at_risk_history": at_risk_history,
    "at_risk_board": at_risk_board,
    "at_risk_vans": at_risk_vans,
    "explain_at_risk_delivery": explain_at_risk_delivery,
}
#: The tools whose result carries a `table` the evidence layer draws.
TABLE_TOOLS = ("at_risk_history", "at_risk_board", "at_risk_vans")


async def run_at_risk_tool(name: str, args: dict, db: AsyncSession, customer_code: str) -> str:
    """One at-risk tool, its problems returned as the result so the model can read them."""
    fn = AT_RISK_DISPATCH[name]
    try:
        return _json(await fn(db, args or {}, customer_code))
    except ValueError as exc:
        return _json({"error": str(exc), "hint": "fix the argument it names and call once more; describe_at_risk lists the vocabulary"})
    except Exception as exc:  # noqa: BLE001 - surfaced to the model, never crashes the loop
        return _json({"error": f"{type(exc).__name__}: {exc}"})
