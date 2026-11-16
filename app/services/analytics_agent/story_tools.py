"""The story tool (step 21): the story of ONE delivery, order, item, request id or pick-list line
from the handheld transaction log, for the analytics profile (Teams Ask).

It reuses the log explorer's `trace` (app/services/logspace_agent/tools.py), which searches the
day first and then the week up to it, and adds what a reader wants at a glance: the picks counted
by outcome, the picks that were not exact, the issues (soft and real errors with their message),
the packages and the photos. The rows carry no link: the log explorer is on the warehouse LAN,
which Teams users outside it cannot open, so the request id travels as text.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.presentation.evidence import _cell
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.log_feed.scope import scope_from
from app.services.logspace_agent import tools as logspace_tools
from app.services.logspace_agent.tools import TRACE_KEYS, Context, Problem

DELIVERY_STORY = "delivery_story"
STORY_TOOL_NAMES = frozenset({DELIVERY_STORY})
ROW_LIMIT = 150

STORY_TOOLS: list[dict[str, Any]] = [
    {
        "name": DELIVERY_STORY,
        "description": (
            "The story of ONE delivery, order, item, request id or pick-list line from the handheld log: "
            "every transaction in time order, the picks counted by outcome (exact, over, partial, zero-pick) "
            "with the ones that were not exact, the issues (soft and real errors with their message), the "
            "packages and the photos, who did it and from when to when. Use it for 'details of delivery …', "
            "'what happened to …', 'has … been picked', 'what is the issue with …'. With no day named it "
            "gathers every day it appears in the 7 days up to today (picked one day, loaded the next), "
            "with `days` saying what happened on each; with a day named it looks on that day first and "
            "then the 7 days up to it. It says where it looked."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "enum": list(TRACE_KEYS), "description": "delivery_number when not said."},
                "value": {"type": "string", "description": "The delivery, order, item, request id or reporting number."},
                "day": {"type": "string", "description": "The day to start from: 'today', 'yesterday' or YYYY-MM-DD; today when not said."},
            },
            "required": ["value"],
            "additionalProperties": False,
        },
    }
]

RECIPES: list[dict[str, Any]] = [
    {"question": "give me the details of delivery 33330", "call": {"tool": DELIVERY_STORY, "value": "33330"},
     "read": "picks, not_exact, issues, packages; the table of transactions is drawn for you"},
    {"question": "has order 7001234 been picked", "call": {"tool": DELIVERY_STORY, "key": "order_number", "value": "7001234"}},
    {"question": "what happened to request 10-2026-11-15_14:15:43.872-3176",
     "call": {"tool": DELIVERY_STORY, "key": "reqid", "value": "10-2026-11-15_14:15:43.872-3176"}},
]


def _day_of(raw: Any, today: date) -> date:
    """The day a story starts from: today, yesterday or an ISO date; today when not understood.

    ("yesterday", 2026-09-18) -> 2026-09-17 ; ("2026-09-01", …) -> 2026-09-01 ; (None, d) -> d
    """
    text = str(raw or "").strip().lower()
    if text in ("", "today"):
        return today
    if text == "yesterday":
        return today - timedelta(days=1)
    try:
        return date.fromisoformat(text)
    except ValueError:
        return today


def _difference(picked: Any, expected: Any) -> str | None:
    """Picked minus expected as a person reads it, so "over by 0.08" is the tool's figure (step 24).

    >>> _difference("4.08", "4.000000"), _difference("4", "7"), _difference(None, "7")
    ('0.08', '-3', None)
    """
    try:
        return format((Decimal(str(picked)) - Decimal(str(expected))).normalize(), "f")
    except (InvalidOperation, TypeError, ValueError):
        return None


def coverage_note(total: int, returned: int) -> str | None:
    """Why the counts may be short: they come from the rows returned, the first ROW_LIMIT in time order.

    >>> coverage_note(43, 43) is None, coverage_note(200, 150)
    (True, 'picks, issues, packages and photos count the first 150 of 200 transactions')
    """
    if returned >= total:
        return None
    return f"picks, issues, packages and photos count the first {returned} of {total} transactions"


def story_days(transactions: list[dict]) -> list[dict]:
    """Each day the rows fall on, in order, with its count, first and last time and who worked it,
    so an answer can say "picked on the 15th, loaded on the 16th".

    >>> story_days([{"date": "2026-11-15", "time": "14:12:16", "user": "B"}, {"date": "2026-11-16", "time": "04:19:15", "user": "P"},
    ...             {"date": "2026-11-16", "time": "05:31:11", "user": "S"}])[1]
    {'date': '2026-11-16', 'transactions': 2, 'first': '04:19:15', 'last': '05:31:11', 'users': ['P', 'S']}
    """
    days: dict[str, dict] = {}
    for t in transactions:
        day = days.setdefault(str(t.get("date") or ""), {"date": str(t.get("date") or ""), "transactions": 0,
                                                          "first": t.get("time"), "last": t.get("time"), "users": set()})
        day["transactions"] += 1
        day["last"] = t.get("time")
        if t.get("user"):
            day["users"].add(t["user"])
    return [{**d, "users": sorted(d["users"])} for d in days.values()]


def story_digest(transactions: list[dict]) -> dict[str, Any]:
    """What a reader wants at a glance from the transaction rows: the picks by outcome, the picks
    that were not exact, the issues with their message, the packages and the photos.

    >>> story_digest([{"method": "ConfirmPickLine", "status": "success", "pick_outcome": "over", "item_number": "1",
    ...     "reporting_number": "9", "QuantityPicked": "4.08", "ExpectedQuantity": "4"},
    ...     {"method": "NewDeliveryPackage", "status": "soft", "time": "07:10:00", "error": "No packages"}])["picks"]
    {'lines': 1, 'exact': 0, 'over': 1, 'partial': 0, 'zero_pick': 0}
    """
    picks = {"lines": 0, "exact": 0, "over": 0, "partial": 0, "zero_pick": 0}
    not_exact: list[dict] = []
    issues: list[dict] = []
    packages = photos = 0
    for t in transactions:
        method, status = t.get("method"), t.get("status")
        outcome = t.get("pick_outcome")
        if method == "ConfirmPickLine" and status == "success":
            picks["lines"] += 1
            key = {"zero-pick": "zero_pick"}.get(outcome or "", outcome or "")
            if key in picks:
                picks[key] += 1
            if outcome and outcome != "exact":
                not_exact.append({"item": t.get("item_number"), "reporting_number": t.get("reporting_number"),
                                  "picked": _cell(t.get("QuantityPicked")), "expected": _cell(t.get("ExpectedQuantity")),
                                  "difference": _difference(t.get("QuantityPicked"), t.get("ExpectedQuantity")),
                                  "outcome": outcome})
        if method == "NewDeliveryPackage" and status == "success":
            packages += 1
        if method == "RecordPackingPhoto" and status == "success":
            photos += 1
        if status in ("soft", "error"):
            issues.append({"time": t.get("time"), "method": method, "status": status, "message": t.get("error")})
    return {"picks": picks, "not_exact": not_exact, "issues": issues, "packages": packages, "photos": photos}


async def story_rows(db: AsyncSession, customer_code: str, args: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """The story of one key's transactions, with the digest; the trace's own search (the day, then
    the week up to it) on the tenant's clock, the rows without links.

    ("acme", {"value": "33330"}) -> {"found": True, "total": 30, "picks": {...}, "transactions": [...], ...}
    """
    # Step 1: Read the key and the day on the warehouse's clock, so "today" is the warehouse's day
    key = str(args.get("key") or "delivery_number")
    value = str(args.get("value") or "").strip()
    if not value:
        raise Problem(["value is required: the delivery, order, item, request id or reporting number"])
    tz_name = await get_customer_timezone(db, customer_code)
    today = now.astimezone(ZoneInfo(tz_name)).date()
    day = _day_of(args.get("day"), today)
    is_day_named = bool(str(args.get("day") or "").strip())
    # Step 2: The log explorer's trace with no feed filter. With no day named, the whole week at
    # once, because stopping at today's rows left out the picking of a delivery loaded today (33330)
    ctx = Context(db, customer_code, scope_from({"date": day.isoformat()}, today), tz_name, today, public_base_url="")
    story = await logspace_tools.trace(ctx, {"key": key, "value": value, "limit": ROW_LIMIT}, whole_week=not is_day_named)
    # Step 3: The rows without the LAN link, and the digest a reader wants first
    rows = [{k: v for k, v in t.items() if k != "link"} for t in story.get("transactions") or []]
    story["transactions"] = rows
    story.update(story_digest(rows))
    # Step 4: Say each day, and put the date on the first and last times when there is more than
    # one day, otherwise "07:10 to 05:05" reads as going backwards
    if rows:
        story["days"] = story_days(rows)
        if len(story["days"]) > 1 and isinstance(story.get("summary"), dict):
            story["summary"]["first"] = f"{rows[0].get('date')} {story['summary'].get('first')}"
            story["summary"]["last"] = f"{rows[-1].get('date')} {story['summary'].get('last')}"
    note = coverage_note(int(story.get("total") or 0), len(rows))
    if note:
        story["counts_note"] = note
    return story


async def run_story_tool(db: AsyncSession, customer_code: str, name: str, args: Mapping[str, Any]) -> str:
    """Run the story tool: the story as JSON, or the problems the model can fix.

    ("delivery_story", {"value": "33330"}) -> '{"found": true, ...}' ; ({"key": "route", ...}) -> problems
    """
    if name != DELIVERY_STORY:
        return json.dumps({"problems": [f"unknown story tool {name!r}"]})
    try:
        out = await story_rows(db, customer_code, args, now=datetime.now(timezone.utc))
    except Problem as exc:
        return json.dumps({"problems": exc.problems})
    return json.dumps(out, default=str)
