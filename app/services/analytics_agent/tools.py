"""The agent's tools, bound to one request's database session and tenant. Chunk 124.

Two families, one list:

- the eight the Claude debugging agent already has (search_transactions … explain_freshness),
  wrapped unchanged from `log_agent.tools`, so the LangGraph agent answers the same questions;
- four over the pick-release settlement, calling `settle_reads`, the same functions the HTTP
  endpoints call. Nothing here queries the database itself.

Every tool takes the tenant from the closure, never from the model. Results are JSON text, which
is what a tool message carries; a problem is returned AS the result, not raised, so the model reads
it and corrects the field name instead of the turn failing.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

from langchain_core.tools import BaseTool, StructuredTool
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import lookup_store
from app.services.analytics import settle_query
from app.services.analytics import settle_reads
from app.services.log_agent import tools as log_tools

MAX_WINDOW_DAYS = 92
DEFAULT_WINDOW = timedelta(hours=24)
LIST_LIMIT = 100
GROUP_LIMIT = 500


def _json(value: Any) -> str:
    return json.dumps(value, default=str)


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _window(start, end) -> tuple[datetime, datetime, list[str]]:
    """The requested window, bounded. Notes say what was changed and why."""
    notes: list[str] = []
    until = _parse_dt(end) or datetime.now(timezone.utc)
    since = _parse_dt(start) or (until - DEFAULT_WINDOW)
    if since >= until:
        raise ValueError("`start` must be before `end`.")
    cap = timedelta(days=MAX_WINDOW_DAYS)
    if until - since > cap:
        since = until - cap
        notes.append(f"window clamped to the last {MAX_WINDOW_DAYS} days ending {until.isoformat()}")
    return since, until, notes


# ============================================================== the four release tools

RELEASE_TOOLS: list[dict] = [
    {
        "name": "describe_releases",
        "description": (
            "What the settled release tables hold for this logspace: each settlement's name, what one "
            "row stands for (its key), the fields you can group, filter and sort by, the settled values, "
            "the lookups reachable as group_by (customer name, item description), the time buckets, the "
            "comparison operators and the stat kinds. CALL THIS FIRST before aggregate_releases or "
            "list_releases so every field name you use is real."
        ),
        "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "aggregate_releases",
        "description": (
            "Count and sum settled releases (one row = one pick-list release) grouped by fields, "
            "lookups or time buckets, with optional filters and per-group statistics. This answers "
            "'how many / how much / what share, by X'. EVERY call returns per group, without asking: "
            "rows (how many releases), deliveries (distinct deliveries), calls, and the SUM of every "
            "settled value (expected, picked, "
            "shortfall, is_short, refused, empty_visits, duration_s). Do NOT put sum:… or count:… in "
            "stat; stat is only for median, p90, p95, p99, mean, min, max and distinct. Ratios are "
            "yours to compute from one call's numbers: zero-pick rate = rows with picked==0 (a second "
            "call with that filter) over rows. Never add zero-pick and partial together."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "settlement": {"type": "string", "description": "Settlement name, default pick_release."},
                "group_by": {"type": "array", "items": {"type": "string"},
                             "description": "Fields, e.g. [\"user_name\"], [\"item_number\", "
                                            "\"lookup:item description.ItemDescription\"], or time "
                                            "buckets hour, hour_start, day, week. Empty = one total."},
                "where": {"type": "array", "items": {"type": "string"},
                          "description": "Filters as field, comparison, value: \"is_short==1\", "
                                         "\"picked==0\", \"shortfall<0\", \"duration_s>300\", "
                                         "\"user_name==BCHAM\". Comparisons: == != < <= > >=."},
                "stat": {"type": "array", "items": {"type": "string"},
                         "description": "Optional per-group statistics as kind:field, kinds median, p90, "
                                        "p95, p99, mean, min, max, distinct only: median:duration_s, "
                                        "distinct:delivery_number. Sums and counts need no stat."},
                "sort": {"type": "string",
                         "description": "Order the groups by this before the limit: units_short (biggest "
                                        "shortfall first; use this for 'top N shorted / by units short'), rows, "
                                        "calls, any settled value's sum (picked, expected, refused, "
                                        "duration_s …) or a stat label."},
                "dir": {"type": "string", "enum": ["asc", "desc"], "description": "Sort direction, default desc."},
                "day": {"type": "string", "description": "One whole day on the warehouse's clock: 'today', "
                                                          "'yesterday' or YYYY-MM-DD. Use this for a single "
                                                          "day instead of start and end."},
                "day": {"type": "string", "description": "One whole day on the warehouse's clock: 'today', "
                                                          "'yesterday' or YYYY-MM-DD. Use this for a single "
                                                          "day instead of start and end."},
                "start": {"type": "string", "description": "ISO-8601 start (inclusive). Default: 24 hours before end."},
                "end": {"type": "string", "description": "ISO-8601 end (exclusive). Default: now."},
                "limit": {"type": "integer", "description": f"Max groups (default 200, max {GROUP_LIMIT})."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "list_releases",
        "description": (
            "The settled releases themselves, one row each, with the customer name, item description and "
            "unit looked up. Use for 'show me the lines that …', 'the longest / most refused / zero-pick "
            "lines', 'what did picker X do'. Filter with where, sort by any field. Returns total (how "
            "many match) and up to limit rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "settlement": {"type": "string", "description": "Settlement name, default pick_release."},
                "where": {"type": "array", "items": {"type": "string"},
                          "description": "Filters, same spelling as aggregate_releases."},
                "sort": {"type": "string", "description": "Field to sort by, e.g. duration_s, shortfall, event_time."},
                "dir": {"type": "string", "enum": ["asc", "desc"], "description": "Sort direction, default desc."},
                "search": {"type": "string", "description": "Matches the key, delivery, item or lot number."},
                "start": {"type": "string", "description": "ISO-8601 start (inclusive). Default: 24 hours before end."},
                "end": {"type": "string", "description": "ISO-8601 end (exclusive). Default: now."},
                "limit": {"type": "integer", "description": f"Max rows (default 20, max {LIST_LIMIT})."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "explain_release",
        "description": (
            "One release by its key (the reporting number): every handheld call behind it, oldest "
            "first, each accepted or refused by the ERP with the quantity it carried, and the settled "
            "row they became. Use to explain why a release says what it says, e.g. 'why does 540551 "
            "show 9 picked'. Calls are listed, never added up."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "settlement": {"type": "string", "description": "Settlement name, default pick_release."},
                "key": {"type": "string", "description": "The release's key value, e.g. 540551."},
            },
            "required": ["key"],
            "additionalProperties": False,
        },
    },
]


def _sort_words(sort, direction) -> tuple[str | None, bool]:
    """`sort="calls desc"` means field calls, direction desc: a model writes SQL habits into one
    string, and refusing it twice cost a live question two tool rounds. Returns (field, descending)."""
    field = (str(sort).strip() or None) if sort else None
    descending = (direction or "desc") != "asc"
    if field and " " in field:
        head, _, tail = field.partition(" ")
        if tail.strip().lower() in ("asc", "desc"):
            field, descending = head.strip(), tail.strip().lower() == "desc"
    return field, descending


#: Worked examples a model copies rather than reasons about. Each is the exact aggregate_releases
#: call for a question people ask; a live question about "deliveries per day" went to the metric
#: tools, was refused, and came back as a table of pickers by volume.
RECIPES: list[dict] = [
    {"question": "trend of deliveries per day over the last 7 days",
     "call": {"group_by": ["day"], "start": "<7 days ago>", "end": "<now>"},
     "read": "one row per day; 'deliveries' is the distinct delivery count, 'rows' the lines"},
    {"question": "lines per hour today", "call": {"group_by": ["hour"], "day": "today"}},
    {"question": "top 5 shorted products this week",
     "call": {"group_by": ["item_number", "lookup:item description.ItemDescription"], "where": ["shortfall<0"],
              "sort": "units_short", "limit": 5, "start": "<7 days ago>", "end": "<now>"}},
    {"question": "top 10 customers by units short this week",
     "call": {"group_by": ["lookup:delivery.customer_name"], "where": ["shortfall<0"], "sort": "units_short", "limit": 10,
              "start": "<7 days ago>", "end": "<now>"}},
    {"question": "who picked the most lines yesterday", "call": {"group_by": ["user_name"], "sort": "rows", "day": "yesterday"}},
    {"question": "who picked the most units today", "call": {"group_by": ["user_name"], "sort": "picked", "day": "today"}},
    {"question": "how many zero-picks (stock-outs) today", "call": {"where": ["picked==0"], "day": "today"},
     "read": "rows is the number of zero-pick releases"},
    {"question": "zero-pick rate by picker today",
     "call": {"group_by": ["user_name"], "day": "today"},
     "read": "then a second call with where ['picked==0'] and the same group_by; divide rows by rows"},
    {"question": "median seconds per line by picker yesterday",
     "call": {"group_by": ["user_name"], "stat": ["median:duration_s"], "day": "yesterday"}},
    {"question": "which items came up empty most in the last 7 days",
     "call": {"group_by": ["item_number", "lookup:item description.ItemDescription"], "where": ["picked==0"], "sort": "rows",
              "limit": 15, "start": "<7 days ago>", "end": "<now>"}},
    {"question": "refusals by picker this week",
     "call": {"group_by": ["user_name"], "sort": "refused", "start": "<7 days ago>", "end": "<now>"}},
    # Chunk 129: locations and zones. Present only when the settlement declares these values.
    {"question": "what share of picks came from the designated location today, by zone",
     "call": {"group_by": ["designated_zone"], "day": "today"},
     "read": "from_designated summed is the picks from the designated location; divide by rows. Releases "
             "whose designated location is unknown have no from_designated; use rows of a second call "
             "with where ['from_designated>=0'] as the denominator"},
    {"question": "which zones are picks taken from, and how often off the designated location",
     "call": {"group_by": ["from_zone"], "day": "today", "sort": "rows"},
     "read": "rows per zone picked from; from_designated summed is how many of those were on location"},
    {"question": "designated locations most often bypassed this week, and where picks went instead",
     "call": {"group_by": ["designated_location", "from_location"], "where": ["from_designated==0"], "sort": "rows",
              "limit": 15, "start": "<7 days ago>", "end": "<now>"}},
    {"question": "how many lookups pickers make before a pick, by picker today",
     "call": {"group_by": ["user_name"], "day": "today"},
     "read": "lookups summed / rows = lookups per release; empty_checks summed = checks that found no stock"},
    {"question": "how often the picker picked from a location they had checked today",
     "call": {"where": ["from_designated==0"], "day": "today"},
     "read": "picked_from_checked summed / rows = off-location picks taken from a location the picker checked"},
]

#: Counted on every aggregate: how many distinct deliveries the rows in each group touch.
ALWAYS_STATS = ("distinct:delivery_number",)


def _clamp(value, default: int, hi: int) -> int:
    try:
        n = int(value) if value is not None else default
    except (TypeError, ValueError):
        n = default
    return max(1, min(n, hi))


async def describe_releases(db: AsyncSession, args: dict, customer_code: str) -> dict:
    out = []
    counts = dict((await db.execute(
        select(AnalyticsSettledRow.settlement, func.count())
        .where(AnalyticsSettledRow.customer_code == customer_code)
        .group_by(AnalyticsSettledRow.settlement))).all())
    lookups = await lookup_store.load(db, customer_code)
    for row, settlement in await settle_reads.declared_all(db, customer_code):
        carried = {settle_query.plain(c) for c in settlement.carry}
        reachable = [f"lookup:{lk.name}.{a.name}" for lk in lookups.values() if lk.key_field in carried
                     for a in lk.attributes]
        out.append({
            "settlement": settlement.name,
            "description": row.description,
            "enabled": row.enabled,
            "rows": counts.get(settlement.name, 0),
            "one_row_is": f"one distinct {', '.join(settle_query.plain(k) for k in settlement.key)}",
            "reads_method": list(settlement.reads),
            "fields": sorted(settle_query.known_fields(settlement)),
            "settled_values": [{"name": v.name, "rule": v.rule.value, "field": v.field,
                                "statuses": sorted(v.statuses), "only": sorted(v.only),
                                "left": v.left, "right": v.right, "op": v.op} for v in settlement.values],
            "group_by_lookups": reachable,
            "time_buckets": list(settle_query.BUCKETS),
            "comparisons": list(settle_query.OPS),
            "stat_kinds": list(settle_query.STAT_KINDS),
        })
    return {"settlements": out,
            "recipes": RECIPES,
            "how_to_read": ("Every number from aggregate_releases is a sum or count over releases, never "
                            "over handheld calls. expected is the amount asked for, picked what was "
                            "accepted, shortfall = picked - expected (negative = short). is_short is 1 on "
                            "a short release. A zero-pick is picked==0 (stock-out); a partial is short "
                            "with picked>0. duration_s is seconds from the picker starting the line to "
                            "the last confirm. Say the grain ('across N releases') in every answer. "
                            "Locations, when declared: from_location is where the pick was confirmed from, "
                            "designated_location the pick-list line's location, from_zone and "
                            "designated_zone their zones, from_designated 1 when they are the same, "
                            "same_zone 1 when the zones are the same; lookups counts the stock checks "
                            "the picker made on that item in the 3 minutes before, empty_checks those "
                            "that found nothing, picked_from_checked 1 when the pick came from a checked "
                            "location, followed_suggestion 1 when it came from the oldest-stock "
                            "suggestion. A zone written 'A1 | C1' is a location seen in two zones.")}


ALWAYS_RETURNED = ("sum", "count", "total", "rows")


def _stats_asked(stat: list) -> tuple[list[str], list[str]]:
    """Keep the stats the read understands; drop, with a note, the ones a model invents for things
    that every grouped read returns anyway. Qwen3 8B asked for `sum:picked` and `count:releases`
    seven times in a row on the first live run, refused each time, and never answered."""
    kept, notes = [], []
    for raw in stat or []:
        text = str(raw).strip()
        kind = text.split(":", 1)[0].strip().lower() if ":" in text else text.lower()
        if kind in ALWAYS_RETURNED:
            notes.append(f"'{text}' dropped: rows and the sum of every settled value are always returned")
        else:
            kept.append(text)
    return kept, notes


async def _day(db: AsyncSession, customer_code: str, args: dict) -> tuple[str | None, list[str]]:
    """`day` resolved on the tenant's clock into a business_date filter, so the model never does date
    arithmetic: twice a live question read "yesterday" or "today" as the day before."""
    raw = str(args.get("day") or "").strip().lower()
    if not raw:
        return None, []
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    today = datetime.now(tz).date()
    if raw == "today":
        d = today
    elif raw == "yesterday":
        d = today - timedelta(days=1)
    else:
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            raise ValueError(f"`day` must be today, yesterday or YYYY-MM-DD, not {raw!r}")
    return d.isoformat(), [f"day {raw} is {d.isoformat()} on the warehouse's clock; start and end were ignored"]


async def aggregate_releases(db: AsyncSession, args: dict, customer_code: str) -> dict:
    name = str(args.get("settlement") or "pick_release")
    day, day_notes = await _day(db, customer_code, args)
    since, until, notes = _window(None if day else args.get("start"), None if day else args.get("end"))
    if day:
        since, until = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) - timedelta(days=1), datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=2)
    notes += day_notes
    stats, dropped = _stats_asked(list(args.get("stat") or []))
    notes += dropped
    stats += [x for x in ALWAYS_STATS if x not in stats]
    where = list(args.get("where") or []) + ([f"business_date=={day}"] if day else [])
    sort, descending = _sort_words(args.get("sort"), args.get("dir"))
    # "units short" is the shortfall with the sign turned round; the biggest shortfall is the most
    # negative sum. A model that asks for shortfall descending on short releases wants the biggest
    # shortfalls, not the smallest: the first Web Chat run listed ten customers short by 1 unit as
    # the "top 10". Both spellings mean the same read.
    units_short = sort == "units_short" or (
        sort == "shortfall" and descending and any(w.replace(" ", "") in ("shortfall<0", "is_short==1") for w in where))
    if units_short:
        sort, descending = "shortfall", False
        notes.append("sorted by units short, biggest shortfall first")
    out = await settle_reads.grouped(db, customer_code, name, group_by=list(args.get("group_by") or []),
                                     start=since, end=until, where=where, stat=stats,
                                     limit=_clamp(args.get("limit"), 200, GROUP_LIMIT), sort=sort, descending=descending)
    if units_short:
        out["sort"] = {"by": "units_short", "dir": "desc"}
    group_by = list(args.get("group_by") or [])
    if not sort and group_by and group_by[0] in ("day", "hour", "hour_start", "week", "business_date"):
        # a trend reads in time order, not by size
        out["rows"].sort(key=lambda r: str(r["dimensions"][0]))
        out["sort"] = {"by": group_by[0], "dir": "asc"}
    out["window"] = {"start": since.isoformat(), "end": until.isoformat()}
    # The grain figure, added here so the model never has to add the groups up itself.
    out["total_rows"] = sum(int(r.get("rows") or 0) for r in out["rows"])
    out["groups"] = len(out["rows"])
    for r in out["rows"]:
        if "distinct_delivery_number" in r:
            r["deliveries"] = r.pop("distinct_delivery_number")
    out["stats"] = ["deliveries" if x == "distinct_delivery_number" else x for x in out.get("stats", [])]
    out["grain"] = "one row = one pick-list release; every figure is over releases, not calls"
    if notes:
        out["notes"] = notes
    return out


async def list_releases(db: AsyncSession, args: dict, customer_code: str) -> dict:
    name = str(args.get("settlement") or "pick_release")
    day, day_notes = await _day(db, customer_code, args)
    since, until, notes = _window(None if day else args.get("start"), None if day else args.get("end"))
    if day:
        since, until = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) - timedelta(days=1), datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=2)
    notes += day_notes
    sort, descending = _sort_words(args.get("sort"), args.get("dir"))
    out = await settle_reads.listed(db, customer_code, name, start=since, end=until,
                                    search=(str(args["search"]).strip() if args.get("search") else None),
                                    where=list(args.get("where") or []) + ([f"business_date=={day}"] if day else []),
                                    sort=sort, descending=descending,
                                    limit=_clamp(args.get("limit"), 20, LIST_LIMIT), offset=0)
    out["window"] = {"start": since.isoformat(), "end": until.isoformat()}
    if notes:
        out["notes"] = notes
    return out


async def explain_release(db: AsyncSession, args: dict, customer_code: str) -> dict:
    name = str(args.get("settlement") or "pick_release")
    key = str(args.get("key") or "").strip()
    if not key:
        raise ValueError("`key` is required: the release's reporting number.")
    return await settle_reads.explained(db, customer_code, name, [key])


RELEASE_DISPATCH = {
    "describe_releases": describe_releases,
    "aggregate_releases": aggregate_releases,
    "list_releases": list_releases,
    "explain_release": explain_release,
}


# ============================================================== binding

async def run_release_tool(name: str, args: dict, db: AsyncSession, customer_code: str) -> str:
    """One release tool, its problems returned as the result so the model can read them."""
    fn = RELEASE_DISPATCH[name]
    try:
        return _json(await fn(db, args or {}, customer_code))
    except settle_reads.UnknownSettlement as exc:
        return _json({"error": str(exc), "hint": "call describe_releases for the settlements that exist"})
    except settle_reads.ReadProblem as exc:
        return _json({"error": "the question cannot be asked as spelled", "problems": exc.problems,
                      "hint": "fix the field or spelling and try again; describe_releases lists the fields"})
    except Exception as exc:  # noqa: BLE001 - surfaced to the model, never crashes the loop
        return _json({"error": f"{type(exc).__name__}: {exc}"})


def _wrap(spec: dict, runner) -> BaseTool:
    async def call(**kwargs) -> str:
        return await runner(spec["name"], kwargs)

    schema = dict(spec["input_schema"])
    schema.pop("additionalProperties", None)  # some providers reject it; the parser ignores extras anyway
    return StructuredTool.from_function(coroutine=call, name=spec["name"], description=spec["description"],
                                        args_schema=schema)


def build_tools(db: AsyncSession, customer_code: str) -> list[BaseTool]:
    """Every tool the agent may call, bound to this request's session and tenant."""
    async def run_log(name: str, args: dict) -> str:
        return await log_tools.execute_tool(name, args, db, customer_code)

    async def run_release(name: str, args: dict) -> str:
        return await run_release_tool(name, args, db, customer_code)

    return ([_wrap(spec, run_log) for spec in log_tools.TOOLS]
            + [_wrap(spec, run_release) for spec in RELEASE_TOOLS])


TOOL_NAMES = [t["name"] for t in log_tools.TOOLS] + [t["name"] for t in RELEASE_TOOLS]
