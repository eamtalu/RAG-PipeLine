"""The logspace agent's tools (chunk 132): the records the feed's filter fetched, and nothing else.

Every query starts from the feed's own scope (`app/services/log_feed/scope.py`), so the agent reads
exactly the records the person sees; with nothing fetched, today's records. Only `trace` looks
further (the same day in the whole logspace, then the 7 days to that day) and it says so.

Raw log transactions only: `log_transactions` (its columns and its `attributes`, the request's
fields such as QuantityPicked) and, for one transaction, its log lines. No settlement, facts,
metrics or lookups. SELECT only; the tenant and the scope are bound in by the server, never set by
the model. Results carry request ids and explorer links so the answer can cite them.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import date as date_type, datetime, timedelta
from decimal import Decimal
from typing import Any

from langchain_core.tools import BaseTool
from sqlalchemy import Numeric, case, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.agent_core.guards import wrap_tool
from app.services.log_agent import tools as log_tools
from app.services.log_feed.scope import FeedScope, day_conditions, describe, scope_conditions
from app.services.mnp_log_ingestion.pipeline import assignments, time_bounds
from app.services.mnp_log_ingestion.timefmt import _zone
from app.services.notifications.links import transaction_path
from app.settings import settings

#: Columns the model may filter or group by, by the names it sees.
COLUMNS: dict[str, Any] = {
    "method": LogTransaction.method,
    "status": LogTransaction.status,
    "user": LogTransaction.user_name,
    "warehouse": LogTransaction.warehouse,
    "transaction_name": LogTransaction.transaction_name,
    "item_number": LogTransaction.item_number,
    "delivery_number": LogTransaction.delivery_number,
    "order_number": LogTransaction.order_number,
    "reporting_number": LogTransaction.reporting_number,
    "reqid": LogTransaction.reqid,
    "route": LogTransaction.route,
}

#: What a trace can follow: one delivery, order, item, request or pick-list line.
TRACE_KEYS = ("delivery_number", "order_number", "item_number", "reqid", "reporting_number")

#: The request fields worth showing on a row (when the call carried them).
ROW_FIELDS = ("QuantityPicked", "ExpectedQuantity", "QuantityToBePicked", "QuantityAlreadyPacked", "FromLocation",
              "ToLocation", "OrderLine", "PackageNumber", "LotNumber", "Route", "Picker", "PickListSuffix")

#: Device and transport noise, left out of the fields overview.
NOISE_FIELDS = frozenset({"ApiHost", "ApiPort", "DeviceLanguage", "DeviceLocale", "WebServiceMode", "Retry",
                          "MethodName", "MessageType", "ReqId", "DeviceID", "DeviceName"})

WIDEN_DAYS = 7
_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CAMEL = re.compile(r"^[A-Z][a-z]+[A-Za-z0-9]*[a-z][A-Za-z0-9]*$")   # QuantityToBePicked: an attribute name
_CONDITION = re.compile(r"^\s*([A-Za-z_][\w:]*)\s*(==|!=|<=|>=|<|>)\s*(.+?)\s*$")
_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")
# a second comparison or a boolean word inside one condition: the model meant OR / AND in one string
_COMPOUND = re.compile(r"(==|!=|<=|>=|<|>|\|\||&&)|\b(or|and)\b", re.I)
PICK_OUTCOMES = ("zero-pick", "partial", "exact", "over")
_QUOTED = re.compile(r"""^(?:'[^']*'|"[^"]*")$""")   # one whole quoted string


class Problem(ValueError):
    def __init__(self, problems: list[str]):
        super().__init__("; ".join(problems))
        self.problems = problems


# ============================================================== schemas (sent to the model)

_FILTERS = {
    "delivery_number": {"type": "string", "description": "Delivery number (exact)."},
    "order_number": {"type": "string", "description": "Order number (exact)."},
    "item_number": {"type": "string", "description": "Item number (exact)."},
    "reqid": {"type": "string", "description": "Request id of one handheld call (exact)."},
    "reporting_number": {"type": "string", "description": "Pick-list line (reporting number), exact."},
    "user": {"type": "string", "description": "WMS user (exact)."},
    "method": {"type": "string", "description": "Handheld API method, e.g. ConfirmPickLine (exact)."},
    "status": {"type": "string", "enum": ["success", "soft", "error", "incomplete"]},
    "time_from": {"type": "string", "description": "Local time HH:MM, inclusive."},
    "time_to": {"type": "string", "description": "Local time HH:MM, inclusive."},
    "where": {"type": "array", "items": {"type": "string"},
              "description": "Extra conditions that must ALL hold, on a column or a request field, e.g. "
                             "\"QuantityPicked==0\", "
                             "\"pick_outcome==partial\", \"FromLocation=='JIT'\", \"status==error\"."},
}

LOGSPACE_TOOLS: list[dict] = [
    {"name": "overview",
     "description": "What the fetched records contain: how many, the time span, counts by method, status and user, "
                    "and which request fields each method carries. Call it first when unsure what exists.",
     "input_schema": {"type": "object", "properties": {}}},
    {"name": "find_transactions",
     "description": "Transactions in the fetched records matching the filters, newest first (order \"oldest\" for "
                    "time order). Each row has time, method, status, user, the numbers it carried, its request "
                    "fields (quantities, locations), error text, request id and a link. `total` counts all matches.",
     "input_schema": {"type": "object", "properties": {
         **_FILTERS,
         "order": {"type": "string", "enum": ["newest", "oldest"]},
         "limit": {"type": "integer", "description": "Rows to return (default 20, max 50)."}}}},
    {"name": "trace",
     "description": "The story of ONE delivery, order, item, request id or pick-list line: every transaction for it "
                    "in time order with a summary (methods, statuses, users, errors). Looks in the fetched records "
                    "first; if not there, the whole logspace that day, then the 7 days to that day, and says where "
                    "it looked (`looked_in`, `outside_filter`). Use it for 'what happened to …', 'has … been picked', "
                    "'what is the issue with …'.",
     "input_schema": {"type": "object", "properties": {
         "key": {"type": "string", "enum": list(TRACE_KEYS)},
         "value": {"type": "string"},
         "limit": {"type": "integer", "description": "Rows to return (default 60, max 150)."}},
         "required": ["key", "value"]}},
    {"name": "aggregate",
     "description": "Counts over the fetched records, grouped by up to 3 of: method, status, user, warehouse, hour, "
                    "transaction_name, item_number, delivery_number, order_number, reporting_number, route, "
                    "pick_outcome (zero-pick / partial / exact / over on ConfirmPickLine) or "
                    "attr:<RequestField>; optional sums of numeric request fields (e.g. QuantityPicked). Each row: "
                    "count, errors, sums. `groups_total` / `truncated` say whether more groups exist than returned. "
                    "`total_rows` is the number of matching transactions; `base_rows` the same "
                    "without the `where` conditions, for \"N of M\" (zero-picks of all ConfirmPickLine calls).",
     "input_schema": {"type": "object", "properties": {
         **_FILTERS,
         "group_by": {"type": "array", "items": {"type": "string"}},
         "sum": {"type": "array", "items": {"type": "string"}, "description": "Numeric request fields to sum."},
         "sort": {"type": "string", "description": "\"count\" (default) or \"sum:<Field>\"; largest first."},
         "limit": {"type": "integer", "description": "Groups to return (default 20, max 50)."}}}},
    {"name": "get_transaction",
     "description": "One transaction in full by its id or its request id: header, request fields and the ordered "
                    "log lines (request, M3 calls and results, errors, response). Use it to explain exactly why one "
                    "call failed.",
     "input_schema": {"type": "object", "properties": {
         "transaction_id": {"type": "string", "description": "The transaction id (UUID) or the request id."},
         "max_entries": {"type": "integer", "description": "Log lines to return (default 80, max 200)."}},
         "required": ["transaction_id"]}},
    {"name": "search_entries",
     "description": "Log lines of the fetched records' day containing a text, an M3 program or a level, newest "
                    "first, each with its transaction id.",
     "input_schema": {"type": "object", "properties": {
         "q": {"type": "string", "description": "Case-insensitive text to find in the line."},
         "mi_program": {"type": "string"},
         "level": {"type": "string", "enum": ["INFO", "WARN", "ERROR"]},
         "transaction_id": {"type": "string"},
         "limit": {"type": "integer", "description": "Lines to return (default 30, max 60)."}}}},
]


# ============================================================== conditions

def _attr(name: str):
    return LogTransaction.attributes[name].astext


def _numeric(expr):
    """A request field as a number when it is one, NULL otherwise (never a cast error)."""
    return case((expr.op("~")(r"^-?[0-9]+(\.[0-9]+)?$"), cast(expr, Numeric)), else_=None)


def _pick_outcome():
    """zero-pick / partial / exact / over for a ConfirmPickLine, judged as numbers in SQL; NULL
    for a call that does not carry both quantities."""
    picked, expected = _numeric(_attr("QuantityPicked")), _numeric(_attr("ExpectedQuantity"))
    ok = LogTransaction.status == LogTransactionStatus.success   # a failed confirm picked nothing
    return case((ok & (picked == 0), "zero-pick"), (ok & (picked < expected), "partial"),
                (ok & (picked > expected), "over"), (ok & (picked == expected), "exact"), else_=None)


def outcome_of(attrs: dict, status: LogTransactionStatus | None = LogTransactionStatus.success) -> str | None:
    """The same outcome for one row, in Python."""
    if status != LogTransactionStatus.success:
        return None
    try:
        picked, expected = Decimal(str(attrs["QuantityPicked"])), Decimal(str(attrs["ExpectedQuantity"]))
    except (KeyError, ArithmeticError, ValueError):
        return None
    if picked == 0:
        return "zero-pick"
    return "partial" if picked < expected else "over" if picked > expected else "exact"


COLUMNS["pick_outcome"] = _pick_outcome()


def _operand(token: str):
    """(expression, is_numeric_capable) for the left side: a known column, else a request field."""
    name = token[5:] if token.startswith("attr:") else token
    if token in COLUMNS:
        return COLUMNS[token], False
    if not _FIELD.match(name):
        raise ValueError(f"not a field: {token!r}")
    return _attr(name), True


def _condition(text: str):
    m = _CONDITION.match(text or "")
    if not m:
        raise ValueError(f"cannot read the condition {text!r}: write field, operator (== != < <= > >=) and value")
    left, op, right = m.groups()
    quoted_value = bool(_QUOTED.match(right))
    if not quoted_value and _COMPOUND.search(right):
        raise ValueError(f"{text!r}: one condition per item, and all of them must hold; there is no OR. "
                         f"To compare values, group_by the field instead.")
    col, is_attr = _operand(left)
    quoted = quoted_value
    if quoted:
        value = right[1:-1]
    elif _NUMBER.match(right):
        if not is_attr:
            raise ValueError(f"{left!r} is not a number")
        lhs, rhs = _numeric(col), float(right)
        return {"==": lhs == rhs, "!=": lhs != rhs, "<": lhs < rhs, "<=": lhs <= rhs, ">": lhs > rhs, ">=": lhs >= rhs}[op]
    elif _CAMEL.match(right) and is_attr:
        other = _numeric(_attr(right))  # field against field: QuantityPicked<ExpectedQuantity
        lhs = _numeric(col)
        return {"==": lhs == other, "!=": lhs != other, "<": lhs < other, "<=": lhs <= other,
                ">": lhs > other, ">=": lhs >= other}[op]
    else:
        value = right
    if op not in ("==", "!="):
        raise ValueError(f"{text!r}: text can only be compared with == or !=")
    if left == "pick_outcome" and value not in PICK_OUTCOMES:
        raise ValueError(f"pick_outcome is one of {', '.join(PICK_OUTCOMES)}, not {value!r}")
    if left == "status":
        try:
            value = LogTransactionStatus(value)
        except ValueError:
            raise ValueError(f"status is one of success, soft, error, incomplete, not {value!r}")
    return col == value if op == "==" else col != value


def _local_time(day: date_type, hhmm: str, tz_name: str) -> datetime:
    h, m = (int(x) for x in hhmm.strip().split(":")[:2])
    return datetime(day.year, day.month, day.day, h, m, tzinfo=_zone(tz_name))


def _filter_conditions(args: dict, day: date_type, tz_name: str) -> list:
    conds, problems = [], []
    for key in ("delivery_number", "order_number", "item_number", "reqid", "reporting_number", "user", "method"):
        if args.get(key):
            conds.append(COLUMNS[key] == str(args[key]).strip())
    if args.get("status"):
        try:
            conds.append(LogTransaction.status == LogTransactionStatus(args["status"]))
        except ValueError:
            problems.append(f"status is one of success, soft, error, incomplete, not {args['status']!r}")
    for key, op in (("time_from", ">="), ("time_to", "<=")):
        if args.get(key):
            try:
                t = _local_time(day, args[key], tz_name)
                conds.append(LogTransaction.started_at >= t if op == ">=" else
                             LogTransaction.started_at <= t + timedelta(seconds=59))
            except (ValueError, TypeError):
                problems.append(f"{key} must be HH:MM, not {args[key]!r}")
    required: dict[str, str] = {}
    for text in args.get("where") or []:
        m = _CONDITION.match(str(text))
        if m and m.group(2) == "==":
            field, value = m.group(1), m.group(3).strip().strip("'\"")
            if field in required and required[field] != value:
                problems.append(f"{field}=={required[field]} and {field}=={value} cannot both hold: conditions in "
                                f"`where` must all hold at once. To compare them, group_by [\"{field}\"] instead.")
                continue
            required[field] = value
        try:
            conds.append(_condition(str(text)))
        except ValueError as exc:
            problems.append(str(exc))
    if problems:
        raise Problem(problems)
    return conds


_QTY_NOTE = ("QuantityPicked conditions also count failed and soft calls. Zero-picks and partials are "
             "pick_outcome==zero-pick / pick_outcome==partial (successful confirms only): use those.")


def _with_note(result: dict, args: dict) -> dict:
    """A hint in the result when the model filtered on QuantityPicked instead of pick_outcome."""
    if any("QuantityPicked" in str(w) for w in (args.get("where") or [])):
        result["note"] = _QTY_NOTE
    return result


def _clamp(value, default: int, hi: int) -> int:
    try:
        return max(1, min(int(value), hi))
    except (TypeError, ValueError):
        return default


# ============================================================== rows

def _error_excerpt(text: str | None, head: int = 160, tail: int = 240) -> str | None:
    """A long error text as its start and its end: a failed M3 call's text is the request URL
    first and the actual exception last, and the exception is the part that explains it."""
    if not text:
        return None
    text = text.strip()
    return text if len(text) <= head + tail + 3 else f"{text[:head]} … {text[-tail:]}"


def _link(t: LogTransaction) -> str | None:
    path = transaction_path({"reqid": t.reqid, "date": t.date.isoformat() if t.date else None,
                             "transaction_id": str(t.id)})
    base = (settings.app_public_base_url or "").rstrip("/")
    return f"{base}{path}" if path else None


def _time(t: LogTransaction, tz_name: str) -> str | None:
    return t.started_at.astimezone(_zone(tz_name)).strftime("%H:%M:%S") if t.started_at else None


def _row(t: LogTransaction, tz_name: str) -> dict:
    attrs = t.attributes if isinstance(t.attributes, dict) else {}
    row = {
        "id": str(t.id), "date": t.date.isoformat() if t.date else None, "time": _time(t, tz_name),
        "method": t.method, "status": t.status.value if t.status else None, "user": t.user_name,
        "delivery_number": t.delivery_number, "order_number": t.order_number, "item_number": t.item_number,
        "reporting_number": t.reporting_number, "reqid": t.reqid, "duration_ms": t.duration_ms,
        "error": _error_excerpt(t.error_text),
        **{k: attrs.get(k) for k in ROW_FIELDS if attrs.get(k) not in (None, "")},
        "pick_outcome": outcome_of(attrs, t.status),
        "link": _link(t),
    }
    return {k: v for k, v in row.items() if v is not None}


# ============================================================== tools

class Context:
    def __init__(self, db: AsyncSession, customer_code: str, scope: FeedScope, tz_name: str, today: date_type):
        self.db, self.customer_code, self.scope, self.tz_name, self.today = db, customer_code, scope, tz_name, today

    @property
    def records(self) -> str:
        return describe(self.scope, today=self.today)

    def conds(self) -> list:
        return scope_conditions(self.customer_code, self.scope, self.tz_name)


async def overview(ctx: Context, args: dict) -> dict:
    conds = ctx.conds()
    db = ctx.db
    total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds)) or 0
    by_status = {s.value: n for s, n in (await db.execute(
        select(LogTransaction.status, func.count()).where(*conds).group_by(LogTransaction.status))).all()}
    errors = func.count().filter(LogTransaction.status == LogTransactionStatus.error)
    soft = func.count().filter(LogTransaction.status == LogTransactionStatus.soft)
    methods = (await db.execute(select(LogTransaction.method, func.count(), errors, soft).where(*conds)
                                .group_by(LogTransaction.method).order_by(func.count().desc()).limit(20))).all()
    users = (await db.execute(select(LogTransaction.user_name, func.count()).where(*conds)
                              .group_by(LogTransaction.user_name).order_by(func.count().desc()).limit(15))).all()
    span = (await db.execute(select(func.min(LogTransaction.started_at), func.max(LogTransaction.started_at))
                             .where(*conds))).one()
    top = [m for m, *_ in methods[:12] if m]
    fields: dict[str, list[str]] = {}
    if top:
        # a row whose attributes are not a JSON object reads as {} (jsonb_object_keys would raise)
        as_object = case((func.jsonb_typeof(LogTransaction.attributes) == "object", LogTransaction.attributes),
                         else_=func.cast("{}", LogTransaction.attributes.type))
        key = func.jsonb_object_keys(as_object).label("key")
        rows = (await db.execute(select(LogTransaction.method, key).where(*conds, LogTransaction.method.in_(top))
                                 .distinct())).all()
        for method, name in rows:
            if name not in NOISE_FIELDS:
                fields.setdefault(method, []).append(name)
        fields = {m: sorted(v) for m, v in fields.items()}
    zone = _zone(ctx.tz_name)
    return {"records": ctx.records, "total": total, "by_status": by_status,
            "by_method": [{"method": m, "count": n, "errors": e, "soft": s} for m, n, e, s in methods],
            "by_user": [{"user": u, "count": n} for u, n in users if u],
            "first": span[0].astimezone(zone).strftime("%H:%M:%S") if span[0] else None,
            "last": span[1].astimezone(zone).strftime("%H:%M:%S") if span[1] else None,
            "fields": fields, "columns": sorted(COLUMNS)}


async def find_transactions(ctx: Context, args: dict) -> dict:
    conds = ctx.conds() + _filter_conditions(args, ctx.scope.day, ctx.tz_name)
    limit = _clamp(args.get("limit"), 20, 50)
    order = LogTransaction.started_at.asc() if args.get("order") == "oldest" else LogTransaction.started_at.desc()
    total = await ctx.db.scalar(select(func.count()).select_from(LogTransaction).where(*conds)) or 0
    rows = (await ctx.db.execute(select(LogTransaction).where(*conds).order_by(order.nullslast(), LogTransaction.id)
                                 .limit(limit))).scalars().all()
    return _with_note({"records": ctx.records, "total": total, "returned": len(rows),
                       "transactions": [_row(t, ctx.tz_name) for t in rows]}, args)


async def trace(ctx: Context, args: dict) -> dict:
    key, value = str(args.get("key") or ""), str(args.get("value") or "").strip()
    if key not in TRACE_KEYS:
        raise Problem([f"key is one of {', '.join(TRACE_KEYS)}, not {key!r}"])
    if not value:
        raise Problem(["value is required"])
    match = COLUMNS[key] == value
    day = ctx.scope.day
    week_from = day - timedelta(days=WIDEN_DAYS - 1)
    week = [LogTransaction.customer_code == ctx.customer_code, LogTransaction.date.between(week_from, day)]
    window = time_bounds.from_local_dates(week_from, day, ctx.tz_name)
    if window is not None:
        week.append(window.covers(LogTransaction.started_at, include_null=False))
    attempts = [
        (ctx.conds(), f"the records your filter fetched: {ctx.records}", False),
        (day_conditions(ctx.customer_code, day, ctx.tz_name), f"all of {day.isoformat()} in this logspace (outside your filter)", True),
        (week, f"this logspace from {week_from.isoformat()} to {day.isoformat()} (outside your filter)", True),
    ]
    if not ctx.scope.explicit or ctx.scope == FeedScope(day=day, explicit=True):
        attempts.pop(1)   # nothing narrowed the day: the whole day is what was already searched
    limit = _clamp(args.get("limit"), 60, 150)
    for conds, where, outside in attempts:
        conds = conds + [match]
        total = await ctx.db.scalar(select(func.count()).select_from(LogTransaction).where(*conds)) or 0
        if not total:
            continue
        rows = (await ctx.db.execute(select(LogTransaction).where(*conds)
                                     .order_by(LogTransaction.started_at.asc().nullslast(), LogTransaction.id)
                                     .limit(limit))).scalars().all()
        by_method = {m: n for m, n in (await ctx.db.execute(
            select(LogTransaction.method, func.count()).where(*conds).group_by(LogTransaction.method)
            .order_by(func.count().desc()))).all()}
        by_status = {s.value: n for s, n in (await ctx.db.execute(
            select(LogTransaction.status, func.count()).where(*conds).group_by(LogTransaction.status))).all()}
        users = sorted({u for (u,) in (await ctx.db.execute(
            select(LogTransaction.user_name).where(*conds).distinct())).all() if u})
        return {"key": key, "value": value, "found": True, "looked_in": where, "outside_filter": outside,
                "total": total, "returned": len(rows),
                "summary": {"by_method": by_method, "by_status": by_status, "users": users,
                            "errors": by_status.get("error", 0),
                            "first": _time(rows[0], ctx.tz_name), "last": _time(rows[-1], ctx.tz_name)},
                "transactions": [_row(t, ctx.tz_name) for t in rows]}
    places = "; ".join(w for _, w, _ in attempts)
    return {"key": key, "value": value, "found": False, "total": 0, "outside_filter": True,
            "looked_in": f"nothing for {key} {value} in {places}", "transactions": []}


def _group(token: str, tz_name: str):
    if token == "hour":
        return func.extract("hour", func.timezone(tz_name, LogTransaction.started_at))
    if token in COLUMNS:
        return COLUMNS[token]
    if token.startswith("attr:") and _FIELD.match(token[5:]):
        return _attr(token[5:])
    raise ValueError(f"cannot group by {token!r}: use {', '.join(sorted(COLUMNS))}, hour or attr:<RequestField>")


def _out(value):
    if isinstance(value, LogTransactionStatus):
        return value.value
    if isinstance(value, (Decimal, float)):
        return int(value) if value == int(value) else float(value)
    return value


async def aggregate(ctx: Context, args: dict) -> dict:
    problems: list[str] = []
    try:
        conds = ctx.conds() + _filter_conditions(args, ctx.scope.day, ctx.tz_name)
    except Problem as exc:
        problems += exc.problems
        conds = []
    group_by = [str(g) for g in (args.get("group_by") or [])][:3]
    groups = []
    for g in group_by:
        try:
            groups.append(_group(g, ctx.tz_name).label(g))
        except ValueError as exc:
            problems.append(str(exc))
    sums = []
    for f in [str(x) for x in (args.get("sum") or [])][:4]:
        if not _FIELD.match(f):
            problems.append(f"cannot sum {f!r}")
            continue
        sums.append(func.sum(_numeric(_attr(f))).label(f"sum:{f}"))
    if problems:
        raise Problem(problems)
    count = func.count().label("count")
    errors = func.count().filter(LogTransaction.status == LogTransactionStatus.error).label("errors")
    sort = str(args.get("sort") or "count")
    order = next((s for s in sums if s.name == sort), count)
    if sort != "count" and order is count:
        raise Problem([f"sort is \"count\" or one of the sums ({', '.join(s.name for s in sums) or 'none asked'})"])
    total = await ctx.db.scalar(select(func.count()).select_from(LogTransaction).where(*conds)) or 0
    # the same count without the `where` conditions: the M in "N of M" ("zero-picks of all confirms")
    base_conds = ctx.conds() + _filter_conditions({k: v for k, v in args.items() if k != "where"},
                                                  ctx.scope.day, ctx.tz_name)
    base = await ctx.db.scalar(select(func.count()).select_from(LogTransaction).where(*base_conds)) or 0
    stmt = select(*groups, count, errors, *sums).where(*conds)
    if groups:
        stmt = stmt.group_by(*groups)
    limit = _clamp(args.get("limit"), 20, 50)
    rows = (await ctx.db.execute(stmt.order_by(order.desc().nullslast(), *[g for g in groups]).limit(limit))).all()
    out = [{k: _out(v) for k, v in r._mapping.items()} for r in rows]
    groups_total = len(out)
    if groups and len(out) == limit:
        groups_total = await ctx.db.scalar(select(func.count()).select_from(
            select(*groups).where(*conds).group_by(*groups).subquery())) or len(out)
    return _with_note({"records": ctx.records, "total_rows": total, "base_rows": base, "groups": len(out),
                       "groups_total": groups_total, "truncated": groups_total > len(out), "group_by": group_by,
                       "sort": {"by": order.name, "dir": "desc"}, "rows": out}, args)


async def _by_request_id(ctx: Context, reqid: str) -> str | None:
    """The transaction id for a request id: the scope's day first, then the 7 days to it."""
    day = ctx.scope.day
    week_from = day - timedelta(days=WIDEN_DAYS - 1)
    conds = [LogTransaction.customer_code == ctx.customer_code, LogTransaction.reqid == reqid,
             LogTransaction.date.between(week_from, day)]
    window = time_bounds.from_local_dates(week_from, day, ctx.tz_name)
    if window is not None:
        conds.append(window.covers(LogTransaction.started_at, include_null=False))
    found = await ctx.db.scalar(select(LogTransaction.id).where(*conds)
                                .order_by(LogTransaction.started_at.desc().nullslast()).limit(1))
    return str(found) if found else None


async def get_transaction(ctx: Context, args: dict) -> dict:
    raw = str(args.get("transaction_id") or "").strip()
    try:
        uuid.UUID(raw)
    except ValueError:
        # the model often has a request id ("13-2026-09-30_18:37:56.933-7133"), not the internal id
        found = await _by_request_id(ctx, raw)
        if found is None:
            return {"error": f"no transaction with id or request id {raw!r} in this logspace "
                             f"from {(ctx.scope.day - timedelta(days=WIDEN_DAYS - 1)).isoformat()} to {ctx.scope.day.isoformat()}"}
        args = {**args, "transaction_id": found}
    result = await log_tools._get_transaction(ctx.db, args, ctx.customer_code)
    txn = result.get("transaction") if isinstance(result, dict) else None
    if txn:
        txn["link"] = (settings.app_public_base_url or "").rstrip("/") + (transaction_path(
            {"reqid": txn.get("reqid"), "date": txn.get("date"), "transaction_id": txn.get("id")}) or "")
    return result


async def search_entries(ctx: Context, args: dict) -> dict:
    conds = [LogEntry.customer_code == ctx.customer_code]
    window = time_bounds.from_local_dates(ctx.scope.day, ctx.scope.day, ctx.tz_name)
    if window is not None:
        conds.append(window.covers(LogEntry.timestamp, include_null=False))  # never the whole 60-day table
    if args.get("q"):
        conds.append(LogEntry.message.ilike(f"%{args['q']}%"))
    if args.get("mi_program"):
        conds.append(LogEntry.mi_program == args["mi_program"])
    if args.get("level"):
        conds.append(LogEntry.level == str(args["level"]).upper())
    if args.get("transaction_id"):
        try:
            conds.append(assignments.belongs_to_transaction(uuid.UUID(str(args["transaction_id"]))))
        except (ValueError, AttributeError):
            raise Problem([f"not a transaction id: {args['transaction_id']!r}"])
    limit = _clamp(args.get("limit"), 30, 60)
    rows = (await ctx.db.execute(select(LogEntry).where(*conds)
                                 .order_by(LogEntry.timestamp.desc().nullslast(), LogEntry.line_number.desc())
                                 .limit(limit))).scalars().all()
    owner = await assignments.load_transaction_by_entry(ctx.db, [e.id for e in rows])
    zone = _zone(ctx.tz_name)
    lines = [{k: v for k, v in {
        "transaction_id": str(owner[e.id]) if e.id in owner else None,
        "time": e.timestamp.astimezone(zone).strftime("%H:%M:%S") if e.timestamp else None,
        "level": e.level, "mi_program": e.mi_program, "mi_transaction": e.mi_transaction,
        "message": (e.message or "")[:400]}.items() if v is not None} for e in rows]
    return {"records": ctx.records, "returned": len(lines), "entries": lines}


DISPATCH = {"overview": overview, "find_transactions": find_transactions, "trace": trace,
            "aggregate": aggregate, "get_transaction": get_transaction, "search_entries": search_entries}


def _json(value) -> str:
    return json.dumps(value, default=str)


async def run_logspace_tool(name: str, args: dict, db: AsyncSession, customer_code: str, *,
                            scope: FeedScope, tz_name: str, today: date_type) -> str:
    """One tool call; its problems come back as the result so the model can read and fix them."""
    fn = DISPATCH.get(name)
    if fn is None:
        return _json({"error": f"unknown tool {name!r}", "tools": sorted(DISPATCH)})
    try:
        return _json(await fn(Context(db, customer_code, scope, tz_name, today), args or {}))
    except Problem as exc:
        return _json({"error": "the question cannot be asked as spelled", "problems": exc.problems})
    except Exception as exc:  # noqa: BLE001 - surfaced to the model, never crashes the loop
        return _json({"error": f"{type(exc).__name__}: {exc}"})


def build_tools(db: AsyncSession, customer_code: str, scope: FeedScope, tz_name: str,
                today: date_type) -> list[BaseTool]:
    """Every tool the logspace agent may call, bound to this request's session, tenant and scope."""
    async def run(name: str, args: dict) -> str:
        return await run_logspace_tool(name, args, db, customer_code, scope=scope, tz_name=tz_name, today=today)

    return [wrap_tool(spec, run) for spec in LOGSPACE_TOOLS]
