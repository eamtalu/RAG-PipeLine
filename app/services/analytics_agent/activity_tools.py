"""The activity tool (step 20): who was active on the WMS, when, on which devices, from the
transaction log.

A user counts as active in an hour (or a day) when they completed at least one transaction in
it, which is how the log explorer's "Day by hour" page reads the same table. Every figure is a
count Postgres gave over `log_transactions` on the tenant's clock; the model names the window
and the cut, never a number. Open to everybody for now (the owner's decision of 14 November 2026;
a role rule comes later).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import Integer, and_, case, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_agent import activity_grid

WMS_ACTIVITY = "wms_activity"
ACTIVITY_TOOL_NAMES = frozenset({WMS_ACTIVITY})
GRAINS = ("hour", "day", "week")
CUTS = ("none", "user", "device", "warehouse", "method")
MAX_DAYS = 92
MAX_MATRIX_DAYS = 62
DEFAULT_DAYS = 14
PEOPLE_LIMIT = 50
GROUP_LIMIT = 200
#: A bound on the people one company filter carries; a site has tens, a whole customer hundreds.
MAX_COMPANY_WORKERS = 2000
GRAIN_NOTE = "a user counts as active in an hour or a day when they completed at least one transaction in it"
HOURS = [f"{h:02d}" for h in range(24)]

#: Worked examples the model reads with the schema, as the other analytics tools carry them.
RECIPES: list[dict[str, Any]] = [
    {"question": "how many different users were active each day in the last 2 weeks",
     "call": {"tool": WMS_ACTIVITY, "grain": "day", "days": 14}, "read": "rows[].users per day; the lift draws the line"},
    {"question": "show active users per hour for the last 3 weeks",
     "call": {"tool": WMS_ACTIVITY, "grain": "hour", "days": 21}, "read": "the matrix; the heat map is drawn for you"},
    {"question": "who was active today and when did each start and finish",
     "call": {"tool": WMS_ACTIVITY, "by": "user", "day": "today"}, "read": "people[].first_at and last_at; show_table from rows"},
    {"question": "how many devices were in use per day last month",
     "call": {"tool": WMS_ACTIVITY, "grain": "day", "days": 30}, "read": "rows[].devices; chart_from_data measure devices"},
    {"question": "which hours had the most users this month",
     "call": {"tool": WMS_ACTIVITY, "grain": "hour", "days": 30}, "read": "the matrix; name the busiest cells with their counts"},
    {"question": "logins per user per day for the last month",
     "call": {"tool": WMS_ACTIVITY, "grain": "day", "by": "user", "days": 30},
     "read": "user_day; the grid with its logins and transactions switch is drawn for you"},
    {"question": "who logged in each day last month for company 920",
     "call": {"tool": WMS_ACTIVITY, "grain": "day", "by": "user", "days": 30, "company": "920"},
     "read": "user_day; name the people with the most logins from its totals per row"},
]

ACTIVITY_TOOLS: list[dict[str, Any]] = [
    {
        "name": WMS_ACTIVITY,
        "description": (
            "Who was active on the warehouse system (the handheld and M3 users), when, and on which "
            "devices, from the transaction log: how many different users per hour, per day or per week, "
            "or by user, device, warehouse or method; with who started and finished when. Call it for "
            "'how many users were active', 'users per hour', 'who was active today', 'devices in use', "
            "'logged in'. For users or devices per day use grain 'day' and no by: every row carries the "
            "users and the devices of its day (devices per day is grain 'day', measure devices); by is for "
            "comparing users, devices, warehouses or methods with each other, and with a grain it comes "
            "back as the breakdown beside the days. Window: days back (14 when not said, up to 92), or "
            "start and end, or one day. "
            "grain 'hour' over several days also returns the day-by-hour matrix the heat map is drawn "
            "from. grain 'day' with by 'user' also returns user_day, a grid of people down and days "
            "across with logins and transactions per cell, for 'logins per user per day' or 'who "
            "logged in each day' (up to 48 days). company narrows to one company. "
            "Answer only with the counts it returns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "How many days back, 1 to 92; 14 when nothing else is said."},
                "start": {"type": "string", "description": "ISO date or datetime; with end."},
                "end": {"type": "string", "description": "ISO date or datetime; with start."},
                "day": {"type": "string", "description": "One day on the warehouse's clock: 'today', 'yesterday' or YYYY-MM-DD."},
                "grain": {"type": "string", "enum": list(GRAINS), "description": "The time bucket; 'day' when not said."},
                "by": {"type": "string", "enum": list(CUTS), "description": "Cut by user, device, warehouse or method instead of time; 'none' when not said."},
                "user": {"type": "string", "description": "One user name, to narrow."},
                "warehouse": {"type": "string", "description": "One warehouse code, to narrow."},
                "method": {"type": "string", "description": "One transaction method, to narrow."},
                "company": {"type": "string", "description": "One M3 company number, such as 920, to narrow."},
                "limit": {"type": "integer", "description": "Groups to return when cut by user, device, warehouse or method; 50 when not said."},
            },
            "additionalProperties": False,
        },
    }
]


def _parse_when(raw: Any, tz: ZoneInfo, *, end_of_day: bool = False) -> datetime | None:
    """An ISO date or datetime on the tenant's clock; a date alone is its midnight (or its end).

    ("2026-09-18", London) -> 2026-09-18 00:00+01:00 ; ("2026-09-18T06:00:00+00:00") -> as given
    """
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        if len(text) == 10:
            d = date.fromisoformat(text)
            base = datetime(d.year, d.month, d.day, tzinfo=tz)
            return base + timedelta(days=1) if end_of_day else base
        when = datetime.fromisoformat(text)
        return when if when.tzinfo else when.replace(tzinfo=tz)
    except ValueError:
        return None


def resolve_window(args: Mapping[str, Any], tz: ZoneInfo, now: datetime) -> tuple[datetime, datetime, list[str]]:
    """The window the call means, on the tenant's clock: one day, or start to end, or N days back
    to now; capped at MAX_DAYS. Returns (start, end, notes).

    ({"day": "2026-09-18"}, London) -> (18 Sep 00:00, 19 Sep 00:00) ; ({"days": 400}) -> 92 days, a note
    """
    notes: list[str] = []
    local_now = now.astimezone(tz)
    day = str(args.get("day") or "").strip().lower()
    if day:
        today = local_now.date()
        d = today if day == "today" else today - timedelta(days=1) if day == "yesterday" else None
        if d is None:
            try:
                d = date.fromisoformat(day)
            except ValueError:
                notes.append(f"day {day!r} not understood; using today")
                d = today
        start = datetime(d.year, d.month, d.day, tzinfo=tz)
        return start, start + timedelta(days=1), notes + [f"day {d.isoformat()} on the warehouse's clock"]
    start = _parse_when(args.get("start"), tz)
    end = _parse_when(args.get("end"), tz, end_of_day=True)
    if start is None or end is None:
        try:
            days = int(args.get("days") or DEFAULT_DAYS)
        except (TypeError, ValueError):
            days = DEFAULT_DAYS
        days = max(1, days)
        if days > MAX_DAYS:
            notes.append(f"window capped at {MAX_DAYS} days")
            days = MAX_DAYS
        first = local_now.date() - timedelta(days=days - 1)
        start = datetime(first.year, first.month, first.day, tzinfo=tz)
        end = now
    if end <= start:
        start, end = end, start
    if end - start > timedelta(days=MAX_DAYS):
        notes.append(f"window capped at {MAX_DAYS} days")
        start = end - timedelta(days=MAX_DAYS)
    return start, end, notes


def _day_label(d: date) -> str:
    """A day as the heat map's row label.

    (2026-11-10) -> 'Tue 10 Nov'
    """
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def matrix_of(cells: list[tuple[date, int, int]], start: datetime, end: datetime, tz: ZoneInfo) -> dict | None:
    """The day-by-hour matrix of distinct users from (day, hour, users) cells, every day of the
    window present, the hours before the start and after the end marked outside; None when the
    window spans more days than the heat map holds.

    ([(18 Sep, 6, 3)], 18 Sep 00:00, 19 Sep 00:00) -> {"rows": ["Fri 18 Sep"], "values": [[0]*6 + [3] + [0]*17], ...}
    """
    first, last = start.astimezone(tz).date(), (end.astimezone(tz) - timedelta(microseconds=1)).date()
    days = [first + timedelta(days=i) for i in range((last - first).days + 1)]
    if not days or len(days) > MAX_MATRIX_DAYS:
        return None
    index = {d: i for i, d in enumerate(days)}
    values = [[0] * 24 for _ in days]
    for d, hour, users in cells:
        if d in index and 0 <= hour < 24:
            values[index[d]][hour] = int(users)
    local_start, local_end = start.astimezone(tz), end.astimezone(tz)
    outside = [
        [
            (d == first and h < local_start.hour)
            or (d == last and (datetime(d.year, d.month, d.day, h, tzinfo=tz) >= local_end))
            for h in range(24)
        ]
        for d in days
    ]
    return {
        "rows": [_day_label(d) for d in days],
        "days": [d.isoformat() for d in days],
        "columns": HOURS,
        "values": values,
        "unit": "users",
        "scale": [1, 2, 4, 9],
        "outside": outside if any(any(r) for r in outside) else None,
    }


async def company_workers(db: AsyncSession, company: str, where: list[ColumnElement[bool]]) -> list[str]:
    """The people who did work tagged with the company under the same `where`, read once as a
    list, because a sub-select in the filter is planned again for every daily partition of
    `log_transactions` and a month then costs about 31 passes instead of one (19 Nov 2026).

    ("920", [tenant, window]) -> ["CPRICE", "RBENJAMIN", ...] ; (no tagged work) -> []
    """
    T = LogTransaction
    q = select(T.user_name).where(*where, T.company == company).distinct().limit(MAX_COMPANY_WORKERS)
    return sorted(u for u in (await db.execute(q)).scalars() if u)


def company_filter(company: str, workers: list[str]) -> ColumnElement[bool]:
    """The condition that keeps one company's calls: tagged with it, or untagged (a login carries
    no company) and made by one of its workers.

    >>> print(company_filter("920", ["UA"]).compile(compile_kwargs={"literal_binds": True}))
    log_transactions.company = '920' OR log_transactions.company IS NULL AND log_transactions.user_name IN ('UA')
    """
    T = LogTransaction
    return or_(T.company == company, and_(T.company.is_(None), T.user_name.in_(workers)))


async def activity_rows(db: AsyncSession, customer_code: str, args: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    """The counts for the window and the cut: rows per bucket or group, the day-by-hour matrix
    for an hour grain, the people for a user cut or a short window, and the totals.

    ("acme", {"grain": "day", "days": 7}) -> {"group_by": ["day"], "rows": [{"dimensions": ["2026-11-08"], "users": 12, ...}], ...}
    """
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    start, end, notes = resolve_window(args, tz, now)
    grain = str(args.get("grain") or "day").lower()
    grain = grain if grain in GRAINS else "day"
    by = str(args.get("by") or "none").lower()
    by = by if by in CUTS else "none"
    try:
        limit = max(1, min(GROUP_LIMIT, int(args.get("limit") or PEOPLE_LIMIT)))
    except (TypeError, ValueError):
        limit = PEOPLE_LIMIT
    T = LogTransaction
    local = func.timezone(tz.key, T.started_at)
    where = [T.customer_code == customer_code, T.started_at >= start, T.started_at < end, T.user_name.is_not(None)]
    for key, column in (("user", T.user_name), ("warehouse", T.warehouse), ("method", T.method)):
        if args.get(key):
            where.append(column == str(args[key]))
    company = str(args.get("company") or "").strip()
    if company:
        where.append(company_filter(company, await company_workers(db, company, list(where))))
        notes.append(f"company {company}: an untagged call, such as a login, counts when its user did "
                     f"company {company} work in the window")
    errors = func.sum(case((T.status == LogTransactionStatus.error, 1), else_=0))
    measures = (
        func.count(func.distinct(T.user_name)).label("users"),
        func.count(func.distinct(T.device_name)).label("devices"),
        func.count().label("calls"),
        cast(errors, Integer).label("errors"),
        func.min(T.started_at).label("first_at"),
        func.max(T.started_at).label("last_at"),
    )
    # Step 1: The rows per time bucket, or per group when the person cut by user, device,
    # warehouse or method, so the chooser reads a trend or a comparison from the same shape. A
    # grain asked for together with a cut keeps the series by time and returns the cut beside it,
    # because "devices per day" came as {grain: day, by: device} and lost its days (14 November)
    asked_grain = bool(str(args.get("grain") or "").strip())
    time_rows: list[dict] | None = None
    cut_rows: list[dict] | None = None
    if by == "none" or asked_grain:
        bucket = func.date_trunc(grain, local).label("bucket")
        q = select(bucket, *measures).where(*where).group_by(bucket).order_by(bucket)
        time_rows = [
            {"dimensions": [_bucket_label(r.bucket, grain)], "users": r.users, "devices": r.devices,
             "calls": r.calls, "errors": r.errors or 0,
             "first_at": _local_iso(r.first_at, tz), "last_at": _local_iso(r.last_at, tz)}
            for r in (await db.execute(q)).all()
        ]
    if by != "none":
        column = {"user": T.user_name, "device": T.device_name, "warehouse": T.warehouse, "method": T.method}[by]
        q = (select(column.label("cut"), *measures).where(*where).group_by(column)
             .order_by(func.count().desc()).limit(limit))
        cut_rows = [
            {"dimensions": [str(r.cut or "unknown")], "users": r.users, "devices": r.devices, "calls": r.calls,
             "errors": r.errors or 0, "first_at": _local_iso(r.first_at, tz), "last_at": _local_iso(r.last_at, tz)}
            for r in (await db.execute(q)).all()
        ]
    if time_rows is not None:
        group_by, rows = [grain], time_rows
    else:
        group_by, rows = [by], cut_rows or []
    # Step 2: The totals over the window, so a single figure never has to be added up by hand
    total_row = (await db.execute(select(*measures[:4]).where(*where))).one()
    total = {"users": total_row.users, "devices": total_row.devices, "calls": total_row.calls, "errors": total_row.errors or 0}
    out: dict[str, Any] = {
        "window": {"start": start.isoformat(), "end": end.isoformat(), "timezone": tz.key},
        "grain": grain, "by": by, "group_by": group_by, "rows": rows, "groups": len(rows), "total": total,
        "grain_note": GRAIN_NOTE,
    }
    if company:
        out["company"] = company
    if time_rows is not None and cut_rows is not None:
        out["breakdown_by"], out["breakdown"] = by, cut_rows
    # Step 3a: The people-by-day grid for a day grain asked for together with the user cut, which
    # the heat map with its logins and transactions switch is drawn from (step 41)
    if asked_grain and grain == "day" and by == "user":
        days = activity_grid.window_days(start, end, tz)
        user_day = await activity_grid.user_day_grid(db, where, local, days, notes)
        if user_day is not None:
            out["user_day"] = user_day
    # Step 3: The day-by-hour matrix for an hour grain over several days, which the heat map is
    # drawn from, filled for every day of the window so a quiet day is a row of zeros
    if time_rows is not None and grain == "hour":
        day_col = func.date_trunc("day", local).label("d")
        hour_col = cast(func.extract("hour", local), Integer).label("h")
        mq = (select(day_col, hour_col, func.count(func.distinct(T.user_name)).label("users"))
              .where(*where).group_by(day_col, hour_col))
        cells = [(r.d.date() if isinstance(r.d, datetime) else r.d, int(r.h), int(r.users)) for r in (await db.execute(mq)).all()]
        matrix = matrix_of(cells, start, end, tz)
        if matrix is not None:
            out["matrix"] = matrix
        else:
            notes.append(f"the heat map holds up to {MAX_MATRIX_DAYS} days; ask for a shorter window to see it")
    # Step 4: The people, for a user cut or a short window, with when each started and finished,
    # so "who was active today" names them without a second call
    if by == "user" or (end - start) <= timedelta(days=2):
        pq = (select(T.user_name.label("user"), func.max(T.employee_name).label("employee"),
                     func.count(func.distinct(T.device_name)).label("devices"), func.count().label("calls"),
                     func.min(T.started_at).label("first_at"), func.max(T.started_at).label("last_at"))
              .where(*where).group_by(T.user_name).order_by(func.count().desc()).limit(PEOPLE_LIMIT))
        out["people"] = [
            {"user": r.user, "employee": r.employee, "devices": r.devices, "calls": r.calls,
             "first_at": _local_iso(r.first_at, tz), "last_at": _local_iso(r.last_at, tz)}
            for r in (await db.execute(pq)).all()
        ]
    if notes:
        out["notes"] = notes
    return out


def _bucket_label(value: Any, grain: str) -> str:
    """A time bucket as a label: the day, the hour on its day, or the week's Monday.

    (2026-11-10 09:00, "hour") -> "2026-11-10 09:00" ; (2026-11-10, "day") -> "2026-11-10"
    """
    if not isinstance(value, datetime):
        return str(value)
    if grain == "hour":
        return value.strftime("%Y-%m-%d %H:00")
    return value.date().isoformat()


def _local_iso(value: datetime | None, tz: ZoneInfo) -> str | None:
    if value is None:
        return None
    when = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return when.astimezone(tz).strftime("%Y-%m-%d %H:%M")


async def run_activity_tool(db: AsyncSession, customer_code: str, name: str, args: Mapping[str, Any]) -> str:
    """Run the activity tool: the counts as JSON, or the problem.

    ("wms_activity", {"grain": "hour", "days": 14}) -> '{"group_by": ["hour"], "rows": [...], "matrix": {...}, ...}'
    """
    if name != WMS_ACTIVITY:
        return json.dumps({"problems": [f"unknown activity tool {name!r}"]})
    return json.dumps(await activity_rows(db, customer_code, args, now=datetime.now(timezone.utc)), default=str)
