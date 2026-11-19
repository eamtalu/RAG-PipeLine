"""The people-by-day grid of `wms_activity` (step 41): one row per person, one column per day of the
window, in two layers the Teams page switches between, logins and transactions.

A login is a `CheckOperator` call that did not fail (the handheld's sign-in screen; a soft result
is M3's location warning on a good sign-in). A transaction is any call but the background ones,
the token refresh and the server check. Every figure is a count Postgres gave over
`log_transactions` on the tenant's clock. Used by app/services/analytics_agent/activity_tools.py;
the heat map is drawn from it by app/agent/presentation/charts.py.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Integer, and_, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

#: The heat map contract holds 62 rows and 48 columns; a month of days fits with room to spare.
MAX_USERS = 62
MAX_DAYS = 48
LOGIN_METHOD = "CheckOperator"
#: Calls the app makes on its own, not work a person did: the token refresh and the server check.
BACKGROUND_METHODS = ("GetAccessToken", "CheckServer")
#: Logins per person per day are small numbers, so their bands are fixed.
LOGIN_SCALE = [1, 2, 4, 8]
LABEL_MAX = 40


def user_label(user: str, employee: str | None) -> str:
    """The row label: the person's name with their user id, cut to fit the contract's 40 characters.

    >>> user_label("UA", "User A"), user_label("UC", None)
    ('User A (UA)', 'UC')
    >>> len(user_label("FNACHONLEO", "Francisco Alejandro Nachon-Leon de la Vega"))
    40
    """
    if not employee:
        return user[:LABEL_MAX]
    suffix = f" ({user})"
    room = LABEL_MAX - len(suffix)
    name = employee if len(employee) <= room else employee[: room - 1].rstrip() + "…"
    return f"{name}{suffix}"


def day_label(d: date) -> str:
    """A column label short enough for a month of columns.

    >>> day_label(date(2026, 10, 19))
    'Mo 19/10'
    """
    return f"{d.strftime('%a')[:2]} {d.strftime('%d/%m')}"


def bands_of(values: list[list[int]]) -> list[int]:
    """Band thresholds that follow a layer's own counts: 1, then the median, upper quartile and
    90th percentile of the cells that are not zero, kept strictly rising.

    >>> bands_of([[4, 0, 2], [0, 2, 0], [0, 1, 0]])
    [1, 2, 4]
    >>> bands_of([[0, 0]]), bands_of([[5, 5, 5]])
    ([1, 2], [1, 5])
    """
    counts = sorted(v for row in values for v in row if v > 0)
    bands = [1]
    for share in (0.5, 0.75, 0.9):
        if counts:
            # nearest rank, so every threshold is a count that occurs in the grid
            bands.append(counts[max(0, math.ceil(share * len(counts)) - 1)])
    rising = [b for i, b in enumerate(bands) if i == 0 or b > max(bands[:i])]
    return rising if len(rising) >= 2 else [1, 2]


def window_days(start: datetime, end: datetime, tz: ZoneInfo) -> list[date]:
    """Every day of the window on the tenant's clock, the last one included when the end is not
    its midnight.

    >>> london = ZoneInfo("Europe/London")
    >>> [d.day for d in window_days(datetime(2026, 11, 17, tzinfo=london), datetime(2026, 11, 20, tzinfo=london), london)]
    [17, 18, 19]
    """
    first, last = start.astimezone(tz).date(), (end.astimezone(tz) - timedelta(microseconds=1)).date()
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


async def user_day_grid(db: AsyncSession, where: list[ColumnElement[bool]], local: Any, days: list[date],
                        notes: list[str]) -> dict[str, Any] | None:
    """The grid for the calls `where` selects: people down, the window's days across, logins and
    transactions per cell; None, with a note, when the window has more days than the grid holds.

    (3 people over 17-19 Nov) -> {"rows": ["User A (UA)", ...], "columns": ["Tu 17/11", ...], "layers": [...]} ; (60 days) -> None
    """
    # Step 1: Refuse a window wider than the contract's columns, so the answer says so instead
    # of drawing a grid that would be cut off at the edge
    if not days:
        return None
    if len(days) > MAX_DAYS:
        notes.append(f"the people-by-day grid holds up to {MAX_DAYS} days; ask for a shorter window to see it")
        return None
    # Step 2: Count both layers per person and day in one grouped query, so the grid costs one
    # pass over the window whatever its size
    T = LogTransaction
    is_login = and_(T.method == LOGIN_METHOD, T.status != LogTransactionStatus.error)
    is_work = T.method.not_in(BACKGROUND_METHODS)
    day_col = func.date_trunc("day", local).label("d")
    q = (
        select(T.user_name.label("user"), func.max(T.employee_name).label("employee"), day_col,
               cast(func.count().filter(is_login), Integer).label("logins"),
               cast(func.count().filter(is_work), Integer).label("transactions"))
        .where(*where)
        .group_by(T.user_name, day_col)
    )
    index = {d: i for i, d in enumerate(days)}
    people: dict[str, dict[str, Any]] = {}
    for r in (await db.execute(q)).all():
        d = r.d.date() if isinstance(r.d, datetime) else r.d
        if d not in index or not (r.logins or r.transactions):
            continue
        person = people.setdefault(r.user, {"employee": None, "logins": [0] * len(days), "transactions": [0] * len(days)})
        person["employee"] = person["employee"] or r.employee
        person["logins"][index[d]] = int(r.logins)
        person["transactions"][index[d]] = int(r.transactions)
    if not people:
        return None
    # Step 3: The busiest people first and at most the rows the contract holds, so a large site
    # shows who matters and the answer says how many were left out
    order = sorted(people, key=lambda u: (-sum(people[u]["logins"]), -sum(people[u]["transactions"]), u))
    if len(order) > MAX_USERS:
        notes.append(f"the grid shows the {MAX_USERS} people with the most logins; {len(order) - MAX_USERS} more not shown")
        order = order[:MAX_USERS]
    logins = [people[u]["logins"] for u in order]
    transactions = [people[u]["transactions"] for u in order]
    return {
        "rows": [user_label(u, people[u]["employee"]) for u in order],
        "users": order,
        "columns": [day_label(d) for d in days],
        "days": [d.isoformat() for d in days],
        "layers": [
            {"key": "logins", "label": "Logins", "unit": "logins", "values": logins, "scale": LOGIN_SCALE},
            {"key": "transactions", "label": "Transactions", "unit": "transactions", "values": transactions,
             "scale": bands_of(transactions)},
        ],
        "totals": {"logins": sum(map(sum, logins)), "transactions": sum(map(sum, transactions))},
    }
