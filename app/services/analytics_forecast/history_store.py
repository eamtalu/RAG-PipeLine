"""What the forecast learns from: the settled pick rows, read through the one grouped read.

`settle_store.read_grouped` is the function behind the pick-releases screen, the agent's tools and
the Teams home card. Reading history through it, and nothing else, means the model trains on, is
scored against, and is displayed next to the SAME numbers. A forecast and an actual that came from
two code paths can disagree for reasons that have nothing to do with the model, and nobody would
know which to believe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics import settle as st
from app.services.analytics import settle_query as sq
from app.services.analytics import settle_reads
from app.services.analytics import settle_store
from app.services.analytics_forecast import hourly, series

#: Days of hourly rows read for the hour-of-day profile and throughput. Four weeks gives four
#: observations of every weekday hour, which is what a median needs to mean something.
HOURLY_DAYS = 28


@dataclass(frozen=True)
class DailyRow:
    day: date
    warehouse: str | None
    transaction_name: str | None
    lines: int
    units: float


@dataclass(frozen=True)
class ItemDayRow:
    day: date
    item_number: str
    lines: int
    units: float


@dataclass(frozen=True)
class HistoryBundle:
    """Everything one run reads, with the bounds it asked for."""

    start: date
    end: date
    daily: list[DailyRow]
    items: list[ItemDayRow]
    items_truncated: bool
    hourly: list[hourly.HourRow]


def _num(value) -> float:
    return float(Decimal(value)) if value not in (None, "") else 0.0


def _day(value) -> date:
    return date.fromisoformat(value) if isinstance(value, str) else value


async def _settlement(db: AsyncSession, cc: str, name: str) -> st.Settlement:
    _row, declared = await settle_reads.declared(db, cc, name)
    return declared


async def read_daily(db: AsyncSession, cc: str, settlement: str, *, since: datetime, until: datetime,
                     tz: tzinfo, units_value: str, limit: int = 20000) -> list[DailyRow]:
    """Lines and units per tenant-local day, warehouse and transaction name."""
    declared = await _settlement(db, cc, settlement)
    rows = await settle_store.read_grouped(db, cc, declared, group_by=("day", "warehouse", "transaction_name"),
                                           since=since, until=until, tz=tz, limit=limit)
    return [DailyRow(day=_day(r["dimensions"][0]), warehouse=r["dimensions"][1],
                     transaction_name=r["dimensions"][2], lines=r["rows"], units=_num(r.get(units_value)))
            for r in rows]


async def read_daily_for(db: AsyncSession, cc: str, settlement: str, *, since: datetime, until: datetime, tz: tzinfo,
                         units_value: str, subject_kind: str, subject: str, limit: int = 1000) -> series.DailySeries | None:
    """Lines and units per day for ONE series (the total, a warehouse, a transaction name or an item),
    for the screens: the same grouped read with a filter, so a chart's actuals are the model's."""
    declared = await _settlement(db, cc, settlement)
    filters = () if subject_kind == "total" else (sq.Filter(subject_kind, "==", subject),)
    rows = await settle_store.read_grouped(db, cc, declared, group_by=("day",), since=since, until=until, tz=tz,
                                           limit=limit, filters=filters)
    return [(_day(r["dimensions"][0]), r["rows"], _num(r.get(units_value))) for r in rows]


async def read_daily_items(db: AsyncSession, cc: str, settlement: str, *, since: datetime, until: datetime,
                           tz: tzinfo, units_value: str, cap: int) -> tuple[list[ItemDayRow], bool]:
    """Lines and units per day and item. `cap + 1` rows are asked for so hitting the cap is
    reported rather than silently dropping the quietest items."""
    declared = await _settlement(db, cc, settlement)
    rows = await settle_store.read_grouped(db, cc, declared, group_by=("day", "item_number"),
                                           since=since, until=until, tz=tz, limit=cap + 1)
    truncated = len(rows) > cap
    return ([ItemDayRow(day=_day(r["dimensions"][0]), item_number=r["dimensions"][1] or "",
                        lines=r["rows"], units=_num(r.get(units_value))) for r in rows[:cap]], truncated)


async def read_hourly(db: AsyncSession, cc: str, settlement: str, *, since: datetime, until: datetime,
                      tz: tzinfo, limit: int = 5000) -> list[hourly.HourRow]:
    """Lines and distinct pickers per tenant-local hour. `start` is a naive local wall-clock hour,
    which is what the profile keys on (a picker's 02:00 is 02:00 whatever the offset)."""
    declared = await _settlement(db, cc, settlement)
    rows = await settle_store.read_grouped(db, cc, declared, group_by=("hour_start",), since=since, until=until,
                                           tz=tz, limit=limit, stats=(sq.Stat("distinct", "user_name"),))
    out = []
    for r in rows:
        start = r["dimensions"][0]
        start = datetime.fromisoformat(start) if isinstance(start, str) else start
        out.append(hourly.HourRow(start=start.replace(tzinfo=None), lines=float(r["rows"]),
                                  pickers=int(r.get("distinct_user_name") or 0)))
    return out


async def read_history(db: AsyncSession, cc: str, settlement: str, *, start: date, end: date, tz: tzinfo,
                       units_value: str, item_cap: int) -> HistoryBundle:
    """One run's reading: `start` to `end` inclusive, in the tenant's zone."""
    since = series.local_midnight(start, tz)
    until = series.local_midnight(end + timedelta(days=1), tz)
    daily = await read_daily(db, cc, settlement, since=since, until=until, tz=tz, units_value=units_value)
    items, truncated = await read_daily_items(db, cc, settlement, since=since, until=until, tz=tz,
                                              units_value=units_value, cap=item_cap)
    hourly_rows = await read_hourly(db, cc, settlement, since=max(since, until - timedelta(days=HOURLY_DAYS)),
                                    until=until, tz=tz)
    return HistoryBundle(start=start, end=end, daily=daily, items=items, items_truncated=truncated,
                         hourly=hourly_rows)
