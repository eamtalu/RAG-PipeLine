"""Hour-of-day shape and picker throughput, from the settled rows grouped by hour.

A daily forecast says how many lines; the operation needs to know WHEN in the day, because the
pickers are rostered by the hour. The profile is the median share of a day's lines that falls in
each hour, per weekday, and the throughput is the median lines one picker gets through in an hour.
Both are measured, never assumed, so a site that picks overnight gets an overnight profile.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date, datetime

HOURS = 24


@dataclass(frozen=True)
class HourRow:
    """One tenant-local hour: lines settled in it and distinct pickers active in it."""

    start: datetime
    lines: float
    pickers: int


@dataclass(frozen=True)
class DayPoint:
    day: date
    p10: float
    p50: float
    p90: float


@dataclass(frozen=True)
class HourPoint:
    day: date
    hour: int
    p10: float
    p50: float
    p90: float


@dataclass(frozen=True)
class Throughput:
    """Lines per picker-hour: by `(weekday, hour)` where measured, `overall` as the fallback."""

    by_slot: dict[tuple[int, int], float]
    overall: float | None

    def at(self, *, dow: int, hour: int) -> float | None:
        return self.by_slot.get((dow, hour), self.overall)


Share = dict[int, list[float]]


def _by_day(rows: list[HourRow]) -> dict[date, list[float]]:
    days: dict[date, list[float]] = {}
    for r in rows:
        days.setdefault(r.start.date(), [0.0] * HOURS)[r.start.hour] += r.lines
    return days


def _pooled_shape(days: list[list[float]]) -> list[float] | None:
    if not days:
        return None
    total_by_hour = [sum(d[h] for d in days) for h in range(HOURS)]
    total = sum(total_by_hour)
    return [v / total for v in total_by_hour] if total > 0 else None


def since(rows: list[HourRow], day: date) -> list[HourRow]:
    """Only the hours on or after `day`: the rollout ramp is no more a shape to learn than a level."""
    return [r for r in rows if r.start.date() >= day]


def profile(rows: list[HourRow]) -> Share:
    """Per weekday, the share of that weekday's lines falling in each hour, summing to one.

    Shares are pooled over lines, not averaged over days, so a 13-line day cannot shape a 520-line
    one (the live Saturday profile came out spiky for exactly that reason). A weekday never observed,
    or observed only empty, borrows the pooled shape of every day; with no data at all every hour
    gets an equal share."""
    per_dow: dict[int, list[list[float]]] = {d: [] for d in range(7)}
    everything: list[list[float]] = []
    for day, hours in _by_day(rows).items():
        if sum(hours) <= 0:
            continue
        per_dow[day.weekday()].append(hours)
        everything.append(hours)
    fallback = _pooled_shape(everything) or [1.0 / HOURS] * HOURS
    return {d: _pooled_shape(per_dow[d]) or fallback for d in range(7)}


def throughput(rows: list[HourRow]) -> Throughput:
    """Median lines per picker-hour, by slot and overall, over the hours that had a picker."""
    rates: dict[tuple[int, int], list[float]] = {}
    for r in rows:
        if r.pickers > 0 and r.lines > 0:
            rates.setdefault((r.start.weekday(), r.start.hour), []).append(r.lines / r.pickers)
    every = [v for vs in rates.values() for v in vs]
    return Throughput(by_slot={k: statistics.median(v) for k, v in rates.items()},
                      overall=statistics.median(every) if every else None)


def spread(daily: list[DayPoint], share: Share) -> list[HourPoint]:
    """Cut each daily point into 24 hourly points by the weekday's share. Sums are preserved."""
    out = []
    for d in daily:
        shape = share[d.day.weekday()]
        for hour in range(HOURS):
            out.append(HourPoint(day=d.day, hour=hour, p10=d.p10 * shape[hour], p50=d.p50 * shape[hour],
                                 p90=d.p90 * shape[hour]))
    return out
