"""Daily series arithmetic: dense days, week and month buckets, demand classification, ramp trim,
and the target instants a forecast is FOR.

Pure. Dates are tenant-local calendar dates; the only instant produced is `Target.target_at`, the
tenant's local midnight that starts the bucket, converted to UTC so it can be stored and compared.
"""

from __future__ import annotations

import calendar
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Iterable, Sequence

GRAINS = ("day", "week", "month")
_SUFFIX = {"day": "d", "week": "w", "month": "m"}

#: Syntetos-Boylan cut-offs: average demand interval 1.32 days, squared coefficient of variation 0.49.
ADI_CUT = 1.32
CV2_CUT = 0.49


@dataclass(frozen=True)
class DailySeries:
    """One value per calendar day, dense from `start`. A day with nothing is 0, not absent."""

    start: date
    values: tuple[float, ...]

    def __len__(self) -> int:
        return len(self.values)

    @property
    def end(self) -> date:
        return self.start + timedelta(days=len(self.values) - 1)

    def dates(self) -> list[date]:
        return [self.start + timedelta(days=i) for i in range(len(self.values))]

    def since(self, day: date) -> "DailySeries":
        """The tail from `day` onwards; the whole series when `day` is before it starts."""
        skip = max(0, (day - self.start).days)
        return DailySeries(start=self.start + timedelta(days=skip), values=self.values[skip:])


@dataclass(frozen=True)
class Bucket:
    start: date
    end: date
    total: float
    #: False while `as_of` falls inside the bucket: its total is still growing.
    complete: bool


@dataclass(frozen=True)
class Target:
    label: str
    start: date
    end: date
    target_at: datetime


# ============================================================== dense days

def dense_daily(rows: Iterable[tuple[date, float]], *, start: date, end: date) -> DailySeries:
    """Sum `rows` into one value per day between `start` and `end` inclusive. Missing days are 0."""
    n = (end - start).days + 1
    values = [0.0] * max(n, 0)
    for day, value in rows:
        i = (day - start).days
        if 0 <= i < n:
            values[i] += float(value)
    return DailySeries(start=start, values=tuple(values))


# ============================================================== buckets

def week_start(day: date) -> date:
    return day - timedelta(days=day.weekday())


def month_start(day: date) -> date:
    return day.replace(day=1)


def month_end(day: date) -> date:
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def add_months(day: date, n: int) -> date:
    """The first of the month `n` months after the month of `day`."""
    y, m = divmod(day.month - 1 + n, 12)
    return date(day.year + y, m + 1, 1)


def bucket_bounds(day: date, grain: str) -> tuple[date, date]:
    if grain == "day":
        return day, day
    if grain == "week":
        ws = week_start(day)
        return ws, ws + timedelta(days=6)
    if grain == "month":
        return month_start(day), month_end(day)
    raise ValueError(f"unknown grain {grain!r}; use one of {', '.join(GRAINS)}")


def _aggregate(ser: DailySeries, grain: str, as_of: date) -> list[Bucket]:
    totals: dict[date, float] = {}
    for day, value in zip(ser.dates(), ser.values):
        totals[bucket_bounds(day, grain)[0]] = totals.get(bucket_bounds(day, grain)[0], 0.0) + value
    out = []
    for start in sorted(totals):
        _, end = bucket_bounds(start, grain)
        out.append(Bucket(start=start, end=end, total=totals[start], complete=end <= as_of))
    return out


def aggregate_weeks(ser: DailySeries, *, as_of: date) -> list[Bucket]:
    """ISO weeks, Monday to Sunday. A week is complete once its Sunday is on or before `as_of`."""
    return _aggregate(ser, "week", as_of)


def aggregate_months(ser: DailySeries, *, as_of: date) -> list[Bucket]:
    return _aggregate(ser, "month", as_of)


# ============================================================== classification

def classify(values: Sequence[float], *, min_hits: int = 2) -> str:
    """Syntetos-Boylan demand class from the inter-demand interval and the size variability.

    `smooth` is regular demand of steady size; `intermittent` is regular size on irregular days;
    `erratic` is every day but wildly sized; `lumpy` is both irregular. `insufficient` means fewer than
    `min_hits` non-zero days, which no model can learn from."""
    hits = [float(v) for v in values if v and v > 0]
    if len(hits) < min_hits:
        return "insufficient"
    adi = len(values) / len(hits)
    mean = statistics.fmean(hits)
    cv2 = (statistics.pvariance(hits) / (mean * mean)) if mean else 0.0
    if adi < ADI_CUT:
        return "smooth" if cv2 < CV2_CUT else "erratic"
    return "intermittent" if cv2 < CV2_CUT else "lumpy"


# ============================================================== ramp trim

def _is_weekend(day: date) -> bool:
    return day.weekday() >= 5


def _reference(ser: DailySeries, window: int) -> dict[bool, float]:
    """The median of the most recent `window` days, per day type. The recent tail is what steady
    operation looks like; the ramp is judged against it."""
    tail = list(zip(ser.dates(), ser.values))[-window:]
    out = {}
    for weekend in (False, True):
        vals = [v for d, v in tail if _is_weekend(d) == weekend]
        out[weekend] = statistics.median(vals) if vals else 0.0
    return out


def steady_start(ser: DailySeries, *, floor_ratio: float = 0.5, window: int = 14, streak: int = 5) -> date:
    """The first day from which `streak` consecutive days all reach `floor_ratio` of the recent
    median for their day type. Everything before it is a rollout ramp and is not history to learn from.

    Only a run of good days from the FRONT counts, so a single low day later (a bank holiday) never
    moves the start. A series with no steady run, or no demand at all, starts where it starts."""
    ref = _reference(ser, window)
    if not any(ref.values()):
        return ser.start
    dates, values = ser.dates(), ser.values
    streak = min(streak, len(values))
    for i in range(len(values) - streak + 1):
        if all(values[j] >= floor_ratio * ref[_is_weekend(dates[j])] for j in range(i, i + streak)):
            return dates[i]
    return ser.start


# ============================================================== horizons and targets

def horizon_label(grain: str, steps: int) -> str:
    if grain not in _SUFFIX:
        raise ValueError(f"unknown grain {grain!r}; use one of {', '.join(GRAINS)}")
    return f"{steps}{_SUFFIX[grain]}"


def local_midnight(day: date, tz: tzinfo) -> datetime:
    """The instant the tenant's `day` begins, in UTC. Built in the zone, not by adding hours, so the
    clock change lands where the tenant's wall clock says it does."""
    return datetime(day.year, day.month, day.day, tzinfo=tz).astimezone(timezone.utc)


def targets(*, as_of: date, grain: str, n: int, tz: tzinfo) -> list[Target]:
    """The buckets a run made on the morning after `as_of` predicts.

    Days are 1..n after `as_of`. Weeks and months start at step 0: the bucket containing the day
    after `as_of`, which is the one in progress when the run happens, then n-1 more."""
    out = []
    if grain == "day":
        for k in range(1, n + 1):
            day = as_of + timedelta(days=k)
            out.append(Target(horizon_label(grain, k), day, day, local_midnight(day, tz)))
        return out
    first, _ = bucket_bounds(as_of + timedelta(days=1), grain)
    for k in range(n):
        start = first + timedelta(weeks=k) if grain == "week" else add_months(first, k)
        start, end = bucket_bounds(start, grain)
        out.append(Target(horizon_label(grain, k), start, end, local_midnight(start, tz)))
    return out
