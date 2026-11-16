"""Step 31 (chunk 209): the web Pick releases page's arithmetic, in Python, for the Teams page.

The web page (matrix-log-explorer `src/lib/pickReleases.ts`, `pickReleaseStats.ts` and
`pickReleaseQueries.ts`) reads the settled `pick_release` rows and computes every panel in the
browser. The Teams tab cannot reach this server, so the consumer computes the same panels here and
mirrors them. Every function below is a port of the one named in its header, kept line for line, so
the two pages give the same numbers; `tests/test_releases_detail_chunk209.py` pins the rules and a
parity script runs the web page's own code on the same rows.

Times are aware datetimes on the warehouse's clock, as the web page's are on the reader's (the same
clock for a reader at the warehouse).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

OUTCOMES = ("exact", "over", "zeroPick", "partial")
TX_ORDER = ("stock", "jit", "milk", "frz", "other")
HOUR_S = 3600
DAY_S = 86400
#: The duration bands of the histogram (pickReleaseStats.ts BANDS).
BANDS = (("< 30 s", 0, 30, False), ("30–60 s", 30, 60, False), ("1–2 min", 60, 120, False),
         ("2–5 min", 120, 300, False), ("5–15 min", 300, 900, True), ("> 15 min", 900, math.inf, True))


# ============================================================== the release (pickReleases.ts)

def tx_code(name: str | None) -> str:
    """The pick type a transaction name belongs to (txCode).

    >>> tx_code("JIT and Shorts Pick (Brighton)"), tx_code("Brighton Stock Pick"), tx_code("Freezer Pick"), tx_code(None)
    ('jit', 'stock', 'frz', 'other')
    """
    n = (name or "").lower()
    if "jit" in n:
        return "jit"
    if "milk" in n:
        return "milk"
    if "freezer" in n or "frozen" in n:
        return "frz"
    if "stock" in n:
        return "stock"
    return "other"


def outcome_of(expected: float, picked: float) -> str:
    """Exact, over, zero-pick or partial: never one "short" (outcomeOf).

    >>> outcome_of(4, 4), outcome_of(4, 5), outcome_of(4, 0), outcome_of(4, 2)
    ('exact', 'over', 'zeroPick', 'partial')
    """
    if picked == expected:
        return "exact"
    if picked > expected:
        return "over"
    return "zeroPick" if picked == 0 else "partial"


def _num(v: Any) -> float:
    if v is None or v == "":
        return 0.0
    try:
        n = float(v)
    except (TypeError, ValueError):
        return 0.0
    return n if math.isfinite(n) else 0.0


def _num_or_none(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) else None


def _date(v: Any) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(frozen=True)
class Release:
    key: str
    released_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    transaction: str
    tx: str
    picker: str | None
    delivery: str | None
    customer: str | None
    item: str | None
    description: str | None
    uom: str | None
    order_line: str | None
    suffix: str | None
    expected: float
    picked: float
    shortfall: float
    calls: int
    refused: float
    empty_visits: float
    duration_s: float | None
    outcome: str

    @property
    def done_at(self) -> datetime:
        return self.finished_at or self.released_at


UTC = ZoneInfo("UTC")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def to_release(row: dict) -> Release:
    """One settled row as a release (toRelease): the row's own time, the looked-up customer and
    description, the outcome from expected and picked.

    >>> r = to_release({"key": ["R1"], "event_time": "2026-09-18T06:00:00+00:00", "transaction_name": "Brighton Stock Pick",
    ...     "calls": 2, "attributes": {"expected": "4", "picked": "0", "shortfall": "-4", "duration_s": "75"}, "looked_up": {}})
    >>> r.tx, r.outcome, r.shortfall, r.duration_s
    ('stock', 'zeroPick', -4.0, 75.0)
    """
    a = row.get("attributes") or {}
    lk = row.get("looked_up") or {}
    expected, picked = _num(a.get("expected")), _num(a.get("picked"))
    name = row.get("transaction_name") or a.get("transaction_name")
    released = _date(row.get("event_time")) or _EPOCH
    return Release(
        key=" · ".join(str(k) for k in row.get("key") or []),
        released_at=released,
        started_at=_date(a.get("started_at")),
        finished_at=_date(a.get("finished_at")) or _date(row.get("event_time")),
        transaction=name or "",
        tx=tx_code(name),
        picker=row.get("user_name") or a.get("user_name") or None,
        delivery=row.get("delivery_number") or a.get("delivery_number") or None,
        customer=lk.get("delivery.customer_name") or None,
        item=row.get("item_number") or a.get("item_number") or None,
        description=lk.get("item description.ItemDescription") or None,
        uom=lk.get("item description.basic_uom") or None,
        order_line=a.get("OrderLine") or None,
        suffix=a.get("PickListSuffix") or None,
        expected=expected,
        picked=picked,
        # the web page's `num(a.shortfall) || picked - expected`: a zero shortfall falls back
        shortfall=_num(a.get("shortfall")) or (picked - expected),
        calls=int(row.get("calls") or 0),
        refused=_num(a.get("refused")),
        empty_visits=_num(a.get("empty_visits")),
        duration_s=_num_or_none(a.get("duration_s")),
        outcome=outcome_of(expected, picked),
    )


# ============================================================== the window (pickReleases.ts)

@dataclass(frozen=True)
class Window:
    kind: str
    start: datetime
    end: datetime
    fixed: bool
    bucket: str


def start_of_day(d: datetime) -> datetime:
    """Local midnight of an aware datetime's day (startOfDay)."""
    return datetime.combine(d.date(), datetime.min.time(), tzinfo=d.tzinfo)


def add_days(d: datetime, days: int, tz: ZoneInfo) -> datetime:
    """The same clock time `days` calendar days on, as the browser's setDate does (addDays)."""
    local = d.astimezone(tz)
    moved: date = local.date() + timedelta(days=days)
    return datetime.combine(moved, local.timetz().replace(tzinfo=None), tzinfo=tz)


def window_for(kind: str, now: datetime, tz: ZoneInfo) -> Window:
    """Today from midnight to now, yesterday fixed, the last 7 days by day (windowFor).

    ("today", Mon 15:30) -> Mon 00:00 → 15:30, by hour ; ("week", …) -> Tue last week 00:00 → now, by day
    """
    now = now.astimezone(tz)
    today = start_of_day(now)
    if kind == "today":
        return Window(kind, today, now, False, "hour")
    if kind == "yesterday":
        return Window(kind, add_days(today, -1, tz), today, True, "hour")
    if kind == "week":
        return Window(kind, add_days(today, -6, tz), now, False, "day")
    raise ValueError(f"no such window: {kind}")


def baseline_range_for(w: Window, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """The seven days before the window's first day (baselineRangeFor)."""
    start = start_of_day(w.start.astimezone(tz))
    return add_days(start, -7, tz), start


# ============================================================== the arithmetic (pickReleaseStats.ts)

def median(xs: list[float]) -> float | None:
    """The middle value, or the mean of the two middle ones.

    >>> median([3, 1, 2]), median([4, 1, 2, 3]), median([])
    (2, 2.5, None)
    """
    if not xs:
        return None
    s = sorted(xs)
    mid = len(s) // 2
    return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2


def percentile(xs: list[float], p: float) -> float | None:
    """Nearest-rank percentile, p in (0, 1].

    >>> percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.95), percentile([], 0.5)
    (10, None)
    """
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, math.ceil(p * len(s)) - 1))]


def _durations(rows: list[Release]) -> list[float]:
    return [r.duration_s for r in rows if r.duration_s is not None]


def outcome_split(rows: list[Release]) -> dict[str, int]:
    """How many releases ended each way.

    >>> outcome_split([])
    {'exact': 0, 'over': 0, 'zeroPick': 0, 'partial': 0, 'total': 0}
    """
    out = {o: 0 for o in OUTCOMES}
    out["total"] = len(rows)
    for r in rows:
        out[r.outcome] += 1
    return out


def share(n: float, total: float) -> float:
    return 0 if total == 0 else n / total


def units_short(rows: list[Release]) -> float:
    """Units the customer did not get: the negative shortfalls, as a positive number."""
    return sum(-r.shortfall for r in rows if r.shortfall < 0)


def fill_rate(rows: list[Release]) -> float | None:
    expected = sum(r.expected for r in rows)
    if expected == 0:
        return None
    return sum(min(r.picked, r.expected) for r in rows) / expected


def per_day(n: float, days: int) -> float:
    return 0 if days == 0 else n / days


def delta_pct(current: float, baseline: float) -> float | None:
    """Percentage change, None when the baseline is nothing.

    >>> delta_pct(110, 100), delta_pct(5, 0)
    (0.1, None)
    """
    if baseline == 0:
        return None
    return (current - baseline) / baseline


def buckets(rows: list[Release], end: datetime, count: int, unit: str, tz: ZoneInfo) -> list[dict]:
    """Fixed-width buckets from `end` backwards, oldest first; hour buckets align to the clock and day
    buckets start at the last local midnight, then step by fixed lengths, as the web page's do."""
    seconds = HOUR_S if unit == "hour" else DAY_S
    local_end = end.astimezone(tz)
    last = local_end.replace(minute=0, second=0, microsecond=0) if unit == "hour" else start_of_day(local_end)
    # Step 1: Step back by fixed lengths in UTC, as the browser's milliseconds do; an aware
    # datetime's own arithmetic is wall-clock and would shift a bucket across a clock change
    last_utc = last.astimezone(UTC)
    out = []
    for i in range(count - 1, -1, -1):
        start = (last_utc - timedelta(seconds=i * seconds)).astimezone(tz)
        out.append({"start": start, "end": (start.astimezone(UTC) + timedelta(seconds=seconds)).astimezone(tz),
                    "byTx": {t: 0 for t in TX_ORDER}, "total": 0, "pickers": 0})
    seen: list[set[str]] = [set() for _ in out]
    # Step 2: Count each release into its bucket, measured in UTC for the same reason
    first = out[0]["start"].astimezone(UTC)
    for r in rows:
        idx = math.floor((r.released_at.astimezone(UTC) - first).total_seconds() / seconds)
        if idx < 0 or idx >= count:
            continue
        out[idx]["byTx"][r.tx] += 1
        out[idx]["total"] += 1
        if r.picker:
            seen[idx].add(r.picker)
    for b, s in zip(out, seen):
        b["pickers"] = len(s)
    return out


def finished_since(rows: list[Release], now: datetime, minutes: int) -> list[Release]:
    cut = now.astimezone(UTC) - timedelta(minutes=minutes)
    return [r for r in rows if r.done_at >= cut]


def distinct(xs: Iterable[str | None]) -> list[str]:
    """The distinct, non-empty values, sorted.

    >>> distinct(["b", None, "a", "", "b"])
    ['a', 'b']
    """
    return sorted({x for x in xs if x})


def active_pickers(rows: list[Release], now: datetime, minutes: int = 15) -> list[str]:
    return distinct(r.picker for r in finished_since(rows, now, minutes))


def refusals(rows: list[Release]) -> float:
    return sum(r.refused for r in rows)


def _group_by(rows: list[Release], key: Callable[[Release], str | None]) -> dict[str, list[Release]]:
    """Rows by key in first-seen order, rows without a key left out (the web page's groupBy)."""
    out: dict[str, list[Release]] = {}
    for r in rows:
        k = key(r)
        if k is None or k == "":
            continue
        out.setdefault(k, []).append(r)
    return out


def per_picker(rows: list[Release]) -> list[dict]:
    """One row per picker, most lines first (perPicker)."""
    total = len(rows)
    out = []
    for picker, rs in _group_by(rows, lambda r: r.picker).items():
        split = outcome_split(rs)
        mix = {t: 0 for t in TX_ORDER}
        for r in rs:
            mix[r.tx] += 1
        top = TX_ORDER[0]
        for t in TX_ORDER:
            if mix[t] > mix[top]:
                top = t
        out.append({
            "picker": picker, "lines": len(rs), "share": share(len(rs), total), "units": sum(r.picked for r in rs),
            "exactPct": share(split["exact"], len(rs)), "zeroPicks": split["zeroPick"], "partials": split["partial"],
            "medianS": median(_durations(rs)), "over5min": sum(1 for r in rs if (r.duration_s or 0) > 300),
            "refusals": refusals(rs), "emptyVisits": sum(r.empty_visits for r in rs), "mix": mix,
            "dominant": {"tx": top, "share": share(mix[top], len(rs))},
        })
    return sorted(out, key=lambda p: -p["lines"])


def deliveries(rows: list[Release]) -> list[dict]:
    """One row per delivery, in first-seen order (deliveries); `releases` stays for the caller."""
    out = []
    for delivery, rs in _group_by(rows, lambda r: r.delivery).items():
        split = outcome_split(rs)
        out.append({
            "delivery": delivery, "customer": next((r.customer for r in rs if r.customer), None), "lines": len(rs),
            "shortLines": split["zeroPick"] + split["partial"], "zeroPicks": split["zeroPick"],
            "partials": split["partial"], "unitsShort": units_short(rs),
            "lastActivity": max(r.done_at for r in rs), "releases": rs,
        })
    return out


def deliveries_incomplete(rows: list[Release]) -> list[dict]:
    """The deliveries with a short line, the most units short first (deliveriesIncomplete)."""
    return sorted([d for d in deliveries(rows) if d["shortLines"] > 0], key=lambda d: -d["unitsShort"])


def duration_stats(rows: list[Release]) -> dict:
    """The histogram bands, the median, the 95th percentile, the lines over five minutes and the
    longest line (durationStats)."""
    ds = _durations(rows)
    bands = []
    for label, lo, hi, tail in BANDS:
        count = sum(1 for d in ds if lo <= d < hi)
        bands.append({"label": label, "count": count, "share": share(count, len(ds)), "tail": tail})
    longest: Release | None = None
    for r in rows:
        if r.duration_s is not None and (longest is None or r.duration_s > (longest.duration_s or 0)):
            longest = r
    return {"bands": bands, "medianS": median(ds), "p95S": percentile(ds, 0.95), "over5min": sum(1 for d in ds if d > 300),
            "longestS": longest.duration_s if longest else None, "longest": longest}


def customer_shortfall(rows: list[Release]) -> list[dict]:
    """The customers with a short line, the most units short first (customerShortfall)."""
    out = []
    for customer, rs in _group_by(rows, lambda r: r.customer).items():
        split = outcome_split(rs)
        out.append({"customer": customer, "unitsShort": units_short(rs), "shortLines": split["zeroPick"] + split["partial"],
                    "zeroPicks": split["zeroPick"], "partials": split["partial"], "fillRate": fill_rate(rs),
                    "lines": len(rs)})
    return sorted([c for c in out if c["shortLines"] > 0], key=lambda c: (-c["unitsShort"], -c["shortLines"]))


def customer_volume(rows: list[Release]) -> list[dict]:
    """Every customer picked for, the most lines first, then by name (customerVolume). The web page
    breaks a tie with localeCompare; a case-blind comparison stands in for it."""
    out = []
    for customer, rs in _group_by(rows, lambda r: r.customer).items():
        split = outcome_split(rs)
        out.append({"customer": customer, "lines": len(rs), "exactPct": share(split["exact"], len(rs)),
                    "zeroPicks": split["zeroPick"], "partials": split["partial"], "over": split["over"],
                    "unitsShort": units_short(rs), "fillRate": fill_rate(rs)})
    return sorted(out, key=lambda c: (-c["lines"], c["customer"].casefold(), c["customer"]))


def zero_pick_feed(rows: list[Release]) -> list[Release]:
    """The zero-picks, newest first (zeroPickFeed)."""
    return sorted([r for r in rows if r.outcome == "zeroPick"], key=lambda r: r.released_at, reverse=True)


def in_range(rows: list[Release], start: datetime, end: datetime) -> list[Release]:
    return [r for r in rows if start <= r.released_at < end]


# ============================================================== the baseline (pickReleaseQueries.ts)

@dataclass(frozen=True)
class HourBucket:
    start: datetime
    rows: int
    is_short: float
    refused: float
    zero: int
    over: int


def merge_hour_buckets(all_rows: list[dict], zero_rows: list[dict], over_rows: list[dict]) -> list[HourBucket]:
    """The three grouped reads by hour merged on the hour (fetchBaselineBuckets).

    >>> [b.zero for b in merge_hour_buckets([{"dimensions": ["2026-09-18T06:00:00+01:00"], "rows": 5, "is_short": "2", "refused": "1"}],
    ...     [{"dimensions": ["2026-09-18T06:00:00+01:00"], "rows": 1}], [])]
    [1]
    """
    by: dict[str, dict] = {}
    for r in all_rows:
        key = str(r["dimensions"][0])
        by[key] = {"start": _date(key), "rows": int(r["rows"]), "is_short": _num(r.get("is_short")),
                   "refused": _num(r.get("refused")), "zero": 0, "over": 0}
    for r in zero_rows:
        if str(r["dimensions"][0]) in by:
            by[str(r["dimensions"][0])]["zero"] = int(r["rows"])
    for r in over_rows:
        if str(r["dimensions"][0]) in by:
            by[str(r["dimensions"][0])]["over"] = int(r["rows"])
    return sorted((HourBucket(**b) for b in by.values() if b["start"] is not None), key=lambda b: b.start)


def _scale(acc: dict[str, float], days: int) -> dict[str, float]:
    d = days if days > 0 else 1
    return {k: v / d for k, v in acc.items()}


def baseline_at_this_time(hours: list[HourBucket], now: datetime, days: int, tz: ZoneInfo) -> dict[str, float]:
    """The full clock hours before now's hour plus the elapsed part of it, over every day, per day."""
    local = now.astimezone(tz)
    h = local.hour
    frac = (local - local.replace(minute=0, second=0, microsecond=0)).total_seconds() / HOUR_S
    acc = {"lines": 0.0, "exact": 0.0, "over": 0.0, "zeroPick": 0.0, "partial": 0.0}
    for b in hours:
        bh = b.start.astimezone(tz).hour
        w = 1 if bh < h else frac if bh == h else 0
        if w == 0:
            continue
        acc["lines"] += b.rows * w
        acc["over"] += b.over * w
        acc["zeroPick"] += b.zero * w
        acc["partial"] += max(0, b.is_short - b.zero) * w
        acc["exact"] += max(0, b.rows - b.is_short - b.over) * w
    return _scale(acc, days)


def baseline_whole_days(hours: list[HourBucket], days: int) -> dict[str, float]:
    acc = {"lines": 0.0, "exact": 0.0, "over": 0.0, "zeroPick": 0.0, "partial": 0.0}
    for b in hours:
        acc["lines"] += b.rows
        acc["over"] += b.over
        acc["zeroPick"] += b.zero
        acc["partial"] += max(0, b.is_short - b.zero)
        acc["exact"] += max(0, b.rows - b.is_short - b.over)
    return _scale(acc, days)


def baseline_refusals_at_this_hour(hours: list[HourBucket], now: datetime, days: int, tz: ZoneInfo) -> float:
    h = now.astimezone(tz).hour
    total = sum(b.refused for b in hours if b.start.astimezone(tz).hour == h)
    return total / days if days > 0 else 0


def baseline_refusals_per_day(hours: list[HourBucket], days: int) -> float:
    return sum(b.refused for b in hours) / days if days > 0 else 0


def per_day_average(values: list[float], days: int) -> float | None:
    """The average over `days`, a day with no releases counting as zero.

    >>> per_day_average([10, 20], 7), per_day_average([], 7)
    (4.285714285714286, None)
    """
    if days <= 0 or not values:
        return None
    return sum(values) / days


def chronic_items(all_rows: list[dict], zero_rows: list[dict], min_releases: int) -> list[dict]:
    """Items that keep coming up empty, from the two grouped reads by item (fetchChronicItems)."""
    zero_by = {str(r["dimensions"][0]): int(r["rows"]) for r in zero_rows}
    out = []
    for r in all_rows:
        item = str(r["dimensions"][0] or "")
        zero_picks = zero_by.get(item, 0)
        if not item or r["rows"] < min_releases or zero_picks == 0:
            continue
        shortfall = _num(r.get("shortfall"))
        out.append({"item": item, "description": r["dimensions"][1] if len(r["dimensions"]) > 1 else None,
                    "releases": int(r["rows"]), "zeroPicks": zero_picks, "zeroPickRate": zero_picks / r["rows"],
                    "unitsShort": -shortfall if shortfall < 0 else 0})
    return sorted(out, key=lambda i: (-i["zeroPickRate"], -i["unitsShort"]))
