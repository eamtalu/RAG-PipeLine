"""Chunk 133: "Shape of the day" for the Teams Home tab.

The pick releases page draws lines per hour for the last 24 hours, stacked by transaction, with
the pickers active each hour on a strip beneath. The Teams tab shows the same chart under its stat
tiles. Two grouped reads make it: lines by (hour_start, transaction_name), and distinct pickers by
hour_start, both on the tenant's clock. Everything the page prints is decided here - the labels, the
axis ticks, which bar carries the peak label - so the page only draws (it never formats a number).

The hours are 24 wall-clock hours ending with the current one, as the page builds them. On the
night the clocks go back, the repeated hour is one bucket, which is what `hour_start` groups to.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_agent.tools import aggregate_releases

HOURS = 24
TX_ORDER = ("stock", "jit", "milk", "frz", "other")
_GROUPS_CAP = 500  # 24 hours x the handful of transactions a site runs; the tool's own ceiling


# ============================================================== the kinds
def tx_code(name: str | None) -> str:
    """Transactions are named per site ("Milk Pick (Brighton)"); the chart keys on the kind. The
    same reading as the pick releases page (`txCode` in the log explorer's `pickReleases.ts`)."""
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


# ============================================================== the axis
def _nice_step(peak: int) -> int:
    """The page's steps, 1, 2, 2.5, 5 or 10 times a power of ten for about four ticks, kept whole:
    lines are counted, so a 2.5 step below 10 (or any step below 1) would print rounded ticks."""
    raw = max(1, peak) / 4
    mag = 10 ** math.floor(math.log10(raw))
    norm = raw / mag
    for choice in (1, 2, 2.5, 5, 10):
        step = choice * mag
        if norm <= choice and (step >= 1 and float(step).is_integer()):
            return int(step)
    return max(1, int(10 * mag))


def axis(peak: int) -> dict:
    step = _nice_step(peak)
    top = math.ceil(max(1, peak) / step) * step
    return {"top": top, "ticks": [{"value": v, "text": f"{v:,}"} for v in range(0, top + 1, step)]}


# ============================================================== the hours
def hour_starts(local_now: datetime) -> list[datetime]:
    """24 naive wall-clock hours on the tenant's clock, oldest first, the last one the current hour."""
    last = local_now.replace(tzinfo=None, minute=0, second=0, microsecond=0)
    return [last - timedelta(hours=i) for i in range(HOURS - 1, -1, -1)]


def _hour_key(dimension) -> datetime | None:
    try:
        return datetime.fromisoformat(str(dimension)).replace(tzinfo=None)
    except ValueError:
        return None


def _hhmm(d: datetime) -> str:
    return d.strftime("%H:%M")


def _day_hhmm(d: datetime) -> str:
    return f"{d.day} {d.strftime('%b')} {_hhmm(d)}"


def _first_peak(values: list[int]) -> int | None:
    """Where the peak label goes: the first of the highest, or nowhere on an empty chart."""
    best = max(values, default=0)
    return values.index(best) if best > 0 else None


def build(hours: list[datetime], line_rows: list[dict], picker_rows: list[dict]) -> dict:
    """The chart from the two reads' rows. Pure: rows outside `hours` are left out."""
    index = {h: i for i, h in enumerate(hours)}
    by_tx = [dict.fromkeys(TX_ORDER, 0) for _ in hours]
    for r in line_rows:
        dims = r.get("dimensions") or [None, None]
        i = index.get(_hour_key(dims[0]))
        if i is not None:
            by_tx[i][tx_code(dims[1] if len(dims) > 1 else None)] += int(r.get("rows") or 0)
    pickers = [0] * len(hours)
    for r in picker_rows:
        i = index.get(_hour_key((r.get("dimensions") or [None])[0]))
        if i is not None:
            pickers[i] = int(r.get("distinct_user_name") or 0)

    totals = [sum(counts.values()) for counts in by_tx]
    peak_pickers = _first_peak(pickers)
    pickers_top = max([1, *pickers])
    return {
        "caption": f"{_day_hhmm(hours[0])} → {_day_hhmm(hours[-1] + timedelta(hours=1))}",
        "legend": [{"tx": t, "label": t} for t in TX_ORDER if any(counts[t] for counts in by_tx)],
        "axis": axis(max(totals, default=0)),
        "hours": [
            {"label": _hhmm(h), "range": f"{_hhmm(h)} – {_hhmm(h + timedelta(hours=1))}",
             "total": totals[i], "total_text": f"{totals[i]:,}",
             "segments": [{"tx": t, "label": t, "lines": by_tx[i][t], "text": f"{by_tx[i][t]:,}"}
                          for t in TX_ORDER if by_tx[i][t]],
             "pickers": pickers[i], "pickers_text": f"{pickers[i]:,}"}
            for i, h in enumerate(hours)
        ],
        "peak": _first_peak(totals),
        "peak_pickers": peak_pickers,
        "peak_pickers_text": "" if peak_pickers is None else _pickers_word(pickers[peak_pickers]),
        "pickers_top": pickers_top,
        "pickers_top_text": f"{pickers_top:,}",
    }


def _pickers_word(n: int) -> str:
    return f"{n:,} picker" + ("" if n == 1 else "s")


# ============================================================== the reads
async def compute(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> dict:
    """The last 24 hours for one customer. Raises what the reads raise; the snapshot decides."""
    hours = hour_starts(now.astimezone(tz))
    window = {"start": hours[0].replace(tzinfo=tz).astimezone(timezone.utc).isoformat(), "end": now.isoformat()}
    lines = await aggregate_releases(db, {**window, "group_by": ["hour_start", "transaction_name"],
                                          "limit": _GROUPS_CAP}, customer_code)
    pickers = await aggregate_releases(db, {**window, "group_by": ["hour_start"], "stat": ["distinct:user_name"],
                                            "limit": _GROUPS_CAP}, customer_code)
    return build(hours, lines["rows"], pickers["rows"])
