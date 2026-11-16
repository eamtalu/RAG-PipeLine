"""Step 31 (chunk 209): the Teams Pick releases page's row, mirrored every five minutes per site.

The web app's Pick releases page (`matrix-log-explorer`, `/matrix/releases`) reads the settled
`pick_release` rows and computes its panels in the browser. The Teams tab cannot reach this server,
so the consumer reads the same rows through the same functions as the web page's API
(`settle_reads.listed` and `settle_reads.grouped`), computes the same panels with
`releases_stats` (a port of the web page's arithmetic) and mirrors one `site_releases` row per site
and for "all", holding three fixed windows: today (to the time of the sweep), yesterday and the last
7 days. The owner chose fixed windows with every panel and no filter chips or drill-downs for the
first version (16 November 2026).

One read serves all three windows: the last 7 days hold today's last 24 hours (the chart) and
yesterday. The arithmetic runs in a worker thread, because the consumer answers Teams questions
on the same event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import settle_reads
from app.services.teams import releases_stats as rs
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import ALL_SITES_ID, Site, list_sites

logger = logging.getLogger(__name__)

ROW_TYPE = "site_releases"
SETTLEMENT = "pick_release"
WINDOWS = ("today", "yesterday", "week")
#: The settled rows are read in pages of this size, as the web page reads them.
PAGE = 1000
#: A runaway guard: a week holds about 6,500 releases on the live tenant.
MAX_ROWS = 40_000
#: The chronic-items panel's floor (the web page's MIN_ITEM_RELEASES) and the feed's length.
MIN_ITEM_RELEASES = 10
FEED_SHOWN = 12
#: Long tables are cut to this many rows, with the full count beside them.
TABLE_ROWS = 100
#: Below DynamoDB's 400 KB item limit, with room for the key and the envelope.
MAX_ROW_BYTES = 350_000
ITEM_DESCRIPTION = "lookup:item description.ItemDescription"
NO_SETTLEMENT_NOTE = "no pick releases yet · the settlement has no rows for this site"
WINDOW_WORD = {"today": "today", "yesterday": "yesterday", "week": "last 7 days"}


# ============================================================== reads


async def _rows(db: AsyncSession, cc: str, start: datetime, end: datetime, where: list[str]) -> list[dict]:
    """Every settled release whose first confirm fell in [start, end), in pages, as the web page reads them.

    (acme, last 7 days) -> [{"key": [...], "event_time": ..., "attributes": {...}, "looked_up": {...}}, ...]
    """
    out: list[dict] = []
    offset = 0
    while True:
        page = await settle_reads.listed(db, cc, SETTLEMENT, start=start, end=end, where=list(where), limit=PAGE, offset=offset)
        out.extend(page["rows"])
        offset += len(page["rows"])
        if len(page["rows"]) < PAGE or offset >= page["total"] or offset >= MAX_ROWS:
            return out


async def _grouped(db: AsyncSession, cc: str, *, group_by: list[str], start: datetime, end: datetime, where: list[str],
                   stat: list[str] | None = None, limit: int = 2000) -> list[dict]:
    out = await settle_reads.grouped(db, cc, SETTLEMENT, group_by=group_by, start=start, end=end, where=list(where),
                                     stat=list(stat or []), limit=limit)
    return out["rows"]


async def _baseline(db: AsyncSession, cc: str, w: rs.Window, tz: ZoneInfo, where: list[str]) -> dict[str, Any]:
    """The seven days before the window: the hour buckets, the chronic items and the deliveries per day,
    the web page's five grouped reads (pickReleaseQueries.ts)."""
    start, end = rs.baseline_range_for(w, tz)
    hours = rs.merge_hour_buckets(
        await _grouped(db, cc, group_by=["hour_start"], start=start, end=end, where=where),
        await _grouped(db, cc, group_by=["hour_start"], start=start, end=end, where=[*where, "picked==0"]),
        await _grouped(db, cc, group_by=["hour_start"], start=start, end=end, where=[*where, "shortfall>0"]))
    try:
        by_item = await _grouped(db, cc, group_by=["item_number", ITEM_DESCRIPTION], start=start, end=w.end, where=where,
                                 limit=5000)
    except settle_reads.ReadProblem:
        # a tenant without the item-description lookup: the items without their names, rather than
        # the web page's error for the whole panel
        by_item = await _grouped(db, cc, group_by=["item_number"], start=start, end=w.end, where=where, limit=5000)
    items = rs.chronic_items(
        by_item,
        await _grouped(db, cc, group_by=["item_number"], start=start, end=w.end, where=[*where, "picked==0"], limit=5000),
        MIN_ITEM_RELEASES)
    per_day = await _grouped(db, cc, group_by=["day"], start=start, end=end, where=where, stat=["distinct:delivery_number"],
                             limit=400)
    return {"hours": hours, "items": items,
            "deliveries_per_day": [rs._num(r.get("distinct_delivery_number")) for r in per_day]}


# ============================================================== one window (PickReleasesPage.tsx, no chips)


def _iso(d: datetime | None, tz: ZoneInfo) -> str | None:
    return d.astimezone(tz).isoformat() if d else None


def _lite(r: rs.Release | None, tz: ZoneInfo) -> dict | None:
    """A release as the feed and the longest line show it."""
    if r is None:
        return None
    return {"key": r.key, "released_at": _iso(r.released_at, tz), "finished_at": _iso(r.finished_at, tz),
            "transaction": r.transaction, "tx": r.tx, "picker": r.picker, "delivery": r.delivery, "customer": r.customer,
            "item": r.item, "description": r.description, "order_line": r.order_line, "expected": r.expected,
            "picked": r.picked, "shortfall": r.shortfall, "refused": r.refused, "duration_s": r.duration_s,
            "outcome": r.outcome}


def _caption(w: rs.Window, tz: ZoneInfo) -> str:
    """The window's words under the controls (windowCaption)."""
    def t(d: datetime) -> str:
        return d.astimezone(tz).strftime("%H:%M")

    def day(d: datetime) -> str:
        local = d.astimezone(tz)
        return f"{local.day} {local.strftime('%b')}"

    if w.kind == "today":
        return f"today 00:00 → {t(w.end)}"
    if w.kind == "yesterday":
        return "yesterday · 00:00 → 23:59 · fixed window"
    return f"{day(w.start)} → {day(w.end)} · 7 days"


def window_panels(kind: str, rows: list[rs.Release], base: dict[str, Any], now: datetime, tz: ZoneInfo) -> dict:
    """Every panel of the web page for one window, from the rows read and the baseline, with no chips.

    ("today", rows of the last 7 days, baseline, 15:30) -> {"tiles": {...}, "split": {...}, "chart": [...], ...}
    """
    # Step 1: The window and the rows in it, as the page derives them before anything is drawn
    w = rs.window_for(kind, now, tz)
    rolling = kind == "today"
    if rolling:
        # the today view's chart reads the last 24 hours (fetchRangeFor)
        chart_rows = rs.in_range(rows, now.astimezone(timezone.utc) - timedelta(hours=24), now)
    else:
        chart_rows = rs.in_range(rows, w.start, w.end)
    filtered = rs.in_range(rows, w.start, w.end)
    base_days = 1 if kind == "week" else 7
    hours = base["hours"]
    split_base = (rs.baseline_at_this_time(hours, now, 7, tz) if rolling else rs.baseline_whole_days(hours, base_days)) if hours else None
    # Step 2: The panels, each from the same filtered rows, so nothing on the page can disagree
    split = rs.outcome_split(filtered)
    span = (w.end - w.start).total_seconds()
    if w.bucket == "day":
        chart = rs.buckets(filtered, w.end, max(2, math.ceil(span / rs.DAY_S)), "day", tz)
    elif rolling:
        chart = rs.buckets(chart_rows, now, 24, "hour", tz)
    else:
        chart = rs.buckets(filtered, w.end, max(1, math.ceil(span / rs.HOUR_S)), "hour", tz)
    present = [t for t in rs.TX_ORDER if any(r.tx == t for r in chart_rows)]
    last15 = rs.finished_since(filtered, now, 15)
    active = rs.active_pickers(filtered, now)
    in_window = rs.distinct(r.picker for r in filtered)
    last_hour = rs.finished_since(filtered, now, 60)
    refused_now = rs.refusals(last_hour) if rolling else rs.refusals(filtered)
    refused_base = rs.baseline_refusals_at_this_hour(hours, now, 7, tz) if rolling else rs.baseline_refusals_per_day(hours, base_days)
    feed = rs.zero_pick_feed(filtered)
    pickers = rs.per_picker(filtered)
    every_delivery = rs.deliveries(filtered)
    incomplete = rs.deliveries_incomplete(filtered)
    deliveries_base = rs.per_day_average(base["deliveries_per_day"], base_days)
    lines_base = split_base["lines"] if split_base else None
    durations = {"all": rs.duration_stats(filtered)}
    for tx in present:
        durations[tx] = rs.duration_stats([r for r in filtered if r.tx == tx])
    customers = rs.customer_shortfall(filtered)
    volume = rs.customer_volume(filtered)
    # Step 3: The window as the page draws it; long tables are cut, with their full count kept
    return {
        "kind": kind, "word": WINDOW_WORD[kind], "rolling": rolling, "fixed": w.fixed, "bucket": w.bucket,
        "start": _iso(w.start, tz), "end": _iso(w.end, tz), "caption": _caption(w, tz),
        "tiles": {
            "last15_lines": len(last15), "last15_deliveries": len(rs.distinct(r.delivery for r in last15)),
            "busiest_hour": max([0, *(b["total"] for b in chart)]),
            "deliveries": len(every_delivery), "deliveries_incomplete": len(incomplete),
            "deliveries_base": deliveries_base,
            "deliveries_delta": None if deliveries_base is None else rs.delta_pct(len(every_delivery), deliveries_base),
            "lines": len(filtered), "lines_base": lines_base,
            "lines_delta": None if lines_base is None else rs.delta_pct(len(filtered), lines_base),
            "pickers_active": len(active), "pickers_in_window": len(in_window),
            "refusals": refused_now, "refusals_base": refused_base, "refusals_delta": rs.delta_pct(refused_now, refused_base),
        },
        "split": split, "split_base": split_base,
        "chart": [{**b, "start": _iso(b["start"], tz), "end": _iso(b["end"], tz)} for b in chart], "present": present,
        "feed": [_lite(r, tz) for r in feed[:FEED_SHOWN]], "feed_total": len(feed),
        "pickers": pickers,
        "top_share": pickers[0]["share"] + pickers[1]["share"] if len(pickers) >= 2 else 0,
        "incomplete": [{k: v for k, v in d.items() if k != "releases"} | {"lastActivity": _iso(d["lastActivity"], tz)}
                       for d in incomplete[:TABLE_ROWS]],
        "durations": {k: {**v, "longest": _lite(v["longest"], tz)} for k, v in durations.items()},
        "customers": customers[:TABLE_ROWS], "customers_total": len(customers),
        "items": base["items"][:TABLE_ROWS], "items_total": len(base["items"]),
        "volume": volume[:TABLE_ROWS], "volume_total": len(volume),
    }


# ============================================================== the row


def _size(row: dict) -> int:
    return len(json.dumps(row, default=str).encode())


def fit(row: dict, *, max_bytes: int = MAX_ROW_BYTES) -> dict:
    """The row as it is when it fits, else with its long tables cut to 25 rows, saying so.

    >>> fit({"windows": {}}, max_bytes=100)
    {'windows': {}}
    """
    if _size(row) <= max_bytes:
        return row
    for w in row.get("windows", {}).values():
        for key in ("pickers", "incomplete", "customers", "items", "volume"):
            w[key] = w[key][:25]
    row["trimmed"] = ["long tables cut to 25 rows"]
    return row


async def compute(db: AsyncSession, customer_code: str, site: Site | None = None, *, now: datetime | None = None) -> dict:
    """The Pick releases page's row for the whole customer or one site, with its three windows.

    (customer with releases) -> {"available": True, "windows": {"today": {...}, "yesterday": {...}, "week": {...}}} ;
    (Site BRI) -> its warehouse's rows only ; (no settlement) -> {"available": False, "note": ...}
    """
    # Step 1: The clock and the site, so "today" is the warehouse's day and a site sees its own rows
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    where = [f"warehouse=={site.site_id}"] if site else []
    head = {"as_of": now.isoformat(), "site_id": site.site_id if site else ALL_SITES_ID,
            "scope": site.name if site else "all sites", "timezone": tz.key}
    week = rs.window_for("week", now, tz)
    # Step 2: One read of the last 7 days serves every window; a missing settlement is said plainly
    try:
        raw = await _rows(db, customer_code, week.start, now, where)
        bases = {k: await _baseline(db, customer_code, rs.window_for(k, now, tz), tz, where) for k in WINDOWS}
    except settle_reads.UnknownSettlement:
        return {"available": False, "note": NO_SETTLEMENT_NOTE, **head}
    # Step 3: The arithmetic off the event loop, which also answers Teams questions
    def build() -> dict:
        rows = [rs.to_release(r) for r in raw]
        return {k: window_panels(k, rows, bases[k], now, tz) for k in WINDOWS}

    windows = await asyncio.to_thread(build)
    return fit({"available": True, **head, "rows_read": len(raw), "windows": windows})


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     max_sites: int = 3, session_factory=None) -> list[str]:
    """Mirror the Pick releases page's rows (the whole customer and each active site) for every bound,
    enabled customer, or `only`; the codes written. One failure skips that customer.

    (acme with sites BRI, LON) -> SITE#all#RELEASES, SITE#BRI#RELEASES, SITE#LON#RELEASES ; (mirror down) -> []
    """
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    written: list[str] = []
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                targets: list[Site | None] = [None, *(await list_sites(db, code))[:max_sites]]
                for site in targets:
                    row = await compute(db, code, site)
                    stamp = datetime.now(timezone.utc).timestamp()
                    env = envelope(ROW_TYPE, code, site_id=row["site_id"], payload=row, as_of=row["as_of"], now=stamp)
                    await write_if_changed(mirror, tracker, env, now=stamp)
                written.append(code)
            except Exception:
                logger.exception("teams.releases_detail_failed customer=%s", code)
                await db.rollback()
    return written
