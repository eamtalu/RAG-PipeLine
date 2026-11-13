"""The executive Home of the eSmart Eye Teams app (stage D, step 7d, screen 02): the operation
across every site for a period, from the same settled rows the supervisor's Home reads.

Four periods (today, this week, the last 30 days, the last 90 days), each with the whole customer
and every site: fill rate and its change against the period before, on-time dispatch from the
closed at-risk departures, lines, releases, deliveries and pickers; the 30-day fill-rate series
per site for the chart; the decisions waiting (the pending approvals). Mirrored under
`EYE#<customer>` / `EXEC#HOME` every few minutes by the consumer. The weekly brief and the open
advisories are not computed here: the Insight agent and the advisory agents are later steps.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import approvals
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases
from app.services.analytics_at_risk import delivery_store
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import ALL_SITES_ID, Site, list_sites, with_site

logger = logging.getLogger(__name__)

PERIODS = ("today", "week", "month", "quarter")
PERIOD_LABELS = {"today": "Today", "week": "This week", "month": "Month", "quarter": "Quarter"}
SERIES_DAYS = 30
PICKERS_CAP = 500


def period_windows(local_now: datetime, period: str) -> tuple[datetime, datetime, datetime, datetime]:
    """The current window and the same stretch of the period before it, on the warehouse's clock:
    this week so far against last week to the same weekday and time.

    >>> now = datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)  # a Friday
    >>> s, e, ps, pe = period_windows(now, "week")
    >>> s.isoformat(), e == now, ps.isoformat(), pe == now - timedelta(days=7)
    ('2026-11-09T00:00:00+00:00', True, '2026-11-02T00:00:00+00:00', True)
    >>> period_windows(now, "today")[0].isoformat()
    '2026-11-13T00:00:00+00:00'
    """
    midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "today":
        start, back = midnight, timedelta(days=1)
    elif period == "week":
        start, back = midnight - timedelta(days=local_now.weekday()), timedelta(days=7)
    elif period == "month":
        start, back = midnight - timedelta(days=30), timedelta(days=30)
    elif period == "quarter":
        start, back = midnight - timedelta(days=90), timedelta(days=90)
    else:
        raise ValueError(f"unknown period {period!r}")
    return start, local_now, start - back, local_now - back


def range_text(start: datetime, end: datetime) -> str:
    """The period as the heading says it.

    >>> range_text(datetime(2026, 11, 9), datetime(2026, 11, 13, 17, 11))
    '9 – 13 November · figures to 17:11'
    >>> range_text(datetime(2026, 10, 14), datetime(2026, 11, 13, 9, 0))
    '14 October – 13 November · figures to 09:00'
    """
    if start.month == end.month:
        days = f"{start.day} – {end.day} {end.strftime('%B')}"
    else:
        days = f"{start.day} {start.strftime('%B')} – {end.day} {end.strftime('%B')}"
    return f"{days} · figures to {end.strftime('%H:%M')}"


def _dec(value) -> Decimal:
    try:
        return Decimal(str(value)) if value is not None else Decimal(0)
    except InvalidOperation:
        return Decimal(0)


def fill_rate(expected, shortfall) -> float | None:
    """The share of expected units that were picked; None when nothing was expected.

    >>> fill_rate(1000, -29), fill_rate(0, 0)
    (0.971, None)
    """
    exp = _dec(expected)
    if exp <= 0:
        return None
    short = -_dec(shortfall)
    return float(round(max(Decimal(0), (exp - short) / exp), 4))


def on_time(counts: dict) -> tuple[float | None, int, int]:
    """On-time dispatch from the history counts: the share of closed departures that were fine,
    with the departures and the missed ones.

    >>> on_time({"fine": 93, "held": 5, "missed": 2, "unknown": 1})
    (0.93, 100, 2)
    >>> on_time({"fine": 0, "held": 0, "missed": 0, "unknown": 0})
    (None, 0, 0)
    """
    fine, held, missed = (int(counts.get(k, 0)) for k in ("fine", "held", "missed"))
    departures = fine + held + missed
    if departures == 0:
        return None, 0, 0
    return round(fine / departures, 4), departures, missed


async def _metrics(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime, *,
                   with_pickers: bool) -> dict:
    args = {"start": start.isoformat(), "end": end.isoformat()}
    totals = await aggregate_releases(db, with_site(dict(args), site), cc)
    row = (totals.get("rows") or [{}])[0]
    pickers = 0
    if with_pickers:
        by_picker = await aggregate_releases(
            db, with_site({**args, "group_by": ["user_name"], "limit": PICKERS_CAP}, site), cc)
        pickers = len(by_picker.get("rows") or [])
    counts = await delivery_store.history_counts(
        db, cc, start=start.date(), end=end.date(), warehouse=site.site_id if site else None)
    rate, departures, missed = on_time(counts["counts"])
    return {
        "fill_rate": fill_rate(row.get("expected"), row.get("shortfall")),
        "on_time": rate,
        "departures": departures,
        "left_behind": missed,
        "lines": int(totals.get("total_rows") or 0),
        "deliveries": int(row.get("deliveries") or 0),
        "pickers": pickers,
    }


async def _series(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime) -> dict[str, float | None]:
    """Fill rate per day over the window: {"2026-11-07": 0.971, ...}; a day with no rows is absent."""
    result = await aggregate_releases(
        db, with_site({"start": start.isoformat(), "end": end.isoformat(), "group_by": ["day"], "limit": 100}, site), cc)
    out: dict[str, float | None] = {}
    for row in result.get("rows") or []:
        day = str((row.get("dimensions") or [""])[0])[:10]
        if day:
            out[day] = fill_rate(row.get("expected"), row.get("shortfall"))
    return out


async def compute(db: AsyncSession, customer_code: str, *, now: datetime | None = None,
                  target: float = 0.98, max_sites: int = 3) -> dict | None:
    """The executive Home for one customer, or None when it has no pick_release settlement.

    (customer with sites BRI, LON) -> {"periods": {"week": {"all": {...}, "sites": {"BRI": {...},
    "LON": {...}}}, ...}, "series": {...}, "decisions": [...], ...}
    """
    now = now or datetime.now(timezone.utc)
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    local_now = now.astimezone(tz)
    sites = (await list_sites(db, customer_code))[:max_sites]
    targets: list[tuple[str, Site | None]] = [(ALL_SITES_ID, None)] + [(s.site_id, s) for s in sites]
    periods: dict[str, dict] = {}
    try:
        # Step 1: Every period for every target, the period before it for the change; pickers only
        # for the current window, because the count is the expensive read
        for period in PERIODS:
            start, end, prev_start, prev_end = period_windows(local_now, period)
            entry = {"label": PERIOD_LABELS[period], "range_text": range_text(start, end), "all": None, "sites": {}}
            for site_id, site in targets:
                current = await _metrics(db, customer_code, site, start, end, with_pickers=True)
                previous = await _metrics(db, customer_code, site, prev_start, prev_end, with_pickers=False)
                metrics = {**current, "fill_rate_prev": previous["fill_rate"], "on_time_prev": previous["on_time"],
                           "lines_prev": previous["lines"]}
                if site is None:
                    entry["all"] = metrics
                else:
                    entry["sites"][site_id] = metrics
            periods[period] = entry
        # Step 2: The 30-day series per target, for the chart
        series_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=SERIES_DAYS - 1)
        labels = [(series_start + timedelta(days=i)).date().isoformat() for i in range(SERIES_DAYS)]
        series: dict = {"labels": labels, "all": [], "sites": {}}
        for site_id, site in targets:
            by_day = await _series(db, customer_code, site, series_start, local_now)
            values = [by_day.get(day) for day in labels]
            if site is None:
                series["all"] = values
            else:
                series["sites"][site_id] = values
    except UnknownSettlement:
        return None
    except ReadProblem as exc:
        logger.warning("executive home for %s could not be read: %s", customer_code, exc.problems)
        return None
    # Step 3: The decisions waiting are the pending approvals; advisories and the brief are later
    decisions = [
        {"reference": a.reference, "kind": a.kind, "title": a.title, "detail": a.detail,
         "requested_by_name": a.requested_by_name, "created_at": a.created_at.isoformat()}
        for a in await approvals.pending(db, customer_code)
    ]
    return {
        "customer_code": customer_code,
        "as_of": now.isoformat(),
        "target_fill_rate": target,
        "sites": [{"site_id": s.site_id, "name": s.name} for s in sites],
        "periods": periods,
        "series": series,
        "decisions": decisions,
        # STUB: the advisory agents (step 9) and the Insight agent (MISSING.md) are not built
        "advisories": {"open": 0, "high": 0, "note": "No advisory agents yet (step 9)."},
        "brief": {"text": None, "note": "The weekly brief is written by an Insight agent that is not built yet."},
    }


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None,
                     tracker: ChangeTracker | None = None, target: float = 0.98, max_sites: int = 3,
                     session_factory=None) -> list[str]:
    """Compute and mirror the executive Home of every bound, enabled customer (or of `only`); the
    codes written. One failure does not stop the rest."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    written: list[str] = []
    async with factory() as db:
        if only is None:
            codes = sorted({b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        else:
            codes = list(only)
        for code in codes:
            try:
                payload = await compute(db, code, target=target, max_sites=max_sites)
                if payload is None:
                    continue
                stamp = datetime.now(timezone.utc).timestamp()
                env = envelope("exec_home", code, payload=payload, as_of=payload["as_of"], now=stamp)
                await write_if_changed(mirror, tracker, env, now=stamp)
                written.append(code)
            except Exception:
                logger.exception("executive home for %s failed", code)
    return written

