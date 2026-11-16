"""Step 34 (chunk 212): Overview's all-sites view of the eSmart Eye Teams app, for the C-level reader.

Shown when "All sites" is chosen: every site side by side (today's departures through the flow, fill
rate, routes out, pickers, what needs fixing), fill rate by site by day for the last 7 days, the last
8 weeks against the target, and what changed against the week before in plain facts. Every figure
comes from what the app already holds: the settled releases (through the executive Home's own
reads), the at-risk deliveries and the advisories. Nothing without a source is shown: there are no
shift times and no rosters. Mirrored under `EYE#<customer>` / `SITE#all#SITES` every five minutes.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases
from app.services.analytics_at_risk import delivery_store, model
from app.services.teams import exec_home
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import Site, list_sites, with_site

logger = logging.getLogger(__name__)

ROW_TYPE = "all_sites"
STAGES = ("to_pick", "picking", "staged", "loading", "departed")
FLAGGED_TIERS = ("left_behind", "at_risk")
BY_DAY_DAYS = 7
WEEKS = 8
ACTIVE_MINUTES = 15
#: The by-day table's bands, as the screen colours them: the target and the low line under it.
LOW_FILL_RATE = 0.96
#: The smallest moves "what changed" names: 0.5 points of fill rate, 1 point of on-time, 2% of volume.
FILL_STEP_PTS = 0.5
ON_TIME_STEP_PTS = 1.0
VOLUME_STEP_PCT = 2
NO_SETTLEMENT_NOTE = "no pick releases yet · this logspace has no pick_release settlement"


# ============================================================== the flow of today's departures


def stage_of(row: AnalyticsAtRiskDelivery) -> str:
    """Where one of today's deliveries is: to pick, picking, staged (picked, nothing on the van yet),
    loading, or departed (the board closed it). Picking is judged as the at-risk board judges it.

    (no line confirmed) -> "to_pick" ; (3 of 10 confirmed) -> "picking" ; (closed) -> "departed"
    """
    if row.status == "closed":
        return "departed"
    if (row.lines_confirmed or 0) == 0:
        return "to_pick"
    if model.picking_open(delivery_store.state_from_row(row)):
        return "picking"
    if row.loading_expected is False or (row.packages_loaded or 0) == 0:
        return "staged"
    return "loading"


def flow(rows: list[AnalyticsAtRiskDelivery]) -> dict:
    """Today's deliveries by stage, and the routes whose every delivery has gone.

    ([open on R1, two closed on R2]) -> {"stages": {...}, "total": 3, "routes_out": 1, "routes_total": 2}
    """
    stages = {s: 0 for s in STAGES}
    by_route: dict[str, list[bool]] = {}
    for r in rows:
        stages[stage_of(r)] += 1
        if r.route:
            by_route.setdefault(r.route, []).append(r.status == "closed")
    return {"stages": stages, "total": len(rows), "routes_out": sum(all(v) for v in by_route.values()),
            "routes_total": len(by_route)}


def status_of(rows: list[AnalyticsAtRiskDelivery]) -> dict:
    """What needs fixing today: the open deliveries flagged left behind or at risk, and who checked them.

    ([left behind checked by Sam, at risk]) -> {"to_fix": 2, "checked_by": ["Sam"], "state": "attention"}
    """
    flagged = [r for r in rows if r.status == "open" and r.tier in FLAGGED_TIERS]
    checked = sorted({r.checked_by for r in flagged if r.checked_by})
    return {"to_fix": len(flagged), "checked_by": checked, "state": "attention" if flagged else "on_track"}


# ============================================================== what changed, in plain facts


def _pts(delta: float) -> str:
    return f"{'+' if delta >= 0 else '−'}{abs(delta):.1f} pts"


def _site_lines(name: str, m: dict, days: list[float | None], target: float) -> list[tuple[float, dict]]:
    out: list[tuple[float, dict]] = []
    if m["fill_rate"] is not None and m["fill_rate_prev"] is not None:
        d = (m["fill_rate"] - m["fill_rate_prev"]) * 100
        if abs(d) >= FILL_STEP_PTS:
            seen = [x for x in days if x is not None]
            if d < 0:
                detail = f"{sum(x < LOW_FILL_RATE for x in seen)} of {len(seen)} days under {LOW_FILL_RATE:.0%}."
            else:
                detail = f"{sum(x >= target for x in seen)} of {len(seen)} days at {target:.0%} or more."
            out.append((abs(d), {"direction": "down" if d < 0 else "up",
                                 "title": f"{name} fill rate {'fell' if d < 0 else 'rose'} to {m['fill_rate']:.1%}.",
                                 "detail": detail, "delta_text": _pts(d)}))
    if m["on_time"] is not None and m["on_time_prev"] is not None:
        d = (m["on_time"] - m["on_time_prev"]) * 100
        if abs(d) >= ON_TIME_STEP_PTS:
            out.append((abs(d), {"direction": "down" if d < 0 else "up",
                                 "title": f"{name} on-time dispatch {'fell' if d < 0 else 'rose'} to {m['on_time']:.1%}.",
                                 "detail": f"{m['left_behind']} left behind in {m['departures']} departures.",
                                 "delta_text": _pts(d)}))
    return out


def what_changed(sites: list[tuple[str, str, dict | None, list[float | None]]], total: dict, *,
                 target: float = 0.98) -> list[dict]:
    """The week against the week before, biggest moves first: each site's fill rate and on-time
    dispatch when they moved, the volume across the sites, then the sites that held steady and those
    with no activity.

    ([Brighton fill 97.6% -> 96.2%, 4 of 7 days under 96%]) -> [{"title": "Brighton fill rate fell to 96.2%.", ...}]
    """
    # Step 1: The moves per site, so the biggest change leads whichever site it is at
    moves: list[tuple[float, dict]] = []
    steady: list[dict] = []
    quiet: list[dict] = []
    for _site_id, name, m, days in sites:
        if m is None or (m["lines"] == 0 and m["lines_prev"] == 0):
            quiet.append({"direction": "none", "title": f"{name}: no activity in either week.", "detail": "",
                          "delta_text": ""})
            continue
        lines = _site_lines(name, m, days, target)
        if lines:
            moves += lines
        else:
            fill = "–" if m["fill_rate"] is None else f"{m['fill_rate']:.1%}"
            on_time = "–" if m["on_time"] is None else f"{m['on_time']:.1%}"
            d = 0.0 if m["fill_rate"] is None or m["fill_rate_prev"] is None else (m["fill_rate"] - m["fill_rate_prev"]) * 100
            steady.append({"direction": "steady", "title": f"{name} held steady on every measure.",
                           "detail": f"Fill rate {fill} · on time {on_time}.", "delta_text": _pts(d)})
    # Step 2: The volume across the sites, as lines only: no headcount, because staffing is the
    # reader's judgement (owner, 16 November 2026)
    volume: list[dict] = []
    if total["lines_prev"]:
        pct = round((total["lines"] - total["lines_prev"]) / total["lines_prev"] * 100)
        if abs(pct) >= VOLUME_STEP_PCT:
            grew = pct > 0
            volume.append({"direction": "up" if grew else "down", "title": f"Volume {'grew' if grew else 'fell'} {abs(pct)}%.",
                           "detail": f"{total['lines']:,} lines across the sites (was {total['lines_prev']:,}).",
                           "delta_text": f"{pct:+d}%"})
    return [x for _, x in sorted(moves, key=lambda t: -t[0])] + volume + steady + quiet


# ============================================================== the row


async def _count(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime, **extra) -> dict:
    return await aggregate_releases(db, with_site({"start": start.isoformat(), "end": end.isoformat(), **extra}, site), cc)


async def _site_today(db: AsyncSession, cc: str, site: Site, midnight: datetime, now: datetime) -> dict:
    # Step 1: The day's figures as the executive Home reads them, so the two views agree
    m = await exec_home.window_metrics(db, cc, site, midnight, now, with_pickers=True)
    short = await _count(db, cc, site, midnight, now, where=["is_short==1"])
    active = await _count(db, cc, site, now - timedelta(minutes=ACTIVE_MINUTES), now, group_by=["user_name"], limit=500)
    return {"fill_rate": m["fill_rate"], "lines": m["lines"], "short_lines": int(short.get("total_rows") or 0),
            "deliveries": m["deliveries"], "pickers_today": m["pickers"], "pickers_active": len(active.get("rows") or [])}


async def _weekly(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime) -> dict[str, float | None]:
    result = await _count(db, cc, site, start, end, group_by=["week"], limit=100)
    return {str((r.get("dimensions") or [""])[0])[:10]: exec_home.fill_rate(r.get("expected"), r.get("shortfall"))
            for r in result.get("rows") or []}


async def compute(db: AsyncSession, customer_code: str, *, now: datetime | None = None, target: float = 0.98,
                  max_sites: int = 3) -> dict:
    """The all-sites view for one customer: every site side by side, by day, by week, what changed.

    (customer with BRI and LON) -> {"available": True, "sites": [{"site_id": "BRI", "flow": {...}, ...}, ...],
    "by_day": {...}, "weeks": {...}, "changes": [...]} ; (no settlement) -> {"available": False, "note": ...}
    """
    # Step 1: The clock and the sites, so "today" and the weeks are the warehouse's
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    sites = (await list_sites(db, customer_code))[:max_sites]
    head = {"customer_code": customer_code, "as_of": now.isoformat(), "timezone": tz.key, "target_fill_rate": target}
    day_start = midnight - timedelta(days=BY_DAY_DAYS - 1)
    week_start = midnight - timedelta(days=midnight.weekday() + 7 * (WEEKS - 1))
    labels = [(day_start + timedelta(days=i)).date().isoformat() for i in range(BY_DAY_DAYS)]
    week_labels = [(week_start + timedelta(days=7 * i)).date().isoformat() for i in range(WEEKS)]
    try:
        # Step 2: Each site's day, its departures through the flow, its week and the week before
        rows_today = await delivery_store.board_rows(db, customer_code, dates=[midnight.date()], include_closed=True,
                                                     limit=5000)
        advisories = (await exec_home.advisories_summary(db, customer_code))["by_site"]
        last7_start, prev_start = now - timedelta(days=7), now - timedelta(days=14)
        out_sites, change_inputs, by_day, weeks = [], [], {"labels": labels, "sites": {}, "week": {}}, {"labels": week_labels, "sites": {}}
        for site in sites:
            mine = [r for r in rows_today if r.warehouse == site.site_id]
            today = await _site_today(db, customer_code, site, midnight, now)
            cur = await exec_home.window_metrics(db, customer_code, site, last7_start, now, with_pickers=True)
            prev = await exec_home.window_metrics(db, customer_code, site, prev_start, last7_start, with_pickers=True)
            days = await exec_home.daily_fill_rates(db, customer_code, site, day_start, now)
            by_day["sites"][site.site_id] = [days.get(d) for d in labels]
            by_day["week"][site.site_id] = cur["fill_rate"]
            per_week = await _weekly(db, customer_code, site, week_start, now)
            weeks["sites"][site.site_id] = [per_week.get(w) for w in week_labels]
            has_activity = bool(mine) or today["lines"] > 0 or cur["lines"] > 0
            out_sites.append({"site_id": site.site_id, "name": site.name, "has_activity": has_activity, "today": today,
                              "flow": flow(mine), "status": status_of(mine),
                              "advisories": advisories.get(site.site_id, {"open": 0, "high": 0}),
                              "week_fill_rate": cur["fill_rate"]})
            metrics = {**cur, "fill_rate_prev": prev["fill_rate"], "on_time_prev": prev["on_time"],
                       "lines_prev": prev["lines"], "pickers_prev": prev["pickers"]}
            change_inputs.append((site.site_id, site.name, metrics, by_day["sites"][site.site_id]))
        # Step 3: The whole customer beside the sites, for the chart's "all sites" line and the volume
        cur_all = await exec_home.window_metrics(db, customer_code, None, last7_start, now, with_pickers=True)
        prev_all = await exec_home.window_metrics(db, customer_code, None, prev_start, last7_start, with_pickers=True)
        days_all = await exec_home.daily_fill_rates(db, customer_code, None, day_start, now)
        by_day["all"], by_day["week"]["all"] = [days_all.get(d) for d in labels], cur_all["fill_rate"]
        weeks_all = await _weekly(db, customer_code, None, week_start, now)
        weeks["all"] = [weeks_all.get(w) for w in week_labels]
    except UnknownSettlement:
        return {"available": False, "note": NO_SETTLEMENT_NOTE, **head}
    except ReadProblem as exc:
        logger.warning("teams.all_sites_unreadable customer=%s problems=%s", customer_code, exc.problems)
        return {"available": False, "note": "the figures could not be read", **head}
    total = {**cur_all, "fill_rate_prev": prev_all["fill_rate"], "on_time_prev": prev_all["on_time"],
             "lines_prev": prev_all["lines"], "pickers_prev": prev_all["pickers"]}
    # Step 4: The row as the screen draws it
    return {"available": True, **head, "sites": out_sites, "by_day": by_day, "weeks": weeks,
            "changes": what_changed(change_inputs, total, target=target), "changes_window": "the last 7 days against the 7 before"}


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     max_sites: int = 3, target: float = 0.98, session_factory=None) -> list[str]:
    """Mirror the all-sites view of every bound, enabled customer, or `only`; the codes written. One
    failure skips that customer.

    (acme) -> SITE#all#SITES written ; (mirror down) -> []
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
                row = await compute(db, code, target=target, max_sites=max_sites)
                stamp = datetime.now(timezone.utc).timestamp()
                env = envelope(ROW_TYPE, code, site_id="all", payload=row, as_of=row["as_of"], now=stamp)
                await write_if_changed(mirror, tracker, env, now=stamp)
                written.append(code)
            except Exception:
                logger.exception("teams.all_sites_failed customer=%s", code)
                await db.rollback()
    return written
