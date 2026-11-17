"""Step 38 (chunk 214): the routine library's reports, each answering what happened, why, and what to do.

Twelve routines in six categories, one for the supervisor and one for the manager in each; four are
ready (the data supports them today), eight are planned. A ready routine is one builder returning one
shape: a headline, tiles (what happened), findings with their evidence (why), numbered actions (what
to do) and placeholders for figures no transaction supplies yet (shown as "not available yet", never
a number). Findings and actions come from fixed rules over the figures and from the advisories' own
recommended actions; nothing is written by the model. Built per site and for "all" by the detail
loop and mirrored under `EYE#<customer>` / `SITE#<id>#REPORTS`; the edge only lays them out. A site
row never carries other sites' figures: the manager's comparison of every site is in the "all" row.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.advisories import store as advisories_store
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases
from app.services.analytics_at_risk import delivery_store, model
from app.services.teams import all_sites, exec_home
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import ALL_SITES_ID, Site, list_sites, with_site

logger = logging.getLogger(__name__)

ROW_TYPE = "site_reports"
SUPERVISOR, MANAGER = "Supervisor", "Manager"
READY, PLANNED, ALL_SITES_ONLY = "ready", "planned", "all_sites_only"
#: A release longer than this is "long-running" in the exceptions report (the histogram's last band).
LONG_RELEASE_S = 900
TOP_FINDINGS = 5
NO_SETTLEMENT_NOTE = "no pick releases yet · this logspace has no pick_release settlement"


@dataclass(frozen=True)
class RoutineSpec:
    """One routine of the library: where it sits, who it is for, what it answers, whether it is built."""

    id: str
    category: str
    role: str
    title: str
    answers: str
    status: str
    scope: str = "site"  # "site": per site and for all ; "all": the whole customer only


LIBRARY: tuple[RoutineSpec, ...] = (
    RoutineSpec("health_check", "Daily operations", SUPERVISOR, "Daily health check",
                "Today's picking, fill rate, routes departed, backlog and open issues.", READY),
    RoutineSpec("site_performance", "Daily operations", MANAGER, "Site performance",
                "Every site side by side: fill rate, dispatch and volume, and what changed.", READY, scope="all"),
    RoutineSpec("bottleneck_detector", "Picking", SUPERVISOR, "Picking bottleneck detector",
                "Where picking slows down: slow lines, long releases, areas causing delays.", PLANNED),
    RoutineSpec("productivity_trends", "Picking", MANAGER, "Productivity trends",
                "Productivity by day, week and month, and where it is declining.", PLANNED),
    RoutineSpec("replenishment_alert", "Inventory", SUPERVISOR, "Replenishment alert",
                "Locations running out and picking blocked by unavailable stock.", PLANNED),
    RoutineSpec("stock_availability", "Inventory", MANAGER, "Stock availability and fill rate",
                "Why orders are not fulfilled: short products and repeat discrepancies.", PLANNED),
    RoutineSpec("dispatch_readiness", "Dispatch", SUPERVISOR, "Dispatch readiness check",
                "Whether today's routes are ready to leave, and what is holding them.", READY),
    RoutineSpec("delivery_performance", "Dispatch", MANAGER, "Delivery performance",
                "On-time departures, late and missed deliveries, recurring problems.", PLANNED),
    RoutineSpec("outstanding_exceptions", "Exceptions", SUPERVISOR, "Outstanding exceptions",
                "Short picks, stock discrepancies, failed transactions and open advisories.", READY),
    RoutineSpec("operational_risk", "Exceptions", MANAGER, "Operational risk and early warning",
                "Problems building up before they become significant.", PLANNED),
    RoutineSpec("picker_performance", "Workforce", SUPERVISOR, "Picker performance today",
                "Lines per person, time per line and how the work is spread.", PLANNED),
    RoutineSpec("labour_utilisation", "Workforce", MANAGER, "Labour utilisation",
                "Whether the available labour is used well, by site and period.", PLANNED),
)


def library(*, site: Site | None = None) -> list[dict]:
    """The twelve routines as the Library lists them; on a site's row the all-sites routine says so.

    >>> [r["status"] for r in library()][:3]
    ['ready', 'ready', 'planned']
    >>> next(r["status"] for r in library(site=Site("BRI", "Brighton")) if r["id"] == "site_performance")
    'all_sites_only'
    """
    out = []
    for spec in LIBRARY:
        status = ALL_SITES_ONLY if site is not None and spec.scope == "all" and spec.status == READY else spec.status
        out.append({"id": spec.id, "category": spec.category, "role": spec.role, "title": spec.title,
                    "answers": spec.answers, "status": status, "scope": spec.scope})
    return out


# ============================================================== shared wording


def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:.1%}"


def _n(count: int, one: str, many: str) -> str:
    return f"{count:,} {one if count == 1 else many}"


def _tile(label: str, value: str, sub: str = "", tone: str = "neutral") -> dict:
    return {"label": label, "value": value, "sub": sub, "tone": tone}


def _placeholder(label: str, note: str) -> dict:
    # STUB: a figure no transaction supplies yet (docs/design-handoff/MISSING.md in the edge repo)
    return {"label": label, "note": note}


def _report(spec_id: str, scope: str, now: datetime, *, headline: str, tiles: list[dict], findings: list[dict],
            actions: list[str], placeholders: list[dict]) -> dict:
    spec = next(s for s in LIBRARY if s.id == spec_id)
    return {"id": spec.id, "title": spec.title, "role": spec.role, "category": spec.category, "scope": scope,
            "as_of": now.isoformat(), "headline": headline, "tiles": tiles, "findings": findings,
            "actions": actions, "placeholders": placeholders}


# ============================================================== the rules


def health_rules(facts: dict) -> tuple[list[dict], list[str]]:
    """The health check's findings and actions, most urgent first: flagged deliveries, fill rate under
    target, routes still to leave, high advisories.

    ({"to_fix": 0, "fill_rate": 0.99, ...all quiet}) -> ([], ["Nothing needs action: keep an eye on the dispatch board."])
    """
    findings: list[dict] = []
    actions: list[str] = []
    # Step 1: Deliveries flagged on the board come first, because a van leaving without them is final
    if facts["to_fix"]:
        n = facts["to_fix"]
        findings.append({"title": f"{_n(n, 'delivery needs', 'deliveries need')} action now",
                         "detail": "Flagged left behind or at risk on today's dispatch board.", "tone": "attention"})
        actions.append(f"Check the {_n(n, 'flagged delivery', 'flagged deliveries')} on the dispatch board before their vans leave.")
    # Step 2: Fill rate under target, with the short lines that explain it
    if facts["fill_rate"] is not None and facts["fill_rate"] < facts["target"]:
        findings.append({"title": f"Fill rate {_pct(facts['fill_rate'])} is under the {facts['target']:.0%} target",
                         "detail": f"{_n(facts['short_lines'], 'short line', 'short lines')} today.", "tone": "attention"})
        actions.append("Review today's short lines on the Pick releases page.")
    # Step 3: Routes still to leave, so the reader sees what the day still holds
    left = facts["routes_total"] - facts["routes_out"]
    if left > 0:
        findings.append({"title": f"{left} of {facts['routes_total']} routes still to leave",
                         "detail": f"{_n(facts['backlog'], 'delivery', 'deliveries')} not yet departed.", "tone": "neutral"})
        actions.append("Follow the remaining routes on the dispatch board.")
    # Step 4: High advisories, each of which carries its own recommended action
    if facts["advisories_high"]:
        n = facts["advisories_high"]
        findings.append({"title": f"{_n(n, 'high advisory', 'high advisories')} open",
                         "detail": "Each has a recommended action in the Advisories tab.", "tone": "attention"})
        actions.append("Work through the high advisories in the Advisories tab.")
    if not actions:
        actions.append("Nothing needs action: keep an eye on the dispatch board.")
    return findings, actions


# ============================================================== the facts of a site's day


@dataclass
class DayFacts:
    """What the three supervisor reports read, read once per site."""

    metrics: dict
    short_lines: int
    zero_picks: int
    long_releases: int | None  # None when the tenant's settlement does not carry duration_s
    failed_calls: int
    deliveries: list
    advisories: list


async def _count(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime, where: list[str]) -> int:
    out = await aggregate_releases(db, with_site({"start": start.isoformat(), "end": end.isoformat(), "where": where}, site), cc)
    return int(out.get("total_rows") or 0)


async def _optional_count(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime,
                          where: list[str]) -> int | None:
    # a figure the tenant's settlement may not declare (duration_s) is "not recorded", not a failed row
    try:
        return await _count(db, cc, site, start, end, where)
    except ReadProblem:
        return None


async def _failed_calls(db: AsyncSession, cc: str, site: Site | None, start: datetime, end: datetime) -> int:
    stmt = select(func.count()).select_from(LogTransaction).where(
        LogTransaction.customer_code == cc, LogTransaction.started_at >= start, LogTransaction.started_at < end,
        LogTransaction.status == LogTransactionStatus.error)
    if site is not None:
        stmt = stmt.where(LogTransaction.warehouse == site.site_id)
    return int(await db.scalar(stmt) or 0)


async def day_facts(db: AsyncSession, cc: str, site: Site | None, midnight: datetime, now: datetime) -> DayFacts:
    """Today's figures for one site or the whole customer: the releases, the board, the advisories.

    (BRI, 13:00) -> DayFacts(metrics={"fill_rate": 0.92, ...}, short_lines=52, deliveries=[...], advisories=[...])
    """
    metrics = await exec_home.window_metrics(db, cc, site, midnight, now, with_pickers=True)
    rows = await delivery_store.board_rows(db, cc, dates=[midnight.date()], include_closed=True, limit=5000,
                                           warehouse=site.site_id if site else None)
    advisories = [a for a in await advisories_store.live_rows(db, cc)
                  if a.status == "open" and (site is None or a.site_id == site.site_id)]
    return DayFacts(
        metrics=metrics,
        short_lines=await _count(db, cc, site, midnight, now, ["is_short==1"]),
        zero_picks=await _count(db, cc, site, midnight, now, ["picked==0"]),
        long_releases=await _optional_count(db, cc, site, midnight, now, [f"duration_s>{LONG_RELEASE_S}"]),
        failed_calls=await _failed_calls(db, cc, site, midnight, now),
        deliveries=rows, advisories=advisories)


# ============================================================== the reports


def health_check(facts: DayFacts, scope: str, now: datetime, *, target: float) -> dict:
    """S1: today's picking, fill rate, routes, backlog and open issues, what needs doing first."""
    flow = all_sites.flow(facts.deliveries)
    status = all_sites.status_of(facts.deliveries)
    m = facts.metrics
    backlog = flow["total"] - flow["stages"]["departed"]
    high = sum(1 for a in facts.advisories if a.severity == "high")
    findings, actions = health_rules({"fill_rate": m["fill_rate"], "target": target, "short_lines": facts.short_lines,
                                      "to_fix": status["to_fix"], "routes_out": flow["routes_out"],
                                      "routes_total": flow["routes_total"], "advisories_high": high, "backlog": backlog})
    tiles = [
        _tile("Lines picked", f"{m['lines']:,}", f"{m['pickers']} pickers"),
        _tile("Deliveries", f"{m['deliveries']:,}", "picked from today"),
        _tile("Fill rate", _pct(m["fill_rate"]), f"target {target:.0%}",
              "neutral" if m["fill_rate"] is None else "good" if m["fill_rate"] >= target else "attention"),
        _tile("Routes departed", f"{flow['routes_out']} / {flow['routes_total']}", "today's departures"),
        _tile("Backlog", f"{backlog:,}", "deliveries not yet departed", "attention" if status["to_fix"] else "neutral"),
        _tile("Open advisories", f"{len(facts.advisories):,}", f"{high} high", "attention" if high else "neutral"),
    ]
    is_quiet = m["lines"] == 0 and flow["total"] == 0 and not facts.advisories
    if is_quiet:
        headline, actions = "No activity today", ["Nothing to report for this site today."]
    else:
        headline = "On track" if not findings else f"{_n(len(findings), 'thing needs', 'things need')} attention"
    return _report("health_check", scope, now, headline=headline, tiles=tiles, findings=findings, actions=actions,
                   placeholders=[_placeholder("Orders received", "Not in the WMS logs yet: shown once an order feed is connected.")])


def dispatch_readiness(facts: DayFacts, scope: str, now: datetime) -> dict:
    """S4: whether today's routes are ready to leave: picking, packing, staging, loading, flagged ones."""
    # Step 1: Each open delivery in one stage, packing split out of "staged" by whether a package exists
    counts = {"not_picked": 0, "awaiting_packing": 0, "staged": 0, "loading": 0}
    for r in facts.deliveries:
        stage = all_sites.stage_of(r)
        if stage in ("to_pick", "picking"):
            counts["not_picked"] += 1
        elif stage == "staged":
            counts["awaiting_packing" if r.loading_expected is not False and not r.packages_created else "staged"] += 1
        elif stage == "loading":
            counts["loading"] += 1
    flow = all_sites.flow(facts.deliveries)
    flagged = [r for r in facts.deliveries if r.status == "open" and r.tier in all_sites.FLAGGED_TIERS]
    # Step 2: The flagged deliveries by name, then the routes still holding deliveries
    findings = [{"title": f"{r.route or 'no route'} · {r.customer_name or r.delivery_number}",
                 "detail": (f"{model.Tier(r.tier).name.replace('_', ' ')} · {r.lines_confirmed or 0} of "
                            f"{r.lines_expected if r.lines_expected is not None else '?'} lines · "
                            f"{r.packages_loaded or 0} of {r.packages_created or 0} packages loaded"),
                 "tone": "attention"} for r in flagged[:TOP_FINDINGS]]
    open_routes: dict[str, int] = {}
    for r in facts.deliveries:
        if r.status == "open" and r.route:
            open_routes[r.route] = open_routes.get(r.route, 0) + 1
    for route, n in sorted(open_routes.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP_FINDINGS]:
        findings.append({"title": f"Route {route}", "detail": f"{_n(n, 'delivery', 'deliveries')} still to leave.", "tone": "neutral"})
    # Step 3: The actions in the order the floor works: chase, pick, pack, load
    actions = []
    if flagged:
        actions.append(f"Chase the {_n(len(flagged), 'flagged delivery', 'flagged deliveries')} first: their vans are due.")
    if counts["not_picked"]:
        actions.append(f"Finish picking {_n(counts['not_picked'], 'delivery', 'deliveries')}.")
    if counts["awaiting_packing"]:
        actions.append(f"Pack {_n(counts['awaiting_packing'], 'picked delivery', 'picked deliveries')} waiting for packing.")
    if counts["staged"]:
        actions.append(f"Load {_n(counts['staged'], 'packed delivery', 'packed deliveries')} onto their vans.")
    if not actions:
        actions.append("Every delivery due today has left or is loading.")
    tiles = [
        _tile("Routes departed", f"{flow['routes_out']} / {flow['routes_total']}", "every delivery on the van"),
        _tile("Not fully picked", f"{counts['not_picked']:,}", "deliveries", "attention" if counts["not_picked"] else "neutral"),
        _tile("Awaiting packing", f"{counts['awaiting_packing']:,}", "picked, no package yet"),
        _tile("Staged, not loaded", f"{counts['staged']:,}", "packed, waiting for the van"),
        _tile("Loading", f"{counts['loading']:,}", "packages going on"),
        _tile("Flagged", f"{len(flagged):,}", "left behind or at risk", "attention" if flagged else "good"),
    ]
    headline = (f"{flow['routes_out']} of {flow['routes_total']} routes departed"
                + (f" · {_n(len(flagged), 'risk', 'risks')} to act on" if flagged else ""))
    return _report("dispatch_readiness", scope, now, headline=headline, tiles=tiles, findings=findings, actions=actions,
                   placeholders=[])


def outstanding_exceptions(facts: DayFacts, scope: str, now: datetime) -> dict:
    """S5: short picks, stock discrepancies, failed calls, long releases and open advisories, each
    advisory with its own recommended action."""
    advisories = facts.advisories
    high = [a for a in advisories if a.severity == "high"]
    stock = sum(1 for a in advisories if a.agent == "stock")
    findings = [{"title": a.title, "detail": a.recommended or a.impact or "",
                 "tone": "attention" if a.severity == "high" else "neutral"} for a in advisories[:TOP_FINDINGS]]
    # Step 1: The advisories' own recommended actions first, the worst first and each once
    actions: list[str] = []
    for a in advisories:
        if a.recommended and a.recommended not in actions:
            actions.append(a.recommended)
        if len(actions) >= 3:
            break
    # Step 2: Then the day's exceptions the advisories do not cover
    if facts.zero_picks:
        actions.append(f"Review the {_n(facts.zero_picks, 'zero-pick', 'zero-picks')} on the Pick releases page.")
    if facts.failed_calls:
        actions.append(f"Look at the {_n(facts.failed_calls, 'failed handheld call', 'failed handheld calls')} in the log explorer.")
    if not actions:
        actions.append("No exceptions open.")
    tiles = [
        _tile("Short lines", f"{facts.short_lines:,}", "today", "attention" if facts.short_lines else "good"),
        _tile("Zero-picks", f"{facts.zero_picks:,}", "nothing picked"),
        _tile("Stock discrepancies", f"{stock:,}", "open stock advisories", "attention" if stock else "good"),
        _tile("Failed transactions", f"{facts.failed_calls:,}", "handheld calls today"),
        _tile("Open advisories", f"{len(advisories):,}", f"{len(high)} high", "attention" if high else "neutral"),
        _tile("Long releases", "–" if facts.long_releases is None else f"{facts.long_releases:,}",
              "not recorded here" if facts.long_releases is None else f"over {LONG_RELEASE_S // 60} min"),
    ]
    headline = f"{_n(len(advisories), 'open advisory', 'open advisories')} · {_n(facts.short_lines, 'short line', 'short lines')}"
    return _report("outstanding_exceptions", scope, now, headline=headline, tiles=tiles, findings=findings,
                   actions=actions, placeholders=[_placeholder("Unallocated orders", "Not in the WMS logs yet: shown once order allocation is connected.")])


async def site_performance(db: AsyncSession, cc: str, sites: list[Site], now: datetime, *, target: float) -> dict:
    """M1: every site side by side over the last 7 days against the 7 before, and what changed."""
    last7, prev7 = now - timedelta(days=7), now - timedelta(days=14)
    findings, change_inputs = [], []
    # Step 1: Each site's week and the week before, for its line and for "what changed"
    for site in sites:
        cur = await exec_home.window_metrics(db, cc, site, last7, now, with_pickers=True)
        prev = await exec_home.window_metrics(db, cc, site, prev7, last7, with_pickers=True)
        if cur["lines"] == 0 and prev["lines"] == 0:
            findings.append({"title": f"{site.name} · no activity this week", "detail": "", "tone": "neutral"})
        else:
            change = ("" if cur["fill_rate"] is None or prev["fill_rate"] is None
                      else f" · {all_sites._pts((cur['fill_rate'] - prev['fill_rate']) * 100)} on fill rate")
            findings.append({"title": f"{site.name} · fill rate {_pct(cur['fill_rate'])} · on time {_pct(cur['on_time'])}",
                             "detail": f"{cur['lines']:,} lines · {cur['deliveries']:,} deliveries · {cur['left_behind']} left behind{change}",
                             "tone": "attention" if cur["fill_rate"] is not None and cur["fill_rate"] < target else "neutral"})
        days = await exec_home.daily_fill_rates(db, cc, site, last7, now)
        change_inputs.append((site.site_id, site.name, {**cur, "fill_rate_prev": prev["fill_rate"], "on_time_prev": prev["on_time"],
                                                        "lines_prev": prev["lines"], "pickers_prev": prev["pickers"]}, list(days.values())))
    # Step 2: The whole customer for the tiles and the volume line
    cur_all = await exec_home.window_metrics(db, cc, None, last7, now, with_pickers=True)
    prev_all = await exec_home.window_metrics(db, cc, None, prev7, last7, with_pickers=True)
    changes = all_sites.what_changed(change_inputs, {**cur_all, "fill_rate_prev": prev_all["fill_rate"],
                                                     "on_time_prev": prev_all["on_time"], "lines_prev": prev_all["lines"],
                                                     "pickers_prev": prev_all["pickers"]}, target=target)
    for c in changes:
        if c["direction"] in ("up", "down"):
            findings.append({"title": c["title"], "detail": c["detail"], "tone": "attention" if c["direction"] == "down" else "good"})
    # Step 3: An action for each fall, so the review ends in something to do
    actions = [f"Look into it: {c['title']}" for c in changes if c["direction"] == "down"] or ["No site moved the wrong way this week."]
    tiles = [
        _tile("Fill rate", _pct(cur_all["fill_rate"]), "last 7 days",
              "neutral" if cur_all["fill_rate"] is None else "good" if cur_all["fill_rate"] >= target else "attention"),
        _tile("On-time dispatch", _pct(cur_all["on_time"]), f"{cur_all['departures']:,} departures"),
        _tile("Lines", f"{cur_all['lines']:,}", f"was {prev_all['lines']:,}"),
        _tile("Deliveries", f"{cur_all['deliveries']:,}", "last 7 days"),
    ]
    active = sum(1 for f in findings[:len(sites)] if "no activity" not in f["title"])
    headline = f"{active} of {len(sites)} sites active · fill rate {_pct(cur_all['fill_rate'])}"
    return _report("site_performance", "all sites", now, headline=headline, tiles=tiles, findings=findings,
                   actions=actions, placeholders=[])


# ============================================================== the row


async def compute(db: AsyncSession, customer_code: str, site: Site | None, *, now: datetime | None = None,
                  target: float = 0.98, max_sites: int = 3) -> dict:
    """The library and its ready reports for one site, or for the whole customer (which adds the
    manager's site performance).

    (Site BRI) -> {"site_id": "BRI", "reports": {health_check, dispatch_readiness, outstanding_exceptions}} ;
    (None) -> the same plus site_performance ; (no settlement) -> {"available": False, ...}
    """
    # Step 1: The warehouse's clock, so "today" is its day
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    scope = site.name if site else "all sites"
    head = {"customer_code": customer_code, "site_id": site.site_id if site else ALL_SITES_ID, "scope": scope,
            "as_of": now.isoformat(), "timezone": tz.key, "library": library(site=site)}
    # Step 2: The day's facts once, every supervisor report from them, so the reports cannot disagree
    try:
        facts = await day_facts(db, customer_code, site, midnight, now)
        reports = {
            "health_check": health_check(facts, scope, now, target=target),
            "dispatch_readiness": dispatch_readiness(facts, scope, now),
            "outstanding_exceptions": outstanding_exceptions(facts, scope, now),
        }
        if site is None:
            sites = (await list_sites(db, customer_code))[:max_sites]
            reports["site_performance"] = await site_performance(db, customer_code, sites, now, target=target)
    except UnknownSettlement:
        return {"available": False, "note": NO_SETTLEMENT_NOTE, "reports": {}, **head}
    except ReadProblem as exc:
        logger.warning("teams.routine_reports_unreadable customer=%s problems=%s", customer_code, exc.problems)
        return {"available": False, "note": "the figures could not be read", "reports": {}, **head}
    return {"available": True, **head, "reports": reports}


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     max_sites: int = 3, target: float = 0.98, session_factory=None) -> list[str]:
    """Mirror the reports of the whole customer and each active site, for every bound, enabled customer
    or `only`; the codes written. One failure skips that customer.

    (acme with sites BRI, LON) -> SITE#all#REPORTS, SITE#BRI#REPORTS, SITE#LON#REPORTS ; (mirror down) -> []
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
                    row = await compute(db, code, site, target=target, max_sites=max_sites)
                    stamp = datetime.now(timezone.utc).timestamp()
                    env = envelope(ROW_TYPE, code, site_id=row["site_id"], payload=row, as_of=row["as_of"], now=stamp)
                    await write_if_changed(mirror, tracker, env, now=stamp)
                written.append(code)
            except Exception:
                logger.exception("teams.routine_reports_failed customer=%s", code)
                await db.rollback()
    return written
