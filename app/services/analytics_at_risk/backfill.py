"""The backfill: closed rows for past days, so the history is not empty on day one.

The worker can only judge a delivery whose routing call was folded after the departure fields were
approved, because approving a field never rewrites old facts (the fold skips a transaction whose source
digest is unchanged). So for the days before that, the departures are read straight from the routing
calls' response text in `log_transactions`, the picks, expected lines and loads are read with the same
bounded reads the board uses, the tier rule is replayed over the clocks (`model.replay_tiers`), and a
CLOSED row is written for each delivery, marked `reconstructed`.

Day by day, in date order: each day's thresholds come from the profiles learned up to the day before,
and once the day is written its profile is learned, so the replay judges as the live system would have.
A live row is never overwritten, and a day whose deliveries have not all closed is refused.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_at_risk import RULE_VERSION, board_store, delivery_store, model, profile_store, settings_store
from app.services.analytics_at_risk.runner import _lock
from app.settings import settings

logger = logging.getLogger(__name__)

ROUTING_METHOD = "GetNextDeliveryByRoute"
#: A routing call names a delivery at most this many days before its departure (the evening before,
#: on the live data; three days is slack for a long weekend).
CALL_LOOKBACK = timedelta(days=3)
CALLS_CAP = 20000
#: The response text is capped at 500 characters by the ingest; these fields sit in its first 350.
_FIELDS = {name: re.compile(rf'"{name}":"([^"]*)"') for name in
           ("DeliveryNumber", "Route", "CustomerName", "CustomerNumber", "DeparatureDate", "DeparatureTime")}


def _field(text: str, name: str) -> str | None:
    m = _FIELDS[name].search(text)
    value = m.group(1).strip() if m else ""
    return value or None


def _local_midnight(day: date, tz: tzinfo) -> datetime:
    return datetime.combine(day, time.min, tzinfo=tz)


# ============================================================== the universe

async def routing_universe(db: AsyncSession, cc: str, *, day: date, tz: tzinfo) -> tuple[dict[str, board_store._Route], int]:
    """Every delivery whose latest routing call put its departure on `day`, from the raw calls of the
    three days up to it. Returns the routes by delivery and how many calls had an unreadable departure.
    Bounded on the partition key and capped."""
    since, until = _local_midnight(day - CALL_LOOKBACK, tz), _local_midnight(day + timedelta(days=1), tz)
    rows = (await db.execute(select(LogTransaction.started_at, LogTransaction.response_summary).where(
        LogTransaction.customer_code == cc, LogTransaction.method == ROUTING_METHOD,
        LogTransaction.started_at >= since, LogTransaction.started_at < until,
        LogTransaction.response_summary.like('%"DeparatureDate"%'),
    ).order_by(LogTransaction.started_at).limit(CALLS_CAP))).all()
    latest: dict[str, board_store._Route] = {}
    unreadable = 0
    for _at, text in rows:
        text = text or ""
        delivery = _field(text, "DeliveryNumber")
        if not delivery:
            continue
        departure = model.departure_at(_field(text, "DeparatureDate"), _field(text, "DeparatureTime"), tz)
        if departure is None:
            unreadable += 1
            continue
        # the latest call wins, even when it moves the delivery off this day
        latest[delivery] = board_store._Route(departure_at=departure, route=_field(text, "Route"),
                                              customer_name=_field(text, "CustomerName"), customer_number=_field(text, "CustomerNumber"))
    return {d: r for d, r in latest.items() if r.departure_at.astimezone(tz).date() == day}, unreadable


# ============================================================== one day

async def read_day(db: AsyncSession, cc: str, *, day: date, tz: tzinfo, grace: timedelta,
                   routes_that_load: set[str]) -> tuple[list[model.DeliveryState], datetime | None, int]:
    """The states of every delivery departing on `day` as they stood when the last of them closed,
    plus that close instant (None when the day has no delivery) and the unreadable-call count."""
    routes, unreadable = await routing_universe(db, cc, day=day, tz=tz)
    if not routes:
        return [], None, unreadable
    close_at = max(r.departure_at for r in routes.values()) + grace
    deliveries = sorted(routes)
    picks = await board_store._picks(db, cc, settlement=settings.analytics_at_risk_pick_settlement, deliveries=deliveries)
    expected = await board_store._expected(db, cc, lookup=settings.analytics_at_risk_pick_line_lookup, deliveries=deliveries)
    packages = await board_store._packages(db, cc, since=_local_midnight(day - timedelta(days=1), tz), until=close_at,
                                           cap=settings.analytics_at_risk_facts_cap, tz=tz)
    if packages.overflow:
        logger.warning("At-risk backfill %s %s: the facts read hit its cap of %d rows", cc, day, settings.analytics_at_risk_facts_cap)
    loading_routes = packages.routes_that_load | routes_that_load
    states = [board_store.assemble(d, routes[d], picks.get(d, board_store._Picks()), packages.by_delivery.get(d),
                                   expected=expected.get(d), loading_routes=loading_routes, route_loaded=packages.route_loaded, tz=tz)
              for d in deliveries]
    return states, close_at, unreadable


async def backfill_day(cc: str, *, day: date, now: datetime, replace: bool = False) -> dict:
    """Write the closed rows of one past day. Returns the day's counts, or `skipped` with the reason.
    With `replace`, the day's RECONSTRUCTED rows are deleted first and written again under the current
    rule; live rows are never touched."""
    async with async_session() as db:
        tz = ZoneInfo(await get_customer_timezone(db, cc))
        cfg = await settings_store.effective(db, cc)
        profiles = await profile_store.latest(db, cc, as_of=day - timedelta(days=1))
        routes_that_load = {route for route, p in profiles.items() if (p.loaded_sample or 0) > 0}
        states, close_at, unreadable = await read_day(db, cc, day=day, tz=tz, grace=timedelta(minutes=cfg.close_grace_min),
                                                       routes_that_load=routes_that_load)
    if close_at is not None and close_at > now:
        return {"date": day.isoformat(), "skipped": "not closed yet"}
    thresholds_for = profile_store.thresholds_for(profiles, cfg)
    counts = {"date": day.isoformat(), "deliveries": len(states), "written": 0, "existing": 0, "unreadable": unreadable,
              "missed": 0, "delayed": 0, "fine": 0}
    async with async_session() as db:
        await _lock(db, cc)
        if replace:
            result = await db.execute(delete(AnalyticsAtRiskDelivery).where(
                AnalyticsAtRiskDelivery.customer_code == cc, AnalyticsAtRiskDelivery.departure_date == day,
                AnalyticsAtRiskDelivery.reconstructed.is_(True)))
            counts["replaced"] = int(result.rowcount or 0)
        existing = await delivery_store.rows_for(db, cc, [s.delivery_number for s in states])
        for state in states:
            if any(r.departure_date == day for r in existing.get(state.delivery_number, [])):
                counts["existing"] += 1
                continue
            thresholds = thresholds_for(state.route)
            replay = model.replay_tiers(state, thresholds, close_at=state.departure_at + timedelta(minutes=cfg.close_grace_min))
            row = delivery_store.write_closed(db, cc, state, thresholds=thresholds, replay=replay,
                                              close_at=state.departure_at + timedelta(minutes=cfg.close_grace_min),
                                              now=now, tz=tz, rule_version=RULE_VERSION)
            word = model.category_for(outcome=row.outcome, max_tier=row.max_tier, lines_expected=row.lines_expected,
                                      lines_confirmed=row.lines_confirmed)
            counts["written"] += 1
            if word in counts:
                counts[word] += 1
        await db.flush()
        await profile_store.compute(db, cc, as_of=day, settings=cfg, rule_version=RULE_VERSION, now=now, tz=tz)
        await db.commit()
    return counts


async def backfill_tenant(cc: str, *, start: date, end: date, now: datetime | None = None, replace: bool = False) -> dict:
    """Every day from `start` to `end` inclusive, oldest first. Raises on a database error: this is a
    hand-run tool, and a half-written range must be seen, not swallowed."""
    now = now or datetime.now(timezone.utc)
    if end < start:
        raise ValueError("end is before start")
    days = []
    day = start
    while day <= end:
        days.append(await backfill_day(cc, day=day, now=now, replace=replace))
        day += timedelta(days=1)
    return {"status": "completed", "days": days}
