"""Route profiles: when each route's van is usually ready, learned from its closed days.

A route's day has one van-ready moment: the dock's last scan (`route_loaded_at`, the same on every row
of that route and day). Over the window those moments, as minutes after local midnight, give a
coverage quantile: the time of day by which the van was ready on nine days in ten. That is the clock
each delivery is judged against. A route that never scans a load learns the same from its last pick
of the day. Below `min_days` a route has no rhythm yet and the WMS departure stands in.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, tzinfo
from decimal import Decimal
from typing import Callable, Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import model
from app.services.analytics_at_risk.settings_store import Settings

#: Outcomes a profile learns from. `unknown` rows have no board data and teach nothing.
LEARNING_OUTCOMES = ("loaded_in_time", "loaded_late", "never_loaded", "picked_in_time", "picked_late")
#: Most closed rows one computation reads. 28 days at a few hundred departures a day is well under it.
ROWS_CAP = 50000


def minutes_of_day(at: datetime, tz: tzinfo) -> Decimal:
    local = at.astimezone(tz)
    return Decimal(local.hour * 60 + local.minute) + Decimal(local.second) / Decimal(60)


def at_minutes(day: date, minutes: Decimal, tz: tzinfo) -> datetime:
    """The instant `minutes` after local midnight on `day`."""
    return datetime.combine(day, time.min, tzinfo=tz) + timedelta(seconds=float(minutes * 60))


def _quantile(values: list[Decimal], p: Decimal) -> Decimal | None:
    return model.coverage_quantile(values, coverage=Decimal(1) - p, min_sample=1)


def _two_places(value: Decimal | None) -> Decimal | None:
    return None if value is None else value.quantize(Decimal("0.01"))


async def _closed_rows(db: AsyncSession, cc: str, *, start: date, end: date) -> list[AnalyticsAtRiskDelivery]:
    return list((await db.execute(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == cc, AnalyticsAtRiskDelivery.status == "closed",
        AnalyticsAtRiskDelivery.outcome.in_(LEARNING_OUTCOMES), AnalyticsAtRiskDelivery.route.is_not(None),
        AnalyticsAtRiskDelivery.departure_date >= start, AnalyticsAtRiskDelivery.departure_date <= end,
    ).order_by(AnalyticsAtRiskDelivery.route, AnalyticsAtRiskDelivery.departure_at).limit(ROWS_CAP))).scalars().all())


async def compute(db: AsyncSession, cc: str, *, as_of: date, settings: Settings, rule_version: str, now: datetime,
                  tz: tzinfo) -> list[AnalyticsAtRiskRouteProfile]:
    """One profile row per route from the closed rows of `window_days` ending on `as_of`, upserted
    for that day. Does NOT commit."""
    start = as_of - timedelta(days=settings.window_days - 1)
    by_route: dict[str, list[AnalyticsAtRiskDelivery]] = defaultdict(list)
    for row in await _closed_rows(db, cc, start=start, end=as_of):
        by_route[row.route].append(row)
    existing = {r.route: r for r in (await db.execute(select(AnalyticsAtRiskRouteProfile).where(
        AnalyticsAtRiskRouteProfile.customer_code == cc, AnalyticsAtRiskRouteProfile.as_of_date == as_of))).scalars().all()}
    out = []
    for route, rows in sorted(by_route.items()):
        van_by_day: dict[date, datetime] = {}
        from_by_day: dict[date, datetime] = {}
        pick_by_day: dict[date, datetime] = {}
        for r in rows:
            if r.route_loaded_at is not None and (r.departure_date not in van_by_day or r.route_loaded_at > van_by_day[r.departure_date]):
                van_by_day[r.departure_date] = r.route_loaded_at
            if r.route_loading_from is not None and (r.departure_date not in from_by_day or r.route_loading_from < from_by_day[r.departure_date]):
                from_by_day[r.departure_date] = r.route_loading_from
            if r.last_pick_at is not None and (r.departure_date not in pick_by_day or r.last_pick_at > pick_by_day[r.departure_date]):
                pick_by_day[r.departure_date] = r.last_pick_at
        van = [minutes_of_day(v, tz) for v in van_by_day.values()]
        loading_from = [minutes_of_day(v, tz) for v in from_by_day.values()]
        picks = [minutes_of_day(v, tz) for v in pick_by_day.values()]
        mode = Counter(r.departure_at.astimezone(tz).strftime("%H%M") for r in rows).most_common(1)
        profile = existing.get(route)
        if profile is None:
            profile = AnalyticsAtRiskRouteProfile(customer_code=cc, route=route, as_of_date=as_of)
            db.add(profile)
        profile.window_days = settings.window_days
        profile.sample, profile.loaded_sample = len(rows), sum(r.last_load_at is not None for r in rows)
        profile.van_days = len(van)
        profile.van_ready_usual_min = _two_places(model.usual_time(van, coverage=settings.coverage, min_days=settings.min_days))
        profile.van_ready_p50_min = _two_places(_quantile(van, Decimal("0.5")))
        profile.van_ready_latest_min = _two_places(max(van) if van else None)
        profile.loading_from_p50_min = _two_places(_quantile(loading_from, Decimal("0.5")))
        profile.pick_days = len(picks)
        profile.pick_done_usual_min = _two_places(model.usual_time(picks, coverage=settings.coverage, min_days=settings.min_days))
        profile.pick_done_p50_min = _two_places(_quantile(picks, Decimal("0.5")))
        profile.departure_time_mode = mode[0][0] if mode else None
        profile.coverage, profile.rule_version, profile.computed_at = settings.coverage, rule_version, now
        out.append(profile)
    await db.flush()
    return out


async def latest(db: AsyncSession, cc: str, *, as_of: date | None = None) -> dict[str, AnalyticsAtRiskRouteProfile]:
    """The newest profile per route; with `as_of`, the newest on or before that day, which is what a
    replay of that day must judge with."""
    clauses = [AnalyticsAtRiskRouteProfile.customer_code == cc]
    if as_of is not None:
        clauses.append(AnalyticsAtRiskRouteProfile.as_of_date <= as_of)
    rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(*clauses).order_by(
        AnalyticsAtRiskRouteProfile.route, AnalyticsAtRiskRouteProfile.as_of_date.desc()).limit(ROWS_CAP))).scalars().all()
    out: dict[str, AnalyticsAtRiskRouteProfile] = {}
    for row in rows:
        out.setdefault(row.route, row)
    return out


def usual_minutes(profile: AnalyticsAtRiskRouteProfile | None, *, loading_expected: bool) -> Decimal | None:
    """The learned time of day for a delivery's clock: the van on a loading route, the last pick otherwise."""
    if profile is None:
        return None
    value = profile.van_ready_usual_min if loading_expected else profile.pick_done_usual_min
    return None if value is None else Decimal(str(value))


def clock_for(profiles: Mapping[str, AnalyticsAtRiskRouteProfile], settings: Settings, tz: tzinfo) -> Callable[[model.DeliveryState], model.RouteClock]:
    """The function the board is judged with: for a delivery, its van's usual ready instant on its
    departure day with the tenant's windows, or the WMS departure when the route has no rhythm yet."""
    warn, gone = timedelta(minutes=settings.warn_before_min), timedelta(minutes=settings.gone_after_min)

    def for_state(state: model.DeliveryState) -> model.RouteClock:
        minutes = usual_minutes(profiles.get(state.route) if state.route else None, loading_expected=state.loading_expected)
        if minutes is None:
            return model.RouteClock(usual_ready_at=state.departure_at, source="wms_departure", warn_before=warn, gone_after=gone)
        day = state.departure_at.astimezone(tz).date()
        return model.RouteClock(usual_ready_at=at_minutes(day, minutes, tz), source="learned", warn_before=warn, gone_after=gone)

    return for_state
