"""Route profiles: what a route's closed deliveries teach about its rhythm, one row per route per day.

The learned lead is the coverage quantile over the leads of the loaded deliveries in the window: the
lead nine in ten of them met or beat. It is unknown below the sample floor. `thresholds_for` turns the
latest profiles and the tenant's settings into the function the board is judged with: the larger of
the learned lead and the floor, per route, and the floor alone for a route nobody has learned yet.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, tzinfo
from decimal import Decimal
from typing import Callable, Mapping

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import model
from app.services.analytics_at_risk.settings_store import Settings

#: Outcomes a profile learns from. `unknown` rows have no board data and teach nothing. A route
#: without a loading step teaches its pick lead only (its load leads are simply absent).
LEARNING_OUTCOMES = ("loaded_in_time", "loaded_late", "never_loaded", "picked_in_time", "picked_late")
#: Most closed rows one computation reads. 28 days at a few hundred departures a day is well under it.
ROWS_CAP = 50000


def _quantile(values: list[Decimal], p: Decimal) -> Decimal | None:
    """A plain quantile over the plausible band, with no sample floor."""
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
        load_leads = [model.minutes_to_departure(r.departure_at, r.last_load_at) for r in rows if r.last_load_at is not None]
        pick_leads = [model.minutes_to_departure(r.departure_at, r.last_pick_at) for r in rows if r.last_pick_at is not None]
        usable_load = [x for x in load_leads if model.LEAD_MIN <= x <= model.LEAD_MAX]
        usable_pick = [x for x in pick_leads if model.LEAD_MIN <= x <= model.LEAD_MAX]
        mode = Counter(r.departure_at.astimezone(tz).strftime("%H%M") for r in rows).most_common(1)
        profile = existing.get(route)
        if profile is None:
            profile = AnalyticsAtRiskRouteProfile(customer_code=cc, route=route, as_of_date=as_of)
            db.add(profile)
        profile.window_days = settings.window_days
        profile.sample, profile.loaded_sample = len(rows), len(load_leads)
        profile.load_lead_p50 = _two_places(_quantile(usable_load, Decimal("0.5")))
        profile.load_lead_min = _two_places(min(usable_load) if usable_load else None)
        profile.pick_lead_p50 = _two_places(_quantile(usable_pick, Decimal("0.5")))
        profile.pick_lead_min = _two_places(min(usable_pick) if usable_pick else None)
        profile.learned_load_min = _two_places(model.coverage_quantile(load_leads, coverage=settings.coverage,
                                                                       min_sample=settings.min_sample))
        profile.learned_pick_min = _two_places(model.coverage_quantile(pick_leads, coverage=settings.coverage,
                                                                       min_sample=settings.min_sample))
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
    rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(*clauses).order_by(AnalyticsAtRiskRouteProfile.route, AnalyticsAtRiskRouteProfile.as_of_date.desc()).limit(ROWS_CAP))).scalars().all()
    out: dict[str, AnalyticsAtRiskRouteProfile] = {}
    for row in rows:
        out.setdefault(row.route, row)
    return out


def thresholds_for(profiles: Mapping[str, AnalyticsAtRiskRouteProfile], settings: Settings) -> Callable[[str | None], model.Thresholds]:
    """The function the board is judged with: per route, the larger of the learned lead and the floor."""
    load_floor, pick_floor = Decimal(settings.load_floor_min), Decimal(settings.pick_floor_min)

    def for_route(route: str | None) -> model.Thresholds:
        profile = profiles.get(route) if route else None
        learned_load = None if profile is None or profile.learned_load_min is None else Decimal(str(profile.learned_load_min))
        learned_pick = None if profile is None or profile.learned_pick_min is None else Decimal(str(profile.learned_pick_min))
        load_min, load_source = model.effective_threshold(learned_load, load_floor)
        pick_min, pick_source = model.effective_threshold(learned_pick, pick_floor)
        return model.Thresholds(load_min=load_min, load_source=load_source, pick_min=pick_min, pick_source=pick_source)

    return for_route
