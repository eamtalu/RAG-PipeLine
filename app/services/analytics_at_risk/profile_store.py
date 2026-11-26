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
    groups = _by_place(await _closed_rows(db, cc, start=start, end=as_of))
    stored = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(
        AnalyticsAtRiskRouteProfile.customer_code == cc, AnalyticsAtRiskRouteProfile.as_of_date == as_of))).scalars().all()
    existing = {(r.company, r.warehouse, r.route): r for r in stored}
    out = []
    in_order = sorted(groups.items(), key=lambda kv: (kv[0][2], kv[0][1] or "", kv[0][0] or ""))
    for (company, warehouse, route), rows in in_order:
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
        # the day's row for this place, or the one stored before norms carried a company, or
        # before they carried a warehouse: either is taken over instead of being doubled
        profile = (existing.get((company, warehouse, route))
                   or existing.pop((None, warehouse, route), None)
                   or existing.pop((None, None, route), None))
        if profile is None:
            profile = AnalyticsAtRiskRouteProfile(customer_code=cc, route=route, as_of_date=as_of)
            db.add(profile)
        profile.company, profile.warehouse = company, warehouse
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


class RouteProfiles(dict):
    """The newest norm per route, as the mapping `latest` has always returned, which also knows
    the norm of each place of a route.

    `norms["BRI03"]` is the route's newest norm; `norms.of("R1", "LON")` is the norm of that
    warehouse when the route spans several, and `norms.of("R1", "BRI", "911")` the norm of that
    company's BRI when two companies share the code; `norms.all()` lists every place's norm.
    """

    def __init__(self, newest_first: list[AnalyticsAtRiskRouteProfile]):
        super().__init__()
        self._by_place: dict[tuple[str | None, str | None, str], AnalyticsAtRiskRouteProfile] = {}
        #: The newest norm of a warehouse on a route, whatever its company.
        self._at_warehouse: dict[tuple[str | None, str], AnalyticsAtRiskRouteProfile] = {}
        for row in newest_first:
            self._by_place.setdefault((row.company, row.warehouse, row.route), row)
        newest = sorted(self._by_place.values(), key=lambda r: r.as_of_date, reverse=True)
        for row in newest:
            self._at_warehouse.setdefault((row.warehouse, row.route), row)
            self.setdefault(row.route, row)

    def _own(self, route: str, warehouse: str | None,
             company: str | None) -> AnalyticsAtRiskRouteProfile | None:
        """The norm of a delivery's own place.

        (915, BRI) -> the 915 BRI norm, else the BRI norm stored before norms had a company
        ; (no company, BRI) -> the newest BRI norm of any company
        """
        if company is None:
            return self._at_warehouse.get((warehouse, route))
        return (self._by_place.get((company, warehouse, route))
                or self._by_place.get((None, warehouse, route)))

    def of(self, route: str | None, warehouse: str | None,
           company: str | None = None) -> AnalyticsAtRiskRouteProfile | None:
        """The norm a delivery is judged with: its own place's, else its route's newest.

        ("R1", "LON") -> the LON norm of R1 ; ("R1", "BRI", "911") -> company 911's BRI norm
        ; ("BRI03", None) -> BRI03's newest ; (None, ...) -> None
        """
        if not route:
            return None
        newest = self.get(route)
        own = self._own(route, warehouse, company)
        # the warehouse's own norm counts only while it is as new as the route's newest: places of
        # one route are learned on the same night, so an older one was left behind by a later run
        if own is not None and newest is not None and own.as_of_date >= newest.as_of_date:
            return own
        return newest

    def all(self) -> list[AnalyticsAtRiskRouteProfile]:
        """Every place's norm, by route, warehouse, then company: [BRI03/BRI, R1/BRI, R1/LON]."""
        return sorted(self._by_place.values(),
                      key=lambda r: (r.route, r.warehouse or "", r.company or ""))


#: What a norm is learned for: (company, warehouse, route). Company and warehouse are None for a
#: route whose deliveries name no warehouse.
PlaceRoute = tuple[str | None, str | None, str]


def _by_place(
        rows: list[AnalyticsAtRiskDelivery]) -> dict[PlaceRoute, list[AnalyticsAtRiskDelivery]]:
    """The learning rows grouped by (company, warehouse, route). A route whose rows name at most
    one place stays one group and takes that place, its rows without a warehouse included, so
    that older rows do not split a route below the day floor; only a route that names two or more
    places is split, and its rows without a warehouse form their own group. A row with a
    warehouse and no company counts with that warehouse's only company on the route.

    (BRI03: 3 rows 915 BRI, 3 rows no warehouse) -> {("915", "BRI", "BRI03"): 6 rows} ;
    (R1: 911 BRI and 920 BRI rows) -> {("911", "BRI", "R1"): [...], ("920", "BRI", "R1"): [...]}
    """
    by_route: dict[str, list[AnalyticsAtRiskDelivery]] = defaultdict(list)
    for row in rows:
        by_route[row.route].append(row)
    groups: dict[PlaceRoute, list[AnalyticsAtRiskDelivery]] = {}
    for route, of_route in by_route.items():
        # Step 1: Find the companies each warehouse is filed under on this route, so that a row
        # closed before deliveries carried a company can join its warehouse's only one.
        companies: dict[str, set[str]] = defaultdict(set)
        for row in of_route:
            if row.warehouse and row.company:
                companies[row.warehouse].add(row.company)

        def place_of(row: AnalyticsAtRiskDelivery) -> tuple[str | None, str] | None:
            if not row.warehouse:
                return None
            only = companies[row.warehouse]
            return (row.company or (next(iter(only)) if len(only) == 1 else None), row.warehouse)

        placed = [(place_of(row), row) for row in of_route]
        named = {place for place, _row in placed if place is not None}
        # Step 2: One place at most: the route is one norm, otherwise rows without a warehouse
        # would split it below the number of days a norm needs.
        if len(named) <= 1:
            company, warehouse = next(iter(named), (None, None))
            groups[(company, warehouse, route)] = of_route
            continue
        # Step 3: Two or more places: one norm each, because their vans keep different hours.
        for place, row in placed:
            company, warehouse = place or (None, None)
            groups.setdefault((company, warehouse, route), []).append(row)
    return groups


async def latest(db: AsyncSession, cc: str, *, as_of: date | None = None) -> RouteProfiles:
    """The newest profile per route; with `as_of`, the newest on or before that day, which is what a
    replay of that day must judge with."""
    clauses = [AnalyticsAtRiskRouteProfile.customer_code == cc]
    if as_of is not None:
        clauses.append(AnalyticsAtRiskRouteProfile.as_of_date <= as_of)
    rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(*clauses).order_by(
        AnalyticsAtRiskRouteProfile.route, AnalyticsAtRiskRouteProfile.as_of_date.desc()).limit(ROWS_CAP))).scalars().all()
    return RouteProfiles(list(rows))


def usual_minutes(profile: AnalyticsAtRiskRouteProfile | None, *, loading_expected: bool) -> Decimal | None:
    """The learned time of day for a delivery's clock: the van on a loading route, the last pick otherwise."""
    if profile is None:
        return None
    value = profile.van_ready_usual_min if loading_expected else profile.pick_done_usual_min
    return None if value is None else Decimal(str(value))


def _norm_for(profiles: Mapping[str, AnalyticsAtRiskRouteProfile], state: model.DeliveryState) -> AnalyticsAtRiskRouteProfile | None:
    """The norm one delivery is judged with: its warehouse's when the norms know places, else its route's.

    (RouteProfiles, R1 at LON) -> the LON norm ; (a plain dict by route, as tests hand over) -> by route
    """
    if isinstance(profiles, RouteProfiles):
        return profiles.of(state.route, state.warehouse, state.company)
    return profiles.get(state.route) if state.route else None


def clock_for(profiles: Mapping[str, AnalyticsAtRiskRouteProfile], settings: Settings, tz: tzinfo) -> Callable[[model.DeliveryState], model.RouteClock]:
    """The function the board is judged with: for a delivery, its van's usual ready instant on its
    departure day with the tenant's windows, or the WMS departure when the route has no rhythm yet."""
    warn, gone = timedelta(minutes=settings.warn_before_min), timedelta(minutes=settings.gone_after_min)

    def for_state(state: model.DeliveryState) -> model.RouteClock:
        minutes = usual_minutes(_norm_for(profiles, state), loading_expected=state.loading_expected)
        if minutes is None:
            return model.RouteClock(usual_ready_at=state.departure_at, source="wms_departure", warn_before=warn, gone_after=gone)
        day = state.departure_at.astimezone(tz).date()
        return model.RouteClock(usual_ready_at=at_minutes(day, minutes, tz), source="learned", warn_before=warn, gone_after=gone)

    return for_state
