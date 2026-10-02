"""What the warehouse has done to each delivery on the board: four bounded reads, one state per delivery.

1. The `delivery_route` settlement: one settled row per delivery as `GetNextDeliveryByRoute` last saw
   it, with the departure date and time, the route and the customer. This is the delivery universe.
2. The `pick_release` settled rows grouped by delivery: lines confirmed, picked and short, last pick.
3. The `pick line` lookup read the other way round: how many pick lines name this delivery. This is
   the expected count, known a few minutes before picking starts.
4. One Core read over the facts of the last `lookback_hours` for three methods: the packages the pick
   confirmations filled (`ConfirmPickLine`), loaded one at a time (`LoadDeliveryPackage`) and loaded as a
   milk list (`LoadDeliveryPackageList`, whose deliveries live inside a JSON string).

Every read pins the tenant first and is bounded (CLAUDE.md rules 3 and 4). Nothing reads
`log_transactions`: facts are within one fold cycle of it and already hold what the fold extracted.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from typing import Any, Sequence

from sqlalchemy import Numeric, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_lookup import AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.services.analytics.settle_store import NUMBER_SHAPE
from app.services.analytics_at_risk import model

#: The packages a delivery is known to have are the package numbers on its pick confirmations that moved
#: stock: 93% of confirmations carry the package the line was packed into, and over a live week 3,160 of
#: those 3,167 packages were loaded. `NewDeliveryPackage` is NOT read: a package made by hand that no pick
#: ever filled is an empty box (2 of 11 were loaded), and a short line's package number is noise (it can
#: even be another delivery's), so each false "never loaded" came from one of those two.
PACKAGE_METHODS = ("ConfirmPickLine", "LoadDeliveryPackage", "LoadDeliveryPackageList")

#: How far back the routing rows are read. A delivery is named by a routing call the evening before
#: its departure at the earliest, and the board keeps yesterday's departures until they are closed.
ROUTE_LOOKBACK = timedelta(days=3)
#: Most deliveries one tenant can have named in that window. Live volume is about 100 a day.
ROUTE_ROWS_CAP = 5000
#: `IN (...)` lists are cut into pieces this long.
BATCH = 500


@dataclass(frozen=True)
class BoardRead:
    states: list[model.DeliveryState]
    #: The facts read hit its cap, so some packages or loads may be missing from the states.
    overflow: bool
    #: Routing rows whose departure date or time could not be read.
    unreadable_departures: int


def _batches(values: Sequence[str]) -> list[Sequence[str]]:
    return [values[i:i + BATCH] for i in range(0, len(values), BATCH)]


def _numeric(name: str):
    """A settled value as a number, NULL when it is not shaped like one (the settle_store rule)."""
    text = AnalyticsSettledRow.attributes[name].astext
    return case((text.op("~")(NUMBER_SHAPE), text.cast(Numeric(30, 6))), else_=None)


# ============================================================== 1. deliveries

@dataclass
class _Route:
    departure_at: datetime
    route: str | None
    customer_name: str | None
    customer_number: str | None


async def _routes(db: AsyncSession, cc: str, *, settlement: str, now: datetime, tz: tzinfo,
                  today: date) -> tuple[dict[str, _Route], int]:
    rows = (await db.execute(select(AnalyticsSettledRow.key_parts, AnalyticsSettledRow.attributes,
                                    AnalyticsSettledRow.warehouse).where(
        AnalyticsSettledRow.customer_code == cc, AnalyticsSettledRow.settlement == settlement,
        AnalyticsSettledRow.event_time >= now - ROUTE_LOOKBACK,
    ).order_by(AnalyticsSettledRow.event_time).limit(ROUTE_ROWS_CAP))).all()
    out: dict[str, _Route] = {}
    unreadable = 0
    for key_parts, attributes, _warehouse in rows:
        delivery = str((key_parts or [None])[0] or "").strip()
        if not delivery:
            continue
        attrs: dict[str, Any] = attributes or {}
        departure = model.departure_at(attrs.get("departure_date"), attrs.get("departure_time"), tz)
        if departure is None:
            unreadable += 1
            continue
        local_date = departure.astimezone(tz).date()
        if not (today - timedelta(days=1) <= local_date <= today + timedelta(days=1)):
            continue
        out[delivery] = _Route(departure_at=departure, route=_text(attrs.get("resp.Route")),
                               customer_name=_text(attrs.get("resp.CustomerName")),
                               customer_number=_text(attrs.get("resp.CustomerNumber")))
    return out, unreadable


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


# ============================================================== 2. picks

@dataclass
class _Picks:
    confirmed: int = 0
    picked: int = 0
    short: int = 0
    last_pick_at: datetime | None = None
    transaction_names: tuple[str, ...] = ()


async def _picks(db: AsyncSession, cc: str, *, settlement: str, deliveries: Sequence[str]) -> dict[str, _Picks]:
    out: dict[str, _Picks] = {}
    picked = _numeric("picked")
    short = _numeric("is_short")
    for batch in _batches(deliveries):
        rows = (await db.execute(select(
            AnalyticsSettledRow.delivery_number, func.count(), func.count().filter(picked > 0),
            func.coalesce(func.sum(short), 0), func.max(AnalyticsSettledRow.event_time),
            func.array_agg(func.distinct(AnalyticsSettledRow.transaction_name)),
        ).where(
            AnalyticsSettledRow.customer_code == cc, AnalyticsSettledRow.settlement == settlement,
            AnalyticsSettledRow.delivery_number.in_(list(batch)),
        ).group_by(AnalyticsSettledRow.delivery_number))).all()
        for delivery, confirmed, picked_n, short_n, last, names in rows:
            out[delivery] = _Picks(confirmed=int(confirmed), picked=int(picked_n), short=int(short_n), last_pick_at=last,
                                   transaction_names=tuple(sorted(n for n in (names or []) if n)))
    return out


# ============================================================== 3. expected lines

async def _expected(db: AsyncSession, cc: str, *, lookup: str, deliveries: Sequence[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for batch in _batches(deliveries):
        rows = (await db.execute(select(
            AnalyticsLookupValue.value, func.count(func.distinct(AnalyticsLookupValue.key)),
        ).where(
            AnalyticsLookupValue.customer_code == cc, AnalyticsLookupValue.lookup == lookup,
            AnalyticsLookupValue.attribute == "DeliveryNumber", AnalyticsLookupValue.valid_to.is_(None),
            AnalyticsLookupValue.value.in_(list(batch)),
        ).group_by(AnalyticsLookupValue.value))).all()
        for delivery, n in rows:
            out[delivery] = int(n)
    return out


# ============================================================== 4. packages and loads

@dataclass
class _Packages:
    #: Known packages: the package numbers on the pick confirmations that moved stock.
    created: set[str]
    loaded: set[str]
    last_load_at: datetime | None = None


@dataclass
class _PackageRead:
    by_delivery: dict[str, _Packages]
    #: Routes (loading docks) that loaded anything in the window: the routes with a loading step.
    routes_that_load: set[str]
    #: When each dock was last loaded on each local day: the route's "van ready" moment.
    route_loaded: dict[tuple[str, date], datetime]
    overflow: bool


async def _packages(db: AsyncSession, cc: str, *, since: datetime, until: datetime | None, cap: int, tz: tzinfo) -> _PackageRead:
    """Packages and loads from the facts with an event time in `[since, until)`; `until` None means
    up to now. The board reads a lookback from now; the backfill reads a closed day."""
    attrs = AnalyticsFact.attributes
    clauses = [AnalyticsFact.customer_code == cc, AnalyticsFact.method.in_(PACKAGE_METHODS),
               AnalyticsFact.status == "success", AnalyticsFact.event_time >= since]
    if until is not None:
        clauses.append(AnalyticsFact.event_time < until)
    rows = (await db.execute(select(
        AnalyticsFact.method, AnalyticsFact.event_time, AnalyticsFact.delivery_number, AnalyticsFact.id,
        AnalyticsFact.quantity, attrs["PackageNumber"].astext, attrs["PackagesToLoad"].astext, attrs["LoadingDock"].astext,
    ).where(*clauses).order_by(AnalyticsFact.event_time).limit(cap + 1))).all()
    overflow = len(rows) > cap
    out: dict[str, _Packages] = defaultdict(lambda: _Packages(created=set(), loaded=set()))
    routes: set[str] = set()
    route_loaded: dict[tuple[str, date], datetime] = {}

    def loaded(delivery: str, package: str, at: datetime, dock: str | None) -> None:
        p = out[delivery]
        p.loaded.add(package)
        if p.last_load_at is None or at > p.last_load_at:
            p.last_load_at = at
        if dock:
            routes.add(dock)
            key = (dock, at.astimezone(tz).date())
            if key not in route_loaded or at > route_loaded[key]:
                route_loaded[key] = at

    for method, at, delivery, fact_id, quantity, package_number, packages_to_load, dock in rows[:cap]:
        if method == "ConfirmPickLine":
            package = _text(package_number)
            if delivery and package and quantity is not None and quantity > 0:
                out[delivery].created.add(package)
        elif method == "LoadDeliveryPackage":
            if delivery:
                loaded(delivery, _text(package_number) or str(fact_id), at, _text(dock))
        else:
            for d, p in model.parse_packages_to_load(packages_to_load):
                loaded(d, p, at, _text(dock))
    return _PackageRead(by_delivery=dict(out), routes_that_load=routes, route_loaded=route_loaded, overflow=overflow)


# ============================================================== one state from the pieces

def assemble(delivery: str, r: _Route, p: _Picks, k: _Packages | None, *, expected: int | None,
             loading_routes: set[str], route_loaded: dict[tuple[str, date], datetime], tz: tzinfo) -> model.DeliveryState:
    return model.DeliveryState(
        delivery_number=delivery, route=r.route, customer_name=r.customer_name, customer_number=r.customer_number,
        departure_at=r.departure_at, lines_expected=expected, lines_confirmed=p.confirmed,
        lines_picked=p.picked, lines_short=p.short,
        packages_created=len(k.created) if k else 0, packages_loaded=len(k.loaded) if k else 0,
        last_pick_at=p.last_pick_at, last_load_at=k.last_load_at if k else None,
        loading_expected=r.route in loading_routes if r.route else True,
        transaction_names=p.transaction_names,
        route_loaded_at=route_loaded.get((r.route, r.departure_at.astimezone(tz).date())) if r.route else None)


# ============================================================== the board

async def read_states(db: AsyncSession, cc: str, *, now: datetime, tz: tzinfo,
                      settlement_name: str = "delivery_route", pick_settlement: str = "pick_release",
                      lookup_name: str = "pick line", lookback_hours: int = 36, facts_cap: int = 20000,
                      routes_that_load: set[str] | None = None) -> BoardRead:
    """Every delivery with a departure yesterday, today or tomorrow on the tenant's clock, with what
    has been done to it so far. Sorted by departure, then delivery number.

    A delivery's route has a loading step when the route loaded anything in the lookback window or
    when the caller says so from the route's history (`routes_that_load`); otherwise the delivery is
    judged on picking alone."""
    today = now.astimezone(tz).date()
    routes, unreadable = await _routes(db, cc, settlement=settlement_name, now=now, tz=tz, today=today)
    deliveries = sorted(routes)
    picks = await _picks(db, cc, settlement=pick_settlement, deliveries=deliveries) if deliveries else {}
    expected = await _expected(db, cc, lookup=lookup_name, deliveries=deliveries) if deliveries else {}
    packages = await _packages(db, cc, since=now - timedelta(hours=lookback_hours), until=None, cap=facts_cap, tz=tz)
    loading_routes = packages.routes_that_load | set(routes_that_load or ())
    states = [assemble(d, routes[d], picks.get(d, _Picks()), packages.by_delivery.get(d), expected=expected.get(d),
                       loading_routes=loading_routes, route_loaded=packages.route_loaded, tz=tz) for d in deliveries]
    states.sort(key=lambda s: (s.departure_at, s.delivery_number))
    return BoardRead(states=states, overflow=packages.overflow, unreadable_departures=unreadable)
