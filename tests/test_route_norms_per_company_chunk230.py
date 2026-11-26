"""Chunk 230: a route norm is learned per company, warehouse and route (step 52; 1d stage 2b).

Since chunk 224 a norm belongs to a warehouse and a route. Deliveries carry their WMS company
since chunk 229, and a customer may file the same warehouse code under two companies, so the
place of a norm is the (company, warehouse) pair.

Pinned here
-----------
    learning   two companies with the same warehouse and route learn two norms; a row with a
               warehouse and no company counts with that warehouse's only company on the route;
               a norm stored before it had a company is taken over, not doubled
    judging    a delivery is judged by the norm of its own company and warehouse; one without a
               company by its warehouse's norm, as before
    backfill   stored norms take the customer's only company for their warehouse
    scope      norms are read by the company and warehouse pair
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskRouteProfile
from app.services import scoped_data
from app.services.analytics_at_risk import (
    RULE_VERSION,
    model,
    place_backfill,
    profile_store,
    settings_store,
)
from app.services.scoped_data import DataScope, Place
from tests import at_risk_fixtures as fx
from tests.test_analytics_at_risk_profiles_chunk146 import _closed as _closed_146

CC = "test_chunk230ar"
LONDON = fx.LONDON
AS_OF = date(2026, 10, 1)
NOW = datetime(2026, 10, 2, 2, 5, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


async def _days(route: str, van_hhmm: str, company: str | None, warehouse: str | None, *,
                days: range, tag: str) -> None:
    rows = []
    for d in days:
        row = _closed_146(route, AS_OF - timedelta(days=d), van_hhmm, number=f"{tag}{d}")
        row.customer_code, row.company, row.warehouse = CC, company, warehouse
        rows.append(row)
    await fx.plant(rows)


async def _compute() -> list[AnalyticsAtRiskRouteProfile]:
    async with async_session() as db:
        rows = await profile_store.compute(db, CC, as_of=AS_OF, settings=settings_store.DEFAULTS,
                                           rule_version=RULE_VERSION, now=NOW, tz=LONDON)
        await db.commit()
    return rows


async def _stored() -> list[tuple]:
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC))).scalars().all()
    return sorted((r.route, r.company, r.warehouse, r.van_days) for r in rows)


def _state(route: str, company: str | None, warehouse: str | None) -> model.DeliveryState:
    return model.DeliveryState(
        delivery_number="x", route=route, customer_name=None, customer_number=None,
        departure_at=fx.local_day(AS_OF, 11, 30), lines_expected=1, lines_confirmed=0,
        lines_picked=0, lines_short=0, packages_created=0, packages_loaded=0, last_pick_at=None,
        last_load_at=None, warehouse=warehouse, company=company)


# --- learning -----------------------------------------------------------------------------------

async def test_a_route_of_one_place_takes_its_company_and_warehouse():
    await _days("BRI03", "0700", "915", "BRI", days=range(6), tag="a")
    await _compute()
    assert await _stored() == [("BRI03", "915", "BRI", 6)]


async def test_two_companies_with_the_same_warehouse_and_route_learn_two_norms():
    await _days("R1", "0700", "911", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "920", "BRI", days=range(6), tag="b")
    norms = {n.company: n for n in await _compute()}
    assert await _stored() == [("R1", "911", "BRI", 6), ("R1", "920", "BRI", 6)]
    assert norms["911"].van_ready_usual_min == Decimal("420")
    assert norms["920"].van_ready_usual_min == Decimal("540")


async def test_a_row_without_a_company_counts_with_its_warehouses_only_company():
    await _days("BRI03", "0700", "915", "BRI", days=range(3), tag="a")
    await _days("BRI03", "0700", None, "BRI", days=range(3, 6), tag="b")
    await _compute()
    assert await _stored() == [("BRI03", "915", "BRI", 6)]


async def test_a_norm_stored_before_it_had_a_company_is_taken_over_not_doubled():
    await _days("BRI03", "0700", "915", "BRI", days=range(6), tag="a")
    async with async_session() as db:
        db.add(AnalyticsAtRiskRouteProfile(
            customer_code=CC, route="BRI03", warehouse="BRI", as_of_date=AS_OF, window_days=28,
            sample=1, loaded_sample=1, coverage=Decimal("0.9"), rule_version="old",
            computed_at=NOW))
        await db.commit()
    await _compute()
    assert await _stored() == [("BRI03", "915", "BRI", 6)]


async def test_a_second_run_for_the_same_day_keeps_one_row_per_place():
    await _days("R1", "0700", "911", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "920", "BRI", days=range(6), tag="b")
    await _compute()
    await _compute()
    assert await _stored() == [("R1", "911", "BRI", 6), ("R1", "920", "BRI", 6)]


# --- judging ------------------------------------------------------------------------------------

async def test_a_delivery_is_judged_by_the_norm_of_its_own_company_and_warehouse():
    await _days("R1", "0700", "911", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "920", "BRI", days=range(6), tag="b")
    await _compute()
    async with async_session() as db:
        norms = await profile_store.latest(db, CC)
    clock_for = profile_store.clock_for(norms, settings_store.DEFAULTS, LONDON)
    assert clock_for(_state("R1", "911", "BRI")).usual_ready_at == fx.local_day(AS_OF, 7, 0)
    assert clock_for(_state("R1", "920", "BRI")).usual_ready_at == fx.local_day(AS_OF, 9, 0)
    assert [(n.route, n.company, n.warehouse) for n in norms.all()] == [
        ("R1", "911", "BRI"), ("R1", "920", "BRI")]


async def test_a_delivery_without_a_company_is_judged_by_its_warehouses_norm():
    await _days("BRI03", "0700", "915", "BRI", days=range(6), tag="a")
    await _compute()
    async with async_session() as db:
        norms = await profile_store.latest(db, CC)
    assert norms.of("BRI03", "BRI").company == "915"
    clock = profile_store.clock_for(norms, settings_store.DEFAULTS, LONDON)(
        _state("BRI03", None, "BRI"))
    assert (clock.source, clock.usual_ready_at) == ("learned", fx.local_day(AS_OF, 7, 0))


# --- the backfill and the scope -----------------------------------------------------------------

async def _plant_norm(route: str, warehouse: str | None) -> None:
    async with async_session() as db:
        db.add(AnalyticsAtRiskRouteProfile(
            customer_code=CC, route=route, warehouse=warehouse, as_of_date=AS_OF, window_days=28,
            sample=1, loaded_sample=1, coverage=Decimal("0.9"), rule_version="old",
            computed_at=NOW))
        await db.commit()


async def test_stored_norms_take_the_customers_only_company_for_their_warehouse():
    await _days("BRI03", "0700", "915", "BRI", days=range(2), tag="a")
    await _days("R9", "0700", "911", "LON", days=range(1), tag="b")
    await _days("R9", "0700", "920", "LON", days=range(1, 2), tag="c")
    await _plant_norm("BRI03", "BRI")
    await _plant_norm("R9", "LON")
    await _plant_norm("OLD", None)
    dry = await place_backfill.fill(async_session, CC, dry_run=True)
    assert dry.norms == 1 and all(company is None for _r, company, _w, _d in await _stored())
    result = await place_backfill.fill(async_session, CC)
    stored = {(route, warehouse): company for route, company, warehouse, _days_ in await _stored()}
    assert stored == {("BRI03", "BRI"): "915", ("R9", "LON"): None, ("OLD", None): None}
    assert result.norms == 1


def test_norms_are_limited_by_the_company_and_warehouse_pair():
    rule = scoped_data.registry.BY_TABLE["analytics_at_risk_route_profiles"]
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
    sql = str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))
    assert "analytics_at_risk_route_profiles.warehouse = 'BRI'" in sql
    assert "analytics_at_risk_route_profiles.company = '915'" in sql
