"""Chunk 224: the usual van time is learned per warehouse and route (stage D, step 48).

A route norm said when a route's van is usually ready, by customer and route. Two warehouses that
use the same route code were averaged together, and the row said nothing a site limit could use.
A norm now carries its warehouse. A route whose deliveries name at most one warehouse is still
learned as one group, rows without a warehouse included, so the figures of a customer with one
warehouse per route do not move; only a route that really spans two warehouses is split.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import (
    AnalyticsAtRiskCheck,
    AnalyticsAtRiskRouteProfile,
)
from app.services import scoped_data
from app.services.analytics_at_risk import (
    RULE_VERSION,
    check_store,
    model,
    profile_store,
    settings_store,
)
from tests import at_risk_fixtures as fx
from tests.test_analytics_at_risk_profiles_chunk146 import _closed as _closed_146

CC = "test_chunk224ar"
LONDON = fx.LONDON
AS_OF = date(2026, 10, 1)
NOW = datetime(2026, 10, 2, 2, 5, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _closed(route: str, day: date, van_hhmm: str, *, number: str, warehouse: str | None):
    row = _closed_146(route, day, van_hhmm, number=number)
    row.customer_code, row.warehouse = CC, warehouse
    return row


async def _days(route: str, van_hhmm: str, warehouse: str | None, *, days: range, tag: str) -> None:
    await fx.plant([_closed(route, AS_OF - timedelta(days=d), van_hhmm, number=f"{tag}{d}",
                            warehouse=warehouse) for d in days])


async def _compute() -> list[AnalyticsAtRiskRouteProfile]:
    async with async_session() as db:
        rows = await profile_store.compute(db, CC, as_of=AS_OF, settings=settings_store.DEFAULTS,
                                           rule_version=RULE_VERSION, now=NOW, tz=LONDON)
        await db.commit()
    return rows


def _state(route: str, warehouse: str | None) -> model.DeliveryState:
    return model.DeliveryState(
        delivery_number="x", route=route, customer_name=None, customer_number=None,
        departure_at=fx.local_day(AS_OF, 11, 30), lines_expected=1, lines_confirmed=0,
        lines_picked=0, lines_short=0, packages_created=0, packages_loaded=0, last_pick_at=None,
        last_load_at=None, warehouse=warehouse)


# --- learning -----------------------------------------------------------------------------------

async def test_a_route_of_one_warehouse_is_one_norm_and_takes_that_warehouse():
    await _days("BRI03", "0700", "BRI", days=range(6), tag="a")
    (norm,) = await _compute()
    assert (norm.route, norm.warehouse, norm.van_days) == ("BRI03", "BRI", 6)
    assert norm.van_ready_usual_min == Decimal("420")


async def test_rows_without_a_warehouse_count_with_their_routes_one_warehouse():
    # three days with the warehouse and three without: split, neither half reaches the five-day
    # floor and the usual time would be lost; together they are six days
    await _days("BRI03", "0700", "BRI", days=range(3), tag="a")
    await _days("BRI03", "0700", None, days=range(3, 6), tag="b")
    (norm,) = await _compute()
    assert (norm.warehouse, norm.van_days, norm.van_ready_usual_min) == ("BRI", 6, Decimal("420"))


async def test_a_route_with_no_warehouse_at_all_is_learned_as_before():
    await _days("BRI03", "0700", None, days=range(6), tag="a")
    (norm,) = await _compute()
    assert (norm.warehouse, norm.van_days, norm.van_ready_usual_min) == (None, 6, Decimal("420"))


async def test_a_route_that_spans_two_warehouses_is_learned_per_warehouse():
    await _days("R1", "0700", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "LON", days=range(6), tag="b")
    norms = {n.warehouse: n for n in await _compute()}
    assert norms["BRI"].van_ready_usual_min == Decimal("420")
    assert norms["LON"].van_ready_usual_min == Decimal("540")


async def test_a_second_run_for_the_same_day_keeps_one_row_per_place():
    await _days("BRI03", "0700", "BRI", days=range(6), tag="a")
    await _compute()
    await _compute()
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC))).scalars().all()
    assert [(r.route, r.warehouse) for r in rows] == [("BRI03", "BRI")]


async def test_a_norm_stored_without_a_warehouse_is_taken_over_not_doubled():
    await _days("BRI03", "0700", "BRI", days=range(6), tag="a")
    async with async_session() as db:
        db.add(AnalyticsAtRiskRouteProfile(
            customer_code=CC, route="BRI03", as_of_date=AS_OF, window_days=28, sample=1,
            loaded_sample=1, coverage=Decimal("0.9"), rule_version="old", computed_at=NOW))
        await db.commit()
    await _compute()
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC))).scalars().all()
    assert [(r.route, r.warehouse, r.rule_version) for r in rows] == [
        ("BRI03", "BRI", RULE_VERSION)]


# --- reading ------------------------------------------------------------------------------------

async def test_the_latest_norms_read_by_route_as_before_and_by_place():
    await _days("R1", "0700", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "LON", days=range(6), tag="b")
    await _days("BRI03", "0800", "BRI", days=range(6), tag="c")
    await _compute()
    async with async_session() as db:
        norms = await profile_store.latest(db, CC)
    assert set(norms) == {"R1", "BRI03"} and norms["BRI03"].warehouse == "BRI"
    assert norms.of("R1", "LON").van_ready_usual_min == Decimal("540")
    assert norms.of("R1", "BRI").van_ready_usual_min == Decimal("420")
    assert norms.of("BRI03", None).warehouse == "BRI"  # the route's only norm
    assert norms.of("NOPE", "BRI") is None and norms.of(None, "BRI") is None
    assert [(n.route, n.warehouse) for n in norms.all()] == [
        ("BRI03", "BRI"), ("R1", "BRI"), ("R1", "LON")]


async def test_the_judge_uses_the_norm_of_the_deliverys_own_warehouse():
    await _days("R1", "0700", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "LON", days=range(6), tag="b")
    await _compute()
    async with async_session() as db:
        norms = await profile_store.latest(db, CC)
    clock_for = profile_store.clock_for(norms, settings_store.DEFAULTS, LONDON)
    assert clock_for(_state("R1", "BRI")).usual_ready_at == fx.local_day(AS_OF, 7, 0)
    assert clock_for(_state("R1", "LON")).usual_ready_at == fx.local_day(AS_OF, 9, 0)


async def test_a_delivery_without_a_warehouse_is_judged_by_its_routes_newest_norm():
    await _days("BRI03", "0700", "BRI", days=range(6), tag="a")
    await _compute()
    async with async_session() as db:
        norms = await profile_store.latest(db, CC)
    clock = profile_store.clock_for(norms, settings_store.DEFAULTS, LONDON)(_state("BRI03", None))
    assert (clock.source, clock.usual_ready_at) == ("learned", fx.local_day(AS_OF, 7, 0))


# --- the check and the limit ----------------------------------------------------------------------

async def test_a_check_keeps_the_warehouse_of_its_delivery():
    await _days("BRI03", "0700", "BRI", days=range(1), tag="a")
    async with async_session() as db:
        row = await check_store.find(db, CC, "a0", departure_date=AS_OF)
        await check_store.record(db, row, action="check", actor="Sam", note=None, at=NOW)
        await db.commit()
        (entry,) = (await db.execute(select(AnalyticsAtRiskCheck).where(
            AnalyticsAtRiskCheck.customer_code == CC))).scalars().all()
    assert (entry.warehouse, entry.company) == ("BRI", None)


async def test_a_site_limited_person_reads_the_norms_and_checks_of_their_warehouses():
    await _days("R1", "0700", "BRI", days=range(6), tag="a")
    await _days("R1", "0900", "LON", days=range(6), tag="b")
    await _compute()
    bri = scoped_data.DataScope(CC, site_ids=("BRI",))
    async with scoped_data.open_session(async_session, bri) as db:
        norms = await profile_store.latest(db, CC)
        assert [(n.route, n.warehouse) for n in norms.all()] == [("R1", "BRI")]
        assert scoped_data.closed_tables(scoped_data.tables_read(db), bri) == []


async def test_a_stale_norm_of_the_same_warehouse_never_beats_the_routes_newer_one():
    # the route was last learned with its warehouse some days ago; since then it has been learned
    # without one, so the older row must not be the one a delivery is judged with
    async with async_session() as db:
        for as_of, warehouse, usual in ((AS_OF - timedelta(days=5), "BRI", Decimal("420")),
                                        (AS_OF, None, Decimal("480"))):
            db.add(AnalyticsAtRiskRouteProfile(
                customer_code=CC, route="BRI03", warehouse=warehouse, as_of_date=as_of,
                window_days=28, sample=10, loaded_sample=10, van_days=6,
                van_ready_usual_min=usual, coverage=Decimal("0.9"), rule_version="r",
                computed_at=NOW))
        await db.commit()
        norms = await profile_store.latest(db, CC)
    assert norms.of("BRI03", "BRI").van_ready_usual_min == Decimal("480")
    assert norms["BRI03"].van_ready_usual_min == Decimal("480")
