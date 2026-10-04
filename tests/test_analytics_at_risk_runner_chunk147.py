"""Chunk 147: one tenant, one pass: read the board, judge it, write it, close what has departed.

The pins: a delivery is flagged once its van's usual ready time is inside the warning window (the WMS
departure standing in until the route has a rhythm) and closes with an outcome once the departure
plus the grace has passed; a tenant whose read fails writes `last_error`
on its state row and never raises, because the loop must go on to the other tenants; two passes at
once serialise on the tenant's advisory lock and leave one row; and the daily profile pass writes the
route profiles and stamps the state row.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import board_store, runner, state_store
from tests import at_risk_fixtures as fx

CC = "test_chunk147ar"
LONDON = fx.LONDON
UTC = timezone.utc
DEP = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)  # 11:30 BST on 2 Oct


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


async def _row(delivery="29616") -> AnalyticsAtRiskDelivery | None:
    async with async_session() as db:
        return await db.scalar(select(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == CC, AnalyticsAtRiskDelivery.delivery_number == delivery))


async def _plant_delivery():
    t0 = DEP - timedelta(hours=7)
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=t0)])
    await fx.plant(fx.pick_line_values(CC, "29616", ["600001", "600002"]))
    await fx.plant([fx.pick_fact(CC, "29616", "600001", t0 + timedelta(minutes=10), expected="2", picked="2"),
                    fx.pick_fact(CC, "29616", "600002", t0 + timedelta(minutes=12), expected="1", picked="1"),
                    # another delivery on the same route was loaded earlier, so BRI03 is a route with a loading step
                    fx.load_fact(CC, "29000", "29000/1-1", t0, dock="BRI03")])
    await fx.settle(CC)


async def test_evaluate_flags_a_delivery_inside_the_window_and_closes_it_after_grace():
    await _plant_delivery()
    calm = await runner.evaluate_tenant(CC, now=DEP - timedelta(hours=6))
    assert calm["status"] == "completed" and calm["evaluated"] == 1 and calm["flagged"] == 0
    row = await _row()
    assert (row.tier, row.lines_expected, row.lines_picked, row.packages_created, row.packages_loaded) == ("none", 2, 2, 1, 0)
    assert (row.usual_ready_at, row.usual_ready_source) == (DEP, "wms_departure")  # no profile yet: the WMS departure stands in

    inside = await runner.evaluate_tenant(CC, now=DEP - timedelta(minutes=20))
    assert inside["flagged"] == 1
    row = await _row()
    assert (row.tier, row.first_flagged_tier) == ("at_risk", "at_risk")

    # the package goes on the van at 11:20
    await fx.plant([fx.load_fact(CC, "29616", "29616/1-1", DEP - timedelta(minutes=10), dock="BRI03")])
    after = await runner.evaluate_tenant(CC, now=DEP + timedelta(hours=3))
    assert after["closed"] == 1
    row = await _row()
    assert (row.status, row.outcome, row.outcome_lead_min, row.tier, row.max_tier) == ("closed", "loaded_in_time", Decimal("10"), "none", "at_risk")
    assert row.route_loaded_at == DEP - timedelta(minutes=10) and row.route_loading_from == DEP - timedelta(hours=7)
    async with async_session() as db:
        state = await state_store.get(db, CC)
    assert state.last_error is None and state.last_evaluated_at == DEP + timedelta(hours=3) and state.open_rows == 0


async def test_a_failing_read_writes_last_error_and_does_not_raise(monkeypatch):
    async def boom(*a, **k):
        raise RuntimeError("the lookup table is on fire")
    monkeypatch.setattr(board_store, "read_states", boom)
    result = await runner.evaluate_tenant(CC, now=DEP)
    assert result["status"] == "failed" and "on fire" in result["error"]
    async with async_session() as db:
        state = await state_store.get(db, CC)
    assert "RuntimeError: the lookup table is on fire" in state.last_error


async def test_two_passes_at_once_serialise_and_leave_one_row():
    await _plant_delivery()
    results = await asyncio.gather(runner.evaluate_tenant(CC, now=DEP - timedelta(minutes=20)),
                                   runner.evaluate_tenant(CC, now=DEP - timedelta(minutes=19)))
    assert {r["status"] for r in results} == {"completed"}
    async with async_session() as db:
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
    assert n == 1
    assert (await _row()).tier == "at_risk"


async def test_a_disabled_tenant_is_skipped():
    await _plant_delivery()
    async with async_session() as db:
        from app.services.analytics_at_risk import settings_store
        await settings_store.put(db, CC, enabled=False)
        await db.commit()
    result = await runner.evaluate_tenant(CC, now=DEP - timedelta(minutes=20))
    assert result["status"] == "skipped"
    assert await _row() is None


async def test_the_profile_pass_writes_route_profiles_and_stamps_the_state_row():
    as_of = date(2026, 10, 1)
    await fx.plant([fx.closed_delivery(CC, f"d{i}", route="BRI03", departure_at=fx.local_day(as_of - timedelta(days=i % 10), 11, 30),
                                       last_load_at=fx.local_day(as_of - timedelta(days=i % 10), 7, 0) - timedelta(minutes=10 * i),
                                       route_loaded_at=fx.local_day(as_of - timedelta(days=i % 10), 7, 0),
                                       outcome="loaded_in_time") for i in range(20)])
    result = await runner.profile_tenant(CC, as_of=as_of, now=DEP - timedelta(hours=8))
    assert result == {"status": "completed", "as_of": "2026-10-01", "routes": 1}
    async with async_session() as db:
        profile = await db.scalar(select(AnalyticsAtRiskRouteProfile).where(AnalyticsAtRiskRouteProfile.customer_code == CC))
        state = await state_store.get(db, CC)
    assert profile.route == "BRI03" and profile.sample == 20 and profile.van_days == 10 and profile.van_ready_usual_min == Decimal("420")
    assert state.last_profiled_date == as_of
