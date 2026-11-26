"""Chunk 229: an at-risk delivery and its checks say which WMS company they belong to (step 51).

Step 1d stage 2b, second part. A delivery on the board takes its place from its settled route
rows, which carry the company since chunk 228. Deliveries closed before November 2026 have no
warehouse at all, which left routes such as BRI07 without a place; both are filled from the same
settled rows.

Pinned here
-----------
    board      a delivery's state carries the company of its settled route row
    store      the row keeps it; a state without one never erases it; a check copies it
    rebuild    a delivery reconstructed from raw routing calls takes the call's company and
               warehouse
    backfill   an older delivery takes warehouse and company from its newest settled route row;
               a delivery already at another warehouse is not given that row's company; a
               delivery with no settled row is left; its checks follow; a dry run writes nothing
    scope      deliveries and checks are read by the company and warehouse pair
"""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskCheck, AnalyticsAtRiskDelivery
from app.services import scoped_data
from app.services.analytics_at_risk import backfill, board_store, check_store, delivery_store
from app.services.analytics_at_risk import place_backfill
from app.services.scoped_data import DataScope, Place
from tests import at_risk_fixtures as fx

CC = "test_chunk229ar"
LONDON = fx.LONDON
UTC = timezone.utc
NOW = datetime(2026, 10, 2, 5, 0, tzinfo=UTC)
DAY = date(2026, 10, 2)
DEPARTURE = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _route_fact(delivery: str, *, company: str | None, warehouse: str | None = "BRI",
                hours_ago: int = 3):
    fact = fx.route_fact(CC, delivery, route="BRI03", dep_date="20261002", dep_time="1130",
                         when=NOW - timedelta(hours=hours_ago))
    fact.company, fact.warehouse = company, warehouse
    return fact


async def _settled_routes(*facts) -> None:
    await fx.plant(list(facts))
    await fx.settle(CC)


def _closed(delivery: str, *, warehouse: str | None = None, company: str | None = None):
    row = fx.closed_delivery(CC, delivery, route="BRI03", departure_at=DEPARTURE,
                             last_load_at=DEPARTURE - timedelta(minutes=40), outcome="on_time")
    row.warehouse, row.company = warehouse, company
    return row


async def _places() -> dict[str, tuple[str | None, str | None]]:
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == CC))).scalars().all()
    return {r.delivery_number: (r.company, r.warehouse) for r in rows}


# ==================================================== 1. the board and the store

async def test_a_delivery_on_the_board_carries_the_company_of_its_route_row():
    await _settled_routes(_route_fact("29616", company="915"), _route_fact("29617", company=None))
    async with async_session() as db:
        read = await board_store.read_states(db, CC, now=NOW, tz=LONDON)
    states = {s.delivery_number: s for s in read.states}
    assert (states["29616"].company, states["29616"].warehouse) == ("915", "BRI")
    assert states["29617"].company is None


def test_a_row_keeps_the_company_and_a_state_without_one_never_erases_it():
    row = _closed("29616", warehouse="BRI", company="915")
    state = delivery_store.state_from_row(row)
    assert state.company == "915"
    delivery_store._write_progress(row, replace(state, company=None))
    assert row.company == "915"
    delivery_store._write_progress(row, replace(state, company="920"))
    assert row.company == "920"


async def test_a_check_keeps_the_company_of_its_delivery():
    await fx.plant([_closed("29616", warehouse="BRI", company="915")])
    async with async_session() as db:
        row = await check_store.find(db, CC, "29616", departure_date=DAY)
        await check_store.record(db, row, action="check", actor="Sam", note=None, at=NOW)
        await db.commit()
        (entry,) = (await db.execute(select(AnalyticsAtRiskCheck).where(
            AnalyticsAtRiskCheck.customer_code == CC))).scalars().all()
    assert (entry.company, entry.warehouse) == ("915", "BRI")


async def test_a_delivery_rebuilt_from_raw_calls_takes_the_calls_company_and_warehouse():
    call = fx.routing_call("29616", route="BRI03", dep_date="20261002", dep_time="1130",
                           when=NOW - timedelta(hours=3))
    await fx.plant_calls(CC, [{**call, "company": "915"}])
    async with async_session() as db:
        routes, _unreadable = await backfill.routing_universe(db, CC, day=DAY, tz=LONDON)
    assert (routes["29616"].company, routes["29616"].warehouse) == ("915", "BRI")


# ==================================================== 2. the backfill

async def test_an_older_delivery_takes_its_place_from_its_newest_settled_route_row():
    await _settled_routes(_route_fact("29616", company="915"))
    await fx.plant([_closed("29616")])
    result = await place_backfill.fill(async_session, CC)
    assert await _places() == {"29616": ("915", "BRI")}
    assert result.deliveries == 1


async def test_a_delivery_at_another_warehouse_is_not_given_the_rows_company():
    await _settled_routes(_route_fact("29616", company="915", warehouse="BRI"))
    await fx.plant([_closed("29616", warehouse="LON")])
    await place_backfill.fill(async_session, CC)
    assert await _places() == {"29616": (None, "LON")}


async def test_a_delivery_with_a_warehouse_gains_only_the_company():
    await _settled_routes(_route_fact("29616", company="915"))
    await fx.plant([_closed("29616", warehouse="BRI")])
    await place_backfill.fill(async_session, CC)
    assert await _places() == {"29616": ("915", "BRI")}


async def test_a_delivery_with_no_settled_route_row_is_left_as_it_is():
    await fx.plant([_closed("29999")])
    result = await place_backfill.fill(async_session, CC)
    assert await _places() == {"29999": (None, None)} and result.deliveries == 0


async def test_the_checks_of_a_filled_delivery_follow_it():
    await _settled_routes(_route_fact("29616", company="915"))
    await fx.plant([_closed("29616")])
    async with async_session() as db:
        row = await check_store.find(db, CC, "29616", departure_date=DAY)
        await check_store.record(db, row, action="check", actor="Sam", note=None, at=NOW)
        await db.commit()
    result = await place_backfill.fill(async_session, CC)
    async with async_session() as db:
        (entry,) = (await db.execute(select(AnalyticsAtRiskCheck).where(
            AnalyticsAtRiskCheck.customer_code == CC))).scalars().all()
    assert (entry.company, entry.warehouse) == ("915", "BRI") and result.checks == 1


async def test_a_dry_run_counts_and_writes_nothing():
    await _settled_routes(_route_fact("29616", company="915"))
    await fx.plant([_closed("29616")])
    result = await place_backfill.fill(async_session, CC, dry_run=True)
    assert result.deliveries == 1 and await _places() == {"29616": (None, None)}


async def test_the_script_fills_and_reports(capsys):
    from scripts import backfill_at_risk_places as script
    await _settled_routes(_route_fact("29616", company="915"))
    await fx.plant([_closed("29616")])
    assert await script.run(script.build_parser().parse_args([CC])) == 0
    assert "1 deliveries" in capsys.readouterr().out
    assert await _places() == {"29616": ("915", "BRI")}


# ==================================================== 3. the scope

@pytest.mark.parametrize("table", ["analytics_at_risk_deliveries", "analytics_at_risk_checks"])
def test_deliveries_and_checks_are_limited_by_the_company_and_warehouse_pair(table):
    rule = scoped_data.registry.BY_TABLE[table]
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
    sql = str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))
    assert f"{table}.warehouse = 'BRI'" in sql and f"{table}.company = '915'" in sql
