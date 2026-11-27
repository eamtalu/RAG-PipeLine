"""Chunk 234: a site-limited person reads the rollups and record facts of their own places.

Step 1d stage 4, last part. Rollup rows and record facts carry their company and warehouse
since chunk 232 and the stored rollups are refolded (chunk 233), so the four tables no longer
have to be closed to everyone who may not see every site.
"""
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.services import scoped_data
from app.services.analytics import consume as n3
from app.services.analytics import read as n6
from app.services.scoped_data import DataScope, Place
from tests import test_rollup_place_chunk232 as base

CC = base.CC
TABLES = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsRecordFact)
BRI = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
LON = DataScope(CC, site_ids=("LON",), places=(Place("915", "LON"),))
ALL = DataScope(CC, all_sites=True)


@pytest.fixture(autouse=True)
async def clean():
    await base._wipe()
    async with async_session() as db:
        db.add(base.Customer(customer_code=CC, name="rollup scope probe", timezone="Europe/London"))
        await db.commit()
    yield
    await base._wipe()


@pytest.mark.parametrize("model", TABLES)
def test_the_four_tables_are_limited_by_the_company_and_warehouse_pair(model):
    table = model.__tablename__
    rule = scoped_data.registry.BY_TABLE[table]
    sql = str(rule.condition(BRI).compile(compile_kwargs={"literal_binds": True}))
    assert f"{table}.warehouse = 'BRI'" in sql and f"{table}.company = '915'" in sql


async def _quantity(scope: DataScope, model, definition_id) -> Decimal:
    """The stored quantity of one metric that this scope can read at one grain."""
    async with scoped_data.open_session(async_session, scope) as db:
        total = await db.scalar(select(func.sum(model.sum_value)).where(
            model.definition_id == definition_id, model.measure_name == "quantity"))
    return Decimal(total or 0)


async def test_each_person_reads_the_figures_of_their_own_places_at_every_grain():
    definition_id = await base._register()
    await base._plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "LON"),
                      (15, "2.0", "920", "BRI"), (20, "3.0", None, None))
    await n3.consume_tenant(CC)
    for model in TABLES[:3]:
        assert await _quantity(BRI, model, definition_id) == Decimal("10"), model.__tablename__
        assert await _quantity(LON, model, definition_id) == Decimal("7")
        assert await _quantity(ALL, model, definition_id) == Decimal("22")


async def test_the_reader_gives_a_site_limited_person_the_total_of_their_site():
    definition_id = await base._register()
    await base._plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "LON"))
    await n3.consume_tenant(CC)
    totals = {}
    for name, scope in (("BRI", BRI), ("LON", LON), ("all", ALL)):
        async with scoped_data.open_session(async_session, scope) as db:
            out = await n6.series(db, CC, definition_id, base.DEFINITION, window=base.WINDOW,
                                  measure="quantity", group_by=("method",),
                                  watermark=base.T0 + base.WIDE, tz="Europe/London")
            assert scoped_data.closed_tables(scoped_data.tables_read(db), scope) == []
        totals[name] = Decimal(out["total"]["sum_value"]) if out["total"] else Decimal(0)
    assert totals == {"BRI": Decimal("10"), "LON": Decimal("7"), "all": Decimal("17")}
