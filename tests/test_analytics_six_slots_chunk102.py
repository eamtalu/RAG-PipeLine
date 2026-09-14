"""Chunk 102: six rollup dimension slots, because the binding constraint is cardinality, not count.

Measured over 9,569 live facts at day grain. "Rows per fact" is how much the pre-aggregation stores
for each fact it summarises, so smaller is better and 1.0 means it saved nothing:

| dimensions                                       | rows per fact | saving |
|--------------------------------------------------|---------------|--------|
| warehouse, user                                   | 0.004         | 245x   |
| warehouse, user, transaction, status              | 0.015         |  68x   |
| the same plus destination and device (SIX)        | 0.032         |  31x   |
| warehouse, user, item                             | 0.147         | 6.8x   |
| warehouse, user, item, lot                        | 0.308         | 3.2x   |

Six LOW-cardinality dimensions still compress thirty-one fold, so the old cap of four was the only
thing preventing a useful wide, shallow cube. It buys nothing for the high-cardinality case, which is
why six rather than more: item and lot stop compressing whatever the cap is, and those questions
belong on the fact table.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_rollup import (DIMENSION_SLOTS, AnalyticsDailyRollup,
                                                     AnalyticsHourlyRollup, AnalyticsMonthlyRollup)
from app.persistence.models.customer import Customer
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk102"
T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
SIX = ("warehouse_id", "user_name", "transaction_name", "status", "to_location", "device_name")
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsMetric)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="slot probe", timezone="UTC"))
        await db.commit()
    yield
    await _wipe()


def _definition(dimensions=SIX):
    return d.MetricDefinition(
        name="wide", dimensions=tuple(dimensions),
        measures=(d.Measure("units", d.Aggregation.sum, field="quantity"),),
        grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",))


def _fact(*, user, to_location, device, qty, at=T0):
    return AnalyticsFact(
        customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=at,
        source_version_hash=uuid.uuid4().hex, revision=1, event_time=at, business_date=at.date(),
        duration_ms=100, method="ConfirmPickLine", transaction_name="Pick", status="success",
        quantity=Decimal(qty), quantity_classification="pick", warehouse="BRI", warehouse_id="1",
        user_name=user, to_location=to_location, device_name=device, attributes={})


async def _fold(definition, facts):
    async with async_session() as db:
        for fact in facts:
            db.add(fact)
        await db.commit()
    did = uuid.uuid4()
    async with async_session() as db:
        await n5.recompute(db, CC, did, definition, hours={n5.hour_of(T0)}, dates={T0.date()},
                           tz=None)
        await db.commit()
    return did


# ==================================================================== the slots exist and are used

def test_the_contract_says_six():
    assert DIMENSION_SLOTS == 6


def test_a_definition_may_now_declare_six_dimensions():
    assert d.validate(_definition(), known_attributes=frozenset()) == []


def test_a_seventh_dimension_is_refused_by_the_reader_rather_than_silently_dropped():
    with pytest.raises(ValueError, match="capped at 6"):
        n6.resolve(_definition(), group_by=SIX + ("item_number",))


async def test_a_six_dimension_fold_writes_all_six_slots():
    definition = _definition()
    did = await _fold(definition, [_fact(user="EDA", to_location="BRI01-P", device="D1", qty="10")])
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsHourlyRollup).where(
            AnalyticsHourlyRollup.customer_code == CC,
            AnalyticsHourlyRollup.definition_id == did))).scalars().one()
    assert [row.dim1, row.dim2, row.dim3, row.dim4, row.dim5, row.dim6] == \
        ["1", "EDA", "Pick", "success", "BRI01-P", "D1"]


async def test_two_rows_differing_only_in_the_sixth_slot_stay_apart():
    """The unique key has to include every slot, or the second row would collide with the first and
    recompute-and-replace would silently drop one of them."""
    definition = _definition()
    did = await _fold(definition, [
        _fact(user="EDA", to_location="BRI01-P", device="D1", qty="10"),
        _fact(user="EDA", to_location="BRI01-P", device="D2", qty="4")])
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsHourlyRollup).where(
            AnalyticsHourlyRollup.customer_code == CC,
            AnalyticsHourlyRollup.definition_id == did))).scalars().all()
    assert sorted((r.dim6, Decimal(r.sum_value)) for r in rows) == [("D1", Decimal(10)),
                                                                    ("D2", Decimal(4))]


async def test_a_narrower_read_still_rolls_the_extra_slots_up_exactly():
    """The property the whole design leans on: one stored shape answers every subset of itself, so
    six dimensions is six slices and every combination of them, not six separate metrics."""
    definition = _definition()
    did = await _fold(definition, [
        _fact(user="EDA", to_location="BRI01-P", device="D1", qty="10"),
        _fact(user="EDA", to_location="JIT", device="D2", qty="4"),
        _fact(user="JON", to_location="BRI01-P", device="D1", qty="6")])
    window = UtcWindow(start=T0.replace(minute=0), end=T0.replace(minute=0) + timedelta(hours=1))
    async with async_session() as db:
        whole = await n6.series(db, CC, did, definition, window=window, measure="units",
                                group_by=(), watermark=T0 + timedelta(hours=2))
        by_user = await n6.series(db, CC, did, definition, window=window, measure="units",
                                  group_by=("user_name",), watermark=T0 + timedelta(hours=2))
        by_device = await n6.series(db, CC, did, definition, window=window, measure="units",
                                    group_by=("device_name",), watermark=T0 + timedelta(hours=2))
    assert whole["from_rollups"] and by_user["from_rollups"] and by_device["from_rollups"]
    assert whole["total"]["sum_value"] == "20"
    assert {tuple(t["dimensions"])[0]: t["roles"]["sum_value"] for t in by_user["totals"]} == \
        {"EDA": "14", "JON": "6"}
    assert {tuple(t["dimensions"])[0]: t["roles"]["sum_value"] for t in by_device["totals"]} == \
        {"D1": "16", "D2": "4"}


async def test_the_daily_and_monthly_levels_carry_the_extra_slots_too():
    """The cascade folds hourly into daily into monthly. A level missing a slot would merge rows the
    level below kept apart, and the totals would still look plausible."""
    definition = _definition()
    did = await _fold(definition, [
        _fact(user="EDA", to_location="BRI01-P", device="D1", qty="10"),
        _fact(user="EDA", to_location="BRI01-P", device="D2", qty="4")])
    async with async_session() as db:
        for model in (AnalyticsDailyRollup, AnalyticsMonthlyRollup):
            rows = (await db.execute(select(model).where(
                model.customer_code == CC, model.definition_id == did))).scalars().all()
            assert sorted((r.dim6, Decimal(r.sum_value)) for r in rows) == \
                [("D1", Decimal(10)), ("D2", Decimal(4))], model.__tablename__
