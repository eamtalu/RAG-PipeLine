"""Chunk 86, part 2 of the metric builder: `rollups_from`, the instant a metric's history starts.

The decision (2026-09-12): a new metric starts folding from the moment it is activated, or from an
earlier instant the person chooses. Earlier means a bounded backfill (part 3), never "all of history
by default". So every definition carries an optional lower bound, and three things respect it:

    the row mapping     `to_row` / `from_row` carry it, NULL means unbounded (every existing metric)
    the fold            `rollups.recompute` and `recompute_records` never read a fact before it, so a
                        dirty bucket entirely before the bound recomputes to nothing and is deleted
    the read            `read.series` clamps the requested window to the bound and says so, instead of
                        falling back to a live scan of facts the metric was never meant to cover

Facts are planted directly: this chunk is about the readers, not about how facts come to exist.
"""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.customer import Customer
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk86"
T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)      # the bound
BEFORE = T0 - timedelta(hours=3)
AFTER = T0 + timedelta(hours=3)


def _definition(rollups_from=None, *, source="transaction", field="quantity", dims=("method",)):
    return d.MetricDefinition(
        name="bounded", dimensions=tuple(dims),
        measures=(d.Measure("quantity", d.Aggregation.sum, field=field),),
        grains=("hourly", "daily", "monthly"),
        method_filter=("ConfirmPickLine",) if source == "transaction" else (),
        source=source, rollups_from=rollups_from)


# =============================================================== 1. the row mapping

def test_the_row_mapping_carries_rollups_from_both_ways():
    original = _definition(rollups_from=T0)
    row = registry.to_row(original, customer_code=CC)
    assert row["rollups_from"] == T0

    class _Row:
        def __init__(self, r):
            self.name, self.dimensions, self.measures = r["name"], r["dimensions"], r["measures"]
            self.grains, self.filter, self.status = r["grains"], r["filter"], r["status"]
            self.source, self.rollups_from = r["source"], r["rollups_from"]

    assert registry.from_row(_Row(row)) == original


def test_a_row_without_the_column_reads_as_unbounded():
    """Fakes and pre-migration rows have no attribute at all. That must mean "no bound", exactly
    as the three metrics that exist today behave."""
    class _Bare:
        name, dimensions, measures, grains = "x", [], [], []
        filter, status = {}, "active"
    assert registry.from_row(_Bare()).rollups_from is None


# =============================================================== fixtures for the DB half

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsRecordFact, AnalyticsMetric)


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
        db.add(Customer(customer_code=CC, name="bound probe", timezone="UTC"))
        await db.commit()
    yield
    await _wipe()


def _fact(at, qty):
    return AnalyticsFact(
        customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=at,
        source_version_hash=uuid.uuid4().hex, revision=1, event_time=at, business_date=at.date(),
        duration_ms=100, method="ConfirmPickLine", transaction_name="Pick", status="success",
        quantity=Decimal(qty), quantity_classification="pick", warehouse="BRI", attributes={})


def _record(at, stqt):
    return AnalyticsRecordFact(
        customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=at, record_index=0,
        event_time=at, business_date=at.date(), method="ConfirmPickLine", transaction_name="Pick",
        mi_program="MMS060MI", mi_transaction="LstBalID", attributes={"rec.STQT": stqt, "rec.ITNO": "1"})


async def _plant_facts():
    async with async_session() as db:
        db.add(_fact(BEFORE, "5"))
        db.add(_fact(AFTER, "7"))
        db.add(_record(BEFORE, "50"))
        db.add(_record(AFTER, "70"))
        await db.commit()


async def _hourly(definition_id):
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsHourlyRollup).where(
            AnalyticsHourlyRollup.customer_code == CC,
            AnalyticsHourlyRollup.definition_id == definition_id))).scalars().all()
        return {r.bucket_start: Decimal(r.sum_value) for r in rows}


# =============================================================== 2. the fold bound

async def test_the_fold_never_reads_a_fact_before_the_bound():
    await _plant_facts()
    did = uuid.uuid4()
    dirty_hours = {n5.hour_of(BEFORE), n5.hour_of(AFTER)}
    dirty_dates = {BEFORE.date(), AFTER.date()}
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(rollups_from=T0),
                           hours=dirty_hours, dates=dirty_dates)
        await db.commit()
    hourly = await _hourly(did)
    assert hourly == {n5.hour_of(AFTER): Decimal(7)}, "the hour before the bound must hold nothing"


async def test_an_unbounded_definition_still_folds_everything():
    await _plant_facts()
    did = uuid.uuid4()
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(rollups_from=None),
                           hours={n5.hour_of(BEFORE), n5.hour_of(AFTER)},
                           dates={BEFORE.date(), AFTER.date()})
        await db.commit()
    assert await _hourly(did) == {n5.hour_of(BEFORE): Decimal(5), n5.hour_of(AFTER): Decimal(7)}


async def test_the_bound_is_inclusive_at_the_instant_itself():
    async with async_session() as db:
        db.add(_fact(T0, "3"))
        await db.commit()
    did = uuid.uuid4()
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(rollups_from=T0),
                           hours={n5.hour_of(T0)}, dates={T0.date()})
        await db.commit()
    assert await _hourly(did) == {n5.hour_of(T0): Decimal(3)}


async def test_the_record_grain_respects_the_same_bound():
    await _plant_facts()
    did = uuid.uuid4()
    async with async_session() as db:
        await n5.recompute_records(db, CC, did,
                                   _definition(rollups_from=T0, source="record",
                                               field="attr:rec.STQT", dims=("attr:rec.ITNO",)),
                                   hours={n5.hour_of(BEFORE), n5.hour_of(AFTER)},
                                   dates={BEFORE.date(), AFTER.date()})
        await db.commit()
    assert await _hourly(did) == {n5.hour_of(AFTER): Decimal(70)}


async def test_a_bucket_that_falls_before_the_bound_is_deleted_not_kept():
    """A definition folded unbounded, then bounded: the earlier bucket's rows must go, because the
    delete in `_replace` covers every dirty bucket and the recompute now yields nothing for it."""
    await _plant_facts()
    did = uuid.uuid4()
    hours = {n5.hour_of(BEFORE), n5.hour_of(AFTER)}
    dates = {BEFORE.date(), AFTER.date()}
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(None), hours=hours, dates=dates)
        await db.commit()
    assert len(await _hourly(did)) == 2
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(rollups_from=T0), hours=hours, dates=dates)
        await db.commit()
    assert await _hourly(did) == {n5.hour_of(AFTER): Decimal(7)}


# =============================================================== 3. the read bound

async def _series(definition, window, *, watermark):
    async with async_session() as db:
        return await n6.series(db, CC, uuid.uuid4(), definition, window=window,
                               measure="quantity", group_by=(), watermark=watermark)


async def test_a_read_that_straddles_the_bound_returns_only_the_bounded_part():
    await _plant_facts()
    out = await _series(_definition(rollups_from=T0),
                        UtcWindow(start=BEFORE - timedelta(hours=1), end=AFTER + timedelta(hours=1)),
                        watermark=None)                       # no watermark: everything is read live
    total = sum(Decimal(p["roles"]["sum_value"]) for p in out["points"])
    assert total == Decimal(7), "the fact before the bound is not this metric's"
    assert out["rollups_from"] == T0.isoformat()
    assert "rollups_from" in (out["reason"] or "")


async def test_a_read_entirely_before_the_bound_is_empty_and_says_why():
    await _plant_facts()
    out = await _series(_definition(rollups_from=T0),
                        UtcWindow(start=BEFORE - timedelta(hours=2), end=BEFORE + timedelta(hours=1)),
                        watermark=None)
    assert out["points"] == []
    assert out["from_rollups"] is False and out["live_spans"] == []
    assert "rollups_from" in out["reason"]


async def test_an_unbounded_read_is_unchanged():
    await _plant_facts()
    out = await _series(_definition(None),
                        UtcWindow(start=BEFORE - timedelta(hours=1), end=AFTER + timedelta(hours=1)),
                        watermark=None)
    assert sum(Decimal(p["roles"]["sum_value"]) for p in out["points"]) == Decimal(12)
    assert out["rollups_from"] is None
