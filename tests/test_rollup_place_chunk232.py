"""Chunk 232: a rollup row says which company and warehouse it belongs to (step 54; 1d stage 4).

A rollup row was one (definition, measure, bucket, dimensions), mixing every site of the
customer, because the live metrics group by method, transaction name or item and not by
warehouse. So the pre-computed figures were closed to everyone who may not see every site. Each
row now carries its place as real columns, the fold writes one row per place, and the reader,
which already merges the rows of a bucket, returns the sum over the places it is allowed to see.

Pinned here
-----------
    fold       two places in one bucket give one row each per measure, at every grain, and the
               rows add up to the bucket's total
    no place   calls with no warehouse fold into rows with an empty place
    replace    a recompute replaces all rows of a bucket, whatever their place
    reader     an unlimited read returns the same total as before the split
    auditor    rollups still equal facts
    records    a record fact carries the place of its parent fact
"""
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup, RollupColumns)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import reconcile as rc
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk232"
#: 10 Sep 2026 09:00 UTC = 10:00 BST; every call below is inside this one London hour.
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
DAY = date(2026, 9, 10)
WIDE = timedelta(hours=6)
WINDOW = UtcWindow(start=T0 - WIDE, end=T0 + WIDE)
GRAINS = ((AnalyticsHourlyRollup, "bucket_start"), (AnalyticsDailyRollup, "business_date"),
          (AnalyticsMonthlyRollup, "month_start"))
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsRecordFact,
          AnalyticsFact, AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsMetric, AnalyticsTransactionRegistry, LogTransaction)
DEFINITION = d.MetricDefinition(
    name="picks", dimensions=("method",),
    measures=(d.Measure(name="quantity", aggregation=d.Aggregation.sum, field="quantity"),),
    grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",),
    status=d.Status.active)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="rollup place probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _register() -> uuid.UUID:
    async with async_session() as db:
        row = AnalyticsMetric(**registry.to_row(DEFINITION, customer_code=CC, created_by="test"))
        db.add(row)
        await db.commit()
        return row.id


async def _ticket(refold: bool = False) -> None:
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE,
                                      refold_rollups=refold))
        await db.commit()


async def _plant(*specs) -> list[uuid.UUID]:
    """`specs` are (minutes after T0, quantity, company, warehouse): one sealed pick each."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        rows = []
        for minutes, quantity, company, warehouse in specs:
            at = T0 + timedelta(minutes=minutes)
            rows.append(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success,
                item_number="101978", user_name="EDA", company=company, warehouse=warehouse,
                attributes={"QuantityPicked": quantity}))
        db.add_all(rows)
        await db.commit()
    await _ticket()
    return [r.id for r in rows]


async def _places(model) -> dict[tuple[str | None, str | None], Decimal]:
    """The stored quantity per place at one grain: {(company, warehouse): sum}."""
    async with async_session() as db:
        rows = (await db.execute(select(model).where(
            model.customer_code == CC, model.measure_name == "quantity"))).scalars().all()
    return {(r.company, r.warehouse): Decimal(r.sum_value) for r in rows}


# ==================================================== 1. the columns and the fold

def test_the_place_is_two_real_columns_on_every_grain():
    assert {"company", "warehouse"} <= set(vars(RollupColumns))
    for model, _bucket in GRAINS:
        key = next(c for c in model.__table__.constraints if c.name and c.name.startswith("uq_"))
        assert {"company", "warehouse"} <= {col.name for col in key.columns}


async def test_two_places_in_one_bucket_give_one_row_each_at_every_grain():
    await _register()
    await _plant((0, "10.0", "915", "BRI"), (5, "4.0", "915", "BRI"), (10, "7.0", "915", "LON"),
                 (15, "2.0", "920", "BRI"))
    await n3.consume_tenant(CC)
    expected = {("915", "BRI"): Decimal("14"), ("915", "LON"): Decimal("7"),
                ("920", "BRI"): Decimal("2")}
    for model, _bucket in GRAINS:
        assert await _places(model) == expected, model.__tablename__


async def test_calls_with_no_warehouse_fold_into_rows_with_an_empty_place():
    await _register()
    await _plant((0, "10.0", "915", "BRI"), (5, "3.0", None, None))
    await n3.consume_tenant(CC)
    for model, _bucket in GRAINS:
        assert await _places(model) == {("915", "BRI"): Decimal("10"), (None, None): Decimal("3")}


async def test_a_recompute_replaces_every_row_of_the_bucket_whatever_its_place():
    await _register()
    (moved,) = await _plant((0, "10.0", "915", "BRI"))
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(update(AnalyticsFact).where(AnalyticsFact.customer_code == CC)
                         .values(warehouse="LON"))
        await db.commit()
    await _ticket(refold=True)
    await n3.consume_tenant(CC)
    for model, _bucket in GRAINS:
        assert await _places(model) == {("915", "LON"): Decimal("10")}, model.__tablename__
    assert moved is not None


# ==================================================== 2. the reader and the auditor

async def test_an_unlimited_read_returns_the_total_over_all_places():
    definition_id = await _register()
    await _plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "LON"), (15, "2.0", None, None))
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await n6.series(db, CC, definition_id, DEFINITION, window=WINDOW, measure="quantity",
                              group_by=("method",), watermark=T0 + WIDE, tz="Europe/London")
    assert out["from_rollups"] is True
    assert Decimal(out["total"]["sum_value"]) == Decimal("19")
    assert len(out["points"]) == 1  # one hour, one method: the places are merged, not listed


async def test_the_auditor_finds_rollups_equal_to_facts_with_several_places():
    await _register()
    await _plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "LON"), (15, "2.0", None, None))
    await n3.consume_tenant(CC)
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert [f for f in report["findings"] if f.check == "rollups_vs_facts"] == []


async def test_the_auditor_sees_a_place_whose_row_is_missing():
    await _register()
    await _plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "LON"))
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(delete(AnalyticsHourlyRollup).where(
            AnalyticsHourlyRollup.customer_code == CC, AnalyticsHourlyRollup.warehouse == "LON",
            AnalyticsHourlyRollup.measure_name == "quantity"))
        await db.commit()
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    found = [f for f in report["findings"] if f.check == "rollups_vs_facts"]
    assert any("hourly" in f.summary and "missing" in f.summary for f in found)


# ==================================================== 3. the pure fold

def test_the_fold_keys_by_place_only_when_asked():
    facts = [{"event_time": T0, "business_date": DAY, "method": "ConfirmPickLine",
              "quantity": Decimal("1"), "quantity_classification": "pick", "status": "success",
              "company": "915", "warehouse": "BRI"}]
    plain = n5.group_fold(facts, DEFINITION, lambda r: r["business_date"])
    placed = n5.group_fold(facts, DEFINITION, lambda r: r["business_date"], with_place=True)
    assert list(plain) == [(DAY, ("ConfirmPickLine", None, None, None, None, None))]
    assert list(placed) == [(DAY, ("ConfirmPickLine", None, None, None, None, None, "915", "BRI"))]


# ==================================================== 4. record facts

def test_a_record_fact_carries_the_place_of_its_parent_fact():
    """Pinned on the writer's source, as the record grain's other rules are (chunk 61): a record
    is written from its parent fact, and the place must be copied in that same row."""
    import inspect

    columns = {c.name for c in AnalyticsRecordFact.__table__.columns}
    assert {"company", "warehouse"} <= columns
    source = inspect.getsource(n3._expand_records)
    assert '"company": fact.get("company")' in source
    assert '"warehouse": fact.get("warehouse")' in source
