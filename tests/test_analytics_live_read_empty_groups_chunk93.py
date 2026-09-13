"""Chunk 93: the live read path drops groups that contributed nothing to the measure.

Seen on the server on 13 Sep 2026: consumption broken down by warehouse (an ad-hoc group-by, served
live) returned a `[None]` group with sum 0 and count 0 in every hourly point and in the window totals.
The facts behind it were connectivity probes with no warehouse and no quantity: the live fold emits a
bucket for every group it SAW, while the write path has always filtered such buckets with `_is_empty`
before storing them. A chart drew a zero series for a group that has no data, which is the exact
"no data reads as zero" failure this station's rules exist to prevent.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk93"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, LogTransaction)


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
        db.add(Customer(customer_code=CC, name="live read probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def test_a_group_that_contributed_nothing_is_absent_from_live_points_and_totals():
    """One real pick in BRI and one connectivity probe with no warehouse and no quantity. The probe
    is a fact - capture keeps them - but it contributes nothing to the quantity measure, so the
    warehouse breakdown must not show a `[None]` group at zero."""
    async with async_session() as db:
        did = (await db.execute(select(AnalyticsMetric.id).where(
            AnalyticsMetric.customer_code == CC))).scalar()
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0, ended_at=T0,
                              date=T0.date(), duration_ms=100, method="ConfirmPickLine",
                              transaction_name="Pick", transaction_type="002001",
                              status=LogTransactionStatus.success, item_number="A", user_name="EDA",
                              warehouse="BRI", attributes={QF["ConfirmPickLine"]: "10.0"}))
        db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0, ended_at=T0,
                              date=T0.date(), duration_ms=5, method="CheckServer", transaction_name=None,
                              transaction_type=None, status=LogTransactionStatus.success,
                              item_number=None, user_name=None, warehouse=None, attributes={}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)

    async with async_session() as db:
        facts = (await db.execute(select(AnalyticsFact.warehouse).where(
            AnalyticsFact.customer_code == CC))).scalars().all()
        assert set(facts) == {None, "BRI"}, "both facts exist; only one carries units"
        did, definition = next((i, dfn) for i, dfn in await registry.active_definitions(db, CC)
                               if dfn.name == d.CONSUMPTION.name)
        state = await db.scalar(select(AnalyticsTenantState).where(AnalyticsTenantState.customer_code == CC))
        out = await n6.series(db, CC, did, definition, measure="quantity",
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE), group_by=("warehouse",),
                              ad_hoc=True, watermark=state.analytics_watermark)
    groups = {tuple(p["dimensions"]) for p in out["points"]}
    assert groups == {("BRI",)}, out["points"]
    assert all(p["roles"].get("count_value") for p in out["points"])
    assert [t["dimensions"] for t in out["totals"]] == [["BRI"]]
    assert out["total"] == {"sum_value": "10.000000", "count_value": 1}
