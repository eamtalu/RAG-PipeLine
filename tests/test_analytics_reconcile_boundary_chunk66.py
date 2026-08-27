"""Chunk 66 (section 18q addendum): the reconciler must only compare buckets its window covers WHOLE.

Found during the 2026-08-27 post-deploy verification, present since the reconcile worker was first
enabled on 2026-08-25: every hourly pass reported 127-463 `rollups_vs_facts` findings for the live
tenant, oscillating with traffic and never converging - the signature of an artifact, not corruption.

The mechanism: `rollups_vs_facts` re-folds the facts INSIDE the rolling window and compares against
stored rollup rows, but a bucket the window only PARTIALLY covers disagrees by construction - the
stored row was folded from the whole bucket, the recount only from the slice. All three finding kinds
were this artifact wearing different clothes: "differs" (bucket straddles a window edge), "missing"
(the stored fetch clipped the bucket out while facts remained), "orphaned" (the bucket's facts all sit
in the clipped-off part). The worker's own docstring names the failure: a check that is always red is
a check nobody reads.

The fix is coverage, not geometry: compare a bucket only when its FULL range lies inside the window -
UTC hours for the hourly grain, the tenant-LOCAL day for the daily grain (`business_date` is local, so
its UTC range needs the tenant timezone). And because a 24 h rolling window can never fully contain a
local day, the default window widens to 48 h so the daily grain is actually audited.

Fixtures follow chunk 49 (committed rows, per-file tenant, real consume_tenant fold)."""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, text

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import reconcile as rc
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
from app.settings import settings

CC = "test_chunk66"
T0 = datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsMetric, LogEntryAssignment, LogTransaction, LogEntry)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant_and_fold(instants):
    """Committed transactions at the given instants, folded into facts and rollups for real."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for at in instants:
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=100, method="ConfirmPickLine",
                transaction_name="Pick", transaction_type="002001",
                status=LogTransactionStatus.success, item_number="101978", user_name="EDA",
                warehouse="BRI", attributes={"QuantityPicked": "10.0"}))
        lo = min(instants) - timedelta(hours=1)
        hi = max(instants) + timedelta(hours=1)
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=lo, range_end=hi))
        await db.commit()
    await n3.consume_tenant(CC)


async def _findings(window: UtcWindow):
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=window)
    return [f for f in report["findings"] if f.check == "rollups_vs_facts"]


# ==================================================== the artifact, both grains

async def test_a_partially_covered_hourly_bucket_is_not_compared():
    """Two facts in the 14:00 hour; the window starts at 14:30, so the recount sees one of them while
    the stored row was folded from both. That is not drift - the window just cannot see the whole
    bucket - and reporting it made the live auditor cry wolf a few hundred times an hour."""
    await _plant_and_fold([T0 - timedelta(minutes=20), T0 + timedelta(minutes=10)])

    findings = await _findings(UtcWindow(start=T0, end=T0 + timedelta(hours=6)))
    hourly = [f for f in findings if f.detail.get("grain") == "hourly"]
    assert hourly == [], [f.detail for f in hourly]


async def test_a_partially_covered_daily_bucket_is_not_compared():
    """Same artifact one grain up: the window covers the afternoon only, the stored daily row was
    folded from the whole (tenant-local) day. A clipped day is not comparable."""
    await _plant_and_fold([T0 - timedelta(hours=4), T0])

    findings = await _findings(UtcWindow(start=T0 - timedelta(hours=2), end=T0 + timedelta(hours=6)))
    daily = [f for f in findings if f.detail.get("grain") == "daily"]
    assert daily == [], [f.detail for f in daily]


# ==================================================== the check must not go blind

async def test_a_fully_covered_bucket_with_real_drift_still_reports():
    """The other direction, so the fix cannot be 'compare nothing': a genuinely stale bucket whose
    hour lies entirely inside the window is exactly the bug this check exists for and must still be
    reported."""
    await _plant_and_fold([T0])
    async with async_session() as db:
        await db.execute(text("""
            UPDATE analytics_hourly_rollups SET sum_value = 999
            WHERE customer_code = :c AND measure_name = 'quantity'"""), {"c": CC})
        await db.commit()

    findings = await _findings(UtcWindow(start=T0 - timedelta(hours=6), end=T0 + timedelta(hours=6)))
    hourly = [f for f in findings if f.detail.get("grain") == "hourly"
              and f.detail.get("kind") == "differs"]
    assert hourly, "a real drift in a fully covered bucket must survive the boundary fix"
    assert Decimal(str(hourly[0].detail["stored"])) == Decimal(999)


async def test_an_orphaned_rollup_in_a_covered_bucket_still_reports():
    """N5's 'recompute to nothing means DELETE' regression shape: facts gone, rollup row surviving,
    bucket fully inside the window - must still be visible."""
    await _plant_and_fold([T0])
    async with async_session() as db:
        await db.execute(delete(AnalyticsFact).where(AnalyticsFact.customer_code == CC))
        await db.commit()

    findings = await _findings(UtcWindow(start=T0 - timedelta(hours=6), end=T0 + timedelta(hours=6)))
    assert any(f.detail.get("kind") == "orphaned" for f in findings)


# ==================================================== the window must fit a whole local day

def test_the_default_window_can_cover_a_full_local_day():
    """A 24 h rolling window can never fully contain a tenant-local day, so with the coverage rule the
    daily grain would silently never be audited. 48 h guarantees at least one whole local day inside
    every pass."""
    assert settings.analytics_reconcile_window_hours >= 48
