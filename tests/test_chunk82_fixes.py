"""Chunk 82: three live-found fixes, one file.

1. **The daily live-edge seam.** `plan_read`'s daily branch says "daily buckets are keyed on the
   tenant-LOCAL business_date" and then computes UTC dates. The settled/live boundary therefore
   sits at UTC midnight while the buckets turn over at LOCAL midnight, so the offset hour at the
   live edge (23:00-00:00 UTC for London) is served by NEITHER tier. Found live via R4b's exact
   SQL cross-check: last night's midnight balance snapshot (18,673 units) vanished from the chart.
   Pre-existing for every non-UTC tenant, on both grains' daily reads, healing silently at each
   day's settling - which is why nothing ever caught it.
2. **Request-less parked streams poison the head lane's response FIFO.** A parked conversation
   with no request line is inheritance context, not open work - but seeded as open work it is
   usually the user's OLDEST candidate, so it steals every new response for that user (34 DIVERGED
   in 24h, all this shape). Never guess: the plan falls back by name.
3. **`record_facts_total` drift.** The counter added inserts and subtracted reverse-deletes, but
   not the rows the replace-per-transaction path deletes - so every re-expansion counted its rows
   twice. Verified live: counter 1,730,110 vs 1,641,626 actual.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_stitch_checkpoint import LogStitchCheckpoint
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import read as n6
from app.services.mnp_log_ingestion.pipeline import head_lane
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk82"
T0 = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

RECORDS = [{"STQT": "624", "ITNO": "101978"}, {"STQT": "12", "ITNO": "101978"}]


# ===================================================== 1. the daily live-edge seam (pure planner)

def test_the_daily_live_edge_starts_at_the_local_day_floor():
    """The exact live shape: watermark mid-local-day, London tenant. The live span must start where
    the first UNSETTLED LOCAL day starts (23:00 UTC the night before), or the offset hour belongs
    to neither tier."""
    window = UtcWindow(start=datetime(2026, 8, 4, tzinfo=timezone.utc),
                       end=datetime(2026, 8, 30, tzinfo=timezone.utc))
    watermark = datetime(2026, 8, 29, 11, 4, tzinfo=timezone.utc)
    plan = n6.plan_read(window, "daily", watermark=watermark, tz="Europe/London")
    assert plan.rollup_dates is not None
    assert plan.rollup_dates[1].isoformat() == "2026-08-28"
    assert plan.live_windows, "the unsettled tail must be read live"
    live_start = plan.live_windows[-1][0]
    assert live_start == datetime(2026, 8, 28, 23, 0, tzinfo=timezone.utc), \
        f"live span starts at {live_start}, leaving the local offset hour unserved"


def test_the_first_rollup_day_is_a_whole_local_day():
    """A local day that starts before the requested window is not whole inside it; serving its
    rollup would report units from before the request. It goes to the live tier instead."""
    window = UtcWindow(start=datetime(2026, 8, 4, tzinfo=timezone.utc),
                       end=datetime(2026, 8, 30, tzinfo=timezone.utc))
    plan = n6.plan_read(window, "daily", watermark=datetime(2026, 8, 29, 11, tzinfo=timezone.utc),
                        tz="Europe/London")
    # local Aug-4 begins 2026-08-03T23:00Z, before the window: not whole
    assert plan.rollup_dates[0].isoformat() == "2026-08-05"
    assert plan.live_windows[0] == (datetime(2026, 8, 4, tzinfo=timezone.utc),
                                    datetime(2026, 8, 4, 23, 0, tzinfo=timezone.utc))


def test_a_utc_tenant_keeps_the_old_boundaries_exactly():
    window = UtcWindow(start=datetime(2026, 8, 4, tzinfo=timezone.utc),
                       end=datetime(2026, 8, 30, tzinfo=timezone.utc))
    watermark = datetime(2026, 8, 29, 11, 4, tzinfo=timezone.utc)
    for tz in (None, "UTC"):
        plan = n6.plan_read(window, "daily", watermark=watermark, tz=tz)
        assert plan.rollup_dates == (datetime(2026, 8, 4).date(), datetime(2026, 8, 28).date())
        assert plan.live_windows[-1][0] == datetime(2026, 8, 29, tzinfo=timezone.utc)


# ===================================================== 2. request-less parked streams (head lane)

async def _wipe_stage2():
    async with async_session() as db:
        for model in (LogStitchCheckpoint, LogOpenStream, LogPendingRequest,
                      LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


async def test_a_request_less_parked_stream_makes_the_plan_fall_back():
    """A parked conversation with no request line must not sit in the seeded response FIFO as the
    user's oldest open work - live, it stole every new response for its user (34 DIVERGED/24h).
    Never guess: fall back by name."""
    await _wipe_stage2()
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        # work lines only - no request: the shape that parks with has_request=False
        for i, kind in enumerate(("info", "mi_call", "mi_result")):
            db.add(LogEntry(customer_code=CC, job_id=job.id,
                            timestamp=T0 + timedelta(seconds=i), line_number=i + 1,
                            raw_body="x", entry_hash=uuid.uuid4().hex, source_file="S/x.log",
                            level="INFO", entry_type=LogEntryType(kind), thread="7",
                            user_ctx="amin", fields={}))
        await db.commit()
    async with async_session() as db:
        await dt.regroup_window(db, CC, T0, T0 + timedelta(seconds=5))
    await head_lane.advance_checkpoint(CC, T0 + timedelta(seconds=5))
    async with async_session() as db:
        parked = (await db.execute(select(LogOpenStream).where(
            LogOpenStream.customer_code == CC))).scalars().all()
    assert parked and not parked[0].has_request, "fixture must park a request-less stream"

    lo2 = T0 + timedelta(seconds=60)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t2.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t2.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogEntry(customer_code=CC, job_id=job.id, timestamp=lo2 + timedelta(seconds=1),
                        line_number=1, raw_body="r", entry_hash=uuid.uuid4().hex,
                        source_file="S/x.log", level="INFO",
                        entry_type=LogEntryType("response"), thread="9", user_ctx="amin",
                        fields={}))
        await db.commit()

    plan = await head_lane.build_plan(CC, lo2, lo2 + timedelta(seconds=5))
    assert not plan.ok and plan.fallback == "parked_requestless"
    await _wipe_stage2()


# ===================================================== 3. the record counter

async def _wipe_analytics():
    from app.persistence.models.consumer_cursor import ConsumerCursor
    async with async_session() as db:
        # consume_tenant publishes the GLOBAL retention cursor; left behind, it breaks the
        # empty-registry expectation of the consumer-cursor suite that sorts after this file.
        await db.execute(delete(ConsumerCursor).where(
            ConsumerCursor.consumer == "analytics:warehouse-v1"))
        for model in (AnalyticsRecordFact, AnalyticsFact, AnalyticsFactLedger,
                      AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup,
                      AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
                      AnalyticsFieldRegistry, AnalyticsTransactionRegistry,
                      LogEntryAssignment, LogEntry, LogTransaction):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


async def test_the_record_counter_survives_a_reexpansion():
    """Replace-per-transaction deletes rows before re-inserting; the counter must subtract them or
    every re-expansion double-counts (live drift: 1,730,110 counted vs 1,641,626 actual)."""
    await _wipe_analytics()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="c82", timezone="Europe/London"))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick",
                                            expand=True))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        txn = LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0,
                             ended_at=T0 + timedelta(seconds=2), date=T0.date(), duration_ms=100,
                             method="MMS060MI", transaction_name="Pick",
                             status=LogTransactionStatus.success,
                             row_fingerprint="fp-1", attributes={})
        db.add(txn)
        await db.flush()
        entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=T0 + timedelta(seconds=1),
                         line_number=1, raw_body="mi", entry_hash=uuid.uuid4().hex,
                         source_file="S/x.log", level="INFO",
                         entry_type=LogEntryType("mi_result"),
                         fields={"result": "OK", "program": "MMS060MI",
                                 "transaction": "LstBalID", "records": RECORDS})
        db.add(entry)
        await db.flush()
        db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id, entry_ts=entry.timestamp,
                                  transaction_id=txn.id, seq=0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                      range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)

    # a field approval changes _exp_v -> the next ticket re-expands (replace-per-transaction)
    async with async_session() as db:
        await db.execute(update(AnalyticsFieldRegistry)
                         .where(AnalyticsFieldRegistry.customer_code == CC,
                                AnalyticsFieldRegistry.field == "rec.STQT")
                         .values(captured=True))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                      range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)

    async with async_session() as db:
        actual = await db.scalar(select(
            __import__("sqlalchemy").func.count()).select_from(AnalyticsRecordFact)
            .where(AnalyticsRecordFact.customer_code == CC))
        state = (await db.execute(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))).scalar_one()
    assert actual == 2
    assert state.record_facts_total == actual, \
        f"counter {state.record_facts_total} drifted from actual {actual}"
    await _wipe_analytics()
