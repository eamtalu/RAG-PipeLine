"""Chunk 75: a declined window names its reason in the journal.

During the chunk-74 shadow watch, two windows in a row produced no verdict and the journal could
not say why: the head lane's fallbacks returned a named reason to the caller but logged nothing,
so "the shadow is quiet because the tenant is idle" and "the shadow is quiet because every window
trips a guard" looked identical. The whole point of naming the fallback reasons (chunk 72) was to
make the difference observable - S4a learned the same lesson with its refusal counts.

One INFO line per declined window, from the head-lane module itself, naming tenant, window and
reason. Volume matches the existing per-window Stage 2 stats line, so it cannot flood the journal.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun
from app.persistence.models.log_stitch_checkpoint import LogStitchCheckpoint
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import head_lane

CC = "test_chunk75"
T0 = datetime(2026, 8, 20, 9, 0, tzinfo=timezone.utc)
_LOGGER = "app.services.mnp_log_ingestion.pipeline.head_lane"


async def _wipe():
    async with async_session() as db:
        for model in (LogStitchCheckpoint, LogOpenStream, LogPendingRequest,
                      AnalyticsPendingWindow, LogRegroupPending, LogRegroupRun,
                      LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def test_a_declined_window_names_its_reason_in_the_journal(caplog):
    """No checkpoint yet is the most common decline; the journal must say so by name."""
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        plan = await head_lane.build_plan(CC, T0, T0 + timedelta(seconds=5))
    assert not plan.ok and plan.fallback == "no_checkpoint"
    fell_back = [r for r in caplog.records if "fell back to the rebuild lane" in r.message]
    assert len(fell_back) == 1
    assert "no_checkpoint" in fell_back[0].message and CC in fell_back[0].message


async def test_an_eligible_window_logs_no_fallback(caplog):
    await head_lane.advance_checkpoint(CC, T0)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, offset, line in (("request", 1, 1), ("request_body", 2, 2), ("response", 3, 3)):
            db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id,
                            timestamp=T0 + timedelta(seconds=offset), source_file="S/x.log",
                            line_number=line, level="INFO", raw_body="x", message="x",
                            entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread="7", user_ctx="amin", fields={}))
        await db.commit()

    with caplog.at_level(logging.INFO, logger=_LOGGER):
        plan = await head_lane.build_plan(CC, T0, T0 + timedelta(seconds=5))
    assert plan.ok, plan.fallback
    assert not [r for r in caplog.records if "fell back" in r.message]
