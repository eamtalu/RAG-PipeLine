"""Chunk 71: a tracked rebuild announces its outcome through the notification pipeline.

The Maintenance screen polls a run while the page is open, but a rebuild takes minutes to an hour
and nobody owes the browser tab their attention. The run's terminal state therefore also publishes an
ordinary notification EVENT - same outbox, same dedupe, same channels, same tenant gate and pacing as
every alert this system sends. Completed is info; failed is error, because a failed rebuild means the
repair someone deliberately started did not happen.

Publishing is enqueue-only (the outbox drain owns actual sending), which is what makes it safe from
the rebuild's subprocess: two committed rows, no HTTP. The dedup key is `rebuild:{run_id}:{status}`,
so re-recording an outcome can never double-announce, while a rerun of the same range (a new run id)
announces on its own."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun, LogRegroupRunStatus
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.notification import NotificationEvent as EventRow
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "test_chunk71"
T0 = datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        await db.execute(delete(EventRow).where(EventRow.customer_code == CC))
        for model in (AnalyticsPendingWindow, LogRegroupPending, LogRegroupRun,
                      LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant_conversation():
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, offset, line in (("request", 0, 1), ("request_body", 1, 2),
                                   ("info", 2, 3), ("response", 4, 4)):
            db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id,
                            timestamp=T0 + timedelta(seconds=offset), source_file="S/x.log",
                            line_number=line, level="INFO", raw_body="x", message="x",
                            entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread="7", user_ctx="amin", fields={}))
        await db.commit()


async def _run_row(**kw) -> uuid.UUID:
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC, kind="full", **kw)
        db.add(run)
        await db.commit()
        return run.id


async def _events() -> list[EventRow]:
    async with async_session() as db:
        return list((await db.execute(select(EventRow).where(
            EventRow.customer_code == CC))).scalars().all())


async def test_a_completed_rebuild_publishes_an_info_event():
    """The tab-free notification: outcome, tenant, and the headline numbers in the payload."""
    await _plant_conversation()
    run_id = await _run_row()

    await dt.run_full_regroup_tracked(run_id)

    events = await _events()
    assert len(events) == 1
    ev = events[0]
    assert ev.dedup_key == f"rebuild:{run_id}:completed"
    assert ev.severity == "info"
    assert "rebuild" in ev.title.lower()
    assert ev.payload.get("run_id") == str(run_id)


async def test_a_failed_rebuild_publishes_an_error_event(monkeypatch):
    """A failed rebuild is a repair that did not happen - error severity, with the error text."""
    run_id = await _run_row(range_start=T0, range_end=T0 + timedelta(hours=1))

    async def _boom(customer_code, lo, hi):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(dt, "ranged_regroup", _boom)
    await dt.run_full_regroup_tracked(run_id)

    async with async_session() as db:
        run = (await db.execute(select(LogRegroupRun).where(
            LogRegroupRun.id == run_id))).scalar_one()
    assert run.status == LogRegroupRunStatus.failed
    events = await _events()
    assert len(events) == 1
    assert events[0].severity == "error"
    assert events[0].dedup_key == f"rebuild:{run_id}:failed"


async def test_the_outcome_is_announced_exactly_once():
    """The dedup key carries the run id and status: re-recording an outcome (a retried recorder, a
    watcher racing the subprocess) can never double-announce."""
    await _plant_conversation()
    run_id = await _run_row()

    await dt.run_full_regroup_tracked(run_id)
    await dt.run_full_regroup_tracked(run_id)  # idempotent re-record

    assert len(await _events()) == 1
