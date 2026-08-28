"""Chunk 70: the ranged rebuild - `POST /logs/regroup/full?start=...&end=...`.

The full rebuild (chunk 69) is the repair hammer: the whole retained history, about an hour, the
tenant's pipelines paused throughout. Most repairs do not need the hammer - a backfill into one known
bad week needs that week re-derived and nothing else. This chunk adds the range: same endpoint, same
tracked run, same subprocess isolation and maintenance pause, but the runner walks ONLY the requested
range, in the same bounded 6-hour slices `finalize_pending` uses, each slice its own transaction under
the tenant advisory lock.

The mechanics are deliberately NOT new: a ranged rebuild is `regroup_window` - the padded, lossless,
fingerprint-skipping rebuild the live worker runs every second - just aimed by hand. What a slice
rebuilds is identical to what a ticket over that range would have rebuilt; the analytics tickets it
publishes are the ordinary site-1 ones.
"""

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
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "test_chunk70"
DAY_A = datetime(2026, 8, 18, 10, 0, tzinfo=timezone.utc)
DAY_B = datetime(2026, 8, 21, 10, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
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


async def _plant(at: datetime):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, offset, line in (("request", 0, 1), ("request_body", 1, 2),
                                   ("info", 2, 3), ("response", 4, 4)):
            db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id,
                            timestamp=at + timedelta(seconds=offset), source_file="S/x.log",
                            line_number=line, level="INFO", raw_body="x", message="x",
                            entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread="7", user_ctx="amin", fields={}))
        await db.commit()


async def test_the_run_row_carries_its_range():
    """A ranged run must survive the process boundary: the endpoint records the range on the row,
    the subprocess reads it back. NULL range = the whole history, exactly as before."""
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC, kind="full",
                            range_start=DAY_A, range_end=DAY_A + timedelta(days=1))
        db.add(run)
        await db.commit()
        stored = (await db.execute(select(LogRegroupRun).where(
            LogRegroupRun.id == run.id))).scalar_one()
    assert stored.range_start == DAY_A
    assert stored.range_end == DAY_A + timedelta(days=1)


async def test_a_ranged_run_rebuilds_only_its_range():
    """Two conversations, days apart, neither stitched. A ranged run over day A's window must stitch
    A and leave B's lines untouched - the whole point of the range is not paying for the rest."""
    await _plant(DAY_A)
    await _plant(DAY_B)
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC, kind="full",
                            range_start=DAY_A - timedelta(hours=1),
                            range_end=DAY_A + timedelta(hours=1))
        db.add(run)
        await db.commit()
        run_id = run.id

    await dt.run_full_regroup_tracked(run_id)

    async with async_session() as db:
        run = (await db.execute(select(LogRegroupRun).where(
            LogRegroupRun.id == run_id))).scalar_one()
        txns = (await db.execute(select(LogTransaction.started_at).where(
            LogTransaction.customer_code == CC))).scalars().all()
    assert run.status == LogRegroupRunStatus.completed
    assert len(txns) == 1, f"only day A's conversation must be stitched, got {txns}"
    assert abs((txns[0] - DAY_A).total_seconds()) < 60


async def test_a_ranged_run_still_publishes_analytics_tickets():
    """A slice goes through regroup_window, so the ordinary site-1 ticket covers it - the facts for
    the range restate without any extra step."""
    await _plant(DAY_A)
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC, kind="full",
                            range_start=DAY_A - timedelta(hours=1),
                            range_end=DAY_A + timedelta(hours=1))
        db.add(run)
        await db.commit()
        run_id = run.id

    await dt.run_full_regroup_tracked(run_id)

    async with async_session() as db:
        covering = (await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC,
            AnalyticsPendingWindow.range_start <= DAY_A,
            AnalyticsPendingWindow.range_end >= DAY_A))).scalars().all()
    assert covering


def test_the_endpoint_accepts_the_optional_range():
    """Same endpoint, two optional params - both or neither. The poll payload carries the range so
    the frontend can label what is being rebuilt."""
    from app.main import app
    spec = {(m.upper(), p): op
            for p, methods in app.openapi()["paths"].items()
            for m, op in methods.items()}[("POST", "/api/v1/logs/regroup/full")]
    params = {p["name"] for p in spec.get("parameters", [])}
    assert {"start", "end"} <= params
