"""Chunk 69 (Part B of the 2026-08-27 API-cleanup plan): the tracked, safe full rebuild.

Chunk 68 removed `POST /logs/regroup` because an inline full rebuild cannot survive its own duration
over HTTP. This chunk builds the replacement, productising the procedure the 18r backlog repair had
to perform by hand four times, and encoding each of that night's lessons as a mechanism:

1. **A full rebuild is a tracked RUN in its own PROCESS.** `run_full_regroup_tracked` executes
   `regroup_all` and records the outcome on a `log_regroup_runs` row (`kind='full'`); the endpoint
   spawns it as a subprocess and returns 202 + a poll URL. Not a web-tier background task: forty
   minutes of grouping CPU inside an event loop freezes the process that hosts it - the web worker
   would be killed by gunicorn, the background worker would stall every other pipeline.

2. **The run row IS the maintenance flag.** While a fresh `kind='full'` run is `running`, BOTH tenant
   sweeps - Stage 2's and analytics' `customers_with_due_work` - skip that tenant. Their tickets
   simply wait; nothing fails, nothing dead-letters, and nobody has to remember to stop the worker
   (the manual repair collided with the live stitcher precisely because someone had to remember).

3. **A stale flag must not pause a tenant forever.** A `running` row older than the TTL (a crashed
   subprocess, a service restart mid-rebuild) stops pausing and is logged CRITICAL - visible and
   restartable, never a silent freeze of the tenant's pipelines.

4. **The analytics re-ticket span derives from `log_entries`.** The old code read the span from
   `log_transactions` BEFORE deleting them - correct for a healthy tenant, silently empty for a
   tenant whose transactions were already gone (the exact state the repair started from, which is
   how eight days of facts went missing until a manual re-ticket). Entries survive every rebuild.

5. **`regroup_all` serialises with the live worker** by taking the same per-tenant advisory lock the
   stitcher holds per window (belt and braces under the skip flag)."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun, LogRegroupRunStatus
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.workers import log_stitch_worker
from app.services.analytics import consume as n3

CC = "test_chunk69"
T0 = datetime(2026, 8, 16, 9, 0, tzinfo=timezone.utc)


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


async def _plant_conversation():
    """A small complete conversation's entries, committed, unstitched."""
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


async def _full_run_row(*, status=LogRegroupRunStatus.running, age_seconds=0) -> uuid.UUID:
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC, kind="full", status=status)
        db.add(run)
        await db.flush()
        if age_seconds:
            await db.execute(update(LogRegroupRun).where(LogRegroupRun.id == run.id).values(
                created_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds)))
        await db.commit()
        return run.id


# ==================================================== 1. the run row and its kind

async def test_the_run_row_carries_its_kind():
    """One table tracks both run flavours; `kind` is what distinguishes the finalize the frontend
    already polls from the full rebuild the maintenance screen starts. Defaults to finalize so every
    existing row and caller keeps its meaning."""
    async with async_session() as db:
        run = LogRegroupRun(customer_code=CC)
        db.add(run)
        await db.commit()
        stored = (await db.execute(select(LogRegroupRun).where(
            LogRegroupRun.id == run.id))).scalar_one()
    assert stored.kind == "finalize"


# ==================================================== 2. the maintenance flag

async def test_a_running_full_rebuild_pauses_both_workers_for_that_tenant():
    """The lesson that cost four repair attempts: the rebuild and the live stitcher must never run
    concurrently for one tenant. The run row is the flag; both sweeps honour it, so the pause is
    automatic and scoped - other tenants keep flowing, and this tenant's tickets WAIT rather than
    fail."""
    async with async_session() as db:
        db.add(LogRegroupPending(customer_code=CC, range_start=T0, range_end=T0 + timedelta(minutes=1)))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0, range_end=T0 + timedelta(minutes=1)))
        await db.commit()

    assert CC in await log_stitch_worker.customers_with_due_work(limit=1000)
    assert CC in await n3.customers_with_due_work(limit=1000)

    run_id = await _full_run_row()
    assert CC not in await log_stitch_worker.customers_with_due_work(limit=1000), (
        "the stitcher must not race a running full rebuild")
    assert CC not in await n3.customers_with_due_work(limit=1000), (
        "the fold must not consume mid-rebuild states")

    async with async_session() as db:
        await db.execute(update(LogRegroupRun).where(LogRegroupRun.id == run_id).values(
            status=LogRegroupRunStatus.completed))
        await db.commit()
    assert CC in await log_stitch_worker.customers_with_due_work(limit=1000)
    assert CC in await n3.customers_with_due_work(limit=1000)


async def test_a_stale_full_run_stops_pausing(caplog):
    """A crashed subprocess or a service restart leaves a `running` row behind. Past the TTL it must
    stop pausing the tenant - a silent permanent pipeline freeze is worse than a rebuild that needs
    re-running - and it must be LOUD about it."""
    async with async_session() as db:
        db.add(LogRegroupPending(customer_code=CC, range_start=T0, range_end=T0 + timedelta(minutes=1)))
        await db.commit()
    await _full_run_row(age_seconds=100 * 3600)

    with caplog.at_level("CRITICAL"):
        due = await log_stitch_worker.customers_with_due_work(limit=1000)
    assert CC in due, "a stale flag must not freeze the tenant forever"
    assert any("full rebuild" in r.message.lower() or "stale" in r.message.lower()
               for r in caplog.records), "the stale flag must be loud"


# ==================================================== 3. tickets from the table that survives

async def test_full_rebuild_tickets_come_from_the_entries():
    """The empty-span lesson: run 4 of the repair started from a tenant whose transactions were
    already gone, so the span read from log_transactions published NOTHING and eight days of facts
    stayed missing until a manual re-ticket. Entries survive every rebuild; the span comes from
    them."""
    await _plant_conversation()
    async with async_session() as db:
        await dt.regroup_all(db, CC)

    async with async_session() as db:
        covering = (await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC,
            AnalyticsPendingWindow.range_start <= T0,
            AnalyticsPendingWindow.range_end >= T0 + timedelta(seconds=4)))).scalars().all()
    assert covering, ("regroup_all must publish analytics tickets covering the ENTRIES span even "
                      "when log_transactions started empty")


# ==================================================== 4. the tracked runner

async def test_run_full_regroup_tracked_records_the_outcome():
    """The runner is the subprocess's whole job: execute regroup_all for the run's tenant and leave
    a pollable record - completed with the stats, or failed with the error, never silence."""
    await _plant_conversation()
    run_id = await _full_run_row()

    await dt.run_full_regroup_tracked(run_id)

    async with async_session() as db:
        run = (await db.execute(select(LogRegroupRun).where(
            LogRegroupRun.id == run_id))).scalar_one()
        txns = (await db.execute(select(LogTransaction.id).where(
            LogTransaction.customer_code == CC))).scalars().all()
    assert run.status == LogRegroupRunStatus.completed
    assert run.finished_at is not None
    assert (run.result or {}).get("transactions_created", 0) >= 1
    assert len(txns) >= 1, "the rebuild must actually have stitched the tenant"


# ==================================================== 5. the surface and the lock

def test_the_full_rebuild_endpoint_is_async_only():
    """The replacement contract: POST /logs/regroup/full exists, answers 202, and the inline
    endpoint chunk 68 removed stays gone."""
    from app.main import app
    surface = {(m.upper(), p): spec
               for p, methods in app.openapi()["paths"].items()
               for m, spec in methods.items()}
    assert ("POST", "/api/v1/logs/regroup/full") in surface
    assert "202" in surface[("POST", "/api/v1/logs/regroup/full")]["responses"]
    assert ("POST", "/api/v1/logs/regroup") not in surface


def test_regroup_all_takes_the_tenant_lock():
    """Belt and braces under the skip flag: the rebuild serialises with any in-flight stitch window
    through the same per-tenant advisory lock the stitcher holds."""
    import inspect
    src = inspect.getsource(dt.regroup_all)
    assert "pg_advisory_xact_lock" in src
