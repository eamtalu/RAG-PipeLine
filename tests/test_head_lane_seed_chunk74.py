"""Chunk 74: a finished conversation must never be parked as an open stream.

Live forensics (2026-08-28, tenant tmp-live, the first evidence-rich DIVERGED line chunk 73
produced): the head-lane plan bound two brand-new responses to conversations from ~3 minutes
earlier that were already complete. The authority was right; the plan's SEED was wrong.

The mechanism, three facts long:

1. `regroup_window`'s save loop parked every builder still inside the quiet gap - including
   FINISHED ones. The comment above it says "an OPEN stream is one whose transaction is still
   receiving entries", but the code never checked closure, so a responded conversation was saved
   to `log_open_stream` as open.
2. `_group` seeds every parked stream into `open_by_key` - open work, by definition.
3. A RESPONSE binds to its user's OLDEST open work (FIFO by `open_pos`). A wrongly-parked finished
   conversation is always older than the fresh request sitting next to the response, so it steals
   every new response for that user.

Two fixes, each side of the contract:

- **Save**: a builder whose entries contain a response is closed - the grouper itself never lets
  it receive another entry (`close()` on response) - so it is not saved as an open stream.
- **Plan**: the head lane must never TRUST state it cannot verify. A parked stream whose reloaded
  entries already contain a response is stale by definition (legacy rows from before this fix, or
  any future save regression) and routes the window to the rebuild lane: fallback
  `parked_closed`, per the standing rule - never guess, fall back.
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
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun
from app.persistence.models.log_stitch_checkpoint import LogStitchCheckpoint
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import head_lane

CC = "test_chunk74"
T0 = datetime(2026, 8, 19, 7, 0, tzinfo=timezone.utc)


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


async def _plant(specs):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, at, line, thread in specs:
            db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id, timestamp=at,
                            source_file="S/x.log", line_number=line, level="INFO", raw_body="x",
                            message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread=thread, user_ctx="amin", fields={}))
        await db.commit()


async def _rebuild(lo, hi):
    async with async_session() as db:
        await dt.regroup_window(db, CC, lo, hi)


async def _parked_txn_ids() -> set:
    async with async_session() as db:
        return set((await db.execute(select(LogOpenStream.transaction_id).where(
            LogOpenStream.customer_code == CC))).scalars().all())


# ==================================== 1. the save side: closed means not parked

async def test_a_finished_conversation_is_not_parked_but_an_open_one_is():
    """The grouper closes a builder on its response and never reopens it, so parking it as 'open'
    stores a claim the grouper itself does not believe. The incomplete conversation on the other
    thread is genuinely open work and must still be parked."""
    await _plant([
        ("request", T0 + timedelta(seconds=1), 1, "7"),        # conversation A: complete
        ("request_body", T0 + timedelta(seconds=2), 2, "7"),
        ("response", T0 + timedelta(seconds=3), 3, "7"),
        ("request", T0 + timedelta(seconds=5), 11, "9"),       # conversation C: no response yet
        ("request_body", T0 + timedelta(seconds=6), 12, "9"),
    ])
    await _rebuild(T0, T0 + timedelta(seconds=10))

    async with async_session() as db:
        rows = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalars().all()
    by_status = {t.status.value: t for t in rows}
    assert set(by_status) == {"success", "incomplete"}

    parked = await _parked_txn_ids()
    assert by_status["incomplete"].id in parked
    assert by_status["success"].id not in parked


# ==================================== 2. the live divergence, end to end

async def test_a_new_response_is_not_stolen_by_a_finished_conversation():
    """The exact live strand: window one completes conversation A for user amin; window two brings
    a fresh complete conversation B for the same user on another thread. With A wrongly parked, the
    response FIFO hands B's response to A (A's open_pos is older). With the fix, the plan builds B
    whole and the shadow agrees with the authority."""
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7"),
                  ("response", T0 + timedelta(seconds=3), 3, "7")])
    await _rebuild(T0, T0 + timedelta(seconds=10))
    await head_lane.advance_checkpoint(CC, T0 + timedelta(seconds=10))

    lo2 = T0 + timedelta(seconds=60)
    await _plant([("request", lo2 + timedelta(seconds=1), 21, "9"),
                  ("request_body", lo2 + timedelta(seconds=2), 22, "9"),
                  ("response", lo2 + timedelta(seconds=3), 23, "9")])

    plan = await head_lane.build_plan(CC, lo2, lo2 + timedelta(seconds=5))
    assert plan.ok, plan.fallback
    assert len(plan.continued) == 0, "the finished conversation must not be continued"
    assert len(plan.created) == 1 and len(plan.created[0].entries) == 3
    assert plan.created[0].values["status"].value == "success"

    await _rebuild(lo2, lo2 + timedelta(seconds=5))
    assert await head_lane.shadow_compare(CC, plan) is True


# ==================================== 3. the plan side: stale state means fall back

async def test_the_head_lane_refuses_a_parked_stream_that_is_already_closed():
    """Legacy rows from before this fix (or any future save regression) can still present a closed
    conversation as parked. The head lane must not trust it and must not guess - the window routes
    to the rebuild lane with a named reason."""
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7"),
                  ("response", T0 + timedelta(seconds=3), 3, "7")])
    await _rebuild(T0, T0 + timedelta(seconds=10))
    await head_lane.advance_checkpoint(CC, T0 + timedelta(seconds=10))

    async with async_session() as db:
        txn = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalar_one()
        db.add(LogOpenStream(  # a pre-fix leftover: the closed conversation parked as open
            id=uuid.uuid4(), customer_code=CC, thread="7", user_ctx="amin",
            transaction_id=txn.id, has_request=True,
            last_entry_ts=T0 + timedelta(seconds=3), open_ts_is_null=False,
            open_ts=T0 + timedelta(seconds=1), open_source_file="S/x.log", open_line_number=1,
            is_current=True, created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)))
        await db.commit()

    lo2 = T0 + timedelta(seconds=60)
    await _plant([("request", lo2 + timedelta(seconds=1), 21, "9"),
                  ("request_body", lo2 + timedelta(seconds=2), 22, "9"),
                  ("response", lo2 + timedelta(seconds=3), 23, "9")])

    plan = await head_lane.build_plan(CC, lo2, lo2 + timedelta(seconds=5))
    assert not plan.ok and plan.fallback == "parked_closed"
