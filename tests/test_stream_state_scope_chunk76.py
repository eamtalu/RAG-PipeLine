"""Chunk 76: the state save may only speak for the transactions its window touched.

Live forensics (2026-08-28 12:49-12:51, tenant tmp-live, journal-reconstructed step by step):

1. 12:49:15 - a window ending 11:49:08.893 stitched a new conversation (request 11:49:08.688) and
   parked it as an open stream. Correct.
2. 12:49:23 - an OVERLAPPING, OLDER ticket (window ending 11:48:51.972 - entirely behind the
   previous one; merged tickets overlap routinely) rebuilt next. Its state save was a whole-tenant
   DELETE-then-INSERT, and since the step-1 conversation was outside its freed range, the save
   WIPED that parked stream.
3. 12:50:34 - the head-lane plan built with the hole in its seed. A response binds to its user's
   OLDEST open work, so with the front conversation missing every response for that user shifted
   one conversation over - the chained ownership mismatches in the DIVERGED evidence.

The rebuild lane is immune ("the state is a cache, not the truth" - it re-derives from raw lines);
the head lane is the first consumer that needs the cache to be COMPLETE. Three legs:

- **Scoped save.** The rebuild lane's save deletes only streams whose transaction it freed or
  rebuilt (plus genuine key collisions); out-of-scope parked streams survive. The head lane's
  apply keeps full-replace, because its plan re-parks everything it still believes in - the plan
  IS the complete state.
- **The server joins the stream key.** (customer, thread, user) forced newest-wins across app
  servers (thread ids are reused per process, 18r), silently dropping one server's open
  conversation - the same hole in another form. Both servers' streams must survive side by side.
- **Never plan around a hole.** A loaded stream whose transaction has no reloadable entries was
  silently dropped from the seed; the head lane now falls back by name: `parked_unreadable`.
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

CC = "test_chunk76"
T0 = datetime(2026, 8, 21, 9, 0, tzinfo=timezone.utc)


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
    """specs: (kind, at, line, thread, source_file)"""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, at, line, thread, src in specs:
            db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id, timestamp=at,
                            source_file=src, line_number=line, level="INFO", raw_body="x",
                            message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread=thread, user_ctx="amin", fields={}))
        await db.commit()


async def _rebuild(lo, hi):
    async with async_session() as db:
        await dt.regroup_window(db, CC, lo, hi)


async def _streams():
    async with async_session() as db:
        return (await db.execute(select(LogOpenStream).where(
            LogOpenStream.customer_code == CC))).scalars().all()


async def _park_open_conversation():
    """Window A: an open conversation S (request + body, no response) on thread 7, parked."""
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7", "S/x.log"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7", "S/x.log")])
    await _rebuild(T0, T0 + timedelta(seconds=10))
    async with async_session() as db:
        s_txn = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalar_one()
    assert s_txn.status.value == "incomplete"
    assert {r.transaction_id for r in await _streams()} == {s_txn.id}
    return s_txn


# ==================================== 1. the wipe: out-of-scope streams must survive

async def test_an_overlapping_older_window_does_not_wipe_a_parked_stream():
    """The exact live strand, step 2: window B is entirely behind window A's range and never
    touches conversation S, so S's parked stream must survive B's rebuild."""
    s_txn = await _park_open_conversation()

    # window B: an older, overlapping ticket with its OWN rows (thread 9), not covering S
    b_lo = T0 - timedelta(seconds=60)
    await _plant([("request", b_lo + timedelta(seconds=1), 11, "9", "S/x.log"),
                  ("request_body", b_lo + timedelta(seconds=2), 12, "9", "S/x.log"),
                  ("response", b_lo + timedelta(seconds=3), 13, "9", "S/x.log")])
    await _rebuild(b_lo, b_lo + timedelta(seconds=10))

    assert s_txn.id in {r.transaction_id for r in await _streams()}, \
        "the overlapping older window wiped the parked stream it never touched"


# ==================================== 2. the consequence: the FIFO shift, end to end

async def test_a_response_still_finds_its_conversation_after_an_overlapping_window():
    """Steps 1-3 of the live strand: after the overlapping window B, a new window C brings S's
    response plus a fresh complete conversation for the same user. The plan must hand S's response
    to S (the user's oldest open work) - not shift it onto the fresh conversation."""
    s_txn = await _park_open_conversation()
    b_lo = T0 - timedelta(seconds=60)
    await _plant([("request", b_lo + timedelta(seconds=1), 11, "9", "S/x.log"),
                  ("request_body", b_lo + timedelta(seconds=2), 12, "9", "S/x.log"),
                  ("response", b_lo + timedelta(seconds=3), 13, "9", "S/x.log")])
    await _rebuild(b_lo, b_lo + timedelta(seconds=10))
    await head_lane.advance_checkpoint(CC, T0 + timedelta(seconds=10))

    c_lo = T0 + timedelta(seconds=15)
    await _plant([("response", T0 + timedelta(seconds=20), 21, "7", "S/x.log"),   # S's response
                  ("request", T0 + timedelta(seconds=21), 31, "11", "S/x.log"),   # fresh conversation
                  ("request_body", T0 + timedelta(seconds=22), 32, "11", "S/x.log"),
                  ("response", T0 + timedelta(seconds=23), 33, "11", "S/x.log")])

    plan = await head_lane.build_plan(CC, c_lo, T0 + timedelta(seconds=30))
    assert plan.ok, plan.fallback
    assert [c.txn_id for c in plan.continued] == [s_txn.id]
    assert plan.continued[0].values["status"].value == "success"
    assert len(plan.created) == 1 and len(plan.created[0].entries) == 3

    await _rebuild(c_lo, T0 + timedelta(seconds=30))
    assert await head_lane.shadow_compare(CC, plan) is True


# ==================================== 3. two servers, same thread and user, both open

async def test_both_servers_open_conversations_are_parked_side_by_side():
    """Thread ids are reused by every server process (18r). One picker's two operations on two app
    servers - same thread number, same user - are two conversations, and BOTH must be parked."""
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7", "BEC01/x.log"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7", "BEC01/x.log"),
                  ("request", T0 + timedelta(seconds=3), 1, "7", "BEC02/y.log"),
                  ("request_body", T0 + timedelta(seconds=4), 2, "7", "BEC02/y.log")])
    await _rebuild(T0, T0 + timedelta(seconds=10))

    rows = await _streams()
    assert len(rows) == 2, f"expected both servers' streams parked, got {len(rows)}"
    assert len({r.transaction_id for r in rows}) == 2


# ==================================== 4. a hole in the seed means fall back, not guess

async def test_a_parked_stream_with_no_reloadable_entries_makes_the_plan_fall_back():
    """A stream row whose transaction has no assignments is a broken pointer. The authority will
    still see that conversation's raw lines; a plan built around the hole would shift the response
    FIFO exactly like the live divergence. Never guess: fall back."""
    await head_lane.advance_checkpoint(CC, T0)
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7", "S/x.log"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7", "S/x.log"),
                  ("response", T0 + timedelta(seconds=3), 3, "7", "S/x.log")])
    async with async_session() as db:
        db.add(LogOpenStream(
            id=uuid.uuid4(), customer_code=CC, server="S", thread="9", user_ctx="amin",
            transaction_id=uuid.uuid4(),  # points at nothing reloadable
            has_request=True, last_entry_ts=T0 - timedelta(seconds=5),
            open_ts_is_null=False, open_ts=T0 - timedelta(seconds=6),
            open_source_file="S/x.log", open_line_number=1, is_current=True,
            created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc)))
        await db.commit()

    plan = await head_lane.build_plan(CC, T0, T0 + timedelta(seconds=5))
    assert not plan.ok and plan.fallback == "parked_unreadable"
