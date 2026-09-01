"""Chunk 83: a parked LONE REQUEST makes the plan fall back - the pending round-trip gap.

Root-caused from live (2026-09-01, 4 DIVERGED) and reproduced exactly before fixing:

In the raw grouper a lone request waits in the PENDING pool, and a later `request_body` POPS it
so the two join. The saved state cannot say "pending": a request alone at a window's tail is
persisted as an incomplete transaction and parked as an open STREAM. When its body arrives in the
next window, the seeded plan applies the raw rule "a body starts a new cycle" to that stream -
closes it, finds no pending request to pop, and mints a BODY-ANCHORED transaction that the
authority (which re-pools the request and builds one request-anchored conversation) never
persists. Every entry after the split then shifts through the user's response FIFO - the cascade
shape of all four live windows.

The fix is the doctrine, not cleverness: a parked stream whose conversation is a lone request is
the ONE shape the body-pop rule can split, so the window routes to the rebuild lane by name
(`parked_lone_request`). The round-trip that would make these windows head-laneable is deliberately
deferred until the shadow MEASURES the decline rate - S4b taught this codebase to buy coverage
from numbers, not assumptions.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_stitch_checkpoint import LogStitchCheckpoint
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import head_lane

CC = "test_chunk83"
T0 = datetime(2026, 8, 23, 9, 0, tzinfo=timezone.utc)
S = timedelta(seconds=1)


async def _wipe():
    async with async_session() as db:
        for m in (LogStitchCheckpoint, LogOpenStream, LogPendingRequest,
                  LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(m).where(m.customer_code == CC))
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
            db.add(LogEntry(customer_code=CC, job_id=job.id, timestamp=at, line_number=line,
                            raw_body=f"{kind}-{line}", entry_hash=uuid.uuid4().hex,
                            source_file="BEC01/x.log", level="INFO",
                            entry_type=LogEntryType(kind), thread=thread, user_ctx="JONEILL",
                            fields={}))
        await db.commit()


async def _park_lone_request():
    await _plant([("request", T0, 1, "37")])
    async with async_session() as db:
        await dt.regroup_window(db, CC, T0 - S, T0 + S)
    await head_lane.advance_checkpoint(CC, T0 + S)
    async with async_session() as db:
        parked = (await db.execute(select(LogOpenStream).where(
            LogOpenStream.customer_code == CC))).scalars().all()
        txn = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalar_one()
    assert len(parked) == 1 and parked[0].has_request
    assert txn.status.value == "incomplete" and txn.entry_count == 1
    return txn


async def test_a_parked_lone_request_makes_the_plan_fall_back():
    """The reproduced live signature: without the guard, the plan minted a body-anchored
    transaction that the authority never persists, and the shadow diverged."""
    await _park_lone_request()

    lo2 = T0 + timedelta(seconds=70)
    await _plant([("request_body", lo2 + S, 2, "37"),
                  ("info", lo2 + 2 * S, 3, "37"),
                  ("response", lo2 + 3 * S, 4, "37")])

    plan = await head_lane.build_plan(CC, lo2, lo2 + timedelta(seconds=10))
    assert not plan.ok and plan.fallback == "parked_lone_request"


async def test_the_declined_window_agrees_once_the_authority_has_joined_the_halves():
    """After the rebuild lane stitches request+body into one conversation, the NEXT window's plan
    sees an ordinary parked-or-closed state and proceeds - the decline is one window wide."""
    await _park_lone_request()
    lo2 = T0 + timedelta(seconds=70)
    await _plant([("request_body", lo2 + S, 2, "37"),
                  ("info", lo2 + 2 * S, 3, "37"),
                  ("response", lo2 + 3 * S, 4, "37")])
    async with async_session() as db:
        await dt.regroup_window(db, CC, lo2, lo2 + timedelta(seconds=10))
    await head_lane.advance_checkpoint(CC, lo2 + timedelta(seconds=10))
    async with async_session() as db:
        joined = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalars().all()
    assert len(joined) == 1 and joined[0].entry_count == 4

    lo3 = lo2 + timedelta(seconds=120)
    await _plant([("request", lo3 + S, 10, "38"),
                  ("request_body", lo3 + 2 * S, 11, "38"),
                  ("response", lo3 + 3 * S, 12, "38")])
    plan = await head_lane.build_plan(CC, lo3, lo3 + timedelta(seconds=10))
    assert plan.ok, plan.fallback
    assert len(plan.created) == 1


async def test_an_ordinary_open_conversation_still_plans():
    """The guard is precise: request+body parked together (the normal open shape) is NOT a lone
    request and must keep planning - otherwise the decline swallows the head lane's bread and
    butter."""
    await _plant([("request", T0, 1, "37"), ("request_body", T0 + S, 2, "37")])
    async with async_session() as db:
        await dt.regroup_window(db, CC, T0 - S, T0 + timedelta(seconds=5))
    await head_lane.advance_checkpoint(CC, T0 + timedelta(seconds=5))

    lo2 = T0 + timedelta(seconds=70)
    await _plant([("response", lo2 + S, 3, "37")])
    plan = await head_lane.build_plan(CC, lo2, lo2 + timedelta(seconds=10))
    assert plan.ok, plan.fallback
    assert len(plan.continued) == 1
