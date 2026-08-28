"""Chunk 72 (P4): the head lane - process only NEW lines against parked state.

The rebuild lane re-reads a padded window every tick to discover that ~98.7% of it did not change;
since S3 the WRITES are already minimal, so the re-READS are the only remaining waste. The head lane
removes them: a durable FRONTIER marks "processed up to here", parked open conversations live in
`log_open_stream` (maintained since S4a), and a window whose range lies entirely beyond the frontier
can be processed from just its own rows - continue a parked conversation with one update, start new
ones with inserts, park what is still open, advance the frontier.

The design rules, each earned earlier in this system's history:

- **The rebuild lane stays the authority.** The head lane runs the SAME grouper (`_group`, seeded)
  and must produce byte-identical fingerprints; in shadow mode it computes its plan, the rebuild
  executes as always, and the two are compared per window. `on` is a manual flip after the shadow
  has earned trust - exactly how S4a/18q said promotion must work, now on the correct axis.
- **Never guess: fall back.** Anything surprising - a window behind the frontier, a line without a
  timestamp, clock-disordered state, a continuation that would merge two parked conversations, a
  minted id that already exists, an anonymous open stream - routes the WINDOW to the rebuild lane
  with a named reason. The fallback is the safety net, not an error.
- **A plan is data.** `build_plan` is read-only and returns what WOULD be written; `apply_plan`
  writes it. Shadow exercises the exact code `on` runs, minus the writes - unexercised persist code
  is how second write authorities go wrong.
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
from app.persistence.models.log_stream_frontier import LogStreamFrontier
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import head_lane

CC = "test_chunk72"
CC2 = "test_chunk72_ctl"
T0 = datetime(2026, 8, 17, 8, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        for cc in (CC, CC2):
            for model in (LogStreamFrontier, LogOpenStream, LogPendingRequest,
                          AnalyticsPendingWindow, LogRegroupPending, LogRegroupRun,
                          LogEntryAssignment, LogTransaction, LogEntry):
                await db.execute(delete(model).where(model.customer_code == cc))
            await db.execute(delete(Job).where(Job.customer_code == cc))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant(specs, *, cc=CC):
    async with async_session() as db:
        job = Job(customer_code=cc, filename="t.log", document_type="transaction_log",
                  storage_key=f"{cc}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for kind, at, line in specs:
            db.add(LogEntry(id=uuid.uuid4(), customer_code=cc, job_id=job.id, timestamp=at,
                            source_file="S/x.log", line_number=line, level="INFO", raw_body="x",
                            message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                            thread="7", user_ctx="amin", fields={}))
        await db.commit()


#: An open conversation: request + body + work, NO response yet. Window one stitches it via the
#: rebuild lane (which also parks its open stream in log_open_stream and advances the frontier).
_OPENING = (
    ("request", T0, 1),
    ("request_body", T0 + timedelta(seconds=1), 2),
    ("info", T0 + timedelta(seconds=3), 3),
)


async def _window_one(*, cc=CC) -> LogTransaction:
    await _plant(_OPENING, cc=cc)
    async with async_session() as db:
        await dt.regroup_window(db, cc, T0, T0 + timedelta(seconds=5))
    await head_lane.advance_frontier(cc, T0 + timedelta(seconds=5))
    async with async_session() as db:
        txn = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == cc))).scalar_one()
    assert txn.status.value == "incomplete"
    return txn


# ==================================================== 1. the frontier

async def test_the_frontier_only_moves_forward():
    """The bookmark: both lanes advance it, nothing may drag it back - a late backfill window is a
    normal event and must not make the head lane think history ended earlier than it did."""
    await head_lane.advance_frontier(CC, T0)
    await head_lane.advance_frontier(CC, T0 - timedelta(hours=1))
    async with async_session() as db:
        assert await head_lane.get_frontier(db, CC) == T0


# ==================================================== 2. the plan

async def test_the_plan_continues_a_parked_conversation():
    """The head lane's whole point: the response arrives in a new window, the parked stream says
    which transaction it belongs to, and the plan is ONE update - no padded re-read of history."""
    before = await _window_one()

    resp_at = T0 + timedelta(seconds=8)
    await _plant([("response", resp_at, 4)])
    plan = await head_lane.build_plan(CC, resp_at, resp_at + timedelta(seconds=1))

    assert plan.ok, plan.fallback
    assert len(plan.continued) == 1 and len(plan.created) == 0
    cont = plan.continued[0]
    assert cont.txn_id == before.id
    assert cont.values["status"].value != "incomplete"
    assert len(cont.entries) == 4


async def test_the_plan_creates_a_new_conversation():
    await _window_one()
    at = T0 + timedelta(minutes=2)
    await _plant([("request", at, 10), ("request_body", at + timedelta(seconds=1), 11),
                  ("response", at + timedelta(seconds=3), 12)])

    plan = await head_lane.build_plan(CC, at, at + timedelta(seconds=5))

    assert plan.ok, plan.fallback
    assert len(plan.created) == 1 and len(plan.continued) == 0


async def test_a_window_behind_the_frontier_falls_back():
    """Out-of-order and backfilled data is the rebuild lane's job, always."""
    await _window_one()
    plan = await head_lane.build_plan(CC, T0 - timedelta(hours=2), T0 - timedelta(hours=1))
    assert not plan.ok and plan.fallback == "behind_frontier"


async def test_a_timestampless_row_is_ignored_like_the_rebuild_lane_ignores_it():
    """Parity fact, verified in both lanes: a windowed read is `timestamp >= lo`, which excludes
    NULL timestamps - the REBUILD lane never sees them in a window either. Only a full rebuild
    reaches them. The head lane must therefore proceed (ignoring the row), not fall back."""
    await _window_one()
    at = T0 + timedelta(minutes=3)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t2.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t2.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id, timestamp=None,
                        source_file="S/x.log", line_number=99, level="INFO", raw_body="x",
                        message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType("info"),
                        thread="7", user_ctx="amin", fields={}))
        db.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job.id, timestamp=at,
                        source_file="S/x.log", line_number=100, level="INFO", raw_body="x",
                        message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType("info"),
                        thread="9", user_ctx="sara", fields={}))
        await db.commit()

    plan = await head_lane.build_plan(CC, at, at + timedelta(seconds=5))
    assert plan.ok, plan.fallback
    planned_lines = {e.line_number for c in plan.created + plan.continued for e in c.entries}
    assert 99 not in planned_lines, "the NULL-timestamp row must not be consumed by a window"


# ==================================================== 3. apply = the rebuild, byte for byte

async def test_applying_the_plan_matches_the_rebuild_exactly():
    """The equivalence test P4 stands on, judged by the authority itself: after the head lane
    applies its plan, the REBUILD LANE re-derives the same window and must find NOTHING to change -
    every transaction unchanged, nothing created, rewritten or deleted. If the head lane had grouped
    or computed anything differently, the rebuild's fingerprints would disagree and it would rewrite;
    an all-unchanged verdict is byte-identity, certified by the one algorithm that is always right."""
    before = await _window_one()

    resp_at = T0 + timedelta(seconds=8)
    await _plant([("response", resp_at, 4)])

    plan = await head_lane.build_plan(CC, resp_at, resp_at + timedelta(seconds=1))
    assert plan.ok, plan.fallback
    await head_lane.apply_plan(CC, plan)

    async with async_session() as db:
        verdict = await dt.regroup_window(db, CC, resp_at, resp_at + timedelta(seconds=1))
    assert verdict.get("transactions_unchanged", 0) >= 1, verdict
    assert verdict.get("transactions_created", 0) == 0
    assert verdict.get("transactions_rewritten", 0) == 0
    assert verdict.get("transactions_row_only", 0) == 0
    assert verdict.get("transactions_deleted", 0) == 0

    async with async_session() as db:
        head = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalar_one()
        head_n = (await db.execute(select(LogEntryAssignment).where(
            LogEntryAssignment.transaction_id == head.id))).scalars().all()
    assert head.id == before.id
    assert head.status.value != "incomplete"
    assert len(head_n) == 4

    # and the analytics contract: a ticket must cover the continued transaction's start
    async with async_session() as db:
        covering = (await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC,
            AnalyticsPendingWindow.range_start <= before.started_at,
            AnalyticsPendingWindow.range_end >= before.started_at))).scalars().all()
    assert covering, "the continued transaction's facts must be restated"

    # and the frontier moved
    async with async_session() as db:
        assert (await head_lane.get_frontier(db, CC)) >= resp_at


async def test_apply_refuses_when_the_parked_row_changed_underneath():
    """Optimistic concurrency: the plan captured the parked transaction's fingerprint; if anything
    rewrote the row between plan and apply, applying blind would clobber it. The apply must raise -
    the window then retries through the normal machinery and the fresh plan sees the new truth."""
    before = await _window_one()
    resp_at = T0 + timedelta(seconds=8)
    await _plant([("response", resp_at, 4)])
    plan = await head_lane.build_plan(CC, resp_at, resp_at + timedelta(seconds=1))
    assert plan.ok

    async with async_session() as db:
        from sqlalchemy import update as sa_update
        await db.execute(sa_update(LogTransaction).where(LogTransaction.id == before.id)
                         .values(row_fingerprint="changed-underneath"))
        await db.commit()

    with pytest.raises(head_lane.PlanStale):
        await head_lane.apply_plan(CC, plan)


# ==================================================== 4. shadow inside the worker

async def test_finalize_in_shadow_mode_compares_and_agrees(monkeypatch, caplog):
    """End to end through finalize_pending: the head lane plans, the rebuild executes as authority,
    the comparison logs agreement, and the database carries exactly what the rebuild wrote."""
    monkeypatch.setattr(dt.settings, "stage2_head_lane", "shadow", raising=False)
    await _window_one()

    resp_at = T0 + timedelta(seconds=8)
    await _plant([("response", resp_at, 4)])
    async with async_session() as db:
        db.add(LogRegroupPending(customer_code=CC, range_start=resp_at,
                                 range_end=resp_at + timedelta(seconds=1)))
        await db.commit()

    import logging
    with caplog.at_level(logging.INFO):
        async with async_session() as db:
            stats = await dt.finalize_pending(db, CC)

    assert stats["windows"] == 1
    assert any("head lane" in r.message.lower() and "agreed" in r.message.lower()
               for r in caplog.records), "the shadow must report its comparison"
    async with async_session() as db:
        txn = (await db.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalar_one()
    assert txn.status.value != "incomplete"
