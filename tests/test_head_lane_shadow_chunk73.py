"""Chunk 73: the head-lane shadow must compare like against like.

Live divergence forensics (2026-08-28, tenant tmp-live) showed 4 of 5 windows DIVERGED with the
rebuild lane and head lane both healthy. The cause is the comparison, not the lanes: the rebuild
reads 900s PAST the window's high edge and re-reads freed history, so on a live tenant it always
folds in entries the plan - by design - never saw (one diverged transaction had 192 of its 321
entries beyond the window hi). Comparing fingerprints across those two horizons flags healthy
windows forever, exactly the window-boundary artifact class chunk 66 fixed in the reconciler.

The horizon-aware comparison:

- **Ownership always.** Every entry the plan assigned must sit in the SAME transaction the
  authority put it in. This is the real question: would the head lane have grouped differently?
- **Fingerprints only on a shared horizon.** Byte-identical row/members digests are demanded only
  where the authority's final member set is exactly the planned set. A transaction the rebuild
  extended past the plan's horizon is checked by ownership alone.

Genuine disagreements (an entry grouped elsewhere, a digest that differs on identical members)
must still DIVERGE - the shadow's job is unchanged, only its false alarms go.
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

CC = "test_chunk73"
T0 = datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc)


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


# ============================================ 1. the artifacts must stop diverging

async def test_a_conversation_the_rebuild_extends_past_the_horizon_still_agrees():
    """The live 6dfc0f04 case: the response lies beyond the window hi, so the rebuild's padded read
    folds it in while the plan - correctly - stops at hi and parks the conversation open. Same
    grouping decision for every entry the plan saw, so the shadow must AGREE."""
    await head_lane.advance_checkpoint(CC, T0)
    hi = T0 + timedelta(seconds=5)
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7"),
                  ("info", T0 + timedelta(seconds=3), 3, "7"),
                  ("response", T0 + timedelta(seconds=8), 4, "7")])  # beyond hi, inside the pad

    plan = await head_lane.build_plan(CC, T0, hi)
    assert plan.ok, plan.fallback
    assert len(plan.created) == 1 and len(plan.created[0].entries) == 3

    await _rebuild(T0, hi)
    assert await head_lane.shadow_compare(CC, plan) is True


async def test_lines_ingested_after_the_plan_was_built_still_agree():
    """Stage 1 keeps committing while the window loop runs: in-window lines can land between
    build_plan and the rebuild. The rebuild sees them, the plan could not have - ownership of the
    plan's own entries is unchanged, so the shadow must AGREE."""
    await head_lane.advance_checkpoint(CC, T0)
    hi = T0 + timedelta(seconds=5)
    await _plant([("request", T0 + timedelta(seconds=1), 1, "7"),
                  ("request_body", T0 + timedelta(seconds=2), 2, "7")])

    plan = await head_lane.build_plan(CC, T0, hi)
    assert plan.ok, plan.fallback
    assert len(plan.created) == 1 and len(plan.created[0].entries) == 2

    await _plant([("info", T0 + timedelta(seconds=3), 3, "7"),
                  ("response", T0 + timedelta(seconds=4), 4, "7")])  # late arrival, in-window
    await _rebuild(T0, hi)
    assert await head_lane.shadow_compare(CC, plan) is True


# ============================================ 2. genuine disagreements must still diverge

async def _agreeing_plan_and_rebuild():
    """Two complete conversations on different threads, planned and rebuilt identically."""
    await head_lane.advance_checkpoint(CC, T0)
    hi = T0 + timedelta(seconds=10)
    specs = []
    for i, thread in enumerate(("7", "9")):
        base = T0 + timedelta(seconds=1 + i * 4)
        specs += [("request", base, 1 + i * 10, thread),
                  ("request_body", base + timedelta(seconds=1), 2 + i * 10, thread),
                  ("response", base + timedelta(seconds=2), 3 + i * 10, thread)]
    await _plant(specs)
    plan = await head_lane.build_plan(CC, T0, hi)
    assert plan.ok, plan.fallback
    assert len(plan.created) == 2
    await _rebuild(T0, hi)
    return plan


async def test_an_entry_grouped_into_a_different_transaction_diverges():
    """The check that matters: if the head lane had put an entry in a different conversation than
    the authority did, promotion would change the system's output - that must DIVERGE."""
    plan = await _agreeing_plan_and_rebuild()
    assert await head_lane.shadow_compare(CC, plan) is True

    # simulate the head lane having grouped one entry into the OTHER conversation
    a, b = plan.created
    b.entries.append(a.entries.pop())
    assert await head_lane.shadow_compare(CC, plan) is False


async def test_a_digest_that_differs_on_identical_members_diverges():
    """On a shared horizon the fingerprints must be byte-identical - a computation difference
    between the lanes (the 'second write authority' failure mode) must DIVERGE."""
    plan = await _agreeing_plan_and_rebuild()
    plan.created[0].row_fp = "not-what-the-authority-computed"
    assert await head_lane.shadow_compare(CC, plan) is False
