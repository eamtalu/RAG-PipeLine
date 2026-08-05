"""Chunk 19: the pending signals must be TRUE, and must distinguish waiting from failing.

Why this matters now. The frontend is moving to poll GET /logs/regroup/status until `up_to_date`
before it reloads the transaction feed after a remote fetch (see
docs/plan/2026-08-05_frontend-stitch-timing-change.md). That makes these two endpoints a completion
SIGNAL rather than a passive hint, so a false "all clear" is no longer cosmetic - it makes the new
frontend wait exit early and silently reload a stale feed.

Two problems, both surfaced by the chunk-18 backoff gate:

1. read_pending_state(?finalize=true) HARDCODED `pending: False, pending_windows: 0` after calling
   finalize_pending. That was near enough true before, when finalize stitched every open window. It
   is not true now: a window that failed within its backoff delay is deliberately skipped, so
   finalize can legitimately leave work behind while the response still claims none.

2. GET /logs/regroup/status cannot distinguish "3 windows queued and about to be stitched" from
   "3 windows that keep FAILING and are waiting out a retry delay". Both are open and retryable, so
   both land in `pending_windows`. On a degraded disk that is exactly the distinction an operator
   needs at 2am.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select

from app.settings import settings
from app.config.database import async_session
from app.api.v1.logs import read_pending_state, regroup_status
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

T = datetime(2026, 8, 5, 10, 0, 0, tzinfo=timezone.utc)


async def _cleanup(cc: str) -> None:
    async with async_session() as s:
        await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
        await s.commit()


async def _mk(cc: str, *, backing_off_for: float | None = None, abandoned: bool = False,
              attempts: int = 0) -> None:
    async with async_session() as s:
        row = LogRegroupPending(customer_code=cc, range_start=T, range_end=T + timedelta(seconds=1),
                                attempts=attempts)
        if backing_off_for is not None:
            row.available_at = datetime.now(timezone.utc) + timedelta(seconds=backing_off_for)
        if abandoned:
            row.abandoned_at = datetime.now(timezone.utc)
        s.add(row)
        await s.commit()


# =============================================================== 1. finalize=true must not lie
async def test_finalize_true_reports_the_real_remaining_backlog(monkeypatch):
    """The load-bearing case: a window inside its backoff delay is skipped by finalize, so the
    response must still say pending. Hardcoding `pending: False` here is what would make the
    frontend's new up_to_date wait exit early and reload a stale feed."""
    cc = "TEST_CHUNK19_LIE"

    async def _fail(wdb, customer_code, lo, hi, commit=True):
        raise RuntimeError('could not read block 3 in file "base/1/2": Input/output error')

    monkeypatch.setattr(d, "regroup_window", _fail)
    await _cleanup(cc)
    await _mk(cc)
    try:
        # first finalize fails the window and pushes available_at into the future
        async with async_session() as s:
            await d.finalize_pending(s, cc)

        # the window is now backing off: finalize can do nothing, so the read must NOT claim clear
        async with async_session() as s:
            res = await read_pending_state(customer=cc, db=s, finalize=True)
        assert res["pending"] is True, "a backing-off window is still outstanding work"
        assert res["pending_windows"] == 1
        assert res["finalized"] is True          # we DID attempt a finalize
        assert res["oldest_pending_at"] is not None
    finally:
        await _cleanup(cc)


async def test_finalize_true_reports_clear_when_the_work_really_is_done(monkeypatch):
    """No-regression guard for the happy path: when finalize genuinely consumes everything, the
    response must still report clear — otherwise the frontend would wait forever."""
    cc = "TEST_CHUNK19_CLEAR"

    async def _ok(wdb, customer_code, lo, hi, commit=True):
        return {"mode": "window", "transactions_created": 0}

    monkeypatch.setattr(d, "regroup_window", _ok)
    await _cleanup(cc)
    await _mk(cc)
    try:
        async with async_session() as s:
            res = await read_pending_state(customer=cc, db=s, finalize=True)
        assert res["pending"] is False
        assert res["pending_windows"] == 0
        assert res["oldest_pending_at"] is None
        assert res["finalized"] is True
    finally:
        await _cleanup(cc)


async def test_finalize_true_ignores_abandoned_windows():
    """A dead-lettered window is parked, not outstanding: it must not hold `pending` true forever,
    or the frontend would wait out its whole timeout on every single fetch."""
    cc = "TEST_CHUNK19_DEAD"
    await _cleanup(cc)
    await _mk(cc, abandoned=True, attempts=settings.log_regroup_max_attempts)
    try:
        async with async_session() as s:
            res = await read_pending_state(customer=cc, db=s, finalize=True)
        assert res["pending"] is False
        assert res["pending_windows"] == 0
    finally:
        await _cleanup(cc)


async def test_plain_read_ignores_abandoned_windows():
    """Same rule without finalize — the soft flag on every transaction read must agree with
    /regroup/status, which already excludes abandoned windows from its backlog count."""
    cc = "TEST_CHUNK19_DEAD2"
    await _cleanup(cc)
    await _mk(cc, abandoned=True, attempts=settings.log_regroup_max_attempts)
    try:
        async with async_session() as s:
            res = await read_pending_state(customer=cc, db=s, finalize=False)
        assert res["pending"] is False
        assert res["pending_windows"] == 0
    finally:
        await _cleanup(cc)


async def test_plain_read_still_reports_open_work():
    cc = "TEST_CHUNK19_OPEN"
    await _cleanup(cc)
    await _mk(cc)
    try:
        async with async_session() as s:
            res = await read_pending_state(customer=cc, db=s, finalize=False)
        assert res["pending"] is True
        assert res["pending_windows"] == 1
        assert "finalized" not in res       # unchanged shape when not finalizing
    finally:
        await _cleanup(cc)


# =============================================================== 2. waiting vs failing
async def test_status_separates_backing_off_from_merely_queued():
    """`pending_windows` alone cannot tell an operator whether work is about to happen or whether it
    keeps failing. Both are 'open and retryable', so both count — the split is what makes a 2am
    disk fault visible."""
    cc = "TEST_CHUNK19_SPLIT"
    await _cleanup(cc)
    await _mk(cc)                                  # queued, due now
    await _mk(cc, backing_off_for=600, attempts=2)  # failed, waiting out its delay
    try:
        async with async_session() as s:
            res = await regroup_status(customer=cc, db=s)
        assert res["pending_windows"] == 2          # both are still outstanding work
        assert res["backing_off_windows"] == 1      # ...but only one is in a retry delay
        assert res["next_retry_at"] is not None     # and when it will be tried again
        assert res["up_to_date"] is False
    finally:
        await _cleanup(cc)


async def test_status_reports_zero_backoff_when_nothing_is_failing():
    cc = "TEST_CHUNK19_CALM"
    await _cleanup(cc)
    await _mk(cc)
    try:
        async with async_session() as s:
            res = await regroup_status(customer=cc, db=s)
        assert res["pending_windows"] == 1
        assert res["backing_off_windows"] == 0
        assert res["next_retry_at"] is None
    finally:
        await _cleanup(cc)


async def test_status_does_not_count_abandoned_as_backing_off():
    """An abandoned window is not waiting for anything — it needs a human. It is already reported
    separately as abandoned_windows and must not be double-counted."""
    cc = "TEST_CHUNK19_DEAD3"
    await _cleanup(cc)
    await _mk(cc, abandoned=True, backing_off_for=600,
              attempts=settings.log_regroup_max_attempts)
    try:
        async with async_session() as s:
            res = await regroup_status(customer=cc, db=s)
        assert res["abandoned_windows"] == 1
        assert res["backing_off_windows"] == 0
        assert res["pending_windows"] == 0
        assert res["up_to_date"] is True        # nothing retryable is outstanding
    finally:
        await _cleanup(cc)


async def test_status_shape_is_backward_compatible():
    """Additive only: every field the frontend already reads must still be present, since
    getRegroupStatus() is about to become the completion signal after a remote fetch."""
    cc = "TEST_CHUNK19_SHAPE"
    await _cleanup(cc)
    try:
        async with async_session() as s:
            res = await regroup_status(customer=cc, db=s)
        for key in ("customer_code", "pending", "pending_windows", "oldest_pending_at",
                    "last_regroup_at", "abandoned_windows", "up_to_date"):
            assert key in res, f"removed a field the frontend depends on: {key}"
        assert res["up_to_date"] == (not res["pending"]) == (res["pending_windows"] == 0)
    finally:
        await _cleanup(cc)
