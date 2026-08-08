"""Chunk 18: give Stage 2 its own consumer, and stop producers from calling it.

The problem this fixes. `log_regroup_pending` has always been a proper work queue - Stage 1 writes a
ticket in the same transaction as the entries (parse_insert.py:178), which is why Stage 2 can fail
completely and still be retried. But that queue had no consumer, so every producer had to remember to
drain it: the SFTP transport (`remote_fetcher._do_finalize`), the directory watcher
(`log_watcher._finalize_customers`) and the parse worker all called `finalize_pending` themselves.
Three modules with no business knowing stitching exists, and a trap where any new ingestion path that
forgets the call silently leaves data unstitched.

A dedicated worker now owns draining it. Producers only write tickets.

Note the unit of work is a CUSTOMER, not a row: `finalize_pending` coalesces all of a tenant's open
rows into clusters (`_coalesce_pending`), so claiming one row at a time would destroy that and turn
one efficient rebuild into many overlapping ones. Concurrency is handled by the per-customer advisory
lock that already lives inside `finalize_pending`, which is why - unlike the Stage 1 queue - no lease
columns are needed here.

Covered:
- the shared retry policy is one module, used by both stages;
- Stage 2 now backs off between attempts instead of retrying on every tick, and abandons a
  permanently-broken window on the first failure rather than burning the whole budget;
- the stitch worker finds tenants with due work by querying the pending table, not count(*) on the
  ~40 GB log_entries heap;
- producers no longer call Stage 2, and explicit user-triggered stitching still does.
"""

import asyncio
import inspect
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d
from app.services.queueing import retry_policy
from app.services.workers import log_stitch_worker as lsw

T = datetime(2026, 8, 5, 9, 0, 0, tzinfo=timezone.utc)


async def _cleanup(cc: str) -> None:
    async with async_session() as s:
        await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
        await s.commit()


async def _mk_pending(cc: str, *, available_at: datetime | None = None,
                      attempts: int = 0, abandoned: bool = False) -> None:
    async with async_session() as s:
        row = LogRegroupPending(customer_code=cc, range_start=T, range_end=T + timedelta(seconds=1),
                                attempts=attempts)
        if available_at is not None:
            row.available_at = available_at
        if abandoned:
            row.abandoned_at = datetime.now(timezone.utc)
        s.add(row)
        await s.commit()


async def _row(cc: str) -> LogRegroupPending:
    async with async_session() as s:
        return (await s.execute(
            select(LogRegroupPending).where(LogRegroupPending.customer_code == cc))).scalars().one()


# =============================================================== shared policy
def test_retry_policy_is_a_single_shared_module():
    """Both stages must use ONE backoff/classification implementation. Two copies drift apart, and
    then the same failure behaves differently depending on which queue it landed in.

    Classification is a pure function, so it is aliased outright. Backoff binds each queue's own
    base/cap from settings, so it delegates rather than aliases — what matters is that neither
    module carries a second implementation."""
    from app.services.workers import log_parse_worker as lpw
    assert lpw._is_transient is retry_policy.is_transient
    assert "retry_policy.backoff_seconds" in inspect.getsource(lpw._backoff_seconds)
    # Stage 2's backoff is applied where its failures are recorded — inside finalize_pending — not in
    # the stitch worker, which only decides WHICH tenants to hand over.
    assert "retry_policy.backoff_seconds" in inspect.getsource(d)
    assert "retry_policy.is_transient" in inspect.getsource(d)
    for mod in (lpw, lsw, d):
        assert "random.random()" not in inspect.getsource(mod), \
            "backoff must not be reimplemented outside retry_policy"


def test_backoff_grows_is_jittered_and_capped():
    b1 = [retry_policy.backoff_seconds(1, base=10.0, cap=100.0) for _ in range(40)]
    b2 = [retry_policy.backoff_seconds(2, base=10.0, cap=100.0) for _ in range(40)]
    assert min(b2) > max(b1) * 0.9
    assert len(set(b1)) > 1                       # jittered
    assert all(10.0 <= x <= 12.5 for x in b1)     # base + up to 25%
    assert all(x <= 125.0 for x in
               [retry_policy.backoff_seconds(50, base=10.0, cap=100.0) for _ in range(20)])


def test_classification_matches_the_stage1_rules():
    assert retry_policy.is_transient(RuntimeError(
        'could not read block 1 in file "base/1/2": Input/output error')) is True
    assert retry_policy.is_transient(asyncio.TimeoutError()) is True
    assert retry_policy.is_transient(ValueError("unparseable")) is False
    assert retry_policy.is_transient(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad")) is False


# =============================================================== Stage 2 backoff
async def test_stage2_backs_off_between_attempts(monkeypatch):
    """Before: a failing window was retried on the very next tick, so all three attempts were gone
    within seconds - before a transient problem had any chance to clear."""
    cc = "TEST_CHUNK18_BACKOFF"

    async def _fail(wdb, customer_code, lo, hi, commit=True):
        raise RuntimeError('could not read block 9 in file "base/1/2": Input/output error')

    monkeypatch.setattr(d, "regroup_window", _fail)
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        before = datetime.now(timezone.utc)
        async with async_session() as s:
            await d.finalize_pending(s, cc)
        row = await _row(cc)
        assert row.attempts == 1
        assert row.abandoned_at is None
        assert row.available_at > before + timedelta(seconds=1), "must actually be delayed"

        # still backing off -> the very next finalize must NOT pick it up
        async with async_session() as s:
            res = await d.finalize_pending(s, cc)
        assert res["windows"] == 0
        assert (await _row(cc)).attempts == 1, "attempts must not advance while backing off"
    finally:
        await _cleanup(cc)


async def test_stage2_abandons_a_permanent_failure_on_the_first_attempt(monkeypatch):
    """A window whose data is genuinely broken will fail identically every time; two of the three
    reads were guaranteed to fail before they started."""
    cc = "TEST_CHUNK18_PERM"

    async def _fail(wdb, customer_code, lo, hi, commit=True):
        raise ValueError("builder produced an unpersistable transaction")

    monkeypatch.setattr(d, "regroup_window", _fail)
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        async with async_session() as s:
            res = await d.finalize_pending(s, cc)
        row = await _row(cc)
        assert row.abandoned_at is not None
        assert row.attempts == 1
        assert row.attempts < settings.log_regroup_max_attempts
        assert res["abandoned"] == 1
    finally:
        await _cleanup(cc)


async def test_stage2_still_dead_letters_transient_failures_at_the_cap(monkeypatch):
    """No-regression guard: the existing 3-strike dead letter must survive the backoff change."""
    cc = "TEST_CHUNK18_CAP"
    n = settings.log_regroup_max_attempts

    async def _fail(wdb, customer_code, lo, hi, commit=True):
        raise RuntimeError('could not read block 9 in file "base/1/2": Input/output error')

    monkeypatch.setattr(d, "regroup_window", _fail)
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        for i in range(1, n + 1):
            async with async_session() as s:
                await s.execute(  # simulate the backoff having elapsed
                    LogRegroupPending.__table__.update()
                    .where(LogRegroupPending.customer_code == cc)
                    .values(available_at=datetime.now(timezone.utc) - timedelta(seconds=1)))
                await s.commit()
            async with async_session() as s:
                await d.finalize_pending(s, cc)
            row = await _row(cc)
            assert row.attempts == i
            assert (row.abandoned_at is not None) == (i == n)
    finally:
        await _cleanup(cc)


def test_stitch_worker_is_on_by_default():
    """Unlike the count(*)-polling grouping worker it replaces, this one is the ONLY thing that
    drains the queue. Shipping it off would leave every ingested entry unstitched.

    Checked in its own test because the registration test monkeypatches this flag."""
    assert settings.log_stitch_worker_enabled is True
    assert settings.log_stitch_poll_seconds > 0
    assert settings.log_stitch_max_customers_per_tick > 0
    assert settings.log_regroup_backoff_base_seconds > 0


async def test_available_at_uses_the_database_clock_not_the_app_host():
    """Regression guard for a genuinely nasty bug: available_at was written with the app host's clock
    and compared against the database's. Any skew - and a host a few ms ahead of its database is
    completely routine, containers drift more - made a freshly written row look "not yet due", so the
    stitch worker intermittently skipped work that was ready. Both sides must use the DB clock.

    Asserted by writing a row and immediately requiring it to be due: with two clocks and the host
    ahead, this fails at random."""
    cc = "TEST_CHUNK18_CLOCK"
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        for _ in range(25):                       # skew is small, so a single check could get lucky
            assert cc in await lsw.customers_with_due_work(limit=1000)
    finally:
        await _cleanup(cc)


# =============================================================== the stitch worker
async def test_worker_finds_tenants_by_querying_the_pending_table():
    cc = "TEST_CHUNK18_DUE"
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        due = await lsw.customers_with_due_work(limit=1000)
        assert cc in due
    finally:
        await _cleanup(cc)


async def test_worker_ignores_backing_off_abandoned_and_consumed_work():
    """Three exclusions, each load-bearing: a backing-off window would defeat the backoff, an
    abandoned one would not be a dead letter, and a consumed one is already done."""
    for cc, kw in (("TEST_CHUNK18_WAIT", {"available_at": datetime.now(timezone.utc) + timedelta(minutes=10)}),
                   ("TEST_CHUNK18_DEAD", {"abandoned": True})):
        await _cleanup(cc)
        await _mk_pending(cc, **kw)
    cc3 = "TEST_CHUNK18_DONE"
    await _cleanup(cc3)
    async with async_session() as s:
        s.add(LogRegroupPending(customer_code=cc3, range_start=T, range_end=T + timedelta(seconds=1),
                                consumed_at=datetime.now(timezone.utc)))
        await s.commit()
    try:
        due = await lsw.customers_with_due_work(limit=1000)
        assert "TEST_CHUNK18_WAIT" not in due
        assert "TEST_CHUNK18_DEAD" not in due
        assert "TEST_CHUNK18_DONE" not in due
    finally:
        for cc in ("TEST_CHUNK18_WAIT", "TEST_CHUNK18_DEAD", cc3):
            await _cleanup(cc)


def test_worker_does_not_count_rows_on_log_entries():
    """The old grouping worker ran SELECT count(*) on the ~40 GB log_entries heap every 5s just to
    detect 'did anything change'. That is why it shipped disabled. The pending table answers the
    question directly, and is tiny and indexed."""
    src = inspect.getsource(lsw)
    assert "LogEntry" not in src, "the stitch worker must not touch log_entries at all"


async def test_worker_drain_stitches_due_tenants_and_reports_counts(monkeypatch):
    cc = "TEST_CHUNK18_DRAIN"
    seen: list[str] = []

    async def _fake_finalize(db, customer_code):
        seen.append(customer_code)
        return {"windows": 1, "pending_consumed": 1, "abandoned": 0}

    monkeypatch.setattr(lsw, "finalize_pending", _fake_finalize)

    async def _only_mine(limit=None):
        return [cc]

    monkeypatch.setattr(lsw, "customers_with_due_work", _only_mine)
    await _cleanup(cc)
    await _mk_pending(cc)
    try:
        stats = await lsw.drain_once()
        assert cc in seen
        assert stats["customers"] == 1
    finally:
        await _cleanup(cc)


async def test_worker_isolates_one_failing_tenant_from_the_others(monkeypatch):
    """One tenant's poison window must never stop another tenant being stitched."""
    bad, good = "TEST_CHUNK18_BAD", "TEST_CHUNK18_GOOD"
    done: list[str] = []

    async def _fake_finalize(db, customer_code):
        if customer_code == bad:
            raise RuntimeError("boom")
        done.append(customer_code)
        return {"windows": 1, "pending_consumed": 1, "abandoned": 0}

    monkeypatch.setattr(lsw, "finalize_pending", _fake_finalize)

    async def _only_mine(limit=None):
        return [bad, good]

    monkeypatch.setattr(lsw, "customers_with_due_work", _only_mine)
    for cc in (bad, good):
        await _cleanup(cc)
        await _mk_pending(cc)
    try:
        stats = await lsw.drain_once()
        assert good in done, "a failing tenant must not block the rest"
        assert stats["failed"] == 1
    finally:
        for cc in (bad, good):
            await _cleanup(cc)


# =============================================================== producers stop calling Stage 2
def test_transport_no_longer_knows_about_stitching():
    """remote_fetcher reads bytes over SFTP. It has no business importing Stage 2."""
    from app.services.mnp_log_ingestion.remote import remote_fetcher
    src = inspect.getsource(remote_fetcher)
    assert "finalize_pending" not in src
    assert "_do_finalize" not in src


def test_watcher_no_longer_calls_stage2():
    from app.services.workers import log_watcher
    src = inspect.getsource(log_watcher)
    assert "finalize_pending" not in src


def test_parse_worker_no_longer_calls_stage2():
    from app.services.workers import log_parse_worker
    src = inspect.getsource(log_parse_worker)
    assert "finalize_pending" not in src


def test_explicit_user_triggered_stitching_is_retained():
    """Removing the BACKGROUND triggers must not remove the two paths where a user explicitly asks
    for stitching right now - those are synchronous intent, not scheduling."""
    from app.api.v1 import logs as logs_api
    src = inspect.getsource(logs_api)
    assert "finalize_pending" in src          # read_pending_state, ?finalize=true
    assert "run_finalize_tracked" in src      # POST /logs/regroup/finalize


async def test_stitch_worker_registered_and_enabled_by_default(monkeypatch):
    from app import background as bg
    from tests.test_background_workers_chunk10 import _stub_loops

    _stub_loops(monkeypatch)

    async def _noop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_log_stitch_worker", _noop)
    monkeypatch.setattr(settings, "ssh_log_fetcher_enabled", False)
    monkeypatch.setattr(settings, "logspace_cleanup_worker_enabled", False)
    monkeypatch.setattr(settings, "log_parse_worker_enabled", False)

    # Isolate the FLAG from the leftover-work safety net (tested separately below): with a real
    # backlog present the worker starts either way, which is correct but would mask the flag.
    async def _no_backlog():
        return 0

    monkeypatch.setattr(bg, "pending_backlog", _no_backlog)

    monkeypatch.setattr(settings, "log_stitch_worker_enabled", True)
    on = len(await bg.start_background_tasks())

    monkeypatch.setattr(settings, "log_stitch_worker_enabled", False)
    off = len(await bg.start_background_tasks())

    assert on == off + 1


async def test_stitch_worker_starts_anyway_when_windows_are_open(monkeypatch):
    """Rollback safety. Nothing else calls Stage 2 any more, so switching the flag off while windows
    are still open would leave that data unstitched with no other path to recover it."""
    from app import background as bg
    from tests.test_background_workers_chunk10 import _stub_loops

    _stub_loops(monkeypatch)

    async def _noop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_log_stitch_worker", _noop)
    for name in ("ssh_log_fetcher_enabled",
                 "logspace_cleanup_worker_enabled", "log_parse_worker_enabled",
                 "log_stitch_worker_enabled"):
        monkeypatch.setattr(settings, name, False)

    async def _empty():
        return 0

    async def _backlog():
        return 7

    monkeypatch.setattr(bg, "pending_backlog", _empty)
    baseline = len(await bg.start_background_tasks())

    monkeypatch.setattr(bg, "pending_backlog", _backlog)
    with_backlog = len(await bg.start_background_tasks())

    assert with_backlog == baseline + 1


# =============================================================== worker loop + policy edge cases
async def test_worker_loop_drains_then_sleeps_and_stops_on_cancel(monkeypatch):
    """The forever loop itself: it must drain, sleep its cadence, survive an error, and stop ONLY on
    CancelledError. Driven by making sleep raise after N ticks, since the loop never returns."""
    ticks = {"drain": 0, "sleep": 0}

    async def _drain():
        ticks["drain"] += 1
        if ticks["drain"] == 2:
            raise RuntimeError("transient blow-up mid-loop")   # must NOT stop the worker
        return {"customers": 1, "windows": 1, "consumed": 1, "abandoned": 0, "failed": 0}

    async def _sleep(_seconds):
        ticks["sleep"] += 1
        if ticks["sleep"] >= 3:
            raise asyncio.CancelledError()

    monkeypatch.setattr(lsw, "drain_once", _drain)
    monkeypatch.setattr(lsw.asyncio, "sleep", _sleep)

    with pytest.raises(asyncio.CancelledError):
        await lsw.run_log_stitch_worker()

    assert ticks["drain"] == 3, "the loop must keep going after an error"
    assert ticks["sleep"] == 3


async def test_pending_backlog_counts_open_windows_regardless_of_backoff():
    """Used at startup to decide whether the worker must run even with the flag off, so it counts
    ALL open windows - a backing-off one is still work that needs draining."""
    cc = "TEST_CHUNK18_BACKLOG"
    await _cleanup(cc)
    await _mk_pending(cc, available_at=datetime.now(timezone.utc) + timedelta(hours=1))
    try:
        assert await lsw.pending_backlog() >= 1
    finally:
        await _cleanup(cc)


def test_policy_follows_the_cause_chain_and_survives_a_self_reference():
    """A wrapped error must be classified by what actually went wrong. The self-referential guard
    exists because `raise X from X` is legal and would otherwise loop forever."""
    inner = ValueError("unparseable")
    outer = RuntimeError("stage 2 failed")
    outer.__cause__ = inner
    assert retry_policy.is_transient(outer) is False, "must see through the wrapper"

    loop = RuntimeError("self-caused")
    loop.__cause__ = loop
    assert retry_policy.is_transient(loop) is True, "must terminate, not hang"


def test_policy_defaults_an_unrecognised_error_to_transient():
    class _Odd(Exception):
        pass
    assert retry_policy.is_transient(_Odd("who knows")) is True
