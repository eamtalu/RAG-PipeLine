"""Chunk 4 of the SSH log-fetch hardening: per-customer poller (gap 7), outage circuit breaker +
computed status + last_attempt_at (gap 9), and stuck-run recovery (gap 4).

Edge cases / exceptional scenarios covered:
- _record_failure increments consecutive_failures and auto-disables at the threshold (only when
  driven by the poller and the source is still enabled); _record_success resets the breaker.
- drive_breaker=False never touches the counter/enabled; ssh_auto_disable_after_failures=0 disables
  the breaker (never auto-disables) while still counting.
- last_attempt_at is stamped on both success and failure; last_ok_at only on success.
- _status derives every state (live/stale/degraded/pending/auto_disabled/disabled); _to_out exposes
  the new fields and never leaks secrets.
- The update endpoint resets the breaker on re-enable.
- sweep_stale_runs flips a `running` run to failed.
- run_ssh_fetch_tracked marks its run failed when cancelled.
- Poller: _reconcile spawns/reaps/restarts loops; _customer_interval resolves the min/fallback;
  _customer_loop survives an exception and keeps polling.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode, LogSshFetchPhase,
)
from app.services.mnp_log_ingestion.remote import remote_fetcher as r
from app.services.workers import ssh_log_fetcher as w
from app.api.v1 import log_sources as api


async def _set_enabled(source_id, value):
    async with async_session() as s:
        row = await s.get(LogSshSource, source_id)
        row.enabled = value
        await s.commit()


async def _reload(source_id) -> LogSshSource:
    async with async_session() as s:
        return await s.get(LogSshSource, source_id)


# =========================================================== gap 9: circuit breaker
async def test_breaker_increments_then_auto_disables(committed_source, monkeypatch):
    monkeypatch.setattr(settings, "ssh_auto_disable_after_failures", 3)
    src = committed_source
    await _set_enabled(src.id, True)
    src.enabled = True

    assert await r._record_failure(src, Exception("boom1"), drive_breaker=True) is False
    assert (await _reload(src.id)).consecutive_failures == 1
    assert await r._record_failure(src, Exception("boom2"), drive_breaker=True) is False
    assert (await _reload(src.id)).consecutive_failures == 2
    # third failure hits the threshold -> auto-disable
    assert await r._record_failure(src, Exception("boom3"), drive_breaker=True) is True
    row = await _reload(src.id)
    assert row.consecutive_failures == 3
    assert row.enabled is False
    assert row.auto_disabled_at is not None
    assert "Auto-disabled" in row.last_error


async def test_breaker_success_resets(committed_source, monkeypatch):
    monkeypatch.setattr(settings, "ssh_auto_disable_after_failures", 2)
    src = committed_source
    await _set_enabled(src.id, True)
    src.enabled = True
    await r._record_failure(src, Exception("x"), drive_breaker=True)
    await r._record_success(src, drive_breaker=True)
    row = await _reload(src.id)
    assert row.consecutive_failures == 0
    assert row.auto_disabled_at is None
    assert row.last_error is None
    assert row.last_ok_at is not None


async def test_breaker_not_driven_when_manual(committed_source):
    src = committed_source
    await _set_enabled(src.id, True)
    src.enabled = True
    # drive_breaker=False (manual/on-demand): only stamps last_attempt_at + last_error
    assert await r._record_failure(src, Exception("m"), drive_breaker=False) is False
    row = await _reload(src.id)
    assert row.consecutive_failures == 0    # untouched
    assert row.enabled is True              # not disabled
    assert row.last_error == "m"
    assert row.last_attempt_at is not None


async def test_breaker_off_when_threshold_zero(committed_source, monkeypatch):
    monkeypatch.setattr(settings, "ssh_auto_disable_after_failures", 0)  # breaker disabled
    src = committed_source
    await _set_enabled(src.id, True)
    src.enabled = True
    for i in range(5):
        assert await r._record_failure(src, Exception(str(i)), drive_breaker=True) is False
    row = await _reload(src.id)
    assert row.consecutive_failures == 5    # still counts
    assert row.enabled is True              # but never auto-disables


async def test_last_attempt_vs_last_ok(committed_source):
    src = committed_source
    await r._record_failure(src, Exception("e"), drive_breaker=False)
    row = await _reload(src.id)
    assert row.last_attempt_at is not None and row.last_ok_at is None  # a failed attempt
    await r._record_success(src, drive_breaker=False)
    row = await _reload(src.id)
    assert row.last_ok_at is not None and row.last_attempt_at is not None


# =========================================================== gap 9: computed status + _to_out
def _mk(**over) -> LogSshSource:
    base = dict(customer_code="C", name="n", host="h", port=22, username="u", remote_log_dir="/",
                enabled=True, poll_interval_seconds=None, auto_disabled_at=None,
                last_attempt_at=None, last_ok_at=None, last_error=None, consecutive_failures=0,
                created_at=None, updated_at=None)
    base.update(over)
    return LogSshSource(**base)


def test_status_disabled_and_auto_disabled():
    assert api._status(_mk(enabled=False, auto_disabled_at=None)) == "disabled"
    assert api._status(_mk(enabled=False, auto_disabled_at=datetime.now(timezone.utc))) == "auto_disabled"


def test_status_pending_degraded_live_stale():
    now = datetime.now(timezone.utc)
    assert api._status(_mk(enabled=True, last_attempt_at=None)) == "pending"
    assert api._status(_mk(enabled=True, last_attempt_at=now, last_error="oops")) == "degraded"
    assert api._status(_mk(enabled=True, last_attempt_at=now, last_ok_at=now)) == "live"
    old = now - timedelta(seconds=10 * settings.ssh_log_fetcher_poll_seconds)
    assert api._status(_mk(enabled=True, last_attempt_at=now, last_ok_at=old)) == "stale"


def test_to_out_exposes_new_fields_and_hides_secrets():
    src = _mk(enabled=True, last_attempt_at=datetime.now(timezone.utc),
              last_ok_at=datetime.now(timezone.utc), consecutive_failures=2)
    src.private_key_path = "/secret/key"
    src.private_key_enc = "ENCRYPTED"
    out = api._to_out(src)
    for key in ("status", "last_attempt_at", "consecutive_failures", "effective_poll_seconds",
                "auto_disabled_at"):
        assert key in out
    assert out["status"] == "live"
    assert out["consecutive_failures"] == 2
    assert out["effective_poll_seconds"] == settings.ssh_log_fetcher_poll_seconds
    # never leak key material
    assert "private_key" not in out and "private_key_enc" not in out and "key_passphrase_enc" not in out
    assert out["auth_method"] == "path"


# =========================================================== gap 9: update endpoint resets breaker
class _FakeRepo:
    def __init__(self, src):
        self._src = src
        self.updated = None

    async def get(self, customer, sid):
        return self._src

    async def update(self, src, **values):
        self.updated = values
        return src


async def test_update_endpoint_resets_breaker_on_reenable(committed_source):
    repo = _FakeRepo(committed_source)
    body = api.SshSourceUpdate(enabled=True)
    await api.update_ssh_source(committed_source.id, body, customer=committed_source.customer_code, repo=repo)
    assert repo.updated["consecutive_failures"] == 0
    assert repo.updated["auto_disabled_at"] is None


async def test_update_endpoint_no_breaker_reset_when_enable_absent(committed_source):
    repo = _FakeRepo(committed_source)
    body = api.SshSourceUpdate(file_glob="*.txt")  # unrelated change; enabled not set
    await api.update_ssh_source(committed_source.id, body, customer=committed_source.customer_code, repo=repo)
    assert "consecutive_failures" not in repo.updated
    assert "auto_disabled_at" not in repo.updated


# =========================================================== gap 4: stuck-run sweep + cancel
async def test_sweep_stale_runs_marks_running_as_failed():
    async with async_session() as s:
        run = LogSshFetchRun(customer_code="TEST_CHUNK4", mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        rid = run.id
    try:
        swept = await r.sweep_stale_runs()
        assert swept >= 1
        row = await _get_run(rid)
        assert row.status == LogSshFetchRunStatus.failed
        assert row.phase == LogSshFetchPhase.done
        assert row.finished_at is not None
    finally:
        await _delete_run(rid)


async def test_tracked_run_marked_failed_on_cancel(monkeypatch):
    async with async_session() as s:
        run = LogSshFetchRun(customer_code="TEST_CHUNK4", mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        rid = run.id
    try:
        async def slow_fetch(*a, **k):
            await asyncio.sleep(30)
        monkeypatch.setattr(r, "fetch_now", slow_fetch)
        task = asyncio.create_task(
            r.run_ssh_fetch_tracked(rid, "TEST_CHUNK4", None, LogSshFetchMode.incremental, None))
        await asyncio.sleep(0.15)  # let it enter fetch_now
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        row = await _get_run(rid)
        assert row.status == LogSshFetchRunStatus.failed
        assert "Cancelled" in (row.error or "")
    finally:
        await _delete_run(rid)


async def _get_run(rid):
    async with async_session() as s:
        return await s.get(LogSshFetchRun, rid)


async def _delete_run(rid):
    from sqlalchemy import delete
    async with async_session() as s:
        await s.execute(delete(LogSshFetchRun).where(LogSshFetchRun.id == rid))
        await s.commit()


# =========================================================== gap 7: poller supervisor + loops
class _FakeTask:
    def __init__(self):
        self.cancelled = False
        self._done = False

    def done(self):
        return self._done

    def cancel(self):
        self.cancelled = True


def test_reconcile_spawns_reaps_and_restarts():
    loops: dict = {}
    made = []

    def make(cc):
        t = _FakeTask()
        made.append((cc, t))
        return t

    # spawn A + B
    w._reconcile(loops, {"A", "B"}, make)
    assert set(loops) == {"A", "B"}
    # B departs -> cancelled and dropped
    w._reconcile(loops, {"A"}, make)
    assert set(loops) == {"A"}
    b_task = next(t for cc, t in made if cc == "B")
    assert b_task.cancelled is True
    # A's loop finished unexpectedly -> restarted with a new task
    old_a = loops["A"]
    old_a._done = True
    w._reconcile(loops, {"A"}, make)
    assert loops["A"] is not old_a
    assert loops["A"].done() is False


async def test_customer_interval_min_and_fallback():
    cc = f"TEST_CHUNK4_INT_{uuid.uuid4().hex[:6]}"
    ids = []
    async with async_session() as s:
        for name, interval in [("a", 90.0), ("b", 30.0)]:
            src = LogSshSource(customer_code=cc, name=name, host="h", username="u",
                               remote_log_dir="/", enabled=True, poll_interval_seconds=interval)
            s.add(src)
        await s.commit()
    try:
        assert await w._customer_interval(cc) == 30.0  # min of the enabled sources
    finally:
        from sqlalchemy import delete
        async with async_session() as s:
            await s.execute(delete(LogSshSource).where(LogSshSource.customer_code == cc))
            await s.commit()

    # fallback to the global default when a customer's sources set no interval
    cc2 = f"TEST_CHUNK4_INT2_{uuid.uuid4().hex[:6]}"
    async with async_session() as s:
        s.add(LogSshSource(customer_code=cc2, name="c", host="h", username="u",
                           remote_log_dir="/", enabled=True, poll_interval_seconds=None))
        await s.commit()
    try:
        assert await w._customer_interval(cc2) == settings.ssh_log_fetcher_poll_seconds
    finally:
        from sqlalchemy import delete
        async with async_session() as s:
            await s.execute(delete(LogSshSource).where(LogSshSource.customer_code == cc2))
            await s.commit()


async def test_customer_loop_survives_errors_and_keeps_polling(monkeypatch):
    calls = {"n": 0}
    done = asyncio.Event()

    async def fake_poll(customer_code):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient boom")  # must NOT kill the loop
        if calls["n"] >= 3:
            done.set()
        return {}

    async def fast_interval(customer_code):
        return 0.001

    monkeypatch.setattr(w, "_poll_customer_once", fake_poll)
    monkeypatch.setattr(w, "_customer_interval", fast_interval)

    task = asyncio.create_task(w._customer_loop("C"))
    try:
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert calls["n"] >= 3  # survived the exception on call #1 and kept polling
