"""Chunk 10: background loops run in exactly one process (web / worker split).

Root cause addressed (see docs/background-workers-web-worker-split.md): gunicorn -w N ran the FastAPI
lifespan in every worker, so N copies of every background loop started. The loops now live in
app.background and start in one place: the web tier only when run_background_workers is true, and the
dedicated app.worker process (guarded by a singleton advisory lock).

Covered:
- the web lifespan starts the loops only when run_background_workers is true;
- start_background_tasks assembles exactly the enabled loops and registers the notification dispatcher
  only when notifications are enabled;
- the worker singleton advisory lock is mutually exclusive (a second worker cannot double-run).
"""

import asyncio

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app import main as appmain
from app import background as bg
from app.settings import settings
from app.worker import _SINGLETON_CLASSID, _SINGLETON_OBJID


async def _noop_loop():
    await asyncio.sleep(3600)


def _stub_loops(monkeypatch):
    """Replace every real loop + the startup sweep with harmless stubs, and count register() calls."""
    for name in ("run_worker", "run_log_watcher", "run_log_stitch_worker", "run_log_parse_worker",
                 "run_ssh_log_fetcher", "run_notification_worker", "run_logspace_cleanup_worker",
                 "run_log_partition_worker"):
        monkeypatch.setattr(bg, name, _noop_loop)

    async def _sweep0():
        return 0
    monkeypatch.setattr(bg, "sweep_stale_runs", _sweep0)

    # Both queue workers also start when leftover work exists (rollback safety), which would make
    # these counts depend on whatever rows happen to be in the shared test DB. Pin them to empty so
    # the tests measure the FLAGS only; the leftover-work behaviour has its own tests.
    async def _zero():
        return 0
    monkeypatch.setattr(bg, "pending_backlog", _zero)
    monkeypatch.setattr(bg, "unfinished_ingest_objects", _zero)

    reg = {"n": 0}
    monkeypatch.setattr(bg.notification_dispatcher, "register", lambda: reg.__setitem__("n", reg["n"] + 1))
    return reg


async def test_lifespan_starts_loops_only_when_enabled(monkeypatch):
    called = {"start": 0}

    async def fake_start():
        called["start"] += 1
        return []

    async def fake_stop(tasks):
        return None

    monkeypatch.setattr(appmain, "start_background_tasks", fake_start)
    monkeypatch.setattr(appmain, "stop_background_tasks", fake_stop)

    monkeypatch.setattr(settings, "run_background_workers", False)
    async with appmain.lifespan(appmain.app):
        pass
    assert called["start"] == 0                 # web tier: loops NOT started

    monkeypatch.setattr(settings, "run_background_workers", True)
    async with appmain.lifespan(appmain.app):
        pass
    assert called["start"] == 1                 # single-process / worker default: loops started


async def test_start_background_tasks_assembles_enabled_loops(monkeypatch):
    reg = _stub_loops(monkeypatch)

    # defaults-ish: stitch off, ssh on, notifications off, cleanup off
    # -> embedding + watcher + ssh + partition.
    # The partition worker is NOT in that off-list on purpose: unlike the queue workers it has no
    # durable backlog to fall back on, so it runs by default and is only silenced explicitly.
    monkeypatch.setattr(settings, "log_stitch_worker_enabled", False)
    monkeypatch.setattr(settings, "ssh_log_fetcher_enabled", True)
    monkeypatch.setattr(settings, "notifications_enabled", False)
    monkeypatch.setattr(settings, "logspace_cleanup_worker_enabled", False)
    monkeypatch.setattr(settings, "log_parse_worker_enabled", False)
    tasks = await bg.start_background_tasks()
    try:
        assert len(tasks) == 4
        assert reg["n"] == 0                     # dispatcher NOT registered when notifications off
    finally:
        await bg.stop_background_tasks(tasks)
    assert all(t.cancelled() or t.done() for t in tasks)   # stop cancels everything

    # everything on -> embedding + watcher + stitch + ssh + parse + notifications + cleanup
    #                  + partition = 8
    monkeypatch.setattr(settings, "log_stitch_worker_enabled", True)
    monkeypatch.setattr(settings, "notifications_enabled", True)
    monkeypatch.setattr(settings, "logspace_cleanup_worker_enabled", True)
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    tasks = await bg.start_background_tasks()
    try:
        assert len(tasks) == 8
        assert reg["n"] == 1                     # dispatcher registered exactly once
    finally:
        await bg.stop_background_tasks(tasks)


async def test_the_partition_worker_can_be_turned_off(monkeypatch):
    """It defaults ON because nothing else provisions partitions, but an operator managing them by
    hand must still be able to silence it — and the count must actually drop when they do."""
    _stub_loops(monkeypatch)
    for flag in ("log_stitch_worker_enabled", "notifications_enabled",
                 "logspace_cleanup_worker_enabled", "log_parse_worker_enabled",
                 "ssh_log_fetcher_enabled"):
        monkeypatch.setattr(settings, flag, False)

    monkeypatch.setattr(settings, "log_partition_worker_enabled", True)
    tasks = await bg.start_background_tasks()
    with_worker = len(tasks)
    await bg.stop_background_tasks(tasks)

    monkeypatch.setattr(settings, "log_partition_worker_enabled", False)
    tasks = await bg.start_background_tasks()
    try:
        assert len(tasks) == with_worker - 1
    finally:
        await bg.stop_background_tasks(tasks)


async def test_worker_singleton_lock_is_exclusive():
    """The second acquirer must fail, which is what makes a second worker exit instead of double-run."""
    e1 = create_async_engine(settings.database_url, poolclass=NullPool)
    e2 = create_async_engine(settings.database_url, poolclass=NullPool)
    c1 = await e1.connect()
    c2 = await e2.connect()
    try:
        got1 = bool(await c1.scalar(select(func.pg_try_advisory_lock(_SINGLETON_CLASSID, _SINGLETON_OBJID))))
        got2 = bool(await c2.scalar(select(func.pg_try_advisory_lock(_SINGLETON_CLASSID, _SINGLETON_OBJID))))
        assert got1 is True                       # first worker wins
        assert got2 is False                      # second worker is locked out (will exit)
    finally:
        await c1.close()                          # releasing lets the next worker take over
        await c2.close()
        await e1.dispose()
        await e2.dispose()
