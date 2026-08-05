"""Chunk 5 of the SSH log-fetch hardening: windowed resume (gap 8) — force_remote + timestamp
forward-only seed — the manual-vs-auto ownership contract (409s), and the run history + cancel
endpoints (with the `cancelled` status).

Edge cases / exceptional scenarios covered:
- force_remote bypasses the timestamp local-coverage short-circuit; without it, coverage suppresses.
- timestamp mode seeds pre-window (non-selected) files to EOF WITHOUT ingesting, so a later
  incremental poll skips them (forward-only resume).
- fetch_remote 409s a manual fetch of an enabled source and 409s (echoing run_id) when a run is
  already in progress.
- cancel: marks a running run cancelled + cancels the task; 409 on a terminal run; 404 cross-tenant.
- history: newest-first, tenant-scoped, filterable.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode, LogSshFetchPhase,
)
from app.services.mnp_log_ingestion.remote import remote_fetcher as r
from app.api.v1 import log_sources as api
from tests.test_ssh_hardening_chunk2 import _patch_sftp, _patch_ingest_counts_lines


# =========================================================== gap 8: force_remote
async def test_force_remote_bypasses_timestamp_shortcircuit(committed_source, monkeypatch):
    src = committed_source
    old = datetime.now(timezone.utc) - timedelta(days=1)

    async def fake_min(db, cc):
        return old  # local data already "covers" any recent from_ts

    called = {"n": 0}

    async def fake_fetch_source(*a, **k):
        called["n"] += 1
        return {"source": "x", "files_considered": 0, "files_fetched": 0,
                "bytes_fetched": 0, "entries_ingested": 0, "by_file": []}

    async def noop_finalize(db, cc):
        return {"windows": 0}

    monkeypatch.setattr(r, "_local_min_ts", fake_min)
    monkeypatch.setattr(r, "_fetch_source", fake_fetch_source)
    from_ts = datetime.now(timezone.utc)

    async with async_session() as db:
        agg1 = await r.fetch_now(db, src.customer_code, source_id=src.id,
                                 mode=LogSshFetchMode.timestamp, from_ts=from_ts, force_remote=False)
    assert agg1.get("already_local") is True
    assert called["n"] == 0  # short-circuited: servers untouched

    async with async_session() as db:
        agg2 = await r.fetch_now(db, src.customer_code, source_id=src.id,
                                 mode=LogSshFetchMode.timestamp, from_ts=from_ts, force_remote=True)
    assert "already_local" not in agg2
    assert called["n"] == 1  # forced through to the fetch


# =========================================================== gap 8: forward-only seed
async def test_timestamp_seeds_prewindow_files_to_eof(committed_source, monkeypatch):
    src = committed_source
    recent, old1, old2 = "C:/logs/recent.log", "C:/logs/old1.log", "C:/logs/old2.log"
    files = {
        recent: (b"r\n" * 3, 2000.0),  # mtime >= cutoff -> selected -> pulled
        old1:   (b"a\n" * 2, 1000.0),  # newest of the older files -> selected -> pulled
        old2:   (b"b\n" * 5, 500.0),   # older, not newest -> NOT selected -> seeded to EOF
    }
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)
    from_ts = datetime.fromtimestamp(1500.0, tz=timezone.utc)  # cutoff = 1500

    stats = await r._fetch_source(src, LogSshFetchMode.timestamp, from_ts)
    assert stats["files_fetched"] == 2                 # recent + old1 pulled; old2 only seeded
    assert stats["entries_ingested"] == 5              # 3 + 2 ; old2's 5 lines NOT ingested

    ckpts = await r._load_ckpts(src)
    assert ckpts[old2][:3] == (len(files[old2][0]), 500.0, len(files[old2][0]))  # seeded offset == size

    # a later incremental poll skips old2 (its checkpoint was seeded to the end)
    _patch_ingest_counts_lines(monkeypatch)
    s2 = await r._fetch_source(src, LogSshFetchMode.incremental, None)
    pulled = {f["file"] for f in s2["by_file"]}
    assert old2 not in pulled  # forward-only: no backfill of the pre-window file


# =========================================================== gap 8: ownership 409s
async def _mk_run(customer, source_id=None, status=LogSshFetchRunStatus.running):
    async with async_session() as s:
        run = LogSshFetchRun(customer_code=customer, source_id=source_id,
                             mode=LogSshFetchMode.incremental, status=status)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        return run.id


async def _del_run(rid):
    from sqlalchemy import delete
    async with async_session() as s:
        await s.execute(delete(LogSshFetchRun).where(LogSshFetchRun.id == rid))
        await s.commit()


class _FakeRepo:
    def __init__(self, src):
        self._src = src

    async def get(self, customer, sid):
        return self._src


async def test_fetch_remote_409_when_source_enabled(committed_source):
    src = committed_source
    src.enabled = True  # the FakeRepo returns this object; simulate an auto-polled source
    body = api.FetchRemoteRequest(source_id=src.id)
    async with async_session() as db:
        with pytest.raises(HTTPException) as ei:
            await api.fetch_remote(body, customer=src.customer_code, db=db, repo=_FakeRepo(src))
    assert ei.value.status_code == 409
    assert "auto-polled" in ei.value.detail


async def test_fetch_remote_409_when_already_running(committed_source):
    src = committed_source            # disabled (default) -> passes the ownership check
    rid = await _mk_run(src.customer_code, source_id=src.id, status=LogSshFetchRunStatus.running)
    try:
        body = api.FetchRemoteRequest(source_id=src.id)
        async with async_session() as db:
            with pytest.raises(HTTPException) as ei:
                await api.fetch_remote(body, customer=src.customer_code, db=db, repo=_FakeRepo(src))
        assert ei.value.status_code == 409
        assert ei.value.detail["run_id"] == str(rid)   # echoes the in-flight run
    finally:
        await _del_run(rid)


# =========================================================== cancel endpoint
async def test_cancel_running_run(monkeypatch):
    cc = "TEST_CHUNK5"
    rid = await _mk_run(cc, status=LogSshFetchRunStatus.running)

    class _FakeTask:
        def __init__(self):
            self.cancelled = False

        def cancel(self):
            self.cancelled = True

    task = _FakeTask()
    api._fetch_tasks[rid] = task
    try:
        async with async_session() as db:
            out = await api.cancel_fetch_run(rid, customer=cc, db=db)
        assert out["status"] == "cancelled"
        assert task.cancelled is True
        async with async_session() as db:
            row = await db.get(LogSshFetchRun, rid)
        assert row.status == LogSshFetchRunStatus.cancelled
        assert row.phase == LogSshFetchPhase.done
        assert row.finished_at is not None
    finally:
        api._fetch_tasks.pop(rid, None)
        await _del_run(rid)


async def test_cancel_terminal_run_is_409():
    cc = "TEST_CHUNK5"
    rid = await _mk_run(cc, status=LogSshFetchRunStatus.completed)
    try:
        async with async_session() as db:
            with pytest.raises(HTTPException) as ei:
                await api.cancel_fetch_run(rid, customer=cc, db=db)
        assert ei.value.status_code == 409
    finally:
        await _del_run(rid)


async def test_cancel_other_tenant_is_404():
    rid = await _mk_run("TENANT_A", status=LogSshFetchRunStatus.running)
    try:
        async with async_session() as db:
            with pytest.raises(HTTPException) as ei:
                await api.cancel_fetch_run(rid, customer="TENANT_B", db=db)
        assert ei.value.status_code == 404
    finally:
        await _del_run(rid)


# =========================================================== history endpoint
async def test_list_fetch_runs_tenant_scoped_and_ordered():
    cc = f"TEST_CHUNK5_{uuid.uuid4().hex[:6]}"
    other = f"OTHER_{uuid.uuid4().hex[:6]}"
    r1 = await _mk_run(cc, status=LogSshFetchRunStatus.completed)
    r2 = await _mk_run(cc, status=LogSshFetchRunStatus.failed)
    r3 = await _mk_run(other, status=LogSshFetchRunStatus.running)
    try:
        async with async_session() as db:
            out = await api.list_fetch_runs(customer=cc, db=db, source_id=None, status=None,
                                            limit=50, offset=0)
        ids = [row["run_id"] for row in out["runs"]]
        assert str(r1) in ids and str(r2) in ids
        assert str(r3) not in ids                      # other tenant's run excluded
        # newest first (r2 created after r1)
        assert ids.index(str(r2)) < ids.index(str(r1))
        # status filter
        async with async_session() as db:
            only_failed = await api.list_fetch_runs(customer=cc, db=db, source_id=None,
                                                    status=LogSshFetchRunStatus.failed,
                                                    limit=50, offset=0)
        assert [row["run_id"] for row in only_failed["runs"]] == [str(r2)]
    finally:
        for rid in (r1, r2, r3):
            await _del_run(rid)
