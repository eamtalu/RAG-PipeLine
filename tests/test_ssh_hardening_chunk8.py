"""Chunk 8: Stage 2 finalize on the fetch path is robust and observable.

Root cause fixed: fetch_now finalized on the caller's `db`, which sits idle across the (possibly
long) SFTP transfer — a dropped connection then made finalize raise, and on the poller path that
exception was swallowed, so log_regroup_pending accumulated silently (the "New data available" banner
stuck / climbing). Now finalize runs on a FRESH session and its failure is surfaced (agg
["finalize_error"], a failed run row for manual, an error log for the poller) instead of being lost.

Covered:
- fetch_now finalizes on a fresh session, not the caller's db (which is closed by then);
- a finalize failure does NOT raise out of fetch_now — it lands in agg["finalize_error"];
- a successful finalize populates agg["finalize"] with no error;
- run_ssh_fetch_tracked marks the run FAILED (with a clear message) when finalize failed even though
  the pull succeeded;
- the empty-sources early-return path also finalizes via the fresh session.
"""

import uuid

import pytest

from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode,
)
from app.services.mnp_log_ingestion.remote import remote_fetcher as r


async def test_finalize_runs_on_fresh_session_not_caller_db(monkeypatch):
    """The caller's db is expunged+rolled-back before the network loop; finalize must NOT use it.
    We assert the session passed to finalize_pending is a *different, open* session."""
    seen = {}

    async def fake_finalize(db, cc):
        seen["is_active"] = db.is_active           # a fresh session is active/usable
        seen["cc"] = cc
        return {"mode": "finalize", "windows": 0}

    # no sources -> the empty-sources branch, which still finalizes
    async def no_sources(*a, **k):
        return []

    monkeypatch.setattr(r, "finalize_pending", fake_finalize)
    monkeypatch.setattr(r, "_load_sources", no_sources)

    async with async_session() as caller_db:
        agg = await r.fetch_now(caller_db, "TEST_CHUNK8")
    assert seen["is_active"] is True               # ran on a live session
    assert seen["cc"] == "TEST_CHUNK8"
    assert agg["finalize"] == {"mode": "finalize", "windows": 0}
    assert "finalize_error" not in agg


async def test_finalize_failure_is_surfaced_not_swallowed(monkeypatch):
    async def boom_finalize(db, cc):
        raise RuntimeError("connection was closed in the middle of operation")

    async def no_sources(*a, **k):
        return []

    monkeypatch.setattr(r, "finalize_pending", boom_finalize)
    monkeypatch.setattr(r, "_load_sources", no_sources)

    async with async_session() as caller_db:
        agg = await r.fetch_now(caller_db, "TEST_CHUNK8")  # must NOT raise
    assert "finalize_error" in agg
    assert "connection was closed" in agg["finalize_error"]
    assert agg["finalize"]["error"] == agg["finalize_error"]


async def test_tracked_run_marked_failed_when_finalize_fails(monkeypatch):
    # a run row to update
    async with async_session() as s:
        run = LogSshFetchRun(customer_code="TEST_CHUNK8", mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        rid = run.id

    async def fake_fetch_now(db, customer_code, **kw):
        # pull succeeded (some entries) but stitching failed
        return {"files_fetched": 2, "entries_ingested": 40, "bytes_fetched": 100,
                "files_considered": 2, "finalize_error": "connection was closed"}

    monkeypatch.setattr(r, "fetch_now", fake_fetch_now)
    try:
        await r.run_ssh_fetch_tracked(rid, "TEST_CHUNK8", None, LogSshFetchMode.incremental, None)
        async with async_session() as s:
            row = await s.get(LogSshFetchRun, rid)
        assert row.status == LogSshFetchRunStatus.failed          # not 'completed'
        assert "stitching failed" in (row.error or "")
        assert row.entries_ingested == 40                         # the pull result is still recorded
    finally:
        from sqlalchemy import delete
        async with async_session() as s:
            await s.execute(delete(LogSshFetchRun).where(LogSshFetchRun.id == rid))
            await s.commit()


async def test_tracked_run_completed_when_finalize_ok(monkeypatch):
    async with async_session() as s:
        run = LogSshFetchRun(customer_code="TEST_CHUNK8", mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        rid = run.id

    async def fake_fetch_now(db, customer_code, **kw):
        return {"files_fetched": 1, "entries_ingested": 5, "bytes_fetched": 10,
                "files_considered": 1, "finalize": {"windows": 1}}  # no finalize_error

    monkeypatch.setattr(r, "fetch_now", fake_fetch_now)
    try:
        await r.run_ssh_fetch_tracked(rid, "TEST_CHUNK8", None, LogSshFetchMode.incremental, None)
        async with async_session() as s:
            row = await s.get(LogSshFetchRun, rid)
        assert row.status == LogSshFetchRunStatus.completed
        assert row.error is None
    finally:
        from sqlalchemy import delete
        async with async_session() as s:
            await s.execute(delete(LogSshFetchRun).where(LogSshFetchRun.id == rid))
            await s.commit()
