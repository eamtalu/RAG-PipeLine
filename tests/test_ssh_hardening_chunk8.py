"""Chunk 8: the fetch path does not stitch — Stage 2 has its own consumer.

ORIGINAL problem (2026-07): fetch_now finalized on the caller's `db`, which sits idle across the
(possibly long) SFTP transfer. A dropped connection made finalize raise, and on the poller path that
exception was swallowed, so log_regroup_pending accumulated silently and the "New data available"
banner stuck. That was fixed by finalizing on a FRESH session and surfacing the failure.

SUPERSEDED (2026-08, chunk 18): the transport no longer calls Stage 2 at all. Stage 1 writes a
log_regroup_pending ticket in the same transaction as its entries, and the stitch worker
(app/services/workers/log_stitch_worker.py) owns draining that queue. The whole class of bug above
is now structurally impossible on this path, because the path no longer exists.

What is covered here now:
- the transport imports and calls NOTHING from Stage 2 (the regression guard for that removal);
- fetch_now returns pull statistics only, and never a stitch result;
- a tracked run reports the PULL outcome, and is no longer failed by a stitching problem — stitch
  failures are visible on log_regroup_pending and via GET /logs/regroup/status instead.
"""

import inspect
import uuid

import pytest

from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_fetch_run import (
    LogSshFetchRun, LogSshFetchRunStatus, LogSshFetchMode,
)
from app.services.mnp_log_ingestion.remote import remote_fetcher as r


def test_transport_does_not_import_or_call_stage2():
    """The regression guard for chunk 18. If a future ingestion path re-adds a finalize call here,
    we are back to every producer having to remember it — and to a new path silently forgetting."""
    src = inspect.getsource(r)
    assert "finalize_pending" not in src
    assert "_do_finalize" not in src
    assert "derive_transactions" not in src


async def test_fetch_now_returns_pull_stats_only(monkeypatch):
    """No 'finalize' key, and no 'finalize_error': stitching is not this function's outcome."""
    async def _no_sources(db, customer_code, source_id, **kw):
        return []

    monkeypatch.setattr(r, "_load_sources", _no_sources)
    async with async_session() as db:
        agg = await r.fetch_now(db, "TEST_CHUNK8")
    assert "finalize" not in agg
    assert "finalize_error" not in agg
    assert agg["customer_code"] == "TEST_CHUNK8"
    assert agg["files_fetched"] == 0


async def test_tracked_run_reports_the_pull_outcome(monkeypatch):
    async with async_session() as s:
        run = LogSshFetchRun(customer_code="TEST_CHUNK8", mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        await s.refresh(run)
        rid = run.id

    async def fake_fetch_now(db, customer_code, **kw):
        return {"files_fetched": 1, "entries_ingested": 5, "bytes_fetched": 10,
                "files_considered": 1}   # pull stats only — stitching is not reported here

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
