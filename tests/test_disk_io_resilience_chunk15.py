"""Chunk 15: resilience to disk I/O (bad-sector) errors on the failing production HDD.

Context (see the disk-io-resilience plan): /dev/sda has unrecoverable bad sectors. The per-file
ingest previously computed its dirty range with `SELECT min/max WHERE job_id=...`, which scans old
rows and hits a dead block, failing every ingest. The fix derives the range from
`INSERT ... RETURNING timestamp` (only the just-written rows), and classifies genuine disk I/O
errors so each stage can skip + report + continue.

Covered:
- is_disk_io_error / disk_io_detail classify the real Postgres "could not read block" message
  (direct and chained) and don't false-positive on ordinary errors;
- _insert_dedup returns the timestamps of the rows ACTUALLY inserted (RETURNING), and nothing for
  duplicates — which is exactly what the pending-range min/max now relies on (no job_id scan).
"""

import hashlib
import uuid
from datetime import datetime, timezone

from sqlalchemy import delete

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.job import Job, JobStatus
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.pipeline.parse_insert import _insert_dedup, run_log_parse_insert

_REAL_MSG = ('(sqlalchemy.dialects.postgresql.asyncpg.Error) '
             '<class \'asyncpg.exceptions.PostgresIOError\'>: could not read block 45991 in file '
             '"base/16388/16634": Input/output error')


def test_is_disk_io_error_classifies_real_message_and_extracts_block():
    err = RuntimeError(_REAL_MSG)
    assert is_disk_io_error(err) is True
    assert disk_io_detail(err) == 'block 45991 in file base/16388/16634'


def test_is_disk_io_error_follows_the_cause_chain():
    inner = RuntimeError('could not read block 7 in file "base/1/2": Input/output error')
    outer = RuntimeError("Stage 1 failed")
    outer.__cause__ = inner
    assert is_disk_io_error(outer) is True


def test_is_disk_io_error_classifies_slow_disk_statement_timeout():
    # On the degraded disk a slow INSERT is cancelled by statement_timeout; treat it as skippable.
    err = RuntimeError("(asyncpg.Error) <class 'asyncpg.exceptions.QueryCanceledError'>: "
                       "canceling statement due to statement timeout")
    assert is_disk_io_error(err) is True
    assert "timeout" in disk_io_detail(err).lower()


def test_is_disk_io_error_ignores_ordinary_errors():
    assert is_disk_io_error(ValueError("bad input")) is False
    assert is_disk_io_error(RuntimeError("connection refused")) is False


async def test_insert_dedup_returns_inserted_timestamps_via_returning(db):
    """The pending range is now min/max of these returned timestamps — never a job_id scan."""
    cc = "TEST_CHUNK15"
    job = Job(customer_code=cc, filename="t.log", storage_key="k")
    db.add(job)
    await db.flush()

    ts = [datetime(2026, 7, 24, 10, 0, i, tzinfo=timezone.utc) for i in range(3)]

    def _row(i: int) -> dict:
        raw = f"entry-{i}"
        return {
            "id": uuid.uuid4(), "job_id": job.id, "customer_code": cc,
            "entry_hash": hashlib.sha256(raw.encode()).hexdigest(),
            "source_file": "t.log", "timestamp": ts[i], "entry_type": LogEntryType.info,
        }

    rows = [_row(0), _row(1), _row(2)]
    returned = await _insert_dedup(db, rows)
    assert sorted(returned) == sorted(ts)          # RETURNING gives back exactly the inserted rows
    assert min(returned) == ts[0] and max(returned) == ts[2]  # drives the pending range

    # Re-inserting the identical rows returns nothing (ON CONFLICT DO NOTHING) -> no phantom range.
    assert await _insert_dedup(db, rows) == []


def test_worker_statement_timeout_is_finite_not_disabled():
    """Guard: the worker's per-op timeout must be a FINITE positive value. 0 (disabled) risks an
    indefinite hang on the failing disk (an insert/window that never returns blocks the worker)."""
    assert settings.log_worker_statement_timeout_ms and settings.log_worker_statement_timeout_ms > 0


async def test_run_log_parse_insert_sets_finite_timeout_without_shadowing(monkeypatch):
    """Full-function regression guard (the _insert_dedup unit test above does NOT run this). Two things:
      (1) the SET LOCAL line must not raise 'str' object is not callable — parse_insert holds the file
          contents in a local var named `text`, so the SQLAlchemy import must be aliased (sa_text);
      (2) it must set statement_timeout to the FINITE worker cap, never 0/disabled.
    Uses its own app sessions (like the real worker) and cleans up."""
    import app.services.mnp_log_ingestion.pipeline.parse_insert as pi

    cc = "TEST_CHUNK15_INGEST"
    line = ("2026-07-24 12:00:00,000 (BENCHUSER) [1] DEBUG "
            "Server.CommonCode.ApiLogHandler MoveNext - REQUEST: http://x/api/test\n").encode()

    class _FakeStorage:
        async def load(self, key):
            return line

        async def save(self, key, data):
            return None

    seen: list[str] = []
    real_sa_text = pi.sa_text
    monkeypatch.setattr(pi, "sa_text", lambda s: (seen.append(str(s)), real_sa_text(s))[1])

    async with async_session() as s:
        job = Job(customer_code=cc, filename="regr.log", storage_key="k")
        s.add(job)
        await s.commit()
        jid = job.id
    try:
        async with async_session() as s:
            # Must NOT raise "'str' object is not callable" — that is the shadowing regression.
            await run_log_parse_insert(jid, s, _FakeStorage())
        async with async_session() as s:
            assert (await s.get(Job, jid)).status == JobStatus.completed
        set_local = [x for x in seen if "statement_timeout" in x.lower()]
        assert set_local, "ingest must issue SET LOCAL statement_timeout"
        assert str(settings.log_worker_statement_timeout_ms) in set_local[0]  # finite cap, not 0
    finally:
        async with async_session() as s:
            await s.execute(delete(LogEntry).where(LogEntry.customer_code == cc))
            await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
            await s.execute(delete(Job).where(Job.id == jid))
            await s.commit()


def test_regroup_max_attempts_setting_is_positive():
    """Guard: the dead-letter cap must be a positive int — 0/negative would either abandon on the
    first failure or never abandon (retry forever), defeating the point."""
    assert settings.log_regroup_max_attempts and settings.log_regroup_max_attempts > 0


async def test_finalize_dead_letters_a_window_after_max_attempts(monkeypatch):
    """A stitch window that keeps failing is retried at most log_regroup_max_attempts times, then
    ABANDONED (abandoned_at set) and excluded from future finalizes - never retried forever. Uses its
    own committed app sessions (finalize's attempt bookkeeping runs on a separate connection, so the
    row must be committed to be visible) and cleans up."""
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import select as sa_select
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

    cc = "TEST_CHUNK15_DEADLETTER"
    T = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
    n = settings.log_regroup_max_attempts

    async def _always_fail(wdb, customer_code, lo, hi, commit=True):
        raise RuntimeError("simulated persistent stitch failure")

    monkeypatch.setattr(d, "regroup_window", _always_fail)

    async def _row():
        async with async_session() as s:
            return (await s.execute(
                sa_select(LogRegroupPending).where(LogRegroupPending.customer_code == cc)
            )).scalars().one()

    async with async_session() as s:
        s.add(LogRegroupPending(customer_code=cc, range_start=T, range_end=T + timedelta(seconds=1)))
        await s.commit()
    try:
        for attempt in range(1, n + 1):
            async with async_session() as s:
                res = await d.finalize_pending(s, cc)
            row = await _row()
            assert row.attempts == attempt
            if attempt < n:
                assert row.abandoned_at is None      # still retried
                assert res["abandoned"] == 0
            else:
                assert row.abandoned_at is not None   # dead-lettered on the Nth failure
                assert res["abandoned"] == 1
        # once abandoned, later finalizes IGNORE it (excluded from the open query) — attempts frozen
        async with async_session() as s:
            res = await d.finalize_pending(s, cc)
        assert res["windows"] == 0 and res.get("abandoned", 0) == 0
        assert (await _row()).attempts == n           # not incremented — no longer attempted
    finally:
        async with async_session() as s:
            await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
            await s.commit()


async def test_finalize_relaxes_statement_timeout_per_window(db, monkeypatch):
    """Stitch guard: each finalize window must SET LOCAL statement_timeout to the finite worker cap,
    so a slow window on the degraded disk can finish instead of dying at the 30s web guard."""
    from datetime import datetime, timedelta, timezone
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

    cc = "TEST_CHUNK15_STITCH"
    T = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
    db.add(LogRegroupPending(customer_code=cc, range_start=T, range_end=T + timedelta(seconds=1)))
    await db.flush()

    seen: list[str] = []
    real_sa_text = d.sa_text
    monkeypatch.setattr(d, "sa_text", lambda s: (seen.append(str(s)), real_sa_text(s))[1])

    async def _noop_regroup(wdb, customer_code, lo, hi, commit=True):
        return {"mode": "window", "transactions_created": 0}

    monkeypatch.setattr(d, "regroup_window", _noop_regroup)

    await d.finalize_pending(db, cc)
    set_local = [x for x in seen if "statement_timeout" in x.lower()]
    assert set_local, "each stitch window must issue SET LOCAL statement_timeout"
    assert str(settings.log_worker_statement_timeout_ms) in set_local[0]
