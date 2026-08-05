"""Chunk 17: decouple SSH fetching from Stage-1 parsing via the log_source_objects queue.

Context (see docs/plan/2026-08-02_15-47_log-source-objects-fetch-parse-decoupling.md): today
`_pull_range` awaits parse+insert inside the byte-read loop (remote_fetcher.py:157), so the SSH
connection and the per-host advisory lock are held while the database works, a crash mid-parse
orphans the file with nothing recording that it still needs work, and a file that always fails is
retried EVERY poll forever - writing another ./uploads copy and another `jobs` row each time.

The fix: the fetcher downloads bytes, saves the file, and in ONE transaction inserts a
`log_source_objects` row and advances the checkpoint. A separate worker leases the row and parses.

The single transaction is load-bearing. Advancing the checkpoint without the row would skip those
bytes forever (silent data loss); writing the row without the checkpoint would re-download them.

Covered here:
- the atomic checkpoint+row write, and that a failure leaves NEITHER;
- claiming: SKIP LOCKED, backoff gating, abandoned rows excluded, lease-expiry recovery;
- the retry budget: transient -> backoff and retry; permanent -> abandon on the FIRST failure;
  budget exhaustion -> abandon with attempts frozen at the cap;
- poison isolation: an abandoned row never blocks the rest of the queue;
- re-arm, purge cleanup, the queue-depth guard;
- and the no-regression guard that with the flag OFF the fetcher still ingests inline.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_source_object import LogSourceObject, SourceObjectStatus
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from app.services.mnp_log_ingestion.remote import remote_fetcher
from app.services.workers import log_parse_worker as lpw

from tests.test_ssh_hardening_chunk2 import _patch_sftp

CC = "TEST_CHUNK17"


# =============================================================== helpers
async def _cleanup(customer_code: str = CC) -> None:
    async with async_session() as s:
        await s.execute(delete(LogSourceObject).where(LogSourceObject.customer_code == customer_code))
        await s.execute(delete(LogEntry).where(LogEntry.customer_code == customer_code))
        await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == customer_code))
        await s.execute(delete(Job).where(Job.customer_code == customer_code))
        await s.commit()


# The shared `committed_source` fixture hardcodes customer_code="TEST_CHUNK2", so tests that use it
# write rows under THAT tenant, not CC. Clean both or leftovers from one test leak into the next.
_TENANTS = (CC, "TEST_CHUNK2")


@pytest.fixture
async def clean():
    for t in _TENANTS:
        await _cleanup(t)
    yield
    for t in _TENANTS:
        await _cleanup(t)


async def _mk_row(customer_code: str = CC, *, status=SourceObjectStatus.pending,
                  attempts: int = 0, available_at: datetime | None = None,
                  storage_key: str | None = None, start: int = 0, end: int = 10,
                  lease_expires_at: datetime | None = None,
                  lease_owner: str | None = None) -> uuid.UUID:
    """Commit one queue row (the worker opens its own session, so it must be committed)."""
    async with async_session() as s:
        row = LogSourceObject(
            customer_code=customer_code, source_name="src-a",
            remote_path="C:/logs/app.log", start_offset=start, end_offset=end,
            storage_key=storage_key or f"{customer_code}/{uuid.uuid4().hex}/app.log",
            status=status, attempts=attempts,
            available_at=available_at or datetime.now(timezone.utc) - timedelta(seconds=1),
            lease_expires_at=lease_expires_at, lease_owner=lease_owner,
        )
        s.add(row)
        await s.commit()
        return row.id


async def _get(row_id: uuid.UUID) -> LogSourceObject:
    async with async_session() as s:
        return (await s.execute(
            select(LogSourceObject).where(LogSourceObject.id == row_id))).scalars().one()


# =============================================================== settings guards
def test_queue_settings_are_sane():
    """The retry cap must be positive (0 would either abandon instantly or never abandon), and the
    queue-depth guard must be positive or the fetcher could outrun the parser without limit."""
    assert settings.log_parse_max_attempts and settings.log_parse_max_attempts > 0
    assert settings.log_parse_lease_seconds and settings.log_parse_lease_seconds > 0
    assert settings.log_parse_queue_max_pending and settings.log_parse_queue_max_pending > 0
    # ON since 2026-08-05. It shipped OFF so the two halves of the decoupling could deploy
    # separately; the queue half has been verified in production, so the queue path is now the
    # default. Rollback is still a flag flip, not a revert - see test_flag_off_keeps_the_inline_path_unchanged.
    assert settings.log_parse_worker_enabled is True


def test_max_attempts_matches_stage2_so_operators_learn_one_number():
    assert settings.log_parse_max_attempts == settings.log_regroup_max_attempts


# =============================================================== backoff
def test_backoff_grows_and_is_bounded_and_jittered():
    """Stage 2 has no backoff at all and retries every tick, hammering a failing disk. Here each
    attempt must wait longer, and jitter must keep retries from synchronising across rows."""
    b1 = [lpw._backoff_seconds(1) for _ in range(40)]
    b2 = [lpw._backoff_seconds(2) for _ in range(40)]
    b3 = [lpw._backoff_seconds(3) for _ in range(40)]
    assert min(b2) > max(b1) * 0.9      # grows between attempts
    assert min(b3) > max(b2) * 0.9
    assert len(set(b1)) > 1             # jittered, not a constant
    base = settings.log_parse_backoff_base_seconds
    assert all(base <= x <= base * 1.5 for x in b1)   # attempt 1 = base + up to 25% jitter


# =============================================================== failure classification
def test_classify_disk_io_and_timeouts_as_transient():
    """Reuses the existing is_disk_io_error helper: a bad sector or a statement timeout may succeed
    on a later attempt, so it must consume the retry budget rather than abandon immediately."""
    real = RuntimeError('could not read block 45991 in file "base/16388/16634": Input/output error')
    assert lpw._is_transient(real) is True
    assert lpw._is_transient(RuntimeError(
        "(asyncpg.Error) <class 'asyncpg.exceptions.QueryCanceledError'>: "
        "canceling statement due to statement timeout")) is True
    assert lpw._is_transient(ConnectionResetError("peer reset")) is True
    assert lpw._is_transient(asyncio.TimeoutError()) is True


def test_classify_parse_and_decode_failures_as_permanent():
    """Retrying a corrupt file cannot help; three attempts would just triple the log noise."""
    assert lpw._is_transient(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid")) is False
    assert lpw._is_transient(ValueError("unparseable log line")) is False
    assert lpw._is_transient(FileNotFoundError("storage key gone")) is False


# =============================================================== claiming
async def test_claim_returns_due_row_and_marks_it_leased(clean):
    rid = await _mk_row()
    async with async_session() as s:
        got = await lpw.claim_one(s, "worker-1")
    assert got is not None and got.id == rid
    row = await _get(rid)
    assert row.status == SourceObjectStatus.leased
    assert row.lease_owner == "worker-1"
    assert row.lease_expires_at is not None


async def test_claim_skips_rows_not_yet_due(clean):
    """A row backing off after a transient failure must not be re-claimed early."""
    await _mk_row(available_at=datetime.now(timezone.utc) + timedelta(minutes=10))
    async with async_session() as s:
        assert await lpw.claim_one(s, "worker-1") is None


async def test_claim_skips_abandoned_rows(clean):
    """Dead-lettered rows are excluded from the claim query, exactly like Stage 2 excludes
    abandoned windows at derive_transactions.py:775 - otherwise it is not a dead letter."""
    await _mk_row(status=SourceObjectStatus.abandoned, attempts=settings.log_parse_max_attempts)
    async with async_session() as s:
        assert await lpw.claim_one(s, "worker-1") is None


async def test_two_workers_never_claim_the_same_row(clean):
    """FOR UPDATE SKIP LOCKED: concurrent workers must get DIFFERENT rows, never the same one."""
    a = await _mk_row(start=0, end=10)
    b = await _mk_row(start=10, end=20)
    async with async_session() as s1, async_session() as s2:
        got1 = await lpw.claim_one(s1, "worker-1")
        got2 = await lpw.claim_one(s2, "worker-2")
        assert got1 is not None and got2 is not None
        assert {got1.id, got2.id} == {a, b}


async def test_expired_lease_returns_row_to_pending(clean):
    """A crashed worker must not strand its row. Neither stage has lease recovery today."""
    rid = await _mk_row(status=SourceObjectStatus.leased, lease_owner="dead-worker",
                        lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=5))
    async with async_session() as s:
        n = await lpw.reclaim_expired_leases(s)
    assert n == 1
    row = await _get(rid)
    assert row.status == SourceObjectStatus.pending and row.lease_owner is None


# =============================================================== retry budget
async def test_transient_failure_backs_off_and_stays_retryable(clean):
    rid = await _mk_row()
    before = datetime.now(timezone.utc)
    async with async_session() as s:
        await lpw.record_failure(s, rid, RuntimeError(
            'could not read block 1 in file "base/1/2": Input/output error'))
    row = await _get(rid)
    assert row.status == SourceObjectStatus.pending
    assert row.attempts == 1
    assert row.available_at > before + timedelta(seconds=1)   # actually delayed
    assert "could not read block" in (row.last_error or "")


async def test_permanent_failure_abandons_on_the_first_attempt(clean):
    """The key difference from Stage 2: a hopeless row must not burn the whole budget."""
    rid = await _mk_row()
    async with async_session() as s:
        await lpw.record_failure(s, rid, ValueError("unparseable"))
    row = await _get(rid)
    assert row.status == SourceObjectStatus.abandoned
    assert row.attempts == 1                       # abandoned immediately, budget untouched
    assert row.attempts < settings.log_parse_max_attempts


async def test_budget_exhaustion_abandons_and_freezes_attempts(clean):
    n = settings.log_parse_max_attempts
    rid = await _mk_row()
    err = RuntimeError('could not read block 1 in file "base/1/2": Input/output error')
    for i in range(1, n + 1):
        async with async_session() as s:
            await lpw.record_failure(s, rid, err)
        row = await _get(rid)
        assert row.attempts == i
        if i < n:
            assert row.status == SourceObjectStatus.pending
        else:
            assert row.status == SourceObjectStatus.abandoned

    # once abandoned it is never claimed again, so attempts stays frozen at the cap
    async with async_session() as s:
        assert await lpw.claim_one(s, "worker-1") is None
    assert (await _get(rid)).attempts == n


async def test_one_abandoned_row_does_not_block_the_queue(clean):
    """Poison isolation - the failure mode B currently causes at SOURCE level, where one bad file
    eventually auto-disables the whole server."""
    dead = await _mk_row(status=SourceObjectStatus.abandoned, start=0, end=10)
    good = await _mk_row(start=10, end=20)
    async with async_session() as s:
        got = await lpw.claim_one(s, "worker-1")
    assert got is not None and got.id == good and got.id != dead


async def test_reset_abandoned_re_arms_for_retry(clean):
    """Mirrors reset_abandoned_windows (derive_transactions.py:869) so operators learn one pattern."""
    rid = await _mk_row(status=SourceObjectStatus.abandoned,
                        attempts=settings.log_parse_max_attempts)
    async with async_session() as s:
        n = await lpw.reset_abandoned_objects(s, CC)
    assert n == 1
    row = await _get(rid)
    assert row.status == SourceObjectStatus.pending
    assert row.attempts == 0 and row.last_error is None
    async with async_session() as s:                 # and it is claimable again
        assert (await lpw.claim_one(s, "worker-1")).id == rid


# =============================================================== the atomic write
async def test_checkpoint_and_queue_row_commit_together(committed_source, clean):
    """The load-bearing guarantee: bookmark and to-do note land as one action."""
    src = committed_source
    await remote_fetcher._queue_and_checkpoint(
        src, "C:/logs/app.log", size=9000, mtime=1000.0, offset=9000,
        head_fp="fp1", storage_key=f"{src.customer_code}/x/app.log",
        start_offset=5000, end_offset=9000)

    ck = await remote_fetcher._load_ckpts(src)
    assert ck["C:/logs/app.log"][2] == 9000          # bookmark advanced
    async with async_session() as s:
        rows = (await s.execute(select(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))).scalars().all()
    assert len(rows) == 1
    assert rows[0].start_offset == 5000 and rows[0].end_offset == 9000
    assert rows[0].status == SourceObjectStatus.pending
    async with async_session() as s:
        await s.execute(delete(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))
        await s.commit()


async def test_failure_during_queue_write_leaves_neither(committed_source, monkeypatch, clean):
    """If the row insert fails, the checkpoint must NOT have advanced - otherwise those bytes are
    skipped forever with nothing recording that they were never parsed."""
    src = committed_source
    path = "C:/logs/app.log"
    await remote_fetcher._save_ckpt(src, path, size=5000, mtime=1.0, offset=5000, head_fingerprint="fp0")

    real_add = remote_fetcher.LogSourceObject

    class _Boom:
        def __init__(self, *a, **k):
            raise RuntimeError("simulated insert failure")

    monkeypatch.setattr(remote_fetcher, "LogSourceObject", _Boom)
    with pytest.raises(RuntimeError):
        await remote_fetcher._queue_and_checkpoint(
            src, path, size=9000, mtime=2.0, offset=9000, head_fp="fp1",
            storage_key="k", start_offset=5000, end_offset=9000)
    monkeypatch.setattr(remote_fetcher, "LogSourceObject", real_add)

    ck = await remote_fetcher._load_ckpts(src)
    assert ck[path][2] == 5000, "checkpoint must not advance when the queue row was not written"
    async with async_session() as s:
        rows = (await s.execute(select(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))).scalars().all()
    assert rows == []


# =============================================================== fetcher integration
async def test_flag_off_keeps_the_inline_path_unchanged(committed_source, monkeypatch, clean):
    """No-regression guard: with the flag off the fetcher must still parse inline and write NO
    queue rows, so rollback really is just flipping the setting."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", False)
    src = committed_source
    files = {"C:/logs/app.log": (b"a\nb\n", 1000.0)}
    _patch_sftp(monkeypatch, files)

    called = {"n": 0}

    async def fake_ingest(source, remote_path, data):
        called["n"] += 1
        return data.count(b"\n")

    monkeypatch.setattr(remote_fetcher, "_ingest_chunk", fake_ingest)

    stats = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert called["n"] == 1, "inline ingest must still run when the flag is off"
    assert stats["entries_ingested"] == 2
    async with async_session() as s:
        rows = (await s.execute(select(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))).scalars().all()
    assert rows == []


async def test_flag_on_queues_instead_of_ingesting(committed_source, monkeypatch, clean):
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    src = committed_source
    files = {"C:/logs/app.log": (b"a\nb\n", 1000.0)}
    _patch_sftp(monkeypatch, files)

    called = {"n": 0}

    async def fake_ingest(source, remote_path, data):
        called["n"] += 1
        return 0

    monkeypatch.setattr(remote_fetcher, "_ingest_chunk", fake_ingest)

    saved = {}

    async def fake_save(self, key, data):
        saved[key] = data
        return key

    monkeypatch.setattr(remote_fetcher.LocalStorage, "save", fake_save)

    stats = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert called["n"] == 0, "the fetcher must NOT parse inline when the flag is on"
    assert stats["objects_queued"] == 1
    async with async_session() as s:
        rows = (await s.execute(select(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))).scalars().all()
    assert len(rows) == 1 and rows[0].status == SourceObjectStatus.pending
    assert rows[0].storage_key in saved
    async with async_session() as s:
        await s.execute(delete(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))
        await s.commit()


async def test_queue_depth_guard_pauses_fetching(committed_source, monkeypatch, clean):
    """Today the inline await gives free backpressure. Once decoupled the fetcher could outrun the
    parser and fill ./uploads, so it must stop when the backlog is too deep."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    monkeypatch.setattr(settings, "log_parse_queue_max_pending", 1)
    src = committed_source
    await _mk_row(customer_code=src.customer_code)
    await _mk_row(customer_code=src.customer_code)   # backlog of 2 > cap of 1

    files = {"C:/logs/app.log": (b"a\nb\n", 1000.0)}
    _patch_sftp(monkeypatch, files)

    over = await remote_fetcher._queue_backlog_exceeded(src.customer_code)
    assert over is True
    async with async_session() as s:
        await s.execute(delete(LogSourceObject).where(
            LogSourceObject.customer_code == src.customer_code))
        await s.commit()


# =============================================================== end-to-end through the worker
async def test_worker_processes_a_row_and_marks_it_ingested(clean, monkeypatch):
    """Happy path: lease -> parse -> ingested, with the job and entry count recorded on the row."""
    line = ("2026-08-05 12:00:00,000 (BENCHUSER) [1] DEBUG "
            "Server.CommonCode.ApiLogHandler MoveNext - REQUEST: http://x/api/test\n").encode()
    key = f"{CC}/{uuid.uuid4().hex}/app.log"
    rid = await _mk_row(storage_key=key)

    class _FakeStorage:
        async def load(self, k):
            assert k == key
            return line

        async def save(self, k, d):
            return k

        async def delete(self, k):
            return None

    async with async_session() as s:
        obj = await lpw.claim_one(s, "worker-1")
    assert obj is not None
    await lpw.process_claimed(obj, _FakeStorage())

    row = await _get(rid)
    assert row.status == SourceObjectStatus.ingested
    assert row.ingested_at is not None
    assert row.job_id is not None
    assert row.entries_inserted == 1

    async with async_session() as s:
        n = await s.scalar(select(LogEntry).where(LogEntry.customer_code == CC).exists().select())
    assert n is True


async def test_worker_drain_returns_counts_and_is_safe_when_empty(clean):
    assert await lpw.drain_once(None) == {"claimed": 0, "ingested": 0, "failed": 0, "abandoned": 0}


# =============================================================== end-to-end + rotation safety
_LOG_LINE = ("2026-08-05 12:00:00,000 (BENCHUSER) [1] DEBUG "
             "Server.CommonCode.ApiLogHandler MoveNext - REQUEST: http://x/api/test\n")


async def test_end_to_end_fetch_then_drain_lands_entries(committed_source, monkeypatch, tmp_path, clean):
    """The whole point, proven: fetch queues without parsing, then the worker parses and entries
    appear. Also asserts the fetch itself created NO log_entries — that is the decoupling."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    monkeypatch.setattr(settings, "upload_dir", tmp_path)
    src = committed_source
    cc = src.customer_code
    _patch_sftp(monkeypatch, {"C:/logs/app.log": (_LOG_LINE.encode(), 1000.0)})

    try:
        stats = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
        assert stats["objects_queued"] == 1
        async with async_session() as s:
            n = await s.scalar(select(LogEntry).where(LogEntry.customer_code == cc).exists().select())
        assert n is False, "the fetch must not have parsed anything yet"

        drained = await lpw.drain_once()
        assert drained["claimed"] == 1 and drained["ingested"] == 1

        async with async_session() as s:
            rows = (await s.execute(select(LogSourceObject).where(
                LogSourceObject.customer_code == cc))).scalars().all()
        assert len(rows) == 1
        assert rows[0].status == SourceObjectStatus.ingested
        assert rows[0].entries_inserted == 1
        assert rows[0].file_deleted_at is not None, "an ingested file must be reclaimed"

        async with async_session() as s:
            n = await s.scalar(select(LogEntry).where(LogEntry.customer_code == cc).exists().select())
        assert n is True, "entries must exist after the drain"
    finally:
        await _cleanup(cc)


async def test_queue_mode_preserves_rotation_detection(committed_source, monkeypatch, tmp_path, clean):
    """Regression guard for a bug I introduced: the queue-mode path writes the checkpoint itself, and
    if it stored a placeholder mtime/fingerprint instead of the real ones, _plan_incremental would
    read every file as 'rotated' on the NEXT poll and re-download the whole directory forever —
    exactly the ~500 MB-per-rotation stall that chunk 12 fixed."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    monkeypatch.setattr(settings, "upload_dir", tmp_path)
    src = committed_source
    body = b"x" * (settings.ssh_fingerprint_bytes + 200) + b"\n"
    _patch_sftp(monkeypatch, {"C:/logs/app.log": (body, 1234.5)})

    try:
        s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
        assert s1["objects_queued"] == 1

        ck = await remote_fetcher._load_ckpts(src)
        size, mtime, offset, fp = ck["C:/logs/app.log"]
        assert mtime == 1234.5, "the real mtime must be checkpointed, not a placeholder"
        assert fp, "the real head fingerprint must be checkpointed, not None"
        assert offset == len(body)

        # unchanged file on the next poll -> skipped entirely. This only holds if mtime+fp above
        # were stored correctly.
        s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
        assert s2["files_fetched"] == 0 and s2["objects_queued"] == 0
    finally:
        await _cleanup(src.customer_code)


async def test_dedup_still_holds_when_the_same_bytes_are_parsed_twice(clean, tmp_path, monkeypatch):
    """entry_hash remains the correctness backstop: re-arming and re-processing an already-parsed
    range must not duplicate entries."""
    monkeypatch.setattr(settings, "upload_dir", tmp_path)
    key = f"{CC}/{uuid.uuid4().hex}/app.log"
    p = tmp_path / key
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(_LOG_LINE.encode())
    monkeypatch.setattr(settings, "log_parse_delete_ingested_files", False)

    await _mk_row(storage_key=key)
    first = await lpw.drain_once()
    assert first["ingested"] == 1

    await _mk_row(storage_key=key)          # same bytes queued again
    second = await lpw.drain_once()
    assert second["ingested"] == 1

    async with async_session() as s:
        rows = (await s.execute(select(LogSourceObject).where(
            LogSourceObject.customer_code == CC).order_by(
            LogSourceObject.created_at))).scalars().all()
    assert rows[0].entries_inserted == 1
    assert rows[1].entries_inserted == 0, "the duplicate range must insert nothing"


async def test_tracked_run_reports_objects_queued(committed_source, monkeypatch, clean):
    """Frontend-visible: in queue mode entries_ingested is necessarily 0 at fetch time, so the run
    row must carry objects_queued or the UI shows a successful fetch that did nothing."""
    from app.persistence.models.log_ssh_fetch_run import LogSshFetchRun, LogSshFetchRunStatus

    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    src = committed_source
    cc = src.customer_code

    async def _fake_fetch_now(db, customer_code, **kw):
        return {"customer_code": customer_code, "files_considered": 1, "files_fetched": 1,
                "bytes_fetched": 42, "entries_ingested": 0, "objects_queued": 5}

    monkeypatch.setattr(remote_fetcher, "fetch_now", _fake_fetch_now)

    async with async_session() as s:
        run = LogSshFetchRun(customer_code=cc, mode=LogSshFetchMode.incremental,
                             status=LogSshFetchRunStatus.running)
        s.add(run)
        await s.commit()
        rid = run.id
    try:
        await remote_fetcher.run_ssh_fetch_tracked(rid, cc, None, LogSshFetchMode.incremental, None)
        async with async_session() as s:
            row = await s.get(LogSshFetchRun, rid)
            assert row.status == LogSshFetchRunStatus.completed
            assert (row.result or {}).get("objects_queued") == 5
    finally:
        async with async_session() as s:
            await s.execute(delete(LogSshFetchRun).where(LogSshFetchRun.id == rid))
            await s.commit()


# =============================================================== deletion paths (no orphans)
async def test_purge_logspace_removes_queue_rows_and_their_files(tmp_path, monkeypatch):
    """A tenant purge must leave NO queue rows and NO files. Missing this is the orphan risk:
    log_source_objects has no job/tenant FK cascade, so purge must delete it explicitly, and the
    stored bytes must be unlinked or they sit on the disk forever with nothing referencing them."""
    from app.persistence.models.customer import Customer
    from app.services.logspace_cleanup import purge_logspace

    monkeypatch.setattr(settings, "upload_dir", tmp_path)
    cc = "TEST_CHUNK17_PURGE"
    key = f"{cc}/abc/app.log"
    f = tmp_path / key
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(b"bytes\n")
    assert f.exists()

    async with async_session() as s:
        s.add(Customer(customer_code=cc, display_name="purge test"))
        await s.commit()
    await _mk_row(customer_code=cc, storage_key=key)

    try:
        async with async_session() as s:
            assert await purge_logspace(s, cc) is True
        async with async_session() as s:
            rows = (await s.execute(select(LogSourceObject).where(
                LogSourceObject.customer_code == cc))).scalars().all()
        assert rows == [], "purge left orphan queue rows"
        assert not f.exists(), "purge left the stored file behind"
    finally:
        await _cleanup(cc)
        async with async_session() as s:
            await s.execute(delete(Customer).where(Customer.customer_code == cc))
            await s.commit()


async def test_full_wipe_clears_queue_rows_and_files(tmp_path, monkeypatch):
    """DELETE /logs/data?confirm=true is the tenant-scoped wipe; it must clear the queue too."""
    from app.api.v1.logs import delete_log_data

    monkeypatch.setattr(settings, "upload_dir", tmp_path)
    cc = "TEST_CHUNK17_WIPE"
    key = f"{cc}/def/app.log"
    f = tmp_path / key
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(b"bytes\n")
    await _mk_row(customer_code=cc, storage_key=key)
    try:
        async with async_session() as s:
            await delete_log_data(customer=cc, date_from=None, date_to=None, confirm=True, db=s)
        async with async_session() as s:
            rows = (await s.execute(select(LogSourceObject).where(
                LogSourceObject.customer_code == cc))).scalars().all()
        assert rows == []
        assert not f.exists()
    finally:
        await _cleanup(cc)


async def test_date_range_delete_leaves_the_queue_alone(clean):
    """No-regression guard: a DATE-RANGE delete must NOT touch the queue. Those rows describe
    downloaded byte ranges, not log dates, and dropping them would strand unparsed work."""
    from app.api.v1.logs import delete_log_data
    from datetime import date

    rid = await _mk_row()
    async with async_session() as s:
        await delete_log_data(customer=CC, date_from=date(2026, 1, 1), date_to=date(2026, 1, 2),
                              confirm=False, db=s)
    assert (await _get(rid)) is not None


# =============================================================== backpressure is actually applied
async def test_fetch_now_skips_a_source_whose_backlog_is_too_deep(committed_source, monkeypatch, clean):
    """The guard must be WIRED IN, not merely available. Without this the fetcher keeps downloading
    while the parser falls behind, and ./uploads grows without limit on an already-failing disk."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    monkeypatch.setattr(settings, "log_parse_queue_max_pending", 1)
    src = committed_source
    cc = src.customer_code
    await _mk_row(customer_code=cc)
    await _mk_row(customer_code=cc)          # backlog 2 > cap 1

    entered = {"n": 0}

    async def _spy(source, mode, from_ts, **kw):
        entered["n"] += 1
        return {"source": source.name, "files_considered": 0, "files_fetched": 0,
                "bytes_fetched": 0, "entries_ingested": 0, "objects_queued": 0,
                "content_skipped": 0, "io_skipped": 0, "by_file": []}

    monkeypatch.setattr(remote_fetcher, "_fetch_source", _spy)

    try:
        async with async_session() as s:
            agg = await remote_fetcher.fetch_now(s, cc, source_id=src.id, enabled_only=False)
        assert entered["n"] == 0, "the source must not be fetched while the backlog is over the cap"
        assert any("backlog" in str(x).lower() for x in agg.get("skipped", []) or [{}]) or \
            agg.get("skipped"), "the skip must be reported, not silent"
    finally:
        async with async_session() as s:
            await s.execute(delete(LogSourceObject).where(LogSourceObject.customer_code == cc))
            await s.commit()


async def test_fetch_now_aggregates_objects_queued(committed_source, monkeypatch, clean):
    """objects_queued must reach the aggregate, or the fetch run reports 0 work done in queue mode
    (entries_ingested is necessarily 0 at that point) and the UI looks broken."""
    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    src = committed_source

    async def _spy(source, mode, from_ts, **kw):
        return {"source": source.name, "files_considered": 1, "files_fetched": 1,
                "bytes_fetched": 10, "entries_ingested": 0, "objects_queued": 3,
                "content_skipped": 0, "io_skipped": 0, "by_file": []}

    monkeypatch.setattr(remote_fetcher, "_fetch_source", _spy)

    async with async_session() as s:
        agg = await remote_fetcher.fetch_now(s, src.customer_code, source_id=src.id,
                                             enabled_only=False)
    assert agg["objects_queued"] == 3


# =============================================================== worker registration
async def test_background_starts_parse_worker_only_when_enabled(monkeypatch):
    from app import background as bg
    from tests.test_background_workers_chunk10 import _stub_loops

    _stub_loops(monkeypatch)

    async def _noop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_log_parse_worker", _noop)
    monkeypatch.setattr(settings, "log_stitch_worker_enabled", False)
    monkeypatch.setattr(settings, "ssh_log_fetcher_enabled", False)
    monkeypatch.setattr(settings, "notifications_enabled", False)
    monkeypatch.setattr(settings, "logspace_cleanup_worker_enabled", False)

    monkeypatch.setattr(settings, "log_parse_worker_enabled", False)
    tasks = await bg.start_background_tasks()
    off = len(tasks)
    await bg.stop_background_tasks(tasks)

    monkeypatch.setattr(settings, "log_parse_worker_enabled", True)
    tasks = await bg.start_background_tasks()
    on = len(tasks)
    await bg.stop_background_tasks(tasks)

    assert on == off + 1, "the parse worker must start when the flag is on"


async def test_parse_worker_still_starts_to_drain_a_leftover_queue(monkeypatch, clean):
    """Rollback safety. If the flag is enabled, rows are queued (so the checkpoint has ALREADY moved
    past those bytes), and the flag is then switched back off, nothing would drain them and the
    fetcher would never re-download them — silent data loss caused by a rollback.

    So the worker also starts when unfinished work exists, regardless of the flag. With the flag off
    and an empty queue it stays absent, which is the normal untouched deployment."""
    from app import background as bg
    from tests.test_background_workers_chunk10 import _stub_loops

    _stub_loops(monkeypatch)

    async def _noop():
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_log_parse_worker", _noop)
    monkeypatch.setattr(settings, "log_stitch_worker_enabled", False)
    monkeypatch.setattr(settings, "ssh_log_fetcher_enabled", False)
    monkeypatch.setattr(settings, "notifications_enabled", False)
    monkeypatch.setattr(settings, "logspace_cleanup_worker_enabled", False)
    monkeypatch.setattr(settings, "log_parse_worker_enabled", False)

    # _stub_loops pins both backlog probes to 0 so the OTHER tests measure flags only. This test is
    # specifically about the backlog probe, so drive it explicitly rather than depending on whatever
    # rows happen to be in the shared test database.
    async def _empty():
        return 0

    async def _leftover():
        return 4

    monkeypatch.setattr(bg, "unfinished_ingest_objects", _empty)
    tasks = await bg.start_background_tasks()          # flag off, empty queue
    baseline = len(tasks)
    await bg.stop_background_tasks(tasks)

    monkeypatch.setattr(bg, "unfinished_ingest_objects", _leftover)
    tasks = await bg.start_background_tasks()          # flag STILL off, but work remains
    with_backlog = len(tasks)
    await bg.stop_background_tasks(tasks)

    assert with_backlog == baseline + 1, (
        "a leftover queue must still be drained after the flag is rolled back")


# =============================================================== operator endpoints
async def test_ingest_queue_endpoint_lists_and_re_arms(clean):
    """Mirrors the Stage 2 pair (logs.py:422) so operators learn one pattern, not two."""
    from app.api.v1.logs import ingest_queue_status, reset_abandoned_ingest_objects

    rid = await _mk_row(status=SourceObjectStatus.abandoned,
                        attempts=settings.log_parse_max_attempts)
    async with async_session() as s:
        res = await ingest_queue_status(customer=CC, status=SourceObjectStatus.abandoned,
                                        limit=50, db=s)
    assert res["customer_code"] == CC
    assert res["count"] == 1
    item = res["objects"][0]
    assert item["id"] == str(rid)
    assert item["remote_path"] == "C:/logs/app.log"
    assert item["attempts"] == settings.log_parse_max_attempts

    async with async_session() as s:
        out = await reset_abandoned_ingest_objects(customer=CC, db=s)
    assert out["reset"] == 1
    assert (await _get(rid)).status == SourceObjectStatus.pending
