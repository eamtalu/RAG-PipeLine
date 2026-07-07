"""Chunk 3 of the SSH log-fetch hardening: rotation head-fingerprint (gap 5) and checkpoint
pruning (gap 6).

Edge cases / exceptional scenarios covered:
- Rotation with a SAME size + SAME mtime collision on a reused path is detected via the head
  fingerprint and forces a full re-read (would be silently skipped without the guard).
- An append PAST the fingerprint window is a tail read, NOT a false rotation (the first N bytes are
  frozen once size >= N).
- A legacy checkpoint with head_fingerprint=NULL is backfilled on the unchanged-skip path.
- Prune deletes only checkpoints that both vanished from the listing AND are older than retention;
  present or recently-seen checkpoints survive; an empty listing prunes nothing.
"""

from datetime import datetime, timedelta, timezone

from app.settings import settings
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint
from app.config.database import async_session
from app.services.mnp_log_ingestion.remote import remote_fetcher

from tests.test_ssh_hardening_chunk2 import _patch_sftp, _patch_ingest_counts_lines

N = settings.ssh_fingerprint_bytes  # 4096


# =========================================================== gap 5: rotation via fingerprint
async def test_rotation_same_size_mtime_detected_via_fingerprint(committed_source, monkeypatch):
    src = committed_source
    path = "C:/logs/app.log"
    content_a = b"A\n" * 3000   # 6000 bytes (>= N so the head hash is a stable identity)
    files = {path: (content_a, 1000.0)}
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)

    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 1
    fp1 = (await remote_fetcher._load_ckpts(src))[path][3]
    assert fp1 is not None

    # ROTATION: different content, but engineered identical size AND mtime (the collision the
    # size/mtime check alone would miss). Only the head fingerprint reveals it.
    files[path] = (b"B\n" * 3000, 1000.0)
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s2["files_fetched"] == 1   # re-read whole; would be 0 (skipped) without the fingerprint
    fp2 = (await remote_fetcher._load_ckpts(src))[path][3]
    assert fp2 != fp1


async def test_append_past_fingerprint_window_is_tail_read(committed_source, monkeypatch):
    src = committed_source
    path = "C:/logs/app.log"
    base = b"X\n" * 3000  # 6000 bytes
    files = {path: (base, 1000.0)}
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)

    await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert (await remote_fetcher._load_ckpts(src))[path][:3] == (6000, 1000.0, 6000)

    # append beyond the first N bytes -> head fingerprint unchanged -> tail read, not rotation
    files[path] = (base + b"Y\n" * 50, 1001.0)  # 6100 bytes
    s = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s["files_fetched"] == 1
    assert s["entries_ingested"] == 50           # only the appended tail, not the whole 3050
    assert (await remote_fetcher._load_ckpts(src))[path][:3] == (6100, 1001.0, 6100)


async def test_legacy_null_fingerprint_backfills_on_skip(committed_source, monkeypatch):
    src = committed_source
    path = "C:/logs/app.log"
    content = b"h\n" * 3000  # 6000 bytes (>= N)
    async with async_session() as s:  # seed a pre-fingerprint (legacy) checkpoint that matches
        s.add(LogSshFileCheckpoint(
            source_id=src.id, customer_code=src.customer_code, remote_path=path,
            last_size=len(content), last_mtime=1000.0, last_offset=len(content),
            last_fetched_at=datetime.now(timezone.utc), head_fingerprint=None))
        await s.commit()
    files = {path: (content, 1000.0)}
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)

    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 0                      # unchanged -> skipped
    assert (await remote_fetcher._load_ckpts(src))[path][3] is not None  # fingerprint backfilled


# =========================================================== gap 6: prune
async def _seed_ckpt(src, path, fetched_at):
    async with async_session() as s:
        s.add(LogSshFileCheckpoint(
            source_id=src.id, customer_code=src.customer_code, remote_path=path,
            last_size=10, last_mtime=1.0, last_offset=10, last_fetched_at=fetched_at))
        await s.commit()


async def test_prune_removes_only_vanished_and_old(committed_source, monkeypatch):
    src = committed_source
    old = datetime.now(timezone.utc) - timedelta(days=settings.ssh_checkpoint_retention_days + 1)
    recent = datetime.now(timezone.utc)
    await _seed_ckpt(src, "C:/logs/present.log", recent)
    await _seed_ckpt(src, "C:/logs/gone-old.log", old)
    await _seed_ckpt(src, "C:/logs/gone-recent.log", recent)

    _patch_sftp(monkeypatch, {"C:/logs/present.log": (b"x\n", 1.0)})  # listing has only present.log
    _patch_ingest_counts_lines(monkeypatch)
    await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)

    ckpts = await remote_fetcher._load_ckpts(src)
    assert "C:/logs/present.log" in ckpts        # still present (re-read + re-saved)
    assert "C:/logs/gone-old.log" not in ckpts    # pruned: vanished AND older than retention
    assert "C:/logs/gone-recent.log" in ckpts     # spared: vanished but within retention


async def test_prune_skipped_on_empty_listing(committed_source, monkeypatch):
    src = committed_source
    old = datetime.now(timezone.utc) - timedelta(days=settings.ssh_checkpoint_retention_days + 1)
    await _seed_ckpt(src, "C:/logs/gone.log", old)

    _patch_sftp(monkeypatch, {})  # transient empty glob
    _patch_ingest_counts_lines(monkeypatch)
    await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)

    ckpts = await remote_fetcher._load_ckpts(src)
    assert "C:/logs/gone.log" in ckpts  # NOT pruned — empty listing must never wipe checkpoints
