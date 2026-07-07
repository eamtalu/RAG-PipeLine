"""Chunk 2 of the SSH log-fetch hardening: SFTP timeouts + keepalive (gap 1), the engine session
refactor (gap 3), and the per-host concurrency lock + checkpoint upsert (gap 2).

Edge cases / exceptional scenarios covered:
- op() times out a stalled SFTP call and raises SshConnectionError; passes a fast value through.
- connect() passes keepalive kwargs to asyncssh.
- _save_ckpt is an idempotent UPSERT (a repeated/overlapping save never raises IntegrityError).
- _host_lock: try-lock excludes a second holder on the same host and re-acquires after release;
  a *different* host is not blocked; the blocking path times out with SshConnectionError.
- _fetch_source incremental: a new file is pulled + checkpointed; an unchanged file is skipped;
  a grown file reads only the new tail; a shrunk file re-reads from 0.
"""

import asyncio
from contextlib import asynccontextmanager

import pytest

from app.settings import settings
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from app.services.mnp_log_ingestion.remote import ssh_client
from app.services.mnp_log_ingestion.remote import remote_fetcher
from app.persistence.models.log_ssh_source import LogSshSource


# =========================================================== gap 1: op() timeout + keepalive
async def test_op_passes_value_through():
    async def fast():
        return 42
    assert await ssh_client.op(fast(), "read") == 42


async def test_op_times_out_and_raises(monkeypatch):
    monkeypatch.setattr(settings, "ssh_operation_timeout_seconds", 0.05)

    async def stalled():
        await asyncio.sleep(5)

    with pytest.raises(ssh_client.SshConnectionError) as ei:
        await ssh_client.op(stalled(), "read")
    assert "read" in str(ei.value)


async def test_connect_passes_keepalive_kwargs(monkeypatch):
    captured = {}

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        raise OSError("short-circuit before a real socket")  # caught -> SshConnectionError

    monkeypatch.setattr(ssh_client.asyncssh, "connect", fake_connect)
    src = LogSshSource(customer_code="C", name="n", host="h", port=2222, username="u",
                       remote_log_dir="/", private_key_path="/does/not/matter")
    with pytest.raises(ssh_client.SshConnectionError):
        async with ssh_client.connect(src):
            pass
    assert captured["keepalive_interval"] == settings.ssh_keepalive_interval_seconds
    assert captured["keepalive_count_max"] == settings.ssh_keepalive_count_max
    assert captured["known_hosts"] is None


# =========================================================== gap 2/3: checkpoint upsert
async def test_save_ckpt_upsert_is_idempotent(committed_source):
    src = committed_source
    path = "C:/logs/app.log"
    # First save inserts; repeated saves for the same (source_id, remote_path) must UPDATE, not raise.
    await remote_fetcher._save_ckpt(src, path, size=100, mtime=1000.0, offset=100)
    await remote_fetcher._save_ckpt(src, path, size=250, mtime=1001.0, offset=250)
    await remote_fetcher._save_ckpt(src, path, size=250, mtime=1001.0, offset=250)  # no-op-ish repeat
    ckpts = await remote_fetcher._load_ckpts(src)
    assert ckpts[path] == (250, 1001.0, 250)


# =========================================================== gap 2: per-host advisory lock
async def test_host_lock_excludes_same_host_and_reacquires(committed_source):
    src = committed_source
    async with remote_fetcher._host_lock(src, skip_if_busy=True) as got1:
        assert got1 is True
        async with remote_fetcher._host_lock(src, skip_if_busy=True) as got2:
            assert got2 is False  # busy: another holder on the same host:port
    # released now -> a fresh acquire succeeds
    async with remote_fetcher._host_lock(src, skip_if_busy=True) as got3:
        assert got3 is True


async def test_host_lock_does_not_block_different_host(committed_source):
    src = committed_source
    other = LogSshSource(customer_code=src.customer_code, name="other",
                         host=src.host + "-other", port=src.port, username="u", remote_log_dir="/")
    async with remote_fetcher._host_lock(src, skip_if_busy=True) as got1:
        assert got1 is True
        async with remote_fetcher._host_lock(other, skip_if_busy=True) as got2:
            assert got2 is True  # different host:port -> independent lock


async def test_host_lock_blocking_times_out(committed_source, monkeypatch):
    monkeypatch.setattr(settings, "ssh_fetch_lock_wait_seconds", 0.2)
    src = committed_source
    async with remote_fetcher._host_lock(src, skip_if_busy=True) as got1:
        assert got1 is True
        with pytest.raises(ssh_client.SshConnectionError):
            async with remote_fetcher._host_lock(src, skip_if_busy=False):
                pass  # pragma: no cover


# =========================================================== gap 3: _fetch_source flow (fake SFTP)
class _FakeFile:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self, size, offset):
        return self._data[offset:offset + size]

    async def close(self):
        pass


class _FakeAttrs:
    def __init__(self, size, mtime):
        self.size = size
        self.mtime = mtime


class _FakeSftp:
    """Minimal SFTP client: files is {path: (bytes, mtime)}."""
    def __init__(self, files):
        self.files = files

    async def glob(self, pattern):
        return list(self.files.keys())

    async def stat(self, path):
        data, mtime = self.files[path]
        return _FakeAttrs(len(data), mtime)

    async def open(self, path, mode):
        return _FakeFile(self.files[path][0])


def _patch_sftp(monkeypatch, files):
    @asynccontextmanager
    async def fake_sftp(source):
        yield _FakeSftp(files), "SHA256:fakefingerprint"
    monkeypatch.setattr(remote_fetcher.ssh_client, "sftp", fake_sftp)


def _patch_ingest_counts_lines(monkeypatch):
    # Isolate the fetch/checkpoint logic from the Stage-1 parser: count newlines as "entries".
    async def fake_ingest(source, remote_path, data):
        return data.count(b"\n")
    monkeypatch.setattr(remote_fetcher, "_ingest_chunk", fake_ingest)


async def test_fetch_source_new_then_skip_then_grow_then_shrink(committed_source, monkeypatch):
    src = committed_source
    path = "C:/logs/app.log"
    files = {path: (b"line1\nline2\n", 1000.0)}  # 12 bytes, ends with newline
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)

    # 1) brand-new file -> pulled whole, checkpoint written
    s1 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s1["files_fetched"] == 1
    assert s1["entries_ingested"] == 2
    assert (await remote_fetcher._load_ckpts(src))[path] == (12, 1000.0, 12)

    # 2) unchanged (same size + mtime) -> skipped, no transfer
    s2 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s2["files_fetched"] == 0 and s2["entries_ingested"] == 0

    # 3) grown -> only the new tail is read (from last_offset=12)
    files[path] = (b"line1\nline2\nline3\n", 1001.0)  # 18 bytes
    s3 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s3["files_fetched"] == 1 and s3["entries_ingested"] == 1  # only "line3\n"
    assert (await remote_fetcher._load_ckpts(src))[path] == (18, 1001.0, 18)

    # 4) shrank (rotation/truncation) -> re-read whole from offset 0
    files[path] = (b"fresh\n", 1002.0)  # 6 bytes < last_size
    s4 = await remote_fetcher._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s4["files_fetched"] == 1 and s4["entries_ingested"] == 1
    assert (await remote_fetcher._load_ckpts(src))[path] == (6, 1002.0, 6)
