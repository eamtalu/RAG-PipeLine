"""Chunk 7: fully frontend-managed auto-poll — the poll supervisor runs by default and is driven
solely by each source's `enabled` flag (no env to set); `ssh_log_fetcher_enabled` is only a global
kill-switch.

Covered:
- the kill-switch defaults ON;
- the supervisor stays idle (spawns no loops) when nothing is enabled;
- the supervisor spawns a per-customer loop as soon as a customer has an enabled source, and cancels
  all children on shutdown.
"""

import asyncio

import pytest

from app.settings import settings
from app.services.workers import ssh_log_fetcher as w
from app.services.mnp_log_ingestion.remote import remote_fetcher as r
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode
from tests.test_ssh_hardening_chunk2 import _patch_sftp, _patch_ingest_counts_lines


def test_poller_kill_switch_defaults_on():
    # ON by default so auto-poll works with no env; frontend controls it per source via `enabled`.
    assert settings.ssh_log_fetcher_enabled is True


async def test_supervisor_idle_when_no_enabled_sources(monkeypatch):
    spawned = []

    async def fake_loop(cc):
        spawned.append(cc)
        await asyncio.sleep(3600)

    async def none():
        return []

    monkeypatch.setattr(w, "_customer_loop", fake_loop)
    monkeypatch.setattr(w, "_customers_with_enabled_sources", none)
    monkeypatch.setattr(settings, "ssh_poll_reconcile_seconds", 0.01)

    sup = asyncio.create_task(w.run_ssh_log_fetcher())
    await asyncio.sleep(0.08)  # several reconcile ticks
    sup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sup
    assert spawned == []  # inert: no loops while nothing is enabled


async def test_supervisor_spawns_loop_for_enabled_customer(monkeypatch):
    spawned = []
    started = asyncio.Event()

    async def fake_loop(cc):
        spawned.append(cc)
        started.set()
        await asyncio.sleep(3600)  # stay alive until the supervisor cancels it

    async def one():
        return ["cust-x"]

    monkeypatch.setattr(w, "_customer_loop", fake_loop)
    monkeypatch.setattr(w, "_customers_with_enabled_sources", one)
    monkeypatch.setattr(settings, "ssh_poll_reconcile_seconds", 0.01)

    sup = asyncio.create_task(w.run_ssh_log_fetcher())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        assert "cust-x" in spawned  # enabling a source (frontend) makes the poller act
    finally:
        sup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sup


# =========================================================== "start from now": seed mode
async def test_seed_mode_ingests_nothing_and_starts_from_now(committed_source, monkeypatch):
    src = committed_source
    a, b = "C:/logs/a.log", "C:/logs/b.log"
    files = {a: (b"x\n" * 100, 1000.0), b: (b"y\n" * 50, 900.0)}  # existing history on the server
    _patch_sftp(monkeypatch, files)
    _patch_ingest_counts_lines(monkeypatch)  # would count lines IF anything were ingested

    s = await r._fetch_source(src, LogSshFetchMode.seed, None)
    assert s["entries_ingested"] == 0 and s["files_fetched"] == 0  # zero backfill
    ckpts = await r._load_ckpts(src)
    assert ckpts[a][:3] == (200, 1000.0, 200)   # seeded to EOF (offset == size)
    assert ckpts[b][:3] == (100, 900.0, 100)

    # now a poll only picks up NEW appends; unchanged files are skipped
    files[a] = (b"x\n" * 100 + b"z\n" * 3, 1001.0)  # 3 new lines appended after the seed point
    s2 = await r._fetch_source(src, LogSshFetchMode.incremental, None)
    assert s2["entries_ingested"] == 3           # only the 3 new lines, not the original 100
    assert b not in {f["file"] for f in s2["by_file"]}  # b unchanged -> skipped


def test_seed_is_a_valid_fetch_mode():
    assert "seed" in LogSshFetchMode.__members__
    # the on-demand request model accepts it
    from app.api.v1.log_sources import FetchRemoteRequest
    assert FetchRemoteRequest(mode="seed").mode == LogSshFetchMode.seed
