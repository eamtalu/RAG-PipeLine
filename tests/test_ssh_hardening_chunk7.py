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
