"""Chunk 141: the nightly loop. When a tenant is due, which tenants are due, and that the loop is
started only by its own flag (chunk 45's lesson about gates that share an `else`).
"""

import ast
import asyncio
import inspect
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app import background as bg
from app.config.database import async_session
from app.services.analytics_forecast import MODEL_VERSION, run_store
from app.services.workers import analytics_forecast_worker as w
from app.settings import settings
from tests import forecast_fixtures as fx
from tests.test_background_workers_chunk10 import _stub_loops

CC = "test_chunk141fc"
CC2 = "test_chunk141fc2"
LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.wipe(CC2)
    await fx.seed_tenant(CC)
    await fx.seed_tenant(CC2)
    yield
    await fx.wipe(CC)
    await fx.wipe(CC2)


# ==================================================== 1. when

def test_local_as_of_is_yesterday_once_the_local_clock_passes_the_run_hour():
    # 03:00 London on 5 Oct (BST) is 02:00Z: due, as of the 4th
    assert w.local_as_of(datetime(2026, 10, 5, 2, 0, tzinfo=UTC), LONDON, run_hour=3) == date(2026, 10, 4)
    # 02:30 London: not yet
    assert w.local_as_of(datetime(2026, 10, 5, 1, 30, tzinfo=UTC), LONDON, run_hour=3) is None
    # after the clocks go back (GMT), 03:00 London IS 03:00Z
    assert w.local_as_of(datetime(2026, 10, 26, 2, 30, tzinfo=UTC), LONDON, run_hour=3) is None
    assert w.local_as_of(datetime(2026, 10, 26, 3, 0, tzinfo=UTC), LONDON, run_hour=3) == date(2026, 10, 25)


# ==================================================== 2. who

async def _due(now, run_hour=3):
    """Only this chunk's tenants: the shared database may hold a real one with the settlement."""
    return [t for t in await w.due_tenants(now, run_hour=run_hour) if t[0] in (CC, CC2)]

async def test_due_tenants_skips_one_already_done_today_and_one_mid_run():
    now = datetime(2026, 10, 5, 2, 0, tzinfo=UTC)
    async with async_session() as db:
        done = await run_store.create(db, CC, as_of_date=date(2026, 10, 4), trigger="nightly", model_version=MODEL_VERSION)
        await db.commit()
        await run_store.claim(db, done.id)
        await run_store.finish(db, done.id, status="completed")
        await db.commit()
    assert await _due(now) == [(CC2, date(2026, 10, 4))]

    async with async_session() as db:
        busy = await run_store.create(db, CC2, as_of_date=date(2026, 10, 4), trigger="manual", model_version=MODEL_VERSION)
        await db.commit()
        await run_store.claim(db, busy.id)
        await db.commit()
    assert await _due(now) == []

    # a skipped run counts as done for the day too: no retrying a tenant that has no history yet
    async with async_session() as db:
        await run_store.finish(db, busy.id, status="skipped", error="insufficient_history")
        await db.commit()
    assert await _due(now) == []
    # but the next day both are due again
    assert sorted(await _due(now + timedelta(days=1))) == [(CC, date(2026, 10, 5)), (CC2, date(2026, 10, 5))]


async def test_a_tenant_without_the_settlement_is_never_due():
    async with async_session() as db:
        from sqlalchemy import delete
        from app.persistence.models.analytics_settlement import AnalyticsSettlement
        await db.execute(delete(AnalyticsSettlement).where(AnalyticsSettlement.customer_code == CC2))
        await db.commit()
    assert await _due(datetime(2026, 10, 5, 2, 0, tzinfo=UTC)) == [(CC, date(2026, 10, 4))]


async def test_forecast_once_runs_each_due_tenant_and_isolates_failures(monkeypatch):
    calls = []

    async def fake_run(cc, *, as_of_date, trigger):
        calls.append((cc, as_of_date, trigger))
        if cc == CC2:
            raise RuntimeError("boom")
        return {"status": "completed"}

    monkeypatch.setattr(w.runner, "run_tenant", fake_run)
    monkeypatch.setattr(w, "_candidates", _candidates_mine)
    stats = await w.forecast_once(datetime(2026, 10, 5, 2, 0, tzinfo=UTC))
    assert sorted(c[0] for c in calls) == [CC, CC2]
    assert all(c[1] == date(2026, 10, 4) and c[2] == "nightly" for c in calls)
    assert stats == {"due": 2, "completed": 1, "skipped": 0, "failed": 1}


async def _candidates_mine():
    return [CC, CC2]


# ==================================================== 3. the gate

def _gate_messages(enabled: bool) -> list[str]:
    tree = ast.parse(inspect.getsource(bg))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and isinstance(node.test, ast.Attribute) \
                and node.test.attr == "analytics_forecast_worker_enabled":
            branch = node.body if enabled else node.orelse
            for stmt in ast.walk(ast.Module(body=branch, type_ignores=[])):
                if isinstance(stmt, ast.Constant) and isinstance(stmt.value, str):
                    out.append(stmt.value)
    return out


def test_the_forecast_gate_owns_its_own_branches():
    off = " ".join(_gate_messages(enabled=False)).lower()
    assert "forecast" in off and "disabled" in off
    assert "reconcil" not in off and "analytics worker disabled" not in off


async def test_the_loop_starts_only_when_its_flag_is_on(monkeypatch):
    _stub_loops(monkeypatch)
    started = {"n": 0}

    async def fake_loop():
        started["n"] += 1
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_analytics_forecast_worker", fake_loop)
    for flag in ("analytics_worker_enabled", "analytics_reconcile_worker_enabled", "logspace_cleanup_worker_enabled"):
        monkeypatch.setattr(settings, flag, False)
    monkeypatch.setattr(settings, "analytics_forecast_worker_enabled", False)
    tasks = await bg.start_background_tasks()
    await asyncio.sleep(0)
    await bg.stop_background_tasks(tasks)
    assert started["n"] == 0
    monkeypatch.setattr(settings, "analytics_forecast_worker_enabled", True)
    tasks = await bg.start_background_tasks()
    await asyncio.sleep(0)
    await bg.stop_background_tasks(tasks)
    assert started["n"] == 1
