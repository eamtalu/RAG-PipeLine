"""Chunk 149: the minute loop. Which tenants it evaluates, when the daily profile runs, and that the
loop is started only by its own flag (chunk 45's lesson about gates that share an `else`).
"""

import ast
import asyncio
import inspect
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from app import background as bg
from app.config.database import async_session
from app.services.analytics_at_risk import settings_store, state_store
from app.services.workers import analytics_at_risk_worker as w
from app.settings import settings
from tests import at_risk_fixtures as fx
from tests.test_background_workers_chunk10 import _stub_loops

CC = "test_chunk149ar"
CC2 = "test_chunk149ar2"
LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc


@pytest.fixture(autouse=True)
async def clean():
    for cc in (CC, CC2):
        await fx.wipe(cc)
        await fx.seed_tenant(cc)
    yield
    for cc in (CC, CC2):
        await fx.wipe(cc)


async def _candidates_mine():
    return [CC, CC2]


# ==================================================== 1. the gate

def _gate_messages(enabled: bool) -> list[str]:
    tree = ast.parse(inspect.getsource(bg))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and isinstance(node.test, ast.Attribute) \
                and node.test.attr == "analytics_at_risk_worker_enabled":
            branch = node.body if enabled else node.orelse
            for stmt in ast.walk(ast.Module(body=branch, type_ignores=[])):
                if isinstance(stmt, ast.Constant) and isinstance(stmt.value, str):
                    out.append(stmt.value)
    return out


def test_the_at_risk_gate_owns_its_own_branches():
    off = " ".join(_gate_messages(enabled=False)).lower()
    assert "at-risk" in off and "disabled" in off
    assert "forecast worker disabled" not in off and "reconcil" not in off


async def test_the_loop_starts_only_when_its_flag_is_on(monkeypatch):
    _stub_loops(monkeypatch)
    started = {"n": 0}

    async def fake_loop():
        started["n"] += 1
        await asyncio.sleep(3600)

    monkeypatch.setattr(bg, "run_analytics_at_risk_worker", fake_loop)
    for flag in ("analytics_worker_enabled", "analytics_reconcile_worker_enabled", "logspace_cleanup_worker_enabled",
                 "analytics_forecast_worker_enabled"):
        monkeypatch.setattr(settings, flag, False)
    monkeypatch.setattr(settings, "analytics_at_risk_worker_enabled", False)
    tasks = await bg.start_background_tasks()
    await asyncio.sleep(0)
    await bg.stop_background_tasks(tasks)
    assert started["n"] == 0
    monkeypatch.setattr(settings, "analytics_at_risk_worker_enabled", True)
    tasks = await bg.start_background_tasks()
    await asyncio.sleep(0)
    await bg.stop_background_tasks(tasks)
    assert started["n"] == 1


# ==================================================== 2. who

async def test_candidates_are_tenants_with_the_settlement_minus_those_switched_off():
    mine = [c for c in await w.candidates() if c in (CC, CC2)]
    assert mine == [CC, CC2]
    async with async_session() as db:
        await settings_store.put(db, CC2, enabled=False)
        await db.commit()
    assert [c for c in await w.candidates() if c in (CC, CC2)] == [CC]


async def test_a_tenant_without_the_settlement_is_never_a_candidate():
    async with async_session() as db:
        from sqlalchemy import delete
        from app.persistence.models.analytics_settlement import AnalyticsSettlement
        await db.execute(delete(AnalyticsSettlement).where(AnalyticsSettlement.customer_code == CC2,
                                                           AnalyticsSettlement.name == fx.ROUTE_SETTLEMENT_NAME))
        await db.commit()
    assert [c for c in await w.candidates() if c in (CC, CC2)] == [CC]


# ==================================================== 3. one pass

async def test_a_pass_evaluates_every_candidate_and_isolates_a_failure(monkeypatch):
    calls = []

    async def fake_evaluate(cc, *, now):
        calls.append(cc)
        if cc == CC2:
            raise RuntimeError("boom")
        return {"status": "completed", "evaluated": 3, "flagged": 1}

    async def fake_profile(cc, *, as_of, now):
        raise AssertionError("not due yet")

    monkeypatch.setattr(w.runner, "evaluate_tenant", fake_evaluate)
    monkeypatch.setattr(w.runner, "profile_tenant", fake_profile)
    monkeypatch.setattr(w, "candidates", _candidates_mine)
    stats = await w.evaluate_once(datetime(2026, 10, 2, 1, 0, tzinfo=UTC))  # 02:00 London: before the profile hour
    assert calls == [CC, CC2]
    assert stats == {"tenants": 2, "completed": 1, "skipped": 0, "failed": 1, "profiled": 0}


async def test_the_profile_runs_once_per_local_day_after_the_hour(monkeypatch):
    profiled = []

    async def fake_evaluate(cc, *, now):
        return {"status": "completed"}

    async def fake_profile(cc, *, as_of, now):
        profiled.append((cc, as_of))
        async with async_session() as db:
            await state_store.touch(db, cc, last_profiled_date=as_of)
            await db.commit()
        return {"status": "completed", "as_of": as_of.isoformat(), "routes": 0}

    monkeypatch.setattr(w.runner, "evaluate_tenant", fake_evaluate)
    monkeypatch.setattr(w.runner, "profile_tenant", fake_profile)
    monkeypatch.setattr(w, "candidates", _candidates_mine)
    before = datetime(2026, 10, 2, 1, 59, tzinfo=UTC)   # 02:59 London
    after = datetime(2026, 10, 2, 2, 0, tzinfo=UTC)     # 03:00 London
    assert (await w.evaluate_once(before))["profiled"] == 0
    assert (await w.evaluate_once(after))["profiled"] == 2
    assert sorted(profiled) == [(CC, date(2026, 10, 1)), (CC2, date(2026, 10, 1))]
    # the next minute: already done for the day
    assert (await w.evaluate_once(after.replace(minute=1)))["profiled"] == 0
    assert len(profiled) == 2
