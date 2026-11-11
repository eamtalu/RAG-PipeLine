"""Step 16 of docs/esmart-agent-execution-plan.md: shared quotas, admission in the gateway.

Nothing on the backend limited how many runs a tenant or a person could start: the edge refuses
Teams users above its hourly numbers, the web had no limit, and the only cap on concurrent runs
was each consumer process's own semaphore. Now the gateway admits a run before any model call
with counters in Postgres that every worker and web process changes by one atomic operation
under a per-tenant lock, so five workers enforce one number. A refusal is a plain sentence through
the normal answer path; an admitted run frees its slot however it ends.
"""

import asyncio
import dataclasses
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage
from sqlalchemy import delete, func, select

from app.agent.contracts import AgentResult
from app.agent.gateway import run_request
from app.agent.gateway.run import QUOTA_REFUSALS
from app.config.database import async_session
from app.persistence.models.agent_in_flight import AgentInFlight
from app.persistence.models.customer import Customer
from app.persistence.models.quota_window import QuotaWindow
from app.services.agent_quotas.admission import PostgresAdmission, QuotaLimits
from app.services.logspace_cleanup import purge_logspace
from app.settings import settings
from tests.agent_helpers import analytics_runtime, request_for
from tests.test_agent_run_deadline_chunk155 import _hanging
from tests.test_analytics_agent_chunk124 import CC as AGENT_CC
from tests.test_analytics_agent_chunk124 import _scripted, clean  # noqa: F401 - autouse fixture

CC = "test_chunk166"
NOW = datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc)
DEADLINE = NOW + timedelta(seconds=120)
WIDE = QuotaLimits(tenant_per_hour=60, user_per_hour=20, tenant_in_flight=8)


async def _wipe(cc: str = CC):
    async with async_session() as db:
        await db.execute(delete(AgentInFlight).where(AgentInFlight.customer_code == cc))
        await db.execute(delete(QuotaWindow).where(QuotaWindow.customer_code == cc))
        await db.execute(delete(Customer).where(Customer.customer_code == cc))
        await db.commit()


@pytest.fixture(autouse=True)
async def fresh():
    await _wipe()
    yield
    await _wipe()


def _admission(**limits) -> PostgresAdmission:
    return PostgresAdmission(async_session, dataclasses.replace(WIDE, **limits))


async def _admit(adm: PostgresAdmission, user: str, trace: str, *, now=NOW, deadline=DEADLINE):
    return await adm.admit(tenant=CC, user_id=user, trace_id=trace, deadline_at=deadline, now=now)


async def _in_flight_rows() -> int:
    async with async_session() as db:
        return await db.scalar(
            select(func.count()).select_from(AgentInFlight).where(AgentInFlight.customer_code == CC)
        )


async def _used(user_id: str) -> int | None:
    async with async_session() as db:
        return await db.scalar(
            select(QuotaWindow.used).where(
                QuotaWindow.customer_code == CC, QuotaWindow.user_id == user_id
            )
        )


# ==================================================== 1. five workers, one limit


async def test_five_concurrent_admissions_share_one_in_flight_limit():
    adm = _admission(tenant_in_flight=3)
    reasons = await asyncio.gather(*(_admit(adm, f"u{i}", f"t{i}") for i in range(5)))
    assert sorted(r or "admitted" for r in reasons) == [
        "admitted",
        "admitted",
        "admitted",
        "tenant_in_flight",
        "tenant_in_flight",
    ]
    assert await _in_flight_rows() == 3
    admitted = [f"t{i}" for i, r in enumerate(reasons) if r is None]
    await adm.release(tenant=CC, trace_id=admitted[0])
    assert await _admit(adm, "u9", "t9") is None and await _in_flight_rows() == 3


# ==================================================== 2. the hourly windows


async def test_a_person_is_refused_at_their_hourly_limit_and_the_refusal_counts_nothing():
    adm = _admission(user_per_hour=2)
    for n in range(2):
        assert await _admit(adm, "amin", f"a{n}") is None
        await adm.release(tenant=CC, trace_id=f"a{n}")
    assert await _admit(adm, "amin", "a2") == "user_per_hour"
    assert (await _used("amin"), await _used("")) == (2, 2)
    assert await _admit(adm, "someone-else", "b0") is None


async def test_the_tenant_limit_refuses_everyone_and_the_next_hour_is_fresh():
    adm = _admission(tenant_per_hour=2)
    for n in range(2):
        assert await _admit(adm, f"u{n}", f"c{n}") is None
        await adm.release(tenant=CC, trace_id=f"c{n}")
    assert await _admit(adm, "u7", "c7") == "tenant_per_hour"
    later = NOW + timedelta(hours=1)
    assert await _admit(adm, "u7", "c8", now=later, deadline=later + timedelta(seconds=120)) is None


# ==================================================== 3. the sweep


async def test_an_expired_in_flight_row_is_swept_and_does_not_count():
    adm = _admission(tenant_in_flight=1)
    assert await _admit(adm, "u1", "old", deadline=NOW - timedelta(seconds=1)) is None
    assert await _admit(adm, "u2", "new") is None
    async with async_session() as db:
        rows = (await db.execute(select(AgentInFlight.trace_id))).scalars().all()
    assert rows == ["new"]


async def test_hourly_rows_older_than_a_day_are_swept():
    adm = _admission()
    two_days_ago = NOW - timedelta(days=2)
    assert await _admit(adm, "u1", "x0", now=two_days_ago, deadline=two_days_ago) is None
    assert await _admit(adm, "u1", "x1") is None
    async with async_session() as db:
        starts = (
            await db.execute(
                select(QuotaWindow.window_start).where(QuotaWindow.customer_code == CC).distinct()
            )
        ).scalars().all()
    assert starts == [NOW.replace(minute=0)]


# ==================================================== 4. the gateway


class SpyAdmission:
    def __init__(self, reason: str | None = None, error: Exception | None = None):
        self.reason, self.error = reason, error
        self.admitted: list[str] = []
        self.released: list[str] = []

    async def admit(self, *, tenant, user_id, trace_id, deadline_at, now):
        if self.error:
            raise self.error
        self.admitted.append(trace_id)
        return self.reason

    async def release(self, *, tenant, trace_id):
        self.released.append(trace_id)


class SpyStore:
    def __init__(self):
        self.rows = []

    async def save(self, row):
        self.rows.append(dict(row))


class _CountingRuntime:
    def __init__(self, error: Exception | None = None):
        self.calls, self.error = 0, error

    async def run(self, context, request, prior_turns=(), *, trace=None):
        self.calls += 1
        if self.error:
            raise self.error
        return AgentResult(
            answer="Fine.",
            stop_reason="end_turn",
            model="m",
            request_id=context.request_id,
            trace_id=context.trace_id,
        )


async def test_a_refused_run_never_reaches_the_runtime_and_leaves_a_trace_row(caplog):
    admission, store, runtime = SpyAdmission("tenant_per_hour"), SpyStore(), _CountingRuntime()
    with caplog.at_level(logging.INFO):
        result = await run_request(
            request_for(AGENT_CC, "hi"), runtime=runtime, admission=admission, trace_store=store
        )
    assert runtime.calls == 0 and admission.released == []
    assert (result.stop_reason, result.declined) == ("declined", "quota_tenant_per_hour")
    assert result.answer == QUOTA_REFUSALS["tenant_per_hour"] and result.status == "declined"
    [row] = store.rows
    assert (row["declined"], row["status"], row["trace_id"]) == (
        "quota_tenant_per_hour",
        "declined",
        result.trace_id,
    )
    assert any("agent.refused" in r.getMessage() for r in caplog.records)
    assert "quota" not in result.to_legacy_dict()["answer"]


async def test_an_admitted_run_releases_its_slot_after_the_answer():
    admission, runtime = SpyAdmission(), _CountingRuntime()
    result = await run_request(request_for(AGENT_CC, "hi"), runtime=runtime, admission=admission)
    assert result.answer == "Fine." and runtime.calls == 1
    assert admission.admitted == [result.trace_id] and admission.released == [result.trace_id]


async def test_the_slot_is_released_when_the_runtime_raises_or_times_out():
    admission = SpyAdmission()
    with pytest.raises(RuntimeError):
        await run_request(
            request_for(AGENT_CC, "hi"),
            runtime=_CountingRuntime(RuntimeError("down")),
            admission=admission,
        )
    assert len(admission.released) == 1
    admission = SpyAdmission()
    result = await run_request(
        request_for(AGENT_CC, "hi", deadline_seconds=0.2),
        runtime=analytics_runtime(AGENT_CC, model=_hanging()),
        admission=admission,
    )
    assert result.stop_reason == "timeout" and admission.released == [result.trace_id]


async def test_a_failing_admission_store_lets_the_run_through_and_says_so(caplog):
    admission, runtime = SpyAdmission(error=RuntimeError("db gone")), _CountingRuntime()
    with caplog.at_level(logging.ERROR):
        result = await run_request(
            request_for(AGENT_CC, "hi"), runtime=runtime, admission=admission
        )
    assert result.answer == "Fine." and runtime.calls == 1
    assert any("agent.admission_failed" in r.getMessage() for r in caplog.records)
    assert admission.released == []  # nothing was admitted, so nothing is released


async def test_the_real_admission_refuses_through_the_gateway():
    adm = _admission(tenant_per_hour=1)
    model = _scripted(AIMessage(content="Fine."), AIMessage(content="Fine again."))
    first = await run_request(
        request_for(CC, "hi"), runtime=analytics_runtime(CC, model=model), admission=adm
    )
    second = await run_request(
        request_for(CC, "hi"), runtime=analytics_runtime(CC, model=model), admission=adm
    )
    assert first.answer == "Fine." and second.declined == "quota_tenant_per_hour"
    assert await _in_flight_rows() == 0


# ==================================================== 5. the roots and the defaults


def test_the_hourly_defaults_equal_the_edges_numbers():
    assert QuotaLimits.from_settings() == QuotaLimits(
        tenant_per_hour=settings.agent_quota_tenant_per_hour,
        user_per_hour=settings.agent_quota_user_per_hour,
        tenant_in_flight=settings.agent_quota_tenant_in_flight,
    )
    # the edge refuses first for Teams at these numbers (teams-agent-edge settings.py)
    assert (settings.agent_quota_tenant_per_hour, settings.agent_quota_user_per_hour) == (60, 20)
    assert settings.agent_quota_tenant_in_flight == 8


def test_the_admission_module_imports_nothing_from_the_agent_package():
    source = Path("app/services/agent_quotas/admission.py").read_text()
    assert "app.agent" not in source


# ==================================================== 6. the purge


async def test_the_purge_deletes_a_tenants_quota_rows():
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="quota probe", timezone="Europe/London"))
        await db.commit()
    assert await _admit(_admission(), "u1", "p0") is None
    assert await _in_flight_rows() == 1 and await _used("") == 1
    async with async_session() as db:
        assert await purge_logspace(db, CC)
        await db.commit()
    assert await _in_flight_rows() == 0 and await _used("") is None
