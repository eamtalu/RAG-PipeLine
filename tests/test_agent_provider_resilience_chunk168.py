"""Step 18 of docs/esmart-agent-execution-plan.md: timeouts, retry on throttling, breakers.

Every provider call ran unbounded until the run deadline, a throttled call failed outright, and a
provider that was down was tried by every run. Now every model call the agent makes, the gate's
and the loop's, goes through a guard owned per provider and shared by every run of the process:
a timeout on the call, one jittered retry only when the provider throttled, a breaker that keeps
a failing provider out of the pick for a cool-down, and a bulkhead that caps the calls in flight.
Each of a timeout, a 429 and a 500 has one test that holds the documented result.
"""

import asyncio
import dataclasses
import logging
import time

import pytest
from botocore.exceptions import ClientError
from langchain_core.messages import AIMessage

from app.agent.contracts import ResilienceSettings
from app.agent.gateway import run_request, snapshot_settings
from app.agent.router.failures import ModelCallTimeout, classify
from app.agent.router.health import ProviderGuard, ProviderHealth
from app.agent.router.router import ModelRouter
from tests.agent_helpers import analytics_runtime, request_for
from tests.test_agent_model_router_chunk167 import FALLBACK, PRIMARY, _factory
from tests.test_agent_run_deadline_chunk155 import HangingModel
from tests.test_analytics_agent_chunk124 import (  # noqa: F401 - autouse fixture
    CC,
    ScriptedModel,
    clean,
)

RESILIENCE = ResilienceSettings(bulkhead=8, breaker_failures=3, breaker_cooldown_seconds=30.0)


def _aws(code: str, status: int) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "Converse",
    )


class RateLimitError(Exception):
    pass


class InternalServerError(Exception):
    pass


class FakeClock:
    """A clock the guard reads and a sleep that only moves it, so nothing waits for real."""

    def __init__(self):
        self.now_seconds = 1000.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.now_seconds

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now_seconds += seconds


class FailingModel(ScriptedModel):
    """Raises the given error on the listed calls (1-based), answers from the script otherwise."""

    fail_on: tuple = ()
    error: object = None
    calls: int = 0

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.calls += 1
        if self.calls in self.fail_on:
            raise self.error
        return super()._generate(messages, stop, run_manager, **kwargs)


def _health(clock: FakeClock) -> ProviderHealth:
    return ProviderHealth(clock=clock.now, sleep=clock.sleep, jitter=lambda: 0.25)


def _run_settings(*, fallbacks=(FALLBACK,), call_timeout=60.0, resilience=RESILIENCE):
    rs = snapshot_settings()
    return dataclasses.replace(
        rs,
        model=dataclasses.replace(rs.model, name=PRIMARY, fallback_models=tuple(fallbacks)),
        budgets=dataclasses.replace(rs.budgets, call_timeout_seconds=call_timeout),
        resilience=resilience,
    )


def _scripted(*messages) -> ScriptedModel:
    return ScriptedModel(script=list(messages), seen=[])


def _call(name: str) -> AIMessage:
    return AIMessage(content="", tool_calls=[{"id": "c1", "name": name, "args": {}}])


# ==================================================== 1. what a failure is


def test_failures_are_classified_by_status_code_and_name():
    assert classify(_aws("ThrottlingException", 429)) == "throttled"
    assert classify(RateLimitError("429 Too Many Requests")) == "throttled"
    assert classify(_aws("ServiceUnavailableException", 503)) == "server_error"
    assert classify(InternalServerError("boom")) == "server_error"
    assert classify(ModelCallTimeout("ollama", 60.0)) == "timeout"
    assert classify(ValueError("not a provider problem")) == "other"


# ==================================================== 2. a 429 is retried once, with jitter


async def test_a_throttled_call_is_retried_once_and_the_primary_answers(monkeypatch, caplog):
    clock = FakeClock()
    primary = FailingModel(
        script=[AIMessage(content="in_scope"), AIMessage(content="Fine.")],
        seen=[],
        fail_on=(1,),
        error=_aws("ThrottlingException", 429),
    )
    asked = _factory(monkeypatch, {PRIMARY: primary, FALLBACK: _scripted()})
    with caplog.at_level(logging.WARNING):
        result = await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC, router=ModelRouter(_health(clock))),
            run_settings=_run_settings(),
        )
    assert result.answer == "Fine." and result.model == PRIMARY and asked == [PRIMARY]
    assert primary.calls == 3 and clock.sleeps == [0.75]  # 0.5 s plus the injected jitter
    assert any("agent.model.retry" in r.getMessage() for r in caplog.records)


async def test_throttled_twice_the_fallback_answers_and_both_failures_count(monkeypatch):
    clock = FakeClock()
    health = _health(clock)
    primary = FailingModel(script=[], seen=[], fail_on=(1, 2), error=_aws("Throttling", 429))
    fallback = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    _factory(monkeypatch, {PRIMARY: primary, FALLBACK: fallback})
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC, router=ModelRouter(health)),
        run_settings=_run_settings(),
    )
    assert result.answer == "Fine." and result.model == FALLBACK and primary.calls == 2
    assert health.guard_for("ollama", RESILIENCE).failures == 2


# ==================================================== 3. a 500 is never retried


async def test_a_server_error_goes_straight_to_the_fallback(monkeypatch):
    clock = FakeClock()
    primary = FailingModel(
        script=[], seen=[], fail_on=(1,), error=_aws("ServiceUnavailableException", 503)
    )
    fallback = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    _factory(monkeypatch, {PRIMARY: primary, FALLBACK: fallback})
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC, router=ModelRouter(_health(clock))),
        run_settings=_run_settings(),
    )
    assert result.answer == "Fine." and primary.calls == 1 and clock.sleeps == []


# ==================================================== 4. a hanging call is cut at the call timeout


async def test_a_hanging_call_is_cancelled_at_the_call_timeout_and_the_fallback_answers(
    monkeypatch,
):
    primary = HangingModel(script=[], seen=[])
    fallback = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    _factory(monkeypatch, {PRIMARY: primary, FALLBACK: fallback})
    started = time.monotonic()
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC, router=ModelRouter(_health(FakeClock()))),
        run_settings=_run_settings(call_timeout=0.1),
    )
    assert result.answer == "Fine." and result.model == FALLBACK
    assert primary.was_cancelled and time.monotonic() - started < 3


async def test_without_a_fallback_the_call_timeout_is_the_error_that_propagates(monkeypatch):
    _factory(monkeypatch, {PRIMARY: HangingModel(script=[], seen=[])})
    with pytest.raises(ModelCallTimeout, match="ollama"):
        await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC, router=ModelRouter(_health(FakeClock()))),
            run_settings=_run_settings(fallbacks=(), call_timeout=0.1),
        )


# ==================================================== 5. the breaker


async def _failing_call():
    raise _aws("ServiceUnavailableException", 503)


async def _fine_call():
    return "fine"


async def test_three_failures_open_the_breaker_and_the_pick_skips_the_provider(
    monkeypatch, caplog
):
    clock = FakeClock()
    health = _health(clock)
    guard = health.guard_for("ollama", RESILIENCE)
    for _ in range(3):
        with pytest.raises(ClientError):
            await guard.call(_failing_call, timeout_seconds=1.0)
    assert guard.state == "open" and guard.availability() == "breaker open"
    fallback = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    asked = _factory(monkeypatch, {PRIMARY: _scripted(), FALLBACK: fallback})
    with caplog.at_level(logging.WARNING):
        result = await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC, router=ModelRouter(health)),
            run_settings=_run_settings(),
        )
    assert result.model == FALLBACK and asked == [FALLBACK]
    assert any("breaker open" in r.getMessage() for r in caplog.records)


async def test_after_the_cool_down_one_call_probes_and_a_success_closes_the_breaker():
    clock = FakeClock()
    guard = _health(clock).guard_for("ollama", RESILIENCE)
    for _ in range(3):
        with pytest.raises(ClientError):
            await guard.call(_failing_call, timeout_seconds=1.0)
    clock.now_seconds += 31
    assert guard.state == "half_open" and guard.availability() is None
    assert await guard.call(_fine_call, timeout_seconds=1.0) == "fine"
    assert guard.state == "closed" and guard.failures == 0


async def test_a_failure_while_half_open_reopens_the_breaker():
    clock = FakeClock()
    guard = _health(clock).guard_for("ollama", RESILIENCE)
    for _ in range(3):
        with pytest.raises(ClientError):
            await guard.call(_failing_call, timeout_seconds=1.0)
    clock.now_seconds += 31
    with pytest.raises(ClientError):
        await guard.call(_failing_call, timeout_seconds=1.0)
    assert guard.state == "open" and guard.availability() == "breaker open"


# ==================================================== 6. the bulkhead


async def test_a_full_bulkhead_skips_the_provider_until_a_call_finishes(monkeypatch):
    clock = FakeClock()
    health = _health(clock)
    one = ResilienceSettings(bulkhead=1, breaker_failures=3, breaker_cooldown_seconds=30.0)
    guard = health.guard_for("ollama", one)
    release = asyncio.Event()

    async def slow_call():
        await release.wait()
        return "done"

    busy = asyncio.create_task(guard.call(slow_call, timeout_seconds=5.0))
    await asyncio.sleep(0)
    assert guard.in_flight == 1 and guard.availability() == "bulkhead full"
    fallback = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    asked = _factory(monkeypatch, {PRIMARY: _scripted(), FALLBACK: fallback})
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC, router=ModelRouter(health)),
        run_settings=_run_settings(resilience=one),
    )
    assert result.model == FALLBACK and asked == [FALLBACK]
    release.set()
    assert await busy == "done"
    assert guard.in_flight == 0 and guard.availability() is None


# ==================================================== 7. the guard wraps the gate and the loop


async def test_every_model_call_of_a_run_goes_through_the_guard(monkeypatch):
    clock = FakeClock()
    health = _health(clock)
    primary = _scripted(
        AIMessage(content="in_scope"),
        _call("describe_releases"),
        AIMessage(content="There are releases."),
    )
    _factory(monkeypatch, {PRIMARY: primary})
    result = await run_request(
        request_for(CC, "what is there?"),
        runtime=analytics_runtime(CC, router=ModelRouter(health)),
        run_settings=_run_settings(fallbacks=()),
    )
    assert result.answer == "There are releases." and len(result.tool_calls) == 1
    guard = health.guard_for("ollama", RESILIENCE)
    assert guard.calls_made == 3 and guard.in_flight == 0 and guard.state == "closed"


# ==================================================== 8. the numbers and the roots


def test_the_snapshot_carries_the_four_numbers_with_their_defaults():
    rs = snapshot_settings()
    assert rs.budgets.call_timeout_seconds == 60.0
    assert rs.resilience == ResilienceSettings(
        bulkhead=8, breaker_failures=3, breaker_cooldown_seconds=30.0
    )


def test_each_process_has_one_router_with_shared_health():
    from app import teams_consumer
    from app.api import deps

    assert deps.model_router() is deps.model_router()
    assert isinstance(teams_consumer._ROUTER, ModelRouter)
    assert isinstance(deps.model_router().health, ProviderHealth)


def test_a_guard_is_one_per_provider_and_keeps_its_numbers():
    health = _health(FakeClock())
    first = health.guard_for("bedrock", RESILIENCE)
    assert health.guard_for("bedrock", RESILIENCE) is first
    assert isinstance(first, ProviderGuard) and first.provider == "bedrock"
    assert health.guard_for("ollama", RESILIENCE) is not first
