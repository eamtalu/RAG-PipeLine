"""Step 19 of docs/esmart-agent-execution-plan.md: usage accounting; failure sentences by kind.

A run's trace summed the tokens of the calls that completed, nothing recorded cost, nothing
survived a run that raised, nothing was summed per tenant per day, and the failed-run sentence
was written for Anthropic errors while the server runs Bedrock. Now the provider guard records
every call on the gateway-owned trace, the gateway sums them into usage_daily per tenant, UTC
day, provider and model, priced from a supplied list, an admin endpoint shows the days, and the
sentence depends on what any provider said. Answers do not change.
"""

import dataclasses
import logging
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from httpx import ASGITransport, AsyncClient
from langchain_core.messages import AIMessage
from sqlalchemy import delete, select

from app.agent.contracts import ModelPrice
from app.agent.gateway import run_request, snapshot_settings
from app.agent.gateway.accounting import cost_of, parse_prices
from app.agent.router.failures import ModelCallTimeout, classify
from app.config.database import async_session
from app.main import app
from app.persistence.models.customer import Customer
from app.persistence.models.usage_daily import UsageDaily
from app.services.agent_usage.store import PostgresUsageStore
from app.services.logspace_cleanup import purge_logspace
from app.services.teams import consumer as c
from app.settings import settings
from tests.agent_helpers import analytics_runtime, request_for
from tests.test_agent_guards_pipeline_chunk164 import SpyStore
from tests.test_agent_model_router_chunk167 import FALLBACK, PRIMARY, _factory
from tests.test_agent_provider_resilience_chunk168 import FailingModel, _aws
from tests.test_analytics_agent_chunk124 import (  # noqa: F401 - autouse fixture
    CC,
    ScriptedModel,
    _scripted,
    clean,
)

PRICES = {PRIMARY: ModelPrice(input_per_million=1.0, output_per_million=4.0)}
TODAY = datetime.now(timezone.utc).date()


def _with_usage(text: str, *, inp: int, out: int, cached: int = 0) -> AIMessage:
    return AIMessage(
        content=text,
        usage_metadata={
            "input_tokens": inp,
            "output_tokens": out,
            "total_tokens": inp + out,
            "input_token_details": {"cache_read": cached},
        },
    )


def _run_settings(prices=PRICES, fallbacks=()):
    rs = snapshot_settings()
    return dataclasses.replace(
        rs,
        model=dataclasses.replace(rs.model, name=PRIMARY, fallback_models=tuple(fallbacks)),
        prices=prices,
    )


async def _wipe_usage(cc: str = CC):
    async with async_session() as db:
        await db.execute(delete(UsageDaily).where(UsageDaily.customer_code == cc))
        await db.commit()


async def _rows(cc: str = CC) -> list[UsageDaily]:
    async with async_session() as db:
        return list(
            (
                await db.execute(
                    select(UsageDaily)
                    .where(UsageDaily.customer_code == cc)
                    .order_by(UsageDaily.day, UsageDaily.provider, UsageDaily.model)
                )
            ).scalars()
        )


@pytest.fixture(autouse=True)
async def no_usage_rows():
    await _wipe_usage()
    yield
    await _wipe_usage()


# ==================================================== 1. prices


def test_the_price_list_is_parsed_from_the_setting(monkeypatch):
    raw = '{"bedrock_converse:x": {"input_per_million": 0.2, "output_per_million": 0.8}}'
    monkeypatch.setattr(settings, "analytics_agent_prices", raw)
    assert snapshot_settings().prices == {
        "bedrock_converse:x": ModelPrice(input_per_million=0.2, output_per_million=0.8)
    }
    assert parse_prices("") == {} and parse_prices(" ") == {}
    with pytest.raises(ValueError, match="ANALYTICS_AGENT_PRICES"):
        parse_prices("{not json")
    with pytest.raises(ValueError, match="ANALYTICS_AGENT_PRICES"):
        parse_prices('{"m": {"input_per_million": 1}}')


def test_the_cost_prices_input_cached_and_output_tokens():
    price = ModelPrice(input_per_million=1.0, output_per_million=4.0, cached_input_per_million=0.1)
    assert cost_of(price, input_tokens=1_000_000, output_tokens=0, cached_tokens=0) == 1.0
    assert cost_of(price, input_tokens=1_000_000, output_tokens=0, cached_tokens=500_000) == 0.55
    assert cost_of(price, input_tokens=0, output_tokens=250_000, cached_tokens=0) == 1.0
    assert cost_of(None, input_tokens=5, output_tokens=5, cached_tokens=0) is None
    plain = ModelPrice(input_per_million=2.0, output_per_million=2.0)
    assert cost_of(plain, input_tokens=1_000_000, output_tokens=0, cached_tokens=1_000_000) == 2.0


# ==================================================== 2. every call is on the trace


async def test_the_trace_records_every_call_with_its_tokens(monkeypatch):
    primary = _scripted(
        _with_usage("in_scope", inp=12, out=1), _with_usage("Fine.", inp=8, out=5, cached=4)
    )
    _factory(monkeypatch, {PRIMARY: primary})
    result = await run_request(
        request_for(CC, "hi"), runtime=analytics_runtime(CC), run_settings=_run_settings()
    )
    usage = dict(result.usage)
    by_model = usage.pop("by_model")
    assert usage == {"model_calls": 2, "input_tokens": 20, "output_tokens": 6, "cached_tokens": 4}
    [row] = by_model
    assert (row["provider"], row["model"], row["calls"], row["failed_calls"]) == (
        "ollama",
        PRIMARY,
        2,
        0,
    )
    assert (row["input_tokens"], row["output_tokens"], row["cached_tokens"]) == (20, 6, 4)
    assert row["latency_ms"] >= 0 and result.trace.usage == result.usage


async def test_an_injected_test_model_is_recorded_under_the_test_provider():
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC, model=_scripted(AIMessage(content="Fine."))),
    )
    [row] = result.usage["by_model"]
    assert (row["provider"], row["model"], row["calls"]) == ("test", "test:injected", 1)


# ==================================================== 3. usage_daily


async def test_a_run_leaves_one_priced_row_per_model_and_a_second_run_adds_up(monkeypatch):
    primary = _scripted(
        _with_usage("in_scope", inp=1_000, out=10),
        _with_usage("Fine.", inp=2_000, out=90),
        _with_usage("in_scope", inp=1_000, out=10),
        _with_usage("Fine again.", inp=2_000, out=90),
    )
    _factory(monkeypatch, {PRIMARY: primary})
    store = PostgresUsageStore(async_session)
    for _ in range(2):
        result = await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(),
            usage_store=store,
        )
        assert result.stop_reason == "end_turn"
    [row] = await _rows()
    assert (row.day, row.provider, row.model) == (TODAY, "ollama", PRIMARY)
    assert (row.calls, row.failed_calls) == (4, 0)
    assert (row.input_tokens, row.output_tokens, row.cached_tokens) == (6_000, 200, 0)
    assert float(row.cost_usd) == pytest.approx(6_000 / 1e6 * 1.0 + 200 / 1e6 * 4.0)
    assert row.latency_ms >= 0


async def test_an_unpriced_model_is_recorded_with_no_cost(monkeypatch, caplog):
    model = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    _factory(monkeypatch, {PRIMARY: model})
    with caplog.at_level(logging.INFO):
        await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(prices={}),
            usage_store=PostgresUsageStore(async_session),
        )
    [row] = await _rows()
    assert row.cost_usd is None and row.calls == 2
    assert any("agent.cost.unpriced" in r.getMessage() for r in caplog.records)


async def test_a_run_that_raised_still_records_its_calls_and_its_tool_call(monkeypatch):
    primary = FailingModel(
        script=[AIMessage(content="in_scope"), _call_describe()],
        seen=[],
        fail_on=(3,),
        error=_aws("ServiceUnavailableException", 503),
    )
    _factory(monkeypatch, {PRIMARY: primary})
    traces = SpyStore()
    with pytest.raises(ClientError):
        await run_request(
            request_for(CC, "what is there?"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(),
            usage_store=PostgresUsageStore(async_session),
            trace_store=traces,
        )
    [row] = await _rows()
    assert (row.calls, row.failed_calls) == (3, 1)
    [error_row] = traces.rows
    assert error_row["status"] == "error" and [t["name"] for t in error_row["tool_calls"]] == [
        "describe_releases"
    ]
    assert error_row["usage"]["model_calls"] == 3


def _call_describe() -> AIMessage:
    return AIMessage(
        content="", tool_calls=[{"id": "c1", "name": "describe_releases", "args": {}}]
    )


# ==================================================== 4. a failing store never fails the run


class RaisingUsageStore:
    async def add(self, rows):
        raise RuntimeError("usage db gone")


async def test_a_failing_usage_store_leaves_the_answer_unchanged(monkeypatch, caplog):
    model = _scripted(AIMessage(content="in_scope"), AIMessage(content="Fine."))
    _factory(monkeypatch, {PRIMARY: model})
    with caplog.at_level(logging.ERROR):
        result = await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(),
            usage_store=RaisingUsageStore(),
        )
    assert result.answer == "Fine."
    assert any("agent.usage_store_failed" in r.getMessage() for r in caplog.records)


# ==================================================== 5. the failure sentence, by kind


def test_the_sentence_depends_on_what_any_provider_said():
    unavailable = (
        _aws("AccessDeniedException", 403),
        _aws("UnrecognizedClientException", 400),
        _aws("ExpiredTokenException", 400),
        RuntimeError("anthropic_api_key is not configured"),
    )
    for exc in unavailable:
        assert c.failure_message(exc) == c.MODEL_UNAVAILABLE, exc
    busy = (
        _aws("ThrottlingException", 429),
        _aws("ServiceUnavailableException", 503),
        ModelCallTimeout("bedrock", 60.0),
        EndpointConnectionError(endpoint_url="https://bedrock-runtime.eu-west-2.amazonaws.com"),
    )
    for exc in busy:
        assert c.failure_message(exc) == c.MODEL_BUSY, exc
    assert c.failure_message(_aws("ValidationException", 400)) == c.ANSWER_FAILED
    assert classify(_aws("AccessDeniedException", 403)) == "auth"
    assert classify(EndpointConnectionError(endpoint_url="https://x")) == "unreachable"


# ==================================================== 6. the endpoint


def _client(token: str) -> AsyncClient:
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        headers={"X-Customer-Code": CC, "Authorization": f"Bearer {token}"},
    )


@pytest.fixture
def admin_and_web(monkeypatch):
    monkeypatch.setattr(settings, "api_service_tokens", "web=tok-web-1,ops=tok-ops-1")
    monkeypatch.setattr(settings, "api_admin_callers", "ops")
    monkeypatch.setattr(settings, "api_auth_required", True)


async def test_the_usage_endpoint_shows_the_tenants_days_newest_first(admin_and_web):
    store = PostgresUsageStore(async_session)
    yesterday = TODAY - timedelta(days=1)
    await store.add(
        [
            _row(TODAY, "ollama", PRIMARY, calls=2, input_tokens=100, output_tokens=10, cost=0.5),
            _row(TODAY, "bedrock", FALLBACK, calls=1, input_tokens=50, output_tokens=5, cost=None),
            _row(
                yesterday, "ollama", PRIMARY, calls=7, input_tokens=700, output_tokens=70, cost=1.25
            ),
        ]
    )
    async with _client("tok-ops-1") as client:
        response = await client.get("/api/v1/analytics/agent/usage", params={"days": 7})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["tenant"] == CC and [d["day"] for d in body["days"]] == [
        TODAY.isoformat(),
        yesterday.isoformat(),
    ]
    today_row = body["days"][0]
    assert (today_row["calls"], today_row["input_tokens"], today_row["cost_usd"]) == (3, 150, None)
    assert [m["model"] for m in today_row["models"]] == [FALLBACK, PRIMARY]
    assert body["days"][1]["cost_usd"] == 1.25
    async with _client("tok-web-1") as client:
        assert (await client.get("/api/v1/analytics/agent/usage")).status_code == 403


def _row(day, provider, model, *, calls, input_tokens, output_tokens, cost):
    return {
        "tenant": CC,
        "day": day,
        "provider": provider,
        "model": model,
        "calls": calls,
        "failed_calls": 0,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cached_tokens": 0,
        "cost_usd": cost,
        "latency_ms": 10,
    }


# ==================================================== 7. the purge and the roots


async def test_the_purge_deletes_a_tenants_usage_rows():
    cc = "test_chunk169"
    async with async_session() as db:
        await db.execute(delete(UsageDaily).where(UsageDaily.customer_code == cc))
        await db.execute(delete(Customer).where(Customer.customer_code == cc))
        db.add(Customer(customer_code=cc, name="usage probe", timezone="Europe/London"))
        await db.commit()
    await PostgresUsageStore(async_session).add(
        [{**_row(TODAY, "ollama", PRIMARY, calls=1, input_tokens=1, output_tokens=1, cost=None),
          "tenant": cc}]
    )
    assert len(await _rows(cc)) == 1
    async with async_session() as db:
        assert await purge_logspace(db, cc)
        await db.commit()
    assert await _rows(cc) == []


def test_each_process_has_one_usage_store():
    from app import teams_consumer
    from app.api import deps

    assert deps.usage_store() is deps.usage_store()
    assert isinstance(teams_consumer._USAGE, PostgresUsageStore)
