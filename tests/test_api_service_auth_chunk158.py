"""Step 7 of docs/esmart-agent-execution-plan.md: every call under /api/v1 names its caller.

The API listened on every interface with nothing but a customer header. Now each caller holds a
named service token sent as a bearer; the name says which system is calling and whether it may
use the admin routes. `api_auth_required` is off during the rollout: a call without a token passes
with a warning so the log shows who still needs one, and a wrong token never passes.
"""

import logging

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from app.api import service_auth
from app.api.deps import require_admin
from app.main import app
from app.settings import settings

TOKENS = "web=tok-web-1,ops=tok-ops-1"


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(settings, "api_service_tokens", TOKENS)
    monkeypatch.setattr(settings, "api_admin_callers", "ops")
    monkeypatch.setattr(settings, "api_auth_required", True)


@pytest.fixture
def optional(monkeypatch):
    monkeypatch.setattr(settings, "api_service_tokens", TOKENS)
    monkeypatch.setattr(settings, "api_admin_callers", "ops")
    monkeypatch.setattr(settings, "api_auth_required", False)


def _client(token: str | None = None) -> AsyncClient:
    headers = {"X-Customer-Code": "acme"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver",
                       headers=headers)


# ==================================================== 1. tokens are named and parsed once

def test_tokens_are_parsed_by_name():
    assert service_auth.parse_service_tokens("web=a1,ops=b2") == {"web": "a1", "ops": "b2"}
    assert service_auth.parse_service_tokens("") == {}
    assert service_auth.parse_service_tokens(" web = a1 , ") == {"web": "a1"}


# ==================================================== 2. required: nothing passes without a token

async def test_health_needs_no_token(configured):
    async with _client() as c:
        assert (await c.get("/health")).status_code == 200


async def test_a_call_without_a_token_is_refused_when_required(configured):
    async with _client() as c:
        r = await c.get("/api/v1/customers/timezones")
    assert r.status_code == 401


async def test_a_wrong_token_is_refused(configured):
    async with _client("tok-nope") as c:
        assert (await c.get("/api/v1/customers/timezones")).status_code == 401


async def test_a_named_token_passes(configured):
    async with _client("tok-web-1") as c:
        assert (await c.get("/api/v1/customers/timezones")).status_code == 200


async def test_admin_routes_need_an_admin_caller(configured):
    async with _client("tok-web-1") as c:
        assert (await c.get("/api/v1/teams/bindings")).status_code == 403
    async with _client("tok-ops-1") as c:
        assert (await c.get("/api/v1/teams/bindings")).status_code == 200


# ==================================================== 3. optional: the rollout mode

async def test_without_a_token_the_call_passes_with_a_warning_when_optional(optional, caplog):
    with caplog.at_level(logging.WARNING, logger="app.api.service_auth"):
        async with _client() as c:
            assert (await c.get("/api/v1/customers/timezones")).status_code == 200
    assert any("api.auth.anonymous" in r.message and "/api/v1/customers/timezones" in r.message
               for r in caplog.records)


async def test_a_wrong_token_is_refused_even_when_optional(optional):
    async with _client("tok-nope") as c:
        assert (await c.get("/api/v1/customers/timezones")).status_code == 401


async def test_admin_routes_stay_open_to_anonymous_callers_only_while_optional(optional):
    async with _client() as c:
        assert (await c.get("/api/v1/teams/bindings")).status_code == 200


# ==================================================== 4. require_admin outside a request

async def test_require_admin_outside_a_request_follows_the_mode(monkeypatch):
    monkeypatch.setattr(settings, "api_auth_required", False)
    await require_admin()
    monkeypatch.setattr(settings, "api_auth_required", True)
    with pytest.raises(HTTPException) as exc:
        await require_admin()
    assert exc.value.status_code == 403
