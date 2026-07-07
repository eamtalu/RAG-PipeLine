"""Chunk 6: docs + end-to-end wiring verification.

The design/behaviour is covered by chunks 1-5; here we assert the whole SSH API surface is actually
mounted on the app and reachable over HTTP (routing + dependencies + serialization), which guards
against wiring regressions from the refactors.
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.api.deps import get_active_customer, get_current_customer


EXPECTED_ROUTES = {
    ("GET", "/api/v1/logs/ssh-sources"),
    ("POST", "/api/v1/logs/ssh-sources"),
    ("GET", "/api/v1/logs/ssh-sources/{source_id}"),
    ("PATCH", "/api/v1/logs/ssh-sources/{source_id}"),
    ("DELETE", "/api/v1/logs/ssh-sources/{source_id}"),
    ("POST", "/api/v1/logs/ssh-sources/{source_id}/test"),
    ("POST", "/api/v1/logs/fetch-remote"),
    ("GET", "/api/v1/logs/fetch-remote/runs"),
    ("GET", "/api/v1/logs/fetch-remote/runs/{run_id}"),
    ("POST", "/api/v1/logs/fetch-remote/runs/{run_id}/cancel"),
}


def test_all_ssh_routes_registered():
    # This app uses a lazy router inclusion, so routes aren't in app.routes statically; the OpenAPI
    # schema forces resolution and lists every registered path + method.
    paths = app.openapi()["paths"]
    missing = [(m, p) for (m, p) in EXPECTED_ROUTES
               if p not in paths or m.lower() not in paths[p]]
    assert not missing, f"unmounted SSH routes: {missing}"


@pytest.fixture
def client_as_customer():
    app.dependency_overrides[get_current_customer] = lambda: "TEST_CHUNK6"
    app.dependency_overrides[get_active_customer] = lambda: "TEST_CHUNK6"
    try:
        yield AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    finally:
        app.dependency_overrides.pop(get_current_customer, None)
        app.dependency_overrides.pop(get_active_customer, None)


async def test_http_list_runs_ok(client_as_customer):
    async with client_as_customer as client:
        resp = await client.get("/api/v1/logs/fetch-remote/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert "runs" in body and isinstance(body["runs"], list)


async def test_http_cancel_unknown_run_404(client_as_customer):
    async with client_as_customer as client:
        resp = await client.post(f"/api/v1/logs/fetch-remote/runs/{uuid.uuid4()}/cancel")
    assert resp.status_code == 404


async def test_http_list_ssh_sources_ok(client_as_customer):
    async with client_as_customer as client:
        resp = await client.get("/api/v1/logs/ssh-sources")
    assert resp.status_code == 200
    assert "sources" in resp.json()
