"""Chunk 14: IdempotencyMiddleware — server-side de-duplication for mutating POSTs.

Integration test through the real app (TestClient) + local DB, exercising POST /logs/saved-views:
- same Idempotency-Key + same body  -> second call REPLAYS the first response; only ONE row created;
- same key + DIFFERENT body         -> 422 (key reused for a different request);
- a pre-existing in_progress claim   -> 409;
- NO key                            -> two independent rows (proves the mechanism is opt-in / no regression).

Uses a unique customer_code per test so runs never interfere; rows are best-effort cleaned up after.
get_current_customer is overridden so the endpoint doesn't require a real customer row; the middleware
still reads the raw X-Customer-Code header (sent explicitly).
"""

import asyncio
import hashlib
import json
import uuid
from datetime import datetime, timezone, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.api.deps import get_current_customer
from app.config.database import engine as app_engine
from app.main import app
from app.persistence.models.idempotency_key import IdempotencyKey, IdempotencyStatus
from app.persistence.models.saved_view import SavedView
from app.settings import settings

PATH = "/api/v1/logs/saved-views"


@pytest.fixture
def client(monkeypatch):
    """A TestClient used as a context manager so ALL requests share ONE event loop (the app's pooled
    async engine is otherwise left bound to a closed per-request loop -> 'Event loop is closed').
    Background workers are disabled so entering the lifespan stays cheap. The app engine is disposed
    after, so the next test's loop gets fresh connections."""
    monkeypatch.setattr(settings, "run_background_workers", False)
    with TestClient(app) as c:
        yield c
    asyncio.run(app_engine.dispose())
_BODY = {"name": "snap", "state": {"schemaVersion": 1, "filters": {"date": "2026-07-10"}}}


def _bytes(obj) -> bytes:
    return json.dumps(obj).encode()


def _fingerprint(body: bytes) -> str:
    return hashlib.sha256(f"POST|{PATH}|".encode() + body).hexdigest()


def _post(client: TestClient, cc: str, key: str | None, body_bytes: bytes):
    headers = {"X-Customer-Code": cc, "Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    return client.post(PATH, content=body_bytes, headers=headers)


def _run(coro):
    return asyncio.run(coro)


async def _count_and_cleanup(cc: str) -> int:
    """Count saved_views for cc (the assertion), then delete all rows this test created."""
    eng = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        async with AsyncSession(eng) as s:
            n = (
                await s.execute(
                    select(func.count()).select_from(SavedView).where(SavedView.customer_code == cc)
                )
            ).scalar_one()
            await s.execute(delete(SavedView).where(SavedView.customer_code == cc))
            await s.execute(delete(IdempotencyKey).where(IdempotencyKey.customer_code == cc))
            await s.commit()
            return n
    finally:
        await eng.dispose()


async def _seed_in_progress(cc: str, key: str, fingerprint: str) -> None:
    eng = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        now = datetime.now(timezone.utc)
        async with AsyncSession(eng) as s:
            s.add(
                IdempotencyKey(
                    customer_code=cc, idem_key=key, method="POST", path=PATH,
                    request_fingerprint=fingerprint, status=IdempotencyStatus.in_progress.value,
                    created_at=now, expires_at=now + timedelta(hours=1),
                )
            )
            await s.commit()
    finally:
        await eng.dispose()


def _cc() -> str:
    return f"IDEMP_{uuid.uuid4().hex[:8]}"


def test_same_key_same_body_replays_and_creates_one(client):
    cc = _cc()
    app.dependency_overrides[get_current_customer] = lambda: cc
    try:
        key = str(uuid.uuid4())
        body = _bytes(_BODY)
        r1 = _post(client, cc, key, body)
        r2 = _post(client, cc, key, body)
        assert r1.status_code == 201, r1.text
        assert r2.status_code == 201, r2.text
        # second call replayed the first response (same generated id), not a new create
        assert r1.json()["id"] == r2.json()["id"]
    finally:
        n = _run(_count_and_cleanup(cc))
        app.dependency_overrides.clear()
    assert n == 1, f"expected exactly one saved_view, got {n}"


def test_same_key_different_body_is_422(client):
    cc = _cc()
    app.dependency_overrides[get_current_customer] = lambda: cc
    try:
        key = str(uuid.uuid4())
        r1 = _post(client, cc, key, _bytes(_BODY))
        r2 = _post(client, cc, key, _bytes({"name": "different", "state": {"x": 2}}))
        assert r1.status_code == 201, r1.text
        assert r2.status_code == 422, r2.text
    finally:
        n = _run(_count_and_cleanup(cc))
        app.dependency_overrides.clear()
    assert n == 1


def test_in_progress_key_is_409(client):
    cc = _cc()
    app.dependency_overrides[get_current_customer] = lambda: cc
    try:
        key = str(uuid.uuid4())
        body = _bytes(_BODY)
        _run(_seed_in_progress(cc, key, _fingerprint(body)))  # a claim already "running"
        r = _post(client, cc, key, body)
        assert r.status_code == 409, r.text
    finally:
        n = _run(_count_and_cleanup(cc))
        app.dependency_overrides.clear()
    assert n == 0  # the 409'd request must NOT have created a saved_view


def test_no_key_creates_two_rows(client):
    cc = _cc()
    app.dependency_overrides[get_current_customer] = lambda: cc
    try:
        r1 = _post(client, cc, None, _bytes(_BODY))
        r2 = _post(client, cc, None, _bytes(_BODY))
        assert r1.status_code == 201 and r2.status_code == 201
        assert r1.json()["id"] != r2.json()["id"]  # no dedup without a key
    finally:
        n = _run(_count_and_cleanup(cc))
        app.dependency_overrides.clear()
    assert n == 2, f"opt-in: keyless POSTs must not dedup; expected 2 rows, got {n}"
