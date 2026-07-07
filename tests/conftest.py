"""Shared pytest fixtures.

`db` yields an AsyncSession bound to a single connection whose outer transaction is rolled back at
the end of each test, so tests share the real Postgres (from settings.database_url) without leaving
any rows behind. Tests use `flush()` (not `commit()`) to exercise INSERT/defaults within that
transaction.
"""

import os
import sys

# Ensure `app` is importable when pytest is invoked from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid

import pytest_asyncio
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.settings import settings
from app.config.database import async_session, engine as app_engine
from app.persistence.models.log_ssh_source import LogSshSource


@pytest_asyncio.fixture(autouse=True)
async def _dispose_app_engine():
    """The app's module-level pooled engine (used by code-under-test via async_session / _host_lock)
    would otherwise reuse a connection bound to a previous test's event loop -> asyncpg's
    'another operation is in progress'. Dispose it after every test so the next test gets fresh
    connections on its own loop."""
    yield
    await app_engine.dispose()


@pytest_asyncio.fixture
async def committed_source():
    """A LogSshSource actually COMMITTED to the DB (so code that opens its own session can see it and
    satisfy the checkpoint FK). Unique host per test so per-host advisory locks never collide across
    tests. Deleted in teardown (checkpoints cascade)."""
    src = LogSshSource(
        customer_code="TEST_CHUNK2", name=f"src-{uuid.uuid4().hex[:8]}",
        host=f"h-{uuid.uuid4().hex[:6]}.example", username="svc", remote_log_dir="C:/logs",
    )
    async with async_session() as s:
        s.add(src)
        await s.commit()
        await s.refresh(src)
    sid = src.id
    try:
        yield src
    finally:
        async with async_session() as s:
            await s.execute(delete(LogSshSource).where(LogSshSource.id == sid))
            await s.commit()


@pytest_asyncio.fixture
async def db():
    # Fresh NullPool engine per test: pytest-asyncio uses a new event loop per test, and a pooled
    # asyncpg connection bound to a previous loop raises "another operation is in progress" when
    # reused. A per-test engine keeps each connection on its own loop; the outer transaction is
    # rolled back so the shared DB stays clean.
    eng = create_async_engine(settings.database_url, poolclass=NullPool)
    conn = await eng.connect()
    trans = await conn.begin()
    session = AsyncSession(bind=conn, expire_on_commit=False)
    try:
        yield session
    finally:
        await session.close()
        await trans.rollback()
        await conn.close()
        await eng.dispose()
