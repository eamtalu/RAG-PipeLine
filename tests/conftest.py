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

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.settings import settings


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
