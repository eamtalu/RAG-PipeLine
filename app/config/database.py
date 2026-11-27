import keyword

# 1. database.py — Database Foundation
#
#   This isn't a table itself. It sets up the async SQLAlchemy infrastructure:
#   - Creates an async PostgreSQL engine from settings
#   - Provides an async_sessionmaker for dependency injection
#   - Defines the Base declarative class that all other models inherit from
#   - Exposes get_session() as an async generator (designed to be a FastAPI dependency)


from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from app.settings import settings

#From setting take the database connection url.
# Optionally apply a per-statement timeout (safety net against a runaway query) via asyncpg's
# server_settings. Off by default (db_statement_timeout_ms=0) so existing deployments are unchanged.
_connect_args: dict = {}
if settings.db_statement_timeout_ms and settings.db_statement_timeout_ms > 0:
    _connect_args["server_settings"] = {"statement_timeout": str(settings.db_statement_timeout_ms)}
engine = create_async_engine(settings.database_url, echo=settings.debug, connect_args=_connect_args)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

#: What code needs to open one short session of its own: `async with factory() as db:`.
#: `async_session` is one; a test may pass a factory that counts or shares sessions.
SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


_agent_engine = None
_agent_sessions: async_sessionmaker | None = None


def agent_session_factory() -> async_sessionmaker:
    """Sessions of the agent's reader user, on its own small pool, made on first use.

    (AGENT_DATABASE_URL set) -> a session factory ; (not set) -> raises RuntimeError
    """
    global _agent_engine, _agent_sessions
    if not settings.agent_database_url:
        raise RuntimeError("AGENT_ROW_RULES is on but AGENT_DATABASE_URL is not set")
    if _agent_sessions is None:
        _agent_engine = create_async_engine(settings.agent_database_url, echo=settings.debug,
                                            connect_args=_connect_args, pool_size=3,
                                            max_overflow=5)
        _agent_sessions = async_sessionmaker(_agent_engine, class_=AsyncSession,
                                             expire_on_commit=False)
    return _agent_sessions


def reader_session_factory(session_factory: SessionFactory) -> SessionFactory:
    """The session factory a data tool reads with: the reader user's when the lock is on.

    (AGENT_ROW_RULES=off) -> the factory given ; (on) -> sessions of the reader user
    ; (on, no AGENT_DATABASE_URL) -> raises, because reading unlocked in silence is worse
    """
    if settings.agent_row_rules != "on":
        return session_factory
    return agent_session_factory()


async def dispose_agent_engine() -> None:
    """Close the reader user's connections, so the next use opens them again."""
    global _agent_engine, _agent_sessions
    if _agent_engine is not None:
        await _agent_engine.dispose()
    _agent_engine, _agent_sessions = None, None


class Base(DeclarativeBase):
    pass

# async_session is a resource/databaseresource which needs explicit cleanup, thats why it is "with" keyword and also asynchronous , hence async keyword.kwlist
# it will yeild a session, once that session is done somewhere in other module, the execution will be back here and the resource will be explicitely closed
async def get_session() -> AsyncSession:  # type: ignore[misc]
    async with async_session() as session:
        yield session
