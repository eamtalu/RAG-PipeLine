import keyword

# 1. database.py — Database Foundation
#
#   This isn't a table itself. It sets up the async SQLAlchemy infrastructure:
#   - Creates an async PostgreSQL engine from settings
#   - Provides an async_sessionmaker for dependency injection
#   - Defines the Base declarative class that all other models inherit from
#   - Exposes get_session() as an async generator (designed to be a FastAPI dependency)


from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase

from app.settings import settings

#From setting take the database connection url
engine = create_async_engine(settings.database_url, echo=settings.debug)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass

# async_session is a resource/databaseresource which needs explicit cleanup, thats why it is "with" keyword and also asynchronous , hence async keyword.kwlist
# it will yeild a session, once that session is done somewhere in other module, the execution will be back here and the resource will be explicitely closed
async def get_session() -> AsyncSession:  # type: ignore[misc]
    async with async_session() as session:
        yield session
