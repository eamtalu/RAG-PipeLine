import asyncio
import re
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from app.config import settings
from app.persistence.models import Base  # noqa: F401 — registers all models
from app.persistence import partitioning as _pt

config = context.config
config.set_main_option("sqlalchemy.url", settings.database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _include_object(obj, name, type_, reflected, compare_to):
    """Hide daily PARTITIONS from autogenerate.

    Every partition of log_entries / log_transactions / log_entry_assignment is a real table in
    pg_class, and each carries local indexes that PostgreSQL creates automatically from the parent's
    partitioned index. Autogenerate does not model partitioning, so it reflects all ~275 of them as
    unknown tables and proposes dropping every one of those indexes — a revision that would be
    silently catastrophic if anyone ran it.

    Matching on the parent name plus a date-or-default suffix rather than a bare prefix, so a real
    table that merely starts with the same characters is never hidden.
    """
    parents = "|".join(t.table for t in _pt.PARTITIONED)
    if re.fullmatch(rf"(?:{parents})_(?:\d{{4}}_\d{{2}}_\d{{2}}|default)", name or ""):
        return False
    if type_ == "index" and getattr(obj, "table", None) is not None and re.fullmatch(
            rf"(?:{parents})_(?:\d{{4}}_\d{{2}}_\d{{2}}|default)", obj.table.name):
        return False
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True,
                      include_object=_include_object)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata,
                      include_object=_include_object)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
