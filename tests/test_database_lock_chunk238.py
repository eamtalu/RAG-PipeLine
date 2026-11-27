"""Chunk 238: the agent's data reads go through a database user that the database itself limits.

Step 1b, the second lock. The scoped session in the application keeps a person inside their
customer and sites; a bug there would read everything, because every service connects as the
user that owns every table. The agent's reads now use `rag_agent`, which can only select, only
from the tables in the registry, and only the rows of the customer the session names. The site
limit inside a customer stays with the application rule (owner's decision, 27 Nov 2026).

Pinned here
-----------
    the rules    every registry table has row security and the reader policy; no other table
    one customer a plain select with no filter returns the named customer's rows only
    nothing set  with no customer named, no row
    read only    the user cannot write, and cannot read a table outside the registry
    the owner    the application's own user reads and writes as before
    the session  with the switch on, a scoped session is that user and names its customer in
                 every transaction; with it off, nothing changes
"""
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.config import database
from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services import scoped_data
from app.services.scoped_data import DataScope, database_lock
from app.settings import settings

MINE, THEIRS = "test_chunk238_a", "test_chunk238_b"
ROLE, PASSWORD = database_lock.ROLE, "rag_agent_test"


def _agent_url() -> str:
    return make_url(settings.database_url).set(username=ROLE, password=PASSWORD) \
        .render_as_string(hide_password=False)


async def _wipe():
    async with async_session() as db:
        for model in (LogTransaction, Job, Customer):
            await db.execute(delete(model).where(model.customer_code.in_([MINE, THEIRS])))
        await db.commit()


@pytest.fixture(autouse=True)
async def two_customers():
    """Two customers with one transaction each, and the reader role with its rights."""
    await _wipe()
    async with async_session() as db:
        exists = await db.scalar(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": ROLE})
        if not exists:
            await db.execute(text(f"CREATE ROLE {ROLE} LOGIN PASSWORD '{PASSWORD}'"))
        else:
            await db.execute(text(f"ALTER ROLE {ROLE} LOGIN PASSWORD '{PASSWORD}'"))
        for statement in database_lock.grant_statements():
            await db.execute(text(statement))
        at = datetime.now(timezone.utc)
        for code in (MINE, THEIRS):
            db.add(Customer(customer_code=code, name=code, timezone="Europe/London"))
            job = Job(customer_code=code, filename="t.log", document_type="transaction_log",
                      storage_key=f"{code}/{uuid.uuid4().hex}/t.log", status="completed")
            db.add(job)
            await db.flush()
            db.add(LogTransaction(customer_code=code, job_id=job.id, sealed=True, started_at=at,
                                  ended_at=at, date=at.date(), method="Pick", warehouse="BRI",
                                  company="915", status=LogTransactionStatus.success))
        await db.commit()
    yield
    await database.dispose_agent_engine()
    await _wipe()


async def _as_agent(sql: str, customer: str | None = None) -> list:
    engine = create_async_engine(_agent_url(), poolclass=NullPool)
    try:
        async with AsyncSession(bind=engine) as db:
            if customer is not None:
                await db.execute(text("SELECT set_config(:k, :v, true)"),
                                 {"k": database_lock.READER_CUSTOMER, "v": customer})
            return list((await db.execute(text(sql))).all())
    finally:
        await engine.dispose()


SEEN = (f"SELECT DISTINCT customer_code FROM log_transactions "
        f"WHERE customer_code IN ('{MINE}', '{THEIRS}') ORDER BY 1")


# ==================================================== 1. the rules in the database

async def test_every_registry_table_has_the_reader_policy_and_no_other_table_does():
    async with async_session() as db:
        secured = {r[0] for r in await db.execute(text(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relrowsecurity"))}
        with_policy = {r[0] for r in await db.execute(text(
            "SELECT tablename FROM pg_policies WHERE schemaname = 'public' AND policyname = :p"),
            {"p": database_lock.POLICY})}
    registered = set(scoped_data.registry.BY_TABLE)
    assert registered - with_policy == set(), "a registry table with no reader policy"
    assert with_policy - registered == set(), "a reader policy on a table outside the registry"
    assert registered <= secured


def test_the_policy_text_follows_the_kind_of_table():
    by_table = {s.split(" ON ")[1].split(" ")[0]: s for s in database_lock.policy_statements()
                if s.startswith("CREATE POLICY")}
    setting = f"current_setting('{database_lock.READER_CUSTOMER}', true)"
    assert f"USING (customer_code = {setting})" in by_table["log_transactions"]
    assert f"USING (customer_code = {setting})" in by_table["log_entries"]
    shared = [t for t, rule in scoped_data.registry.BY_TABLE.items()
              if isinstance(rule, scoped_data.registry.Shared)]
    owned = [t for t, rule in scoped_data.registry.BY_TABLE.items()
             if isinstance(rule, scoped_data.registry.OwnedBy)]
    assert shared and all("USING (true)" in by_table[t] for t in shared)
    assert all(" IN (SELECT id FROM " in by_table[t] and setting in by_table[t] for t in owned)


def test_a_migration_can_keep_to_the_tables_that_exist_at_its_point_in_history():
    # the registry a migration imports is today's and may name a table created later
    only = database_lock.policy_statements(["log_transactions", "not_created_yet"])
    assert len(only) == 3 and all(" log_transactions" in s for s in only)
    assert database_lock.grant_statements(["customers"])[1:] == [
        f"GRANT SELECT ON customers TO {database_lock.ROLE}"]


# ==================================================== 2. what the reader user can do

async def test_a_plain_select_returns_the_named_customers_rows_only():
    assert await _as_agent(SEEN, MINE) == [(MINE,)]
    assert await _as_agent(SEEN, THEIRS) == [(THEIRS,)]


async def test_with_no_customer_named_no_row_is_returned():
    assert await _as_agent(SEEN) == []
    assert await _as_agent(SEEN, "") == []


@pytest.mark.parametrize("sql", [
    f"UPDATE log_transactions SET method = 'x' WHERE customer_code = '{MINE}'",
    f"DELETE FROM log_transactions WHERE customer_code = '{MINE}'",
    f"INSERT INTO customers (customer_code, name) VALUES ('{MINE}_x', 'x')",
    "SELECT count(*) FROM jobs",
    "SELECT count(*) FROM log_ssh_sources",
])
async def test_the_reader_cannot_write_or_read_outside_the_registry(sql):
    with pytest.raises(DBAPIError, match="permission denied"):
        await _as_agent(sql, MINE)


async def test_the_reader_cannot_reach_a_partition_directly():
    async with async_session() as db:
        partition = await db.scalar(text(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'log_transactions' LIMIT 1"))
    with pytest.raises(DBAPIError, match="permission denied"):
        await _as_agent(f"SELECT count(*) FROM {partition}", MINE)


async def test_the_applications_own_user_reads_and_writes_as_before():
    async with async_session() as db:
        seen = [r[0] for r in await db.execute(text(SEEN))]
        await db.execute(text(f"UPDATE log_transactions SET method = 'Pick2' "
                              f"WHERE customer_code = '{THEIRS}'"))
        await db.commit()
    assert seen == [MINE, THEIRS]


# ==================================================== 3. the scoped session

async def test_with_the_switch_on_a_scoped_session_is_the_reader_and_names_its_customer(
        monkeypatch):
    monkeypatch.setattr(settings, "agent_row_rules", "on")
    monkeypatch.setattr(settings, "agent_database_url", _agent_url())
    factory = database.reader_session_factory(async_session)
    async with scoped_data.open_session(factory, DataScope(MINE, all_sites=True)) as db:
        assert await db.scalar(text("SELECT current_user")) == ROLE
        assert [r[0] for r in await db.execute(text(SEEN))] == [MINE]
        # the application's own rule still applies on top
        rows = (await db.execute(select(LogTransaction.customer_code))).scalars().all()
        assert set(rows) == {MINE}
        # and the customer is named again in the next transaction of the same session
        await db.rollback()
        assert [r[0] for r in await db.execute(text(SEEN))] == [MINE]


async def test_with_the_switch_off_the_given_factory_is_used(monkeypatch):
    monkeypatch.setattr(settings, "agent_row_rules", "off")
    assert database.reader_session_factory(async_session) is async_session
    async with scoped_data.open_session(async_session, DataScope(MINE, all_sites=True)) as db:
        assert await db.scalar(text("SELECT current_user")) != ROLE


def test_the_switch_on_without_a_connection_line_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "agent_row_rules", "on")
    monkeypatch.setattr(settings, "agent_database_url", None)
    with pytest.raises(RuntimeError, match="AGENT_DATABASE_URL"):
        database.reader_session_factory(async_session)


def test_the_agents_tools_open_their_sessions_through_the_reader_factory():
    import inspect
    from app.services.analytics_agent import namespaces
    assert "reader_session_factory(session_factory)" in inspect.getsource(namespaces._scoped)
