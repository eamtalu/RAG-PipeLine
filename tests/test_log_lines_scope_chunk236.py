"""Chunk 236: a site-limited person reads the log lines of their own sites' transactions.

Step 1d stage 5. A log line has no site and must not get one (`log_entries` stays insert-only),
but it belongs to a transaction through `log_entry_assignment`, and the transaction has its
company and warehouse. So a line is read by whoever may read its transaction. A person who may
see every site reads exactly as before.

Pinned here
-----------
    the rule     both tables are limited through the transaction; an all-sites read is unchanged
    one txn      a Brighton person reads the lines of a Brighton transaction, in order
    another      and no line of a London transaction, by either tool
    the pair     the same warehouse code under another company is another place
    no owner     a line with no transaction is read by everyone of the customer who has a
                 site (owner's decision, 27 Nov 2026); one with no timestamp only by an
                 all-sites person
    nobody       a person with no site reads nothing
    not closed   the two tables are no longer reported as closed
    the search   with no time range it looks at a set number of days and says so; a search
                 inside one transaction covers that transaction's own time, however old
    cheap        each check is bounded to the line's own day, and a person whose sites have
                 no transaction in the range is shown the last day's ungrouped lines only
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services import scoped_data
from app.services.log_agent import tools
from app.services.mnp_log_ingestion.pipeline import assignments
from app.services.scoped_data import DataScope, Place
from app.settings import settings

CC = "test_chunk236"
BRI = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
LON = DataScope(CC, site_ids=("LON",), places=(Place("915", "LON"),))
ALL = DataScope(CC, all_sites=True)
NOBODY = DataScope(CC)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=2)


async def _wipe():
    async with async_session() as db:
        for model in (LogEntryAssignment, LogEntry, LogTransaction, Job, Customer):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="log line scope probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(company: str | None, warehouse: str | None, words: list[str], *,
                 minutes: int = 0, is_assigned: bool = True) -> uuid.UUID:
    """One transaction at a place with one line per word; the transaction's id."""
    at = _now() + timedelta(minutes=minutes)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        txn = LogTransaction(
            customer_code=CC, job_id=job.id, sealed=True, started_at=at,
            ended_at=at + timedelta(seconds=len(words)), date=at.date(), duration_ms=100,
            method="ConfirmPickLine", transaction_name="Pick", transaction_type="002001",
            status=LogTransactionStatus.success, company=company, warehouse=warehouse)
        db.add(txn)
        lines = [LogEntry(customer_code=CC, job_id=job.id, source_file="t.log", line_number=n,
                          timestamp=at + timedelta(seconds=n), level="INFO",
                          entry_type=LogEntryType.info, message=word,
                          entry_hash=uuid.uuid4().hex)
                 for n, word in enumerate(words)]
        db.add_all(lines)
        await db.flush()
        if is_assigned:
            await assignments.write(db, transaction_id=txn.id, entries=lines, customer_code=CC)
        await db.commit()
        return txn.id


async def _timeline(scope: DataScope, txn_id: uuid.UUID) -> list[str] | str:
    async with scoped_data.open_session(async_session, scope) as db:
        out = await tools._get_transaction(db, {"transaction_id": str(txn_id)}, CC)
    return out["error"] if "error" in out else [step["message"] for step in out["timeline"]]


async def _found(scope: DataScope, **args) -> list[str]:
    async with scoped_data.open_session(async_session, scope) as db:
        out = await tools._search_entries(db, args, CC)
    return sorted(e["message"] for e in out["entries"])


# ==================================================== 1. the rule

@pytest.mark.parametrize("model", [LogEntry, LogEntryAssignment])
def test_both_tables_are_limited_through_the_transaction(model):
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    sql = str(rule.condition(BRI).compile(compile_kwargs={"literal_binds": True}))
    assert "EXISTS" in sql and "log_transactions" in sql
    assert "warehouse = 'BRI'" in sql and "company = '915'" in sql


@pytest.mark.parametrize("model", [LogEntry, LogEntryAssignment])
def test_an_all_sites_read_is_limited_by_the_customer_alone_as_before(model):
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    sql = str(rule.condition(ALL).compile(compile_kwargs={"literal_binds": True}))
    assert sql == f"{model.__tablename__}.customer_code = '{CC}'"


@pytest.mark.parametrize("model", [LogEntry, LogEntryAssignment])
def test_a_person_with_no_site_reads_nothing(model):
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    assert str(rule.condition(NOBODY).compile()) == "false"


def test_the_two_tables_are_no_longer_reported_as_closed():
    assert scoped_data.closed_tables({"log_entries", "log_entry_assignment"}, BRI) == []


# ==================================================== 2. the lines of one transaction

async def test_a_person_reads_the_lines_of_their_own_sites_transaction_in_order():
    bri = await _plant("915", "BRI", ["request", "MMS200MI call", "response"])
    assert await _timeline(BRI, bri) == ["request", "MMS200MI call", "response"]
    assert await _timeline(ALL, bri) == ["request", "MMS200MI call", "response"]


async def test_a_person_gets_no_line_of_another_sites_transaction():
    lon = await _plant("915", "LON", ["london request"])
    assert "No transaction found" in await _timeline(BRI, lon)
    async with scoped_data.open_session(async_session, BRI) as db:
        rows = await assignments.load_entries(db, [lon], limit=10)
        ids = (await db.execute(select(LogEntryAssignment.entry_id).where(
            LogEntryAssignment.transaction_id == lon))).all()
    assert rows == [] and ids == []


async def test_the_same_warehouse_code_under_another_company_is_another_place():
    other = await _plant("920", "BRI", ["other company line"])
    assert "No transaction found" in await _timeline(BRI, other)
    assert await _found(BRI, q="other company") == []
    assert await _found(ALL, q="other company") == ["other company line"]


# ==================================================== 3. the search

async def test_the_search_returns_only_the_lines_of_the_persons_sites():
    await _plant("915", "BRI", ["pick shared-word bri"])
    await _plant("915", "LON", ["pick shared-word lon"], minutes=1)
    assert await _found(BRI, q="shared-word") == ["pick shared-word bri"]
    assert await _found(LON, q="shared-word") == ["pick shared-word lon"]
    assert await _found(ALL, q="shared-word") == ["pick shared-word bri", "pick shared-word lon"]
    assert await _found(NOBODY, q="shared-word") == []


async def test_a_line_with_no_transaction_is_read_by_everyone_who_has_a_site():
    await _plant("915", "BRI", ["orphan-word line"], is_assigned=False)
    assert await _found(BRI, q="orphan-word") == ["orphan-word line"]
    assert await _found(LON, q="orphan-word") == ["orphan-word line"]
    assert await _found(ALL, q="orphan-word") == ["orphan-word line"]
    assert await _found(NOBODY, q="orphan-word") == []


async def test_a_line_with_no_timestamp_is_read_by_an_all_sites_person_only():
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogEntry(customer_code=CC, job_id=job.id, source_file="t.log", line_number=1,
                        timestamp=None, level="INFO", entry_type=LogEntryType.info,
                        message="no clock", entry_hash=uuid.uuid4().hex))
        await db.commit()
    seen = {}
    for label, scope in (("BRI", BRI), ("ALL", ALL)):
        async with scoped_data.open_session(async_session, scope) as db:
            seen[label] = (await db.execute(select(LogEntry.message).where(
                LogEntry.message == "no clock"))).scalars().all()
    assert seen == {"BRI": [], "ALL": ["no clock"]}


async def test_a_limited_read_reports_no_closed_table():
    bri = await _plant("915", "BRI", ["request"])
    async with scoped_data.open_session(async_session, BRI) as db:
        await tools._get_transaction(db, {"transaction_id": str(bri)}, CC)
        assert scoped_data.closed_tables(scoped_data.tables_read(db), BRI) == []


# ==================================================== 4. the search window

async def test_a_search_with_no_time_range_looks_at_the_set_days_and_says_so(monkeypatch):
    monkeypatch.setattr(settings, "log_search_default_days", 7)
    await _plant("915", "BRI", ["window-word recent"])
    await _plant("915", "BRI", ["window-word old"], minutes=-60 * 24 * 10)
    async with async_session() as db:
        out = await tools._search_entries(db, {"q": "window-word"}, CC)
    assert [e["message"] for e in out["entries"]] == ["window-word recent"]
    assert out["window"]["defaulted"] is True and out["window"]["days"] == 7
    assert "7 days" in out["note"] and "time_from" in out["note"]


async def test_the_number_of_days_is_a_setting(monkeypatch):
    monkeypatch.setattr(settings, "log_search_default_days", 30)
    await _plant("915", "BRI", ["window-word old"], minutes=-60 * 24 * 10)
    async with async_session() as db:
        out = await tools._search_entries(db, {"q": "window-word"}, CC)
    assert [e["message"] for e in out["entries"]] == ["window-word old"]
    assert out["window"]["days"] == 30 and "30 days" in out["note"]


async def test_a_search_with_its_own_time_range_is_not_changed():
    await _plant("915", "BRI", ["window-word old"], minutes=-60 * 24 * 10)
    start = (_now() - timedelta(days=11)).isoformat()
    async with async_session() as db:
        out = await tools._search_entries(db, {"q": "window-word", "time_from": start}, CC)
    assert [e["message"] for e in out["entries"]] == ["window-word old"]
    assert out["window"]["defaulted"] is False and "note" not in out


async def test_a_search_inside_one_transaction_covers_that_transactions_own_time():
    old = await _plant("915", "BRI", ["inside-word a", "inside-word b"], minutes=-60 * 24 * 20)
    async with scoped_data.open_session(async_session, BRI) as db:
        out = await tools._search_entries(db, {"transaction_id": str(old)}, CC)
    assert sorted(e["message"] for e in out["entries"]) == ["inside-word a", "inside-word b"]
    assert out["window"]["defaulted"] is False and "note" not in out
    async with scoped_data.open_session(async_session, LON) as db:
        out = await tools._search_entries(db, {"transaction_id": str(old)}, CC)
    assert out["entries"] == []


async def test_a_person_whose_sites_have_no_transaction_sees_the_last_days_ungrouped_lines():
    await _plant("915", "BRI", ["quiet-word grouped"])
    await _plant("915", "BRI", ["quiet-word loose new"], minutes=-30, is_assigned=False)
    await _plant("915", "BRI", ["quiet-word loose old"], minutes=-60 * 24 * 3, is_assigned=False)
    async with scoped_data.open_session(async_session, LON) as db:
        out = await tools._search_entries(db, {"q": "quiet-word"}, CC)
    assert [e["message"] for e in out["entries"]] == ["quiet-word loose new"]
    assert "none of your sites" in out["note"].lower() and "24 hours" in out["note"]
    # a person of a site with transactions searches the whole range
    assert await _found(BRI, q="quiet-word") == [
        "quiet-word grouped", "quiet-word loose new", "quiet-word loose old"]


async def test_a_limited_search_widens_step_by_step_and_keeps_newest_first():
    for minutes in (-10, -60 * 3, -60 * 24 * 2, -60 * 24 * 6):
        await _plant("915", "BRI", [f"widen-word {-minutes:05d}"], minutes=minutes)
    await _plant("915", "LON", ["widen-word london"], minutes=-30)
    expected = ["widen-word 00010", "widen-word 00180", "widen-word 02880", "widen-word 08640"]
    for scope in (BRI, ALL):
        async with scoped_data.open_session(async_session, scope) as db:
            out = await tools._search_entries(db, {"q": "widen-word 0"}, CC)
            two = await tools._search_entries(db, {"q": "widen-word 0", "limit": 2}, CC)
        assert [e["message"] for e in out["entries"]] == expected, scope
        assert [e["message"] for e in two["entries"]] == expected[:2], scope


def test_the_steps_of_a_widening_search_cover_the_range_once_newest_first():
    end = datetime(2026, 11, 27, 12, tzinfo=timezone.utc)
    steps = tools._widening_steps(end - timedelta(days=7), end)
    assert steps[0] == (end - timedelta(hours=1), end)
    assert steps[-1][0] == end - timedelta(days=7)
    assert all(later[1] == earlier[0] for earlier, later in zip(steps, steps[1:]))
    assert len(steps) <= 6
    assert tools._widening_steps(end - timedelta(minutes=20), end) == [
        (end - timedelta(minutes=20), end)]


def test_a_session_knows_whether_it_is_limited_to_some_sites():
    async def probe(scope) -> bool:
        async with scoped_data.open_session(async_session, scope) as db:
            return scoped_data.is_site_limited(db)
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        assert loop.run_until_complete(probe(BRI)) is True
        assert loop.run_until_complete(probe(ALL)) is False
    finally:
        loop.close()


@pytest.mark.parametrize("model", [LogEntry, LogEntryAssignment])
def test_each_check_is_bounded_to_the_lines_own_day(model):
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    sql = str(rule.condition(BRI).compile(compile_kwargs={"literal_binds": True}))
    assert "owner_txn.started_at <=" in sql and "owner_txn.started_at >=" in sql


def test_the_tool_description_tells_the_model_about_the_default_range():
    spec = next(t for t in tools.TOOLS if t["name"] == "search_entries")
    assert "time_from" in spec["description"] and "days" in spec["description"]
