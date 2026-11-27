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
    no owner     a line with no transaction is read by an all-sites person only
    nobody       a person with no site reads nothing
    not closed   the two tables are no longer reported as closed
    the search   with no time range it looks at a set number of days and says so
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


async def test_a_line_with_no_transaction_is_read_by_an_all_sites_person_only():
    await _plant("915", "BRI", ["orphan-word line"], is_assigned=False)
    assert await _found(BRI, q="orphan-word") == []
    assert await _found(ALL, q="orphan-word") == ["orphan-word line"]


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


def test_the_tool_description_tells_the_model_about_the_default_range():
    spec = next(t for t in tools.TOOLS if t["name"] == "search_entries")
    assert "time_from" in spec["description"] and "days" in spec["description"]
