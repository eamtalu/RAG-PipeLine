"""Chunk 237: a call that names no warehouse and no person belongs to the whole customer.

Step 1d stage 6. About one transaction in a hundred has no warehouse: the server check a
handheld sends before sign-in, unnamed fragments, sign-ins and operator checks. The first two
name nobody and say something about the server, so everyone of the customer who has a site
reads them (owner's decision, 27 Nov 2026). A sign-in or an operator check names a user and
stays with the people who may see every site.

Pinned here
-----------
    nobody named   a server check is read by every person who has a site, and by nobody else
    a user named   a sign-in with no warehouse is read by an all-sites person only
    lines          the lines follow: a server check's are read, a sign-in's are not
    facts          the same split holds for facts
    with a place   a call with a warehouse is limited exactly as before
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services import scoped_data
from app.services.log_agent import tools
from app.services.mnp_log_ingestion.pipeline import assignments
from app.services.scoped_data import DataScope, Place

CC = "test_chunk237"
BRI = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
LON = DataScope(CC, site_ids=("LON",), places=(Place("915", "LON"),))
ALL = DataScope(CC, all_sites=True)
NOBODY = DataScope(CC)
SCOPES = {"BRI": BRI, "LON": LON, "ALL": ALL, "NOBODY": NOBODY}


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsFact, LogEntryAssignment, LogEntry, LogTransaction, Job, Customer):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="no place probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(method: str, *, user: str | None, warehouse: str | None = None,
                 company: str | None = None, minutes: int = 0) -> uuid.UUID:
    """One transaction with one line and one fact; the line's message is the method."""
    at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1, minutes=minutes)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        txn = LogTransaction(
            customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
            date=at.date(), duration_ms=5, method=method, status=LogTransactionStatus.success,
            user_name=user, company=company, warehouse=warehouse)
        line = LogEntry(customer_code=CC, job_id=job.id, source_file="t.log", line_number=1,
                        timestamp=at, level="INFO", entry_type=LogEntryType.info,
                        message=f"line of {method}", entry_hash=uuid.uuid4().hex)
        db.add_all([txn, line])
        await db.flush()
        await assignments.write(db, transaction_id=txn.id, entries=[line], customer_code=CC)
        db.add(AnalyticsFact(id=uuid.uuid4(), customer_code=CC, created_at=at,
                             source_transaction_id=txn.id, source_started_at=at,
                             source_version_hash="h", revision=1, event_time=at,
                             business_date=at.date(), method=method, user_name=user,
                             company=company, warehouse=warehouse, attributes={}))
        await db.commit()
        return txn.id


async def _methods(model) -> dict[str, list[str]]:
    """Which methods each kind of person reads from one table."""
    seen = {}
    for label, scope in SCOPES.items():
        async with scoped_data.open_session(async_session, scope) as db:
            seen[label] = sorted((await db.execute(select(model.method).where(
                model.customer_code == CC))).scalars().all())
    return seen


async def _calls() -> None:
    await _plant("CheckServer", user=None)
    await _plant("GetAccessToken", user="EDA", minutes=1)
    await _plant("ConfirmPickLine", user="EDA", warehouse="BRI", company="915", minutes=2)
    await _plant("ReportCount", user=None, warehouse="LON", company="915", minutes=3)


@pytest.mark.parametrize("model", [LogTransaction, AnalyticsFact])
async def test_a_call_that_names_nobody_is_read_by_every_person_who_has_a_site(model):
    await _calls()
    assert await _methods(model) == {
        "BRI": ["CheckServer", "ConfirmPickLine"],
        "LON": ["CheckServer", "ReportCount"],
        "ALL": ["CheckServer", "ConfirmPickLine", "GetAccessToken", "ReportCount"],
        "NOBODY": [],
    }


async def test_the_lines_follow_their_call():
    await _calls()
    seen = {}
    for label, scope in SCOPES.items():
        async with scoped_data.open_session(async_session, scope) as db:
            out = await tools._search_entries(db, {"q": "line of"}, CC)
        seen[label] = sorted(e["message"] for e in out["entries"])
    assert seen["BRI"] == ["line of CheckServer", "line of ConfirmPickLine"]
    assert seen["LON"] == ["line of CheckServer", "line of ReportCount"]
    assert len(seen["ALL"]) == 4 and seen["NOBODY"] == []


async def test_a_sign_in_can_be_opened_by_an_all_sites_person_only():
    sign_in = await _plant("GetAccessToken", user="EDA")
    check = await _plant("CheckServer", user=None, minutes=1)
    async with scoped_data.open_session(async_session, BRI) as db:
        hidden = await tools._get_transaction(db, {"transaction_id": str(sign_in)}, CC)
        shown = await tools._get_transaction(db, {"transaction_id": str(check)}, CC)
    assert "No transaction found" in hidden["error"]
    assert [step["message"] for step in shown["timeline"]] == ["line of CheckServer"]


@pytest.mark.parametrize("model", [LogTransaction, AnalyticsFact])
def test_the_rule_names_the_two_empty_columns(model):
    table = model.__tablename__
    rule = scoped_data.registry.BY_TABLE[table]
    sql = str(rule.condition(BRI).compile(compile_kwargs={"literal_binds": True}))
    assert f"{table}.warehouse = 'BRI' AND {table}.company = '915'" in sql
    assert f"{table}.warehouse IS NULL AND {table}.user_name IS NULL" in sql
    everyone = str(rule.condition(ALL).compile(compile_kwargs={"literal_binds": True}))
    assert everyone == f"{table}.customer_code = '{CC}'"


def test_other_site_limited_tables_keep_their_rule():
    from app.persistence.models.analytics_settlement import AnalyticsSettledRow
    rule = scoped_data.registry.BY_TABLE[AnalyticsSettledRow.__tablename__]
    assert "IS NULL" not in str(rule.condition(BRI).compile(compile_kwargs={"literal_binds": True}))
