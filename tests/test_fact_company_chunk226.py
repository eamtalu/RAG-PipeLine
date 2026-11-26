"""Chunk 226: a fact says which WMS company it belongs to (stage D, step 49; step 1d stage 2a).

A data row is identified by customer, company and warehouse. Until this chunk only the transaction
knew its company; the fact, and every table derived from it, knew the warehouse alone, so the
same warehouse code under two companies could not be told apart downstream.

Pinned here
-----------
    normaliser   the company of the source row lands on the fact, and is part of the fingerprint
    fold         the fact and its ledger version carry it; an unchanged transaction still writes
                 nothing on the second fold
    backfill     facts and ledger versions written before this chunk take the company of their
                 transaction; a row whose transaction is gone takes the customer's only company
                 for that warehouse, and stays empty when there are two; a dry run writes nothing;
                 a run that stops early is finished by the next one
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import company_backfill as backfill
from app.services.analytics import consume as n3
from app.services.analytics import contract as c
from app.services.analytics import normalizer as n2

CC = "test_chunk226"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsFact, AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsTransactionRegistry, LogTransaction)


def _source_row(**over) -> dict:
    row = {"id": uuid.UUID("11111111-1111-1111-1111-111111111111"), "started_at": T0,
           "duration_ms": 100, "method": "ConfirmPickLine", "transaction_name": "Pick",
           "transaction_type": "002001", "status": "success", "item_number": "101978",
           "warehouse": "BRI", "warehouse_id": "1", "user_name": "EDA",
           "attributes": {"QuantityPicked": "10.0"}}
    row.update(over)
    return row


# ==================================================== 1. the normaliser, pure

def test_the_company_is_one_of_the_fields_a_fact_carries():
    assert "company" in c.FACT_FIELDS


def test_a_fact_takes_the_company_of_its_transaction():
    fact, issue = n2.normalise(_source_row(company="915"), tenant_timezone="Europe/London")
    assert issue is None and fact["company"] == "915" and fact["warehouse"] == "BRI"


def test_a_transaction_without_a_company_gives_a_fact_without_one():
    fact, _issue = n2.normalise(_source_row(), tenant_timezone="Europe/London")
    assert fact["company"] is None


def test_the_same_call_under_another_company_is_another_version():
    a, _ = n2.normalise(_source_row(company="915"), tenant_timezone="Europe/London")
    b, _ = n2.normalise(_source_row(company="920"), tenant_timezone="Europe/London")
    assert a["source_version_hash"] != b["source_version_hash"]


# ==================================================== 2. the fold

async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="company probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(*specs, ticket: bool = True) -> list[uuid.UUID]:
    """`specs` are (minutes after T0, company, warehouse) tuples: one sealed pick each."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        rows = []
        for minutes, company, warehouse in specs:
            at = T0 + timedelta(minutes=minutes)
            rows.append(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success,
                item_number="101978",
                user_name="EDA", company=company, warehouse=warehouse,
                attributes={"QuantityPicked": "10.0"}))
        db.add_all(rows)
        await db.flush()
        if ticket:
            db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                          range_end=T0 + WIDE))
        await db.commit()
        return [r.id for r in rows]


async def _companies(model) -> dict[uuid.UUID, list[str | None]]:
    """Every stored company per source transaction: {txn id: ["915"]}; two versions give two."""
    async with async_session() as db:
        rows = (await db.execute(select(model.source_transaction_id, model.company).where(
            model.customer_code == CC))).all()
    found: dict[uuid.UUID, list[str | None]] = {}
    for txn_id, company in rows:
        found.setdefault(txn_id, []).append(company)
    return found


async def _ticket() -> None:
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()


async def test_the_fold_writes_the_company_on_the_fact_and_on_its_ledger_version():
    with_company, without = await _plant((0, "915", "BRI"), (1, None, None))
    await n3.consume_tenant(CC)
    assert await _companies(AnalyticsFact) == {with_company: ["915"], without: [None]}
    assert await _companies(AnalyticsFactLedger) == {with_company: ["915"], without: [None]}


async def test_an_unchanged_transaction_still_writes_nothing_on_the_second_fold():
    (txn_id,) = await _plant((0, "915", "BRI"))
    await n3.consume_tenant(CC)
    await _ticket()
    await n3.consume_tenant(CC)
    assert await _companies(AnalyticsFactLedger) == {txn_id: ["915"]}


# ==================================================== 3. the backfill

async def _forget_companies() -> None:
    """Put the stored rows back to how they were before this chunk: no company on any of them."""
    async with async_session() as db:
        for model in (AnalyticsFact, AnalyticsFactLedger):
            await db.execute(update(model).where(model.customer_code == CC).values(company=None))
        await db.commit()


async def _folded_then_forgotten(*specs) -> list[uuid.UUID]:
    ids = await _plant(*specs)
    await n3.consume_tenant(CC)
    await _forget_companies()
    return ids


async def _drop_transactions(*ids: uuid.UUID) -> None:
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(ids)))
        await db.commit()


@pytest.mark.parametrize("table, model", [("analytics_facts", AnalyticsFact),
                                           ("analytics_fact_ledger", AnalyticsFactLedger)])
async def test_old_rows_take_the_company_of_their_transaction(table, model):
    a, b, none = await _folded_then_forgotten((0, "915", "BRI"), (1, "920", "BRI"), (2, None, None))
    totals = await backfill.fill_table(async_session, table, CC, batch=2)
    assert await _companies(model) == {a: ["915"], b: ["920"], none: [None]}
    assert totals.filled == 2


async def test_a_row_that_already_has_a_company_is_left_alone():
    (txn_id,) = await _plant((0, "915", "BRI"))
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(update(AnalyticsFact).where(AnalyticsFact.customer_code == CC)
                         .values(company="777"))
        await db.commit()
    totals = await backfill.fill_table(async_session, "analytics_facts", CC)
    assert await _companies(AnalyticsFact) == {txn_id: ["777"]} and totals.filled == 0


async def test_a_row_whose_transaction_is_gone_takes_the_customers_only_company_for_its_warehouse():
    gone, kept = await _folded_then_forgotten((0, "915", "BRI"), (1, "915", "BRI"))
    await _drop_transactions(gone)
    await backfill.fill_table(async_session, "analytics_facts", CC)
    assert await _companies(AnalyticsFact) == {gone: ["915"], kept: ["915"]}


async def test_a_row_whose_transaction_is_gone_stays_empty_when_the_warehouse_has_two_companies():
    gone, kept_915, kept_920 = await _folded_then_forgotten((0, "915", "BRI"), (1, "915", "BRI"),
                                                           (2, "920", "BRI"))
    await _drop_transactions(gone)
    await backfill.fill_table(async_session, "analytics_facts", CC)
    assert await _companies(AnalyticsFact) == {gone: [None], kept_915: ["915"], kept_920: ["920"]}


async def test_a_dry_run_counts_what_it_would_fill_and_writes_nothing():
    ids = await _folded_then_forgotten((0, "915", "BRI"), (1, "915", "BRI"))
    totals = await backfill.fill_table(async_session, "analytics_facts", CC, dry_run=True)
    assert totals.filled == 2 and totals.read == 2
    assert await _companies(AnalyticsFact) == {i: [None] for i in ids}


async def test_a_run_that_stops_early_is_finished_by_the_next_one():
    ids = await _folded_then_forgotten(*[(m, "915", "BRI") for m in range(5)])
    first = await backfill.fill_table(async_session, "analytics_facts", CC, batch=2, max_batches=1)
    assert first.filled == 2 and first.is_finished is False
    second = await backfill.fill_table(async_session, "analytics_facts", CC, batch=2)
    assert second.filled == 3 and second.is_finished is True
    assert await _companies(AnalyticsFact) == {i: ["915"] for i in ids}


async def test_only_the_two_fact_tables_can_be_backfilled():
    with pytest.raises(ValueError):
        await backfill.fill_table(async_session, "log_entries", CC)


# ==================================================== 4. the script

async def test_the_script_reports_a_dry_run_and_then_fills(capsys):
    from scripts import backfill_fact_company as script
    ids = await _folded_then_forgotten((0, "915", "BRI"), (1, "915", "BRI"))
    parser = script.build_parser()
    assert await script.run(parser.parse_args([CC, "--dry-run", "--pause", "0"])) == 0
    assert "would fill 2" in capsys.readouterr().out
    assert await _companies(AnalyticsFact) == {i: [None] for i in ids}
    assert await script.run(parser.parse_args([CC, "--pause", "0", "--vacuum"])) == 0
    out = capsys.readouterr().out
    assert "filled 2" in out and "finished" in out and "vacuumed" in out
    assert await _companies(AnalyticsFact) == {i: ["915"] for i in ids}
