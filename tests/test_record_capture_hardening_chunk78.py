"""Chunk 78 (R4b part 1): the record capture path, hardened before any fold reads it.

R4 shipped capture-only, and exploration for R4b found the holes a record fold would amplify into
plausible wrong numbers. Each test here pins one:

- A REVERSED parent fact left its record rows behind forever (the diff's reverse outcomes were
  filtered out at the top of `_expand_records`) - orphans in a KEEP_FOREVER table that a future
  record rollup would silently count. Invariant 5, honoured one grain down. The delete is
  UNCONDITIONAL - a name whose expand was later turned OFF still gets its orphans cleaned.
- An event-time move is reverse+insert at the diff (the key is (txn, event_time)), so the same fix
  covers it - pinned separately because the failure shape (duplicates under two event times)
  differs from the orphan shape.
- Ticking `expand` ON published no ticket ("changes nothing until R4 exists" - stale since R4
  shipped), and even with a ticket a settled window expanded nothing (diff all-unchanged, entries
  unread). The `_exp_v` presence/staleness diff fixes both: late expand flips and late field
  approvals BACKFILL through ordinary tickets.
- Zero-record transactions must not re-expand forever: "missing" is gated on the stored fact's
  `mi.record_count` (a seed-approved attribute), because an expanded NAME spans methods that
  legitimately return no records.
- A settled window whose rows are present and current writes nothing - S3's economics, one grain
  down, pinned byte-identically like 18n did.
- `analytics_record_facts` joins `_DESTINATION_TABLES` (the one-way-door default-partition sink),
  and the run finally REPORTS its record counts (they were computed and discarded).
"""

import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3

CC = "test_chunk78"
T0 = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

RECORDS = [{"BANO": "2608031215", "STQT": "624", "ITNO": "101978", "WHSL": "A-01-02"},
           {"BANO": "2608031216", "STQT": "12", "ITNO": "101978", "WHSL": "A-01-03"}]


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsRecordFact, AnalyticsFact, AnalyticsFactLedger,
                      AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup,
                      AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
                      AnalyticsFieldRegistry, AnalyticsTransactionRegistry,
                      LogEntryAssignment, LogEntry, LogTransaction):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="chunk78 probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(*, name="Pick", expand=True, records=RECORDS, at=T0, mi_scalars=True):
    """One sealed transaction with an mi_result entry, its registry row, and a ticket."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name=name,
                                            expand=expand))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        txn = LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                             ended_at=at + timedelta(seconds=2), date=at.date(), duration_ms=100,
                             method="MMS060MI", transaction_name=name,
                             status=LogTransactionStatus.success,
                             row_fingerprint="fp-1", attributes={})
        db.add(txn)
        await db.flush()
        fields = {"result": "OK", "program": "MMS060MI", "transaction": "LstBalID"}
        if mi_scalars and records is not None:
            fields["records"] = records
        entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=at + timedelta(seconds=1),
                         line_number=1, raw_body="mi", entry_hash=uuid.uuid4().hex,
                         source_file="S/x.log", level="INFO",
                         entry_type=LogEntryType("mi_result"), fields=fields)
        db.add(entry)
        await db.flush()
        db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id, entry_ts=entry.timestamp,
                                  transaction_id=txn.id, seq=0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=at - WIDE,
                                      range_end=at + WIDE))
        await db.commit()
        return txn.id


async def _ticket(at=T0):
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=at - WIDE,
                                      range_end=at + WIDE))
        await db.commit()


async def _fold():
    return await n3.consume_tenant(CC)


async def _record_rows():
    async with async_session() as db:
        return (await db.execute(select(AnalyticsRecordFact).where(
            AnalyticsRecordFact.customer_code == CC)
            .order_by(AnalyticsRecordFact.record_index))).scalars().all()


async def _approve_rec(field: str):
    async with async_session() as db:
        await db.execute(update(AnalyticsFieldRegistry)
                         .where(AnalyticsFieldRegistry.customer_code == CC,
                                AnalyticsFieldRegistry.field == field)
                         .values(captured=True))
        await db.commit()


# ===================================================== 1. reversal hygiene (invariant 5)

async def test_a_reversed_transaction_deletes_its_record_rows_even_with_expand_off():
    """The parent fact reverses (source vanished); its record rows must go with it - INCLUDING when
    expand was turned off in between, because rows kept after an OFF flip still orphan."""
    txn_id = await _plant()
    await _fold()
    assert len(await _record_rows()) == 2

    async with async_session() as db:
        await db.execute(update(AnalyticsTransactionRegistry)
                         .where(AnalyticsTransactionRegistry.customer_code == CC)
                         .values(expand=False))
        await db.execute(delete(LogEntryAssignment).where(
            LogEntryAssignment.transaction_id == txn_id))
        await db.execute(delete(LogTransaction).where(LogTransaction.id == txn_id))
        await db.commit()
    await _ticket()
    await _fold()

    assert await _record_rows() == [], "reversed parent left orphan record rows behind"


async def test_an_event_time_move_leaves_no_duplicate_record_rows():
    """A moved started_at is reverse+insert at the diff (the key carries event_time); the record
    rows must follow - one set under the NEW time, nothing under the old."""
    txn_id = await _plant()
    await _fold()

    moved = T0 + timedelta(hours=2)
    async with async_session() as db:
        await db.execute(update(LogTransaction).where(LogTransaction.id == txn_id)
                         .values(started_at=moved, date=moved.date(), row_fingerprint="fp-2"))
        await db.commit()
    await _ticket()
    await _fold()

    rows = await _record_rows()
    assert len(rows) == 2, f"expected one replaced set, got {len(rows)} rows"
    assert all(r.event_time == moved for r in rows)


# ===================================================== 2. late flips backfill

async def test_ticking_expand_on_publishes_a_backfill_ticket():
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick"))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0,
                                    history_starts_at=T0 - timedelta(days=2)))
        await db.commit()
    async with async_session() as db:
        res = await api.set_transaction_switches("Pick", payload={"expand": True},
                                                 customer=CC, db=db)
    assert res["tickets_published"] > 0, "expand ON must re-examine the retention range"


async def test_a_late_expand_flip_backfills_a_settled_window():
    """The live gap: expand was off when the window folded (facts settled, entries present), and
    flipping it on used to change nothing forever - the diff says unchanged and the entries are
    never read. The presence diff must expand it through an ordinary ticket."""
    await _plant(expand=False)
    await _fold()
    assert await _record_rows() == []

    async with async_session() as db:
        await db.execute(update(AnalyticsTransactionRegistry)
                         .where(AnalyticsTransactionRegistry.customer_code == CC)
                         .values(expand=True))
        await db.commit()
    await _ticket()
    await _fold()

    rows = await _record_rows()
    assert len(rows) == 2, "the settled window did not backfill after expand flipped on"
    assert {r.mi_program for r in rows} == {"MMS060MI"}


async def test_a_late_field_approval_reaches_existing_record_rows():
    """Rows are written before anyone can approve their fields (discovery precedes review), so the
    first write always has empty attributes. Approving later must refresh them via `_exp_v`."""
    await _plant()
    await _fold()
    rows = await _record_rows()
    assert rows and all("rec.STQT" not in (r.attributes or {}) for r in rows)

    await _approve_rec("rec.STQT")
    await _ticket()
    await _fold()

    rows = await _record_rows()
    assert [r.attributes.get("rec.STQT") for r in rows] == ["624", "12"], \
        "approved field did not reach the already-written record rows"


# ===================================================== 3. no thrash (invariant 6, one grain down)

async def test_a_zero_record_transaction_does_not_reexpand_forever():
    """An expanded NAME spans methods that return no records. 'Missing rows' alone would re-expand
    those on every fold forever; the stored fact's mi.record_count gates it."""
    await _plant(records=None)  # mi_result without records[]
    await _fold()
    stats = await _fold()  # a second, settled fold
    assert stats.get("record_facts", 0) == 0
    assert await _record_rows() == []


async def test_a_settled_window_with_current_rows_rewrites_nothing():
    """S3 one grain down: rows present, `_exp_v` current -> the second fold must not touch them.
    Pinned byte-identically, as 18n pinned the diff-driven version."""
    await _plant()
    await _fold()
    before = [(r.id, r.created_at) for r in await _record_rows()]
    assert before

    await _ticket()
    await _fold()
    after = [(r.id, r.created_at) for r in await _record_rows()]
    assert after == before, "a settled window rewrote record rows it should have skipped"


# ===================================================== 4. plumbing

def test_record_facts_is_a_provisioned_destination():
    """The one-way-door: a historic fold writing into an unprovisioned month lands in the DEFAULT
    partition, and that period's real partition can then never be created."""
    assert "analytics_record_facts" in n3._DESTINATION_TABLES


async def test_the_run_reports_its_record_counts():
    """`record_stats` was computed and discarded; the run and the tenant state must carry it."""
    await _plant()
    stats = await _fold()
    assert stats.get("record_facts") == 2
    async with async_session() as db:
        state = (await db.execute(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))).scalar_one()
    assert state.record_facts_total == 2


async def test_the_registry_detail_reports_the_record_count():
    """The console's drill-down shows what `expand` has actually produced for a name."""
    await _plant()
    await _fold()
    async with async_session() as db:
        d = await api.transaction_registry_detail("Pick", customer=CC, db=db)
    assert d["records"]["count"] == 2
