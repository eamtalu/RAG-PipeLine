"""Step 33 (chunk 211): the live fold harvests list lookups, not only the backfill.

Found on 16 November 2026 while porting the locations panel: the `pick line` lookup (the pick-list
line's planned location, delivery, expected quantity and status, read from the `ListPickLinesByUser`
response list) had no new value since 2 October 19:00, while the WMS kept sending 200 to 300 of those
calls a day. The fold handed `harvest_lists` its source rows, which carry `started_at`; the harvest
times an observation by `event_time` and skipped every row without one. Only the two manual backfills,
which build their rows with `event_time`, ever filled the lookup. The planned location of every
release since then is unknown, and deliveries at risk lost its expected line count.

The chunk 129 tests handed `harvest_lists` rows that already carried `event_time`, so they never saw
the fold's row shape. This test goes through the real fold: a transaction, its response entry and
`consume_tenant`."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
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
from app.services.analytics import lookup as lk
from app.services.analytics import lookup_store

CC = "test_chunk211"
T0 = datetime(2026, 11, 14, 1, 53, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsFieldRegistry, AnalyticsTransactionRegistry, AnalyticsLookup, AnalyticsLookupValue,
          LogEntryAssignment, LogEntry, LogTransaction)

PICK_LINE = lk.Lookup(name="pick line", key_field="attr:ReportingNumber", attributes=(
    lk.Attribute("Location", sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "Location", list=True),)),
    lk.Attribute("DeliveryNumber", sources=(
        lk.Source("ListPickLinesByUser", "ReportingNumber", "DeliveryNumber", list=True),)),
))


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
        db.add(Customer(customer_code=CC, name="pick line probe", timezone="Europe/London"))
        db.add(AnalyticsLookup(customer_code=CC, name=PICK_LINE.name, key_field=PICK_LINE.key_field,
                               attributes=lookup_store.to_row(PICK_LINE)))
        await db.commit()
    yield
    await _wipe()


async def _plant_pick_list_calls(*calls: tuple[datetime, list[dict]]) -> None:
    """`ListPickLinesByUser` calls as Stage 2 leaves them, then folded: each a transaction and its response
    entry whose `response` is the list of pick-list lines (the live shape, measured 14 November 2026)."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for n, (at, lines) in enumerate(calls):
            txn = LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                                 ended_at=at + timedelta(seconds=2), date=at.date(), duration_ms=900,
                                 method="ListPickLinesByUser", transaction_name="Brighton Stock Pick",
                                 status=LogTransactionStatus.success, row_fingerprint=f"fp-{n}", attributes={})
            db.add(txn)
            await db.flush()
            entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=at + timedelta(seconds=1), line_number=1,
                             raw_body="resp", entry_hash=uuid.uuid4().hex, source_file="S/x.log", level="INFO",
                             entry_type=LogEntryType("response"), fields={"reqid": f"r-{n}", "response": lines})
            db.add(entry)
            await db.flush()
            db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id, entry_ts=entry.timestamp,
                                      transaction_id=txn.id, seq=0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=min(c[0] for c in calls) - WIDE,
                                      range_end=max(c[0] for c in calls) + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def test_the_live_fold_harvests_every_line_of_a_pick_list_response():
    await _plant_pick_list_calls((T0, [
        {"ReportingNumber": "620498", "Location": "H01B", "DeliveryNumber": "33344", "ExpectedQty": "2"},
        {"ReportingNumber": "620499", "Location": "D07C", "DeliveryNumber": "33344", "ExpectedQty": "9"},
    ]))
    async with async_session() as db:
        values = (await db.execute(select(AnalyticsLookupValue).where(
            AnalyticsLookupValue.customer_code == CC))).scalars().all()
    assert sorted((v.key, v.attribute, v.value) for v in values) == [
        ("620498", "DeliveryNumber", "33344"), ("620498", "Location", "H01B"),
        ("620499", "DeliveryNumber", "33344"), ("620499", "Location", "D07C")]
    # timed at the call, as the backfill times them, so a value is never dated by when it was folded
    assert all(v.source_method == "ListPickLinesByUser" and v.last_seen_at == T0 for v in values)


# ==================================================== the repair: re-settle the releases since a date, in place


async def test_the_keys_since_a_date_are_only_the_releases_folded_from_then():
    from app.services.analytics import settle_store
    from tests.golden import plant

    cc = "test_chunk211_settle"
    await plant.wipe_analytics(cc)
    await plant.register_analytics_tenant(cc)
    try:
        async with async_session() as db:
            for f in (plant.fact(cc, 0, 4, 4, rep="OLD1"), plant.fact(cc, 60, 4, 4, rep="NEW1"),
                      plant.fact(cc, 61, 4, 2, rep="NEW1"), plant.fact(cc, 90, 4, 0, rep="NEW2")):
                db.add(f)
            await db.commit()
            await plant.api.create_settlement(body=plant.BODY, backfill=True, customer=cc, db=db)
            settlement = (await settle_store.load(db, cc))["pick_release"]
            since = plant.T0 + timedelta(minutes=30)
            keys = await settle_store.keys_since(db, cc, settlement, since)
            assert keys == [("NEW1",), ("NEW2",)]
            # upserted in place, so a reader never sees the rows missing
            assert await settle_store.settle_keys(db, cc, settlement, keys) == 2
    finally:
        await plant.wipe_analytics(cc)


# ==================================================== the backfill's entry read (chunk 211b)


async def test_the_backfill_reads_list_responses_in_time_bounded_batches(monkeypatch):
    # Live, 16 November 2026: the backfill read every call's entries in one query per 2,000 calls,
    # joined on the entry id alone, so Postgres scanned all 77 entry partitions and hit the 30 s
    # statement timeout. It now reads in batches, each bounded by its calls' times on both tables.
    day_before = datetime.now(timezone.utc) - timedelta(days=2)
    await _plant_pick_list_calls(
        (day_before, [{"ReportingNumber": "700001", "Location": "A01A"}]),
        (day_before + timedelta(days=1), [{"ReportingNumber": "700002", "Location": "B02B"}]))
    async with async_session() as db:
        await db.execute(delete(AnalyticsLookupValue).where(AnalyticsLookupValue.customer_code == CC))
        await db.commit()
    monkeypatch.setattr(lookup_store, "LIST_ENTRY_BATCH", 1)
    async with async_session() as db:
        await lookup_store.backfill(db, CC, PICK_LINE, days=5)
        await db.commit()
        values = (await db.execute(select(AnalyticsLookupValue).where(
            AnalyticsLookupValue.customer_code == CC))).scalars().all()
    assert sorted((v.key, v.value) for v in values) == [("700001", "A01A"), ("700002", "B02B")]
