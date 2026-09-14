"""Chunk 105: a transaction's composition says which looked-up fields it can reach.

The composition screen answers "what is one fact of this transaction made of". Until now it could
only answer that from the fact itself, which is exactly the limit lookups exist to lift: a pick
carries its delivery number and no customer name, and the name is one hop away.

So the card is a fifth source beside the request, the response, the M3 calls and the records. It is
the moment somebody realises they want a lookup, and it is the honest place to say how far the
existing one reaches: measured live, the customer name is available on 1,528 of 1,530 picks.

A lookup is NOT declared here. One delivery lookup serves picking, packing and routing, so it cannot
belong to any one of them; this card shows what is reachable and points at where they are managed.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
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
from app.services.analytics import lookup as lk
from app.services.analytics import lookup_store
from app.services.analytics.contract import QUANTITY_FIELD as QF

CC = "test_chunk105"
T0 = datetime.now(timezone.utc) - timedelta(hours=2)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsRecordFact, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsFieldRegistry, AnalyticsTransactionRegistry,
          AnalyticsLookupValue, AnalyticsLookup, LogEntryAssignment, LogEntry, LogTransaction)

DELIVERY = lk.Lookup(
    name="delivery", key_field="delivery_number",
    attributes=(lk.Attribute("customer_name", stable=True, sources=(
        lk.Source("NewDeliveryPackage", "delivery_number", "CustomerName"),)),))


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
        db.add(Customer(customer_code=CC, name="lookup card probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _declare(lookup=DELIVERY, *, enabled=True):
    async with async_session() as db:
        db.add(AnalyticsLookup(customer_code=CC, name=lookup.name, key_field=lookup.key_field,
                               attributes=lookup_store.to_row(lookup), enabled=enabled))
        await db.commit()


async def _plant(picks, *, packings=()):
    """`picks` are (delivery_number or None) and `packings` are (delivery, customer)."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick"))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0 + WIDE,
                                    history_starts_at=T0 - WIDE))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        seq = 0
        for delivery in picks:
            at = T0 + timedelta(seconds=seq)
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                ended_at=at + timedelta(seconds=1), date=at.date(), duration_ms=100,
                method="ConfirmPickLine", transaction_name="Pick", transaction_type="002001",
                status=LogTransactionStatus.success, item_number="A", user_name="EDA",
                warehouse="BRI", delivery_number=delivery, row_fingerprint=f"p-{seq}",
                attributes={QF["ConfirmPickLine"]: "3"}))
            seq += 1
        for delivery, customer in packings:
            at = T0 + timedelta(seconds=seq)
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                ended_at=at + timedelta(seconds=1), date=at.date(), duration_ms=100,
                method="NewDeliveryPackage", transaction_name="Pick", transaction_type="002001",
                status=LogTransactionStatus.success, user_name="EDA", warehouse="BRI",
                delivery_number=delivery, row_fingerprint=f"n-{seq}",
                attributes={"CustomerName": customer}))
            seq += 1
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def _composition():
    async with async_session() as db:
        return await api.transaction_composition("Pick", customer=CC, db=db)


# ==================================================== the fifth card

async def test_a_transaction_carrying_a_key_is_told_what_it_can_reach():
    await _declare()
    await _plant(["25810", "25810"], packings=[("25810", "BAGELMAN BRIGHTON")])
    out = await _composition()
    assert "looked_up" in out["fields"]
    entry = out["fields"]["looked_up"][0]
    assert entry["lookup"] == "delivery"
    assert entry["field"] == "lookup:delivery.customer_name"
    assert entry["key_field"] == "delivery_number"


async def test_it_says_how_far_the_lookup_actually_reaches():
    """Measured live, the customer name is available on 1,528 of 1,530 picks. The two that are not
    are real, and a card claiming full coverage would be the more useful-looking lie."""
    await _declare()
    await _plant(["25810", "25810", "unnamed"], packings=[("25810", "BAGELMAN BRIGHTON")])
    out = await _composition()
    entry = out["fields"]["looked_up"][0]
    # Four facts carry a delivery number: three picks and the packing record, which belongs to the
    # same transaction. That is the real shape - `Brighton Stock Pick` is served by eleven methods,
    # `NewDeliveryPackage` among them - and the count is of the transaction, not of one method.
    assert entry["facts_with_key"] == 4
    assert entry["facts_resolved"] == 3, "everything on 25810; the unnamed delivery resolves to nothing"
    assert entry["percent_resolved"] == pytest.approx(75.0, abs=0.1)


async def test_a_transaction_that_carries_no_key_is_offered_nothing():
    """A lookup keyed by something this transaction never records is not reachable from it, and
    offering it would be a slice that reads as "not known" on every row."""
    await _declare()
    await _plant([None, None])
    out = await _composition()
    assert out["fields"]["looked_up"] == []


async def test_a_lookup_that_is_switched_off_is_not_offered():
    await _declare(enabled=False)
    await _plant(["25810"], packings=[("25810", "X")])
    out = await _composition()
    assert out["fields"]["looked_up"] == []


async def test_no_lookup_declared_means_an_empty_card_rather_than_a_missing_key():
    """The screen reads the key unconditionally; a missing one is what blanked this page once."""
    await _plant(["25810"])
    out = await _composition()
    assert out["fields"]["looked_up"] == []


async def test_a_key_present_but_never_named_reads_as_reachable_by_nothing():
    """Honest rather than absent: the transaction CAN be grouped by it, and every row would say not
    known. That is worth seeing before somebody builds a figure on it."""
    await _declare()
    await _plant(["25810", "25811"])
    out = await _composition()
    entry = out["fields"]["looked_up"][0]
    assert entry["facts_with_key"] == 2
    assert entry["facts_resolved"] == 0
    assert entry["percent_resolved"] == 0.0


async def test_every_attribute_of_a_lookup_is_offered_separately():
    """Two attributes are two slices, and they need not resolve equally well."""
    two = lk.Lookup(name="delivery", key_field="delivery_number", attributes=(
        lk.Attribute("customer_name", sources=(lk.Source("NewDeliveryPackage", "delivery_number",
                                                         "CustomerName"),)),
        lk.Attribute("route", sources=(lk.Source("NewDeliveryPackage", "delivery_number", "Route"),)),
    ))
    await _declare(two)
    await _plant(["25810"], packings=[("25810", "BAGELMAN BRIGHTON")])
    out = await _composition()
    fields = sorted(e["field"] for e in out["fields"]["looked_up"])
    assert fields == ["lookup:delivery.customer_name", "lookup:delivery.route"]
    by_field = {e["field"]: e for e in out["fields"]["looked_up"]}
    # Two facts carry the delivery: the pick and the packing record that named it.
    assert by_field["lookup:delivery.customer_name"]["facts_resolved"] == 2
    assert by_field["lookup:delivery.route"]["facts_resolved"] == 0, "no Route on the packing record"
    assert by_field["lookup:delivery.route"]["percent_resolved"] == 0.0
