"""Chunk 104: the composition cards show four sources, not three with the request half hidden inside one.

Chunk 99 began registering the fields the handheld SENT, but the endpoint sorted a registry row by
asking "record? MI? otherwise response", and request rows had not existed when that was written. So
they fell through the last branch. Seen live on Brighton Stock Pick the day it shipped: 77 request
names filed among 36 genuine response ones, on one card, with no way to tell them apart.

The two are not interchangeable and must not look it. What the server RETURNED is stored only when
somebody ticks it. What the handheld SENT is already on every fact whatever anybody ticks, so its tick
decides whether the field may be REPORTED on. Same box, two meanings, and the screen has to say which.

This chunk also adds what the cards need to be readable rather than merely correct. Of the 46 request
fields on a pick, 24 hold the same single value for the whole tenant - the server address, the port,
the company, the division - and a few hold a different value on every single record. Both kinds are
useless to group by, so every entry now reports how many DIFFERENT values it holds and the screen
ranks by it.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

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
from app.services.analytics.contract import QUANTITY_FIELD as QF

CC = "test_chunk104"
T0 = datetime.now(timezone.utc) - timedelta(hours=2)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsRecordFact, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsFieldRegistry, AnalyticsTransactionRegistry,
          LogEntryAssignment, LogEntry, LogTransaction)


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
        db.add(Customer(customer_code=CC, name="composition probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(picks):
    """`picks` is a list of request attribute dicts, one per ConfirmPickLine."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick"))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0 + WIDE,
                                    history_starts_at=T0 - WIDE))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for i, attributes in enumerate(picks):
            at = T0 + timedelta(seconds=i)
            txn = LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                ended_at=at + timedelta(seconds=1), date=at.date(), duration_ms=100,
                method="ConfirmPickLine", transaction_name="Pick", transaction_type="002001",
                status=LogTransactionStatus.success, item_number="101998", user_name="EDA",
                warehouse="BRI", row_fingerprint=f"fp-{i}", attributes=attributes)
            db.add(txn)
            await db.flush()
            entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=at + timedelta(milliseconds=500),
                             line_number=i + 1, raw_body="response", entry_hash=uuid.uuid4().hex,
                             source_file="S/x.log", level="INFO", entry_type=LogEntryType("response"),
                             fields={"response": {"QuantityOnHand": str(10 + i), "Warehouse": "BRI"}})
            db.add(entry)
            await db.flush()
            db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id, entry_ts=entry.timestamp,
                                      transaction_id=txn.id, seq=0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def _composition():
    async with async_session() as db:
        return await api.transaction_composition("Pick", customer=CC, db=db)


def _one(out, group, field):
    return next((e for e in out["fields"][group] if e["field"] == field), None)


# ==================================================== 1. four sources, told apart

async def test_the_two_halves_of_the_exchange_are_separate_cards():
    await _plant([{QF["ConfirmPickLine"]: "3", "DeliveryNumber": "27383", "ApiPort": "443"}])
    out = await _composition()
    assert set(out["fields"]) == {"request", "response", "mi", "record"}

    sent = {e["field"] for e in out["fields"]["request"]}
    returned = {e["field"] for e in out["fields"]["response"]}
    assert {"DeliveryNumber", "ApiPort", QF["ConfirmPickLine"]} <= sent
    assert {"resp.QuantityOnHand", "resp.Warehouse"} <= returned
    assert not any(f.startswith("resp.") for f in sent), \
        "a response name must never appear under what the handheld sent"
    assert not returned & sent


async def test_every_entry_says_which_source_it_came_from():
    """So the screen never has to infer it from a prefix, which is what the endpoint was doing."""
    await _plant([{QF["ConfirmPickLine"]: "3", "DeliveryNumber": "27383"}])
    out = await _composition()
    assert _one(out, "request", "DeliveryNumber")["source"] == "request"
    assert _one(out, "response", "resp.Warehouse")["source"] == "response"


async def test_a_request_field_says_its_value_is_stored_whatever_the_tick_says():
    """The difference that matters. A response value is stored only once somebody ticks it; a request
    value is on every fact already, so its tick decides whether it may be REPORTED on. Same box, two
    meanings, and the screen has to be able to say which."""
    await _plant([{QF["ConfirmPickLine"]: "3", "DeliveryNumber": "27383"}])
    out = await _composition()
    assert _one(out, "request", "DeliveryNumber")["stored_regardless"] is True
    assert _one(out, "response", "resp.Warehouse")["stored_regardless"] is False


async def test_the_bookkeeping_keys_appear_on_no_card_at_all():
    await _plant([{QF["ConfirmPickLine"]: "3"}])
    out = await _composition()
    for group in out["fields"].values():
        assert not [e for e in group if e["field"].startswith("__")]


# ==================================================== 2. what makes a card readable

async def test_every_field_reports_how_many_different_values_it_holds():
    """The single signal that separates a useful slice from protocol noise. Measured live: of the 46
    request fields on a pick, 24 hold ONE value for the whole tenant."""
    await _plant([
        {QF["ConfirmPickLine"]: "3", "ApiPort": "443", "ItemNumber": "A", "ReqId": "r1"},
        {QF["ConfirmPickLine"]: "4", "ApiPort": "443", "ItemNumber": "B", "ReqId": "r2"},
        {QF["ConfirmPickLine"]: "5", "ApiPort": "443", "ItemNumber": "A", "ReqId": "r3"},
    ])
    out = await _composition()
    assert _one(out, "request", "ApiPort")["distinct_values"] == 1, "one value on every record"
    assert _one(out, "request", "ItemNumber")["distinct_values"] == 2
    assert _one(out, "request", "ReqId")["distinct_values"] == 3, "a different value every time"
    assert _one(out, "request", "ItemNumber")["recent_facts"] == 3


#: Four picks where the item repeats. Small, but it has to be big enough to tell a CONSTANT apart
#: from an IDENTIFIER, and with two records those two look identical.
_FOUR = [
    {QF["ConfirmPickLine"]: "3", "ApiPort": "443", "ItemNumber": "A", "ReqId": "r1"},
    {QF["ConfirmPickLine"]: "4", "ApiPort": "443", "ItemNumber": "A", "ReqId": "r2"},
    {QF["ConfirmPickLine"]: "5", "ApiPort": "443", "ItemNumber": "B", "ReqId": "r3"},
    {QF["ConfirmPickLine"]: "6", "ApiPort": "443", "ItemNumber": "B", "ReqId": "r4"},
]


async def test_a_field_holding_one_value_everywhere_is_useless_to_group_by():
    """Grouping by it puts every record in a single row. Measured live: 24 of the 46 request fields
    on a pick are like this - the server address, the port, the company, the division."""
    await _plant(_FOUR)
    out = await _composition()
    assert _one(out, "request", "ApiPort")["distinct_values"] == 1
    assert _one(out, "request", "ApiPort")["useful"] is False


async def test_a_field_with_a_different_value_on_every_record_is_useless_in_the_same_way():
    """`ReqId` and `StartDateTime` are unique per record. Grouping by one gives as many rows as there
    are records, which is the pathological case the summaries exist to avoid. It looks nothing like a
    constant and is just as useless."""
    await _plant(_FOUR)
    out = await _composition()
    assert _one(out, "request", "ReqId")["distinct_values"] == 4
    assert _one(out, "request", "ReqId")["useful"] is False


async def test_a_field_whose_values_repeat_is_the_one_worth_offering():
    """The whole point of the two tests above. A slice is a field that puts SEVERAL records in each
    group, which is neither a constant nor an identifier."""
    await _plant(_FOUR)
    out = await _composition()
    entry = _one(out, "request", "ItemNumber")
    assert (entry["distinct_values"], entry["recent_facts"]) == (2, 4)
    assert entry["useful"] is True


async def test_a_quantity_that_differs_every_time_is_not_offered_as_a_slice_either():
    """It is a measure, not a slice, and the same rule catches it without a special case."""
    await _plant(_FOUR)
    out = await _composition()
    assert _one(out, "response", "resp.QuantityOnHand")["useful"] is False
    assert _one(out, "response", "resp.Warehouse")["useful"] is False, "constant across the four"


async def test_a_field_nothing_recent_carries_is_not_called_useless_on_no_evidence():
    """A field seen last month but not this fortnight has no measurement behind it. Absent is not
    zero, and it must not be presented as "useless" on the strength of nothing."""
    await _plant([{QF["ConfirmPickLine"]: "3"}])
    async with async_session() as db:
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="request",
                                      field="SomethingOld", captured=True))
        await db.commit()
    out = await _composition()
    old = _one(out, "request", "SomethingOld")
    assert old["distinct_values"] is None
    assert old["useful"] is None


# ==================================================== 3. nothing that worked before broke

async def test_the_response_card_still_reports_its_frequency_per_method():
    await _plant([{QF["ConfirmPickLine"]: "3"}, {QF["ConfirmPickLine"]: "4"}])
    out = await _composition()
    entry = _one(out, "response", "resp.QuantityOnHand")
    assert entry["recent_by_method"] == {"ConfirmPickLine": 2}
    assert entry["recent_facts"] == 2


async def test_a_credential_shaped_request_name_is_still_flagged_and_still_unticked():
    await _plant([{QF["ConfirmPickLine"]: "3", "SessionToken": "hunter2"}])
    out = await _composition()
    entry = _one(out, "request", "SessionToken")
    assert entry["credential"] is True and entry["captured"] is False


async def test_one_entry_per_field_name_even_when_several_methods_register_it():
    await _plant([{QF["ConfirmPickLine"]: "3", "ItemNumber": "A"}])
    async with async_session() as db:
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="StockMove", source="request",
                                      field="ItemNumber", captured=True))
        await db.commit()
    out = await _composition()
    entries = [e for e in out["fields"]["request"] if e["field"] == "ItemNumber"]
    assert len(entries) == 1
    assert len(entries[0]["ids"]) >= 1
