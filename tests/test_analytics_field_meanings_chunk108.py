"""Chunk 108: what a field MEANS, recorded once per name.

Meaning lived on the field registry, whose rows are per field PER METHOD. That is right for a
DECISION - `EmployeeName` may be ticked on picking and not on counting - and wrong for a MEANING,
because the name means the same thing on all 44 methods that carry it. Describing it meant writing
the same sentence 44 times, so nobody did: 1,492 rows on the live tenant, 0 described, 0 with a unit.

`kind` is the column no amount of looking at the data can fill. Discovery over the live facts
classified `ItemNumber`, `DeliveryNumber`, `LotNumber`, `UserID` and `DeviceID` as measures, because
they are numeric and they repeat exactly as a quantity does. A delivery number is a name spelled with
digits. One person saying so, once, is what stops an agent adding delivery numbers together and
calling the answer picked quantity.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select

from app.config.database import async_session
from app.main import app
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_meaning import KINDS, AnalyticsFieldMeaning
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction
from app.services.analytics import catalog as n8

CC = "test_chunk108"
BASE = "/api/v1/analytics"
T0 = datetime.now(timezone.utc) - timedelta(hours=1)
MODELS = (AnalyticsFact, AnalyticsFieldMeaning, AnalyticsFieldRegistry,
          AnalyticsTransactionRegistry, AnalyticsMetric, LogTransaction)


async def call(method: str, url: str, **kw):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver",
                           headers={"X-Customer-Code": CC}) as c:
        return await c.request(method, BASE + url, **kw)


async def _reset():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        db.add(Customer(customer_code=CC, name="meaning probe", timezone="Europe/London"))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _reset()
    yield
    await _reset()


async def _register(field: str, methods, *, source="request", captured=True):
    async with async_session() as db:
        for m in methods:
            db.add(AnalyticsFieldRegistry(customer_code=CC, method=m, source=source,
                                          field=field, captured=captured, seen_count=10))
        await db.commit()


async def _meanings():
    async with async_session() as db:
        return (await db.execute(select(AnalyticsFieldMeaning).where(
            AnalyticsFieldMeaning.customer_code == CC))).scalars().all()


# ==================================================== 1. one row per NAME

async def test_one_sentence_covers_every_method_that_carries_the_name():
    """The whole point. `EmployeeName` is on 44 methods live; describing it must be one act."""
    await _register("EmployeeName", ["ConfirmPickLine", "ReportCount", "StockMove"])
    r = await call("PATCH", "/registry/meanings", json={
        "field": "EmployeeName", "description": "Who was holding the handheld.", "kind": "slice"})
    assert r.status_code == 200, r.text

    rows = await _meanings()
    assert len(rows) == 1, "one row for the name, not one per method"
    assert rows[0].field == "EmployeeName"
    assert rows[0].description == "Who was holding the handheld."
    assert rows[0].kind == "slice"


async def test_describing_it_again_edits_rather_than_duplicates():
    await _register("EmployeeName", ["ConfirmPickLine"])
    await call("PATCH", "/registry/meanings", json={"field": "EmployeeName", "description": "First."})
    await call("PATCH", "/registry/meanings", json={"field": "EmployeeName", "description": "Second."})
    rows = await _meanings()
    assert len(rows) == 1 and rows[0].description == "Second."


async def test_writing_a_meaning_records_who_and_when():
    """So a wrong sentence can be traced, and so "nobody has looked at this" stays distinguishable."""
    await _register("QuantityPicked", ["ConfirmPickLine"])
    await call("PATCH", "/registry/meanings",
               json={"field": "QuantityPicked", "description": "Units confirmed.", "unit": "units",
                     "kind": "measure", "reviewed_by": "amin"})
    row = (await _meanings())[0]
    assert row.reviewed_by == "amin" and row.reviewed_at is not None


async def test_a_meaning_may_be_cleared_back_to_nothing():
    """Undoing a mistake must be possible, and an empty sentence is not a sentence."""
    await _register("EmployeeName", ["ConfirmPickLine"])
    await call("PATCH", "/registry/meanings", json={"field": "EmployeeName", "description": "x"})
    await call("PATCH", "/registry/meanings", json={"field": "EmployeeName", "description": "   "})
    assert (await _meanings())[0].description is None


# ==================================================== 2. the kind, which the data cannot supply

@pytest.mark.parametrize("kind", KINDS)
async def test_every_kind_the_contract_names_is_accepted(kind):
    await _register("Quantity", ["StockMove"])
    r = await call("PATCH", "/registry/meanings", json={"field": "Quantity", "kind": kind})
    assert r.status_code == 200
    assert (await _meanings())[0].kind == kind


async def test_a_kind_outside_the_contract_is_refused_and_says_what_is_allowed():
    await _register("Quantity", ["StockMove"])
    r = await call("PATCH", "/registry/meanings", json={"field": "Quantity", "kind": "quantity"})
    assert r.status_code == 400
    for k in KINDS:
        assert k in r.json()["detail"]


async def test_not_yet_decided_is_not_the_same_as_noise():
    """A field nobody has looked at must not read as one somebody judged useless. Absent is not
    zero, applied to a decision rather than to a number."""
    await _register("Quantity", ["StockMove"])
    await call("PATCH", "/registry/meanings", json={"field": "Quantity", "description": "moved"})
    assert (await _meanings())[0].kind is None

    await call("PATCH", "/registry/meanings", json={"field": "Quantity", "kind": "noise"})
    assert (await _meanings())[0].kind == "noise"

    await call("PATCH", "/registry/meanings", json={"field": "Quantity", "kind": None})
    assert (await _meanings())[0].kind is None, "a decision can be taken back"


async def test_a_name_spelled_with_digits_can_be_called_a_slice():
    """The case the column exists for. Nothing in the values of `DeliveryNumber` separates it from
    a quantity; both are numeric and both repeat."""
    await _register("DeliveryNumber", ["ConfirmPickLine", "NewDeliveryPackage"])
    await call("PATCH", "/registry/meanings", json={
        "field": "DeliveryNumber", "kind": "slice",
        "description": "Which delivery the line belongs to. A name, not a quantity."})
    assert (await _meanings())[0].kind == "slice"


# ==================================================== 3. what refuses

async def test_a_field_nothing_has_ever_registered_is_refused():
    """Fails closed, like every other name in this system. A typo would otherwise sit in the
    catalogue describing a field that does not exist."""
    r = await call("PATCH", "/registry/meanings", json={"field": "NoSuchField", "description": "x"})
    assert r.status_code == 404
    assert "NoSuchField" in r.json()["detail"]


async def test_a_body_with_no_field_is_refused():
    assert (await call("PATCH", "/registry/meanings", json={"description": "x"})).status_code == 400


async def test_a_body_that_changes_nothing_is_refused_rather_than_silently_accepted():
    await _register("EmployeeName", ["ConfirmPickLine"])
    r = await call("PATCH", "/registry/meanings", json={"field": "EmployeeName"})
    assert r.status_code == 400


# ==================================================== 4. it reaches the readers

async def test_the_listing_shows_every_registered_name_whether_described_or_not():
    """The screen has to show the gap. A name absent from the list cannot be filled in."""
    await _register("EmployeeName", ["ConfirmPickLine", "ReportCount"])
    await _register("QuantityPicked", ["ConfirmPickLine"])
    await call("PATCH", "/registry/meanings", json={"field": "QuantityPicked", "kind": "measure"})

    body = (await call("GET", "/registry/meanings")).json()
    by_field = {m["field"]: m for m in body["meanings"]}
    assert set(by_field) == {"EmployeeName", "QuantityPicked"}
    assert by_field["EmployeeName"]["kind"] is None
    assert by_field["EmployeeName"]["methods"] == 2
    assert by_field["QuantityPicked"]["kind"] == "measure"
    assert body["described"] == 0 and body["total"] == 2


async def test_the_catalogue_reads_the_meaning_from_the_name():
    """The catalogue feeds the builder and the chat agent. It has to read one source of truth."""
    await _register("QuantityPicked", ["ConfirmPickLine", "ReportCount"])
    await call("PATCH", "/registry/meanings", json={
        "field": "QuantityPicked", "description": "Units confirmed by the picker.", "unit": "units"})
    async with async_session() as db:
        fields = await n8._fields(db, CC)
    entry = next(f for f in fields if f.field == "QuantityPicked")
    assert entry.description == "Units confirmed by the picker."
    assert entry.unit == "units"
    assert sorted(entry.methods) == ["ConfirmPickLine", "ReportCount"], "still one entry per name"


async def test_the_composition_card_carries_the_meaning():
    """So the sentence appears where the tick is, which is where somebody decides."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick"))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0,
                              ended_at=T0, date=T0.date(), duration_ms=1, method="ConfirmPickLine",
                              transaction_name="Pick", attributes={}))
        await db.commit()
    await _register("QuantityPicked", ["ConfirmPickLine"])
    await call("PATCH", "/registry/meanings", json={
        "field": "QuantityPicked", "description": "Units confirmed.", "unit": "units",
        "kind": "measure"})

    body = (await call("GET", "/registry/transactions/Pick/composition")).json()
    entry = next(e for e in body["fields"]["request"] if e["field"] == "QuantityPicked")
    assert entry["description"] == "Units confirmed."
    assert entry["unit"] == "units"
    assert entry["kind"] == "measure"
