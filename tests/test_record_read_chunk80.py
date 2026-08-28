"""Chunk 80 (R4b part 3): the read API learns the record grain - and ad-hoc grouping learns to group.

Two families of fix, one shared mechanism:

- **Record metrics are readable.** `resolve` validates group-bys against the definition's SOURCE
  (record rows have their own field list), `attr:` paths are legal group-bys (a record metric's
  natural dimensions are `attr:rec.*`), and the live scan reads the record table for record
  definitions - an explicit two-entry model map, never parameterised magic.
- **Ad-hoc grouping actually groups.** Verified LIVE before this chunk: `/breakdown` on the default
  metric by `item_number` returned ONE row, value null, with the whole tenant's sum lumped into it -
  the live fold keyed by the DEFINITION's dimensions and then narrowed to the requested ones,
  discarding any requested field that was not already a dimension. The fix folds the live tier by
  the REQUESTED group-by (resolve_field reads plain columns and attr paths alike), and an ad-hoc
  request is served entirely live - mixing grouped live points with ungrouped rollup points was the
  other half of the bug.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi import HTTPException
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
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
from app.services.analytics import definition as d
from app.services.analytics import read as n6

CC = "test_chunk80"
T0 = datetime(2026, 8, 17, 10, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsRecordFact, AnalyticsFact, AnalyticsFactLedger,
                      AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup,
                      AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
                      AnalyticsFieldRegistry, AnalyticsTransactionRegistry, AnalyticsMetric,
                      LogEntryAssignment, LogEntry, LogTransaction):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="chunk80 probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant_expanded(records_by_txn):
    """N transactions with mi_result records, expand on, rec fields approved, folded."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick",
                                            expand=True))
        for f in ("rec.STQT", "rec.ITNO", "rec.WHSL"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="MMS060MI", source="record",
                                          field=f, captured=True))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for i, records in enumerate(records_by_txn):
            at = T0 + timedelta(minutes=i)
            txn = LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                                 ended_at=at + timedelta(seconds=2), date=at.date(),
                                 duration_ms=100, method="MMS060MI", transaction_name="Pick",
                                 status=LogTransactionStatus.success,
                                 row_fingerprint=f"fp-{i}", attributes={})
            db.add(txn)
            await db.flush()
            entry = LogEntry(customer_code=CC, job_id=job.id, timestamp=at + timedelta(seconds=1),
                             line_number=1, raw_body="mi", entry_hash=uuid.uuid4().hex,
                             source_file="S/x.log", level="INFO",
                             entry_type=LogEntryType("mi_result"),
                             fields={"result": "OK", "program": "MMS060MI",
                                     "transaction": "LstBalID", "records": records})
            db.add(entry)
            await db.flush()
            db.add(LogEntryAssignment(customer_code=CC, entry_id=entry.id,
                                      entry_ts=entry.timestamp, transaction_id=txn.id, seq=0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                      range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def _add_record_metric(name="units-by-item", dimensions=("attr:rec.ITNO",)):
    async with async_session() as db:
        row = AnalyticsMetric(
            customer_code=CC, name=name, source="record", dimensions=list(dimensions),
            measures=[{"name": "units", "aggregation": "sum", "field": "attr:rec.STQT",
                       "only": [], "statuses": []}],
            filter={"methods": [], "transactions": ["Pick"]},
            grains=["hourly", "daily", "monthly"], status="active")
        db.add(row)
        await db.commit()


async def _series(**kw):
    async with async_session() as db:
        return await api.analytics_series(customer=CC, db=db,
                                          metric=kw.pop("metric", "consumption"),
                                          measure=kw.pop("measure", "quantity"),
                                          start=kw.pop("start", T0 - WIDE),
                                          end=kw.pop("end", T0 + WIDE),
                                          group_by=kw.pop("group_by", None))


async def _breakdown(**kw):
    async with async_session() as db:
        return await api.analytics_breakdown(customer=CC, db=db,
                                             metric=kw.pop("metric", "consumption"),
                                             measure=kw.pop("measure", "quantity"),
                                             dimension=kw.pop("dimension"),
                                             start=kw.pop("start", T0 - WIDE),
                                             end=kw.pop("end", T0 + WIDE),
                                             top=kw.pop("top", 10))


# ================================= 1. record metrics are readable

async def test_a_record_series_grouped_by_a_rec_attribute():
    """The flagship read: units by item number, served from the record metric's rollups."""
    await _plant_expanded([[{"STQT": "624", "ITNO": "A"}, {"STQT": "12", "ITNO": "B"}],
                           [{"STQT": "8", "ITNO": "A"}]])
    await _add_record_metric()
    body = await _series(metric="units-by-item", measure="units", group_by="attr:rec.ITNO")
    by_item = {}
    for p in body["points"]:
        by_item[p["dimensions"][0]] = (by_item.get(p["dimensions"][0], Decimal(0))
                                       + Decimal(p["roles"]["sum_value"]))
    assert by_item == {"A": Decimal("632"), "B": Decimal("12")}


async def test_a_record_breakdown_ranks_by_an_undimensioned_rec_attribute():
    """Ad-hoc on the record grain: WHSL is approved but not a dimension, so the breakdown is served
    from a live scan of the RECORD table - grouped for real."""
    await _plant_expanded([[{"STQT": "624", "ITNO": "A", "WHSL": "L1"},
                            {"STQT": "12", "ITNO": "B", "WHSL": "L2"}]])
    await _add_record_metric()
    body = await _breakdown(metric="units-by-item", measure="units", dimension="attr:rec.WHSL")
    assert body["ad_hoc"] is True
    rows = {r["value"]: r["sum_value"] for r in body["rows"]}
    assert rows == {"L1": "624", "L2": "12"}


async def test_a_record_group_by_of_a_fact_only_field_is_a_400():
    await _plant_expanded([[{"STQT": "1", "ITNO": "A"}]])
    await _add_record_metric()
    with pytest.raises(HTTPException) as e:
        await _series(metric="units-by-item", measure="units", group_by="quantity")
    assert e.value.status_code == 400


async def test_an_unapproved_attr_group_by_is_a_400_not_an_empty_chart():
    await _plant_expanded([[{"STQT": "1", "ITNO": "A"}]])
    await _add_record_metric()
    with pytest.raises(HTTPException) as e:
        await _series(metric="units-by-item", measure="units", group_by="attr:rec.Bogus")
    assert e.value.status_code == 400


# ================================= 2. ad-hoc grouping actually groups (the live bug)

async def _plant_transaction_facts():
    """Two items' worth of ordinary transaction facts through the real fold."""
    from app.services.analytics import contract as c
    qf = c.QUANTITY_FIELD["ConfirmPickLine"]
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for i, (item, qty) in enumerate((("101978", "10.0"), ("101978", "5.0"),
                                         ("202000", "7.0"))):
            at = T0 + timedelta(minutes=i)
            db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                                  ended_at=at, date=at.date(), duration_ms=50,
                                  method="ConfirmPickLine", transaction_name="Pick",
                                  transaction_type="002001", item_number=item,
                                  status=LogTransactionStatus.success,
                                  attributes={qf: qty}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                      range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def test_an_ad_hoc_breakdown_returns_real_groups_not_one_null_row():
    """Pinned from live: /breakdown by item_number on the seed metric returned ONE row with value
    null and the whole sum lumped in - the ad-hoc fold discarded the requested field."""
    await _plant_transaction_facts()
    body = await _breakdown(dimension="item_number")
    assert body["ad_hoc"] is True
    rows = {r["value"]: Decimal(r["sum_value"]) for r in body["rows"]}
    assert rows == {"101978": Decimal("15"), "202000": Decimal("7")}, \
        f"ad-hoc grouping lumped rows together: {rows}"


async def test_an_ad_hoc_series_is_served_entirely_live():
    """Mixing grouped live points with ungrouped rollup points was the other half of the bug: an
    ad-hoc request cannot be answered by rollups at all, so none may contribute."""
    await _plant_transaction_facts()
    body = await _series(group_by="item_number")
    assert body["ad_hoc"] is True
    assert body["from_rollups"] is False
    assert all(p["dimensions"][0] in ("101978", "202000") for p in body["points"])


async def test_breakdown_validates_the_measure():
    """/breakdown accepted any measure and returned empty rows; a typo must be a 400 that lists
    what exists, exactly as /series already did."""
    await _plant_transaction_facts()
    with pytest.raises(HTTPException) as e:
        await _breakdown(dimension="method", measure="nope")
    assert e.value.status_code == 400 and "quantity" in str(e.value.detail)
