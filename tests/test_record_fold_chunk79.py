"""Chunk 79 (R4b part 2): the record-grain fold and reconciliation.

Metric definitions gain a SOURCE ("transaction" | "record") - a promoted column, because the fold
partitions definitions by it every cycle, and deliberately not called "grain" because `grains`
already means time resolution. The fold gets a PARALLEL reader and recompute for the record table
(never a parameterised one - the 18n structural-separation guard stays true in both directions),
folding record definitions into the SAME definition-keyed rollup tables.

The corrections the adversarial plan review demanded are pinned here:

- the `_roll_up` empty-guard tests the UNION of transaction and record dirty buckets - otherwise
  the expand-on backfill (windows whose fact diff is all-unchanged) would never roll anything up;
- the reconciler's existing loops are PARTITIONED by source - un-partitioned, `rollups_vs_facts`
  would flag every record definition "orphaned" forever, and the repair path would DELETE record
  rollups and replace them with transaction-fact folds (corruption through the repair);
- `validate` fails closed in both directions: a record metric cannot filter on status or
  classification (records carry neither), and a TRANSACTION metric can no longer name `attr:rec.*`
  (accepted-but-silently-empty was a live hole);
- `show=off` hides a transaction from BOTH grains.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select, update

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
from app.services.analytics import reconcile as rc
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk79"
T0 = datetime(2026, 8, 16, 10, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

RECORDS = [{"STQT": "624", "ITNO": "101978", "WHSL": "A-01-02"},
           {"STQT": "12", "ITNO": "101978", "WHSL": "A-01-03"}]


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
        db.add(Customer(customer_code=CC, name="chunk79 probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


async def _plant(*, name="Pick", expand=True, records=RECORDS, at=T0):
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
        if records is not None:
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


async def _approve(*fields):
    async with async_session() as db:
        for f in fields:
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="MMS060MI", source="record",
                                          field=f, captured=True))
        await db.commit()


RECORD_METRIC = dict(
    name="units-by-item", dimensions=["attr:rec.ITNO"],
    measures=[{"name": "units", "aggregation": "sum", "field": "attr:rec.STQT",
               "only": [], "statuses": []}],
    filter={"methods": [], "transactions": ["Pick"]},
    grains=["hourly", "daily", "monthly"], status="active")


async def _add_metric(**overrides):
    spec = {**RECORD_METRIC, **overrides}
    async with async_session() as db:
        row = AnalyticsMetric(customer_code=CC, source="record", **spec)
        db.add(row)
        await db.commit()
        return row.id


async def _ticket(at=T0):
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=at - WIDE,
                                      range_end=at + WIDE))
        await db.commit()


async def _rollups(model, definition_id):
    async with async_session() as db:
        return (await db.execute(select(model).where(
            model.customer_code == CC, model.definition_id == definition_id))).scalars().all()


# ============================================ 1. the fold

async def test_a_record_metric_folds_into_all_three_grains():
    """The flagship: sum STQT by item number, folded from record rows into the same
    definition-keyed rollup tables the transaction grain uses."""
    await _approve("rec.STQT", "rec.ITNO")
    def_id = await _add_metric()
    await _plant()
    await n3.consume_tenant(CC)

    hourly = await _rollups(AnalyticsHourlyRollup, def_id)
    daily = await _rollups(AnalyticsDailyRollup, def_id)
    monthly = await _rollups(AnalyticsMonthlyRollup, def_id)
    assert len(hourly) == 1 and len(daily) == 1 and len(monthly) == 1
    assert hourly[0].dim1 == "101978"
    assert hourly[0].sum_value == Decimal("636")
    assert hourly[0].count_value == 2


async def test_the_expand_backfill_reaches_the_rollups():
    """The union guard: a settled window's fact diff is all-unchanged (no transaction dirty
    buckets), so only the record driver's buckets exist - the rollup step must still run."""
    await _approve("rec.STQT", "rec.ITNO")
    def_id = await _add_metric()
    await _plant(expand=False)
    await n3.consume_tenant(CC)
    assert await _rollups(AnalyticsHourlyRollup, def_id) == []

    async with async_session() as db:
        await db.execute(update(AnalyticsTransactionRegistry)
                         .where(AnalyticsTransactionRegistry.customer_code == CC)
                         .values(expand=True))
        await db.commit()
    await _ticket()
    await n3.consume_tenant(CC)

    hourly = await _rollups(AnalyticsHourlyRollup, def_id)
    assert len(hourly) == 1 and hourly[0].sum_value == Decimal("636"), \
        "the backfill wrote record rows but never rolled them up"


async def test_a_transaction_metric_never_sees_record_rows_and_vice_versa():
    """The 18n structural separation, now pinned in BOTH directions."""
    import inspect
    src_txn = inspect.getsource(n5._read_dirty_facts)
    assert "AnalyticsRecordFact" not in src_txn and "AnalyticsFact" in src_txn
    src_rec = inspect.getsource(n5._read_dirty_record_facts)
    assert "AnalyticsRecordFact" in src_rec
    assert "AnalyticsFact." not in src_rec  # attribute access on the transaction model


async def test_show_off_hides_the_record_grain_too():
    """A hidden transaction must vanish from BOTH grains - records still charting while their
    transaction is hidden would be a silent inconsistency."""
    await _approve("rec.STQT", "rec.ITNO")
    def_id = await _add_metric()
    await _plant()
    async with async_session() as db:
        await db.execute(update(AnalyticsTransactionRegistry)
                         .where(AnalyticsTransactionRegistry.customer_code == CC)
                         .values(show=False))
        await db.commit()
    await n3.consume_tenant(CC)
    assert await _rollups(AnalyticsHourlyRollup, def_id) == []


# ============================================ 2. the definition

def test_validate_refuses_status_and_classification_filters_on_record_metrics():
    m = d.Measure(name="n", aggregation=d.Aggregation.count, field=None,
                  statuses=frozenset({"success"}))
    defn = d.MetricDefinition(name="x", dimensions=("mi_program",), measures=(m,),
                              grains=("daily",), source="record")
    problems = d.validate(defn, known_attributes={"rec.STQT"})
    assert any("status" in p for p in problems)


def test_validate_refuses_rec_attrs_on_transaction_metrics():
    """The live hole: a transaction metric naming attr:rec.* validated fine and charted empty."""
    m = d.Measure(name="units", aggregation=d.Aggregation.sum, field="attr:rec.STQT")
    defn = d.MetricDefinition(name="x", dimensions=("method",), measures=(m,), grains=("daily",))
    problems = d.validate(defn, known_attributes={"rec.STQT"})
    assert any("record" in p for p in problems)


def test_validate_refuses_non_rec_attrs_and_fact_fields_on_record_metrics():
    m = d.Measure(name="units", aggregation=d.Aggregation.sum, field="attr:resp.Qty")
    defn = d.MetricDefinition(name="x", dimensions=("quantity",), measures=(m,),
                              grains=("daily",), source="record")
    problems = d.validate(defn, known_attributes={"resp.Qty", "rec.STQT"})
    assert len(problems) >= 2  # the resp. measure AND the fact-only dimension


def test_validate_accepts_the_flagship_record_metric():
    m = d.Measure(name="units", aggregation=d.Aggregation.sum, field="attr:rec.STQT")
    defn = d.MetricDefinition(name="units-by-item", dimensions=("attr:rec.ITNO", "mi_program"),
                              measures=(m,), grains=("hourly", "daily"), source="record",
                              transaction_filter=("Pick",))
    assert d.validate(defn, known_attributes={"rec.STQT", "rec.ITNO"}) == []


def test_the_source_round_trips_through_the_registry_row():
    m = d.Measure(name="units", aggregation=d.Aggregation.sum, field="attr:rec.STQT")
    defn = d.MetricDefinition(name="rt", dimensions=("attr:rec.ITNO",), measures=(m,),
                              grains=("daily",), source="record")
    row = registry.to_row(defn, customer_code=CC)
    assert row["source"] == "record"

    class _Row:
        pass
    fake = _Row()
    for k, v in row.items():
        setattr(fake, k, v)
    assert registry.from_row(fake).source == "record"


# ============================================ 3. the reconciler

async def test_a_clean_window_reconciles_green_with_both_metric_kinds_active():
    """The partition pin: un-partitioned, the transaction check folds record definitions against
    transaction facts and flags every record rollup orphaned - permanently red."""
    await _approve("rec.STQT", "rec.ITNO")
    await _add_metric()
    await _plant()
    await n3.consume_tenant(CC)

    async with async_session() as db:
        report = await rc.reconcile_tenant(
            db, CC, window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE))
    assert report["healthy"], report["by_check"]


async def test_a_drifted_record_rollup_is_found():
    await _approve("rec.STQT", "rec.ITNO")
    def_id = await _add_metric()
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(update(AnalyticsHourlyRollup)
                         .where(AnalyticsHourlyRollup.definition_id == def_id)
                         .values(sum_value=Decimal("999")))
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(
            db, CC, window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE))
    assert report["by_check"].get("record_rollups_vs_record_facts") == 1


async def test_missing_record_rows_for_an_expanded_fact_are_found():
    await _approve("rec.STQT", "rec.ITNO")
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(delete(AnalyticsRecordFact).where(
            AnalyticsRecordFact.customer_code == CC))
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(
            db, CC, window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE))
    assert report["by_check"].get("records_vs_facts", 0) >= 1


async def test_a_record_metric_is_creatable_through_the_api():
    from app.api.v1 import analytics as api
    await _approve("rec.STQT", "rec.ITNO")
    async with async_session() as db:
        res = await api.create_metric(payload={**RECORD_METRIC, "source": "record",
                                               "name": "api-made"}, customer=CC, db=db)
    assert res.get("source") == "record"

    async with async_session() as db:
        listed = await api.list_metrics(customer=CC, db=db, limit=50)
    made = [m for m in listed["metrics"] if m["name"] == "api-made"]
    assert made and made[0]["source"] == "record"


async def test_a_cross_grain_metric_is_refused_by_the_api():
    from fastapi import HTTPException
    from app.api.v1 import analytics as api
    await _approve("rec.STQT")
    bad = {**RECORD_METRIC, "name": "bad", "source": "transaction"}  # rec.* on transaction grain
    async with async_session() as db:
        with pytest.raises(HTTPException) as e:
            await api.create_metric(payload=bad, customer=CC, db=db)
    assert e.value.status_code == 400
