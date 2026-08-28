"""Chunk 77: the registry console's two read endpoints.

The switches and PATCH endpoints exist since R2; what the console lacked was the reading side:
counts at a glance, and one place that answers "tell me everything about this registered
transaction". Two endpoints:

- `GET /analytics/registry/summary` - per-registry counts (transactions / fields / metrics), all
  single-table indexed counts over small tenant-scoped tables.
- `GET /analytics/registry/transactions/{name}` - the drill-down: switches + review audit, the
  fields observed for it, how many facts it has produced, and which metric definitions reference it.

Two data-model facts drive the shapes (verified, not assumed):

- Fields are registered per M3 METHOD (`MMS060MI/...`), transactions per NAME, and the two are
  many-to-many. The bridge is `log_transactions`, which carries both columns - so the detail first
  resolves the name to its methods, then lists fields for those methods.
- A metric references a transaction ONLY via `filter -> "transactions"`, and an EMPTY list means
  "applies to every transaction". The detail reports the two groups separately (`referencing` vs
  `apply_to_all`), because "3 metrics mention this name" and "2 metrics cover everything anyway"
  are different answers to different questions.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

CC = "test_chunk77"
T0 = datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsTransactionRegistry, AnalyticsFieldRegistry, AnalyticsMetric,
                      AnalyticsPendingWindow, AnalyticsTenantState, AnalyticsFact,
                      LogTransaction, Job):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _seed():
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="registry console probe",
                        timezone="Europe/London"))
        # two registered transactions: one reviewed with a switch off, one untouched
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Pick",
                                            capture=True, show=False, expand=True,
                                            reviewed_at=T0, reviewed_by="amin"))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Count"))
        # three fields across two methods; one captured
        for method, field, cap in (("MMS060MI", "resp.BaseUoM", True),
                                   ("MMS060MI", "resp.NewThing", False),
                                   ("PMS420MI", "mi.STQT", False)):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method=method, source="response",
                                          field=field, captured=cap))
        # three metrics: one referencing Pick, one covering everything, one for another name
        for name, transactions, status in (("picks-by-hour", ["Pick"], "active"),
                                           ("all-traffic", [], "active"),
                                           ("counts-only", ["Count"], "draft")):
            db.add(AnalyticsMetric(
                customer_code=CC, name=name, dimensions=["method"],
                measures=[{"name": "n", "aggregation": "count", "field": None,
                           "only": [], "statuses": []}],
                filter={"methods": [], "transactions": transactions},
                grains=["daily"], status=status))
        await db.commit()


async def _seed_bridge_and_facts():
    """The name->methods bridge rows and a few facts, for the detail endpoint."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        # "Pick" was served by BOTH methods; "Count" by neither of Pick's
        for method, name in (("MMS060MI", "Pick"), ("PMS420MI", "Pick"), ("CNT100MI", "Count")):
            db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=T0,
                                  ended_at=T0, date=T0.date(), duration_ms=10, method=method,
                                  transaction_name=name, status=LogTransactionStatus.success,
                                  attributes={}))
        for i, name in enumerate(("Pick", "Pick", "Pick", "Count")):
            db.add(AnalyticsFact(id=uuid.uuid4(), customer_code=CC,
                                 source_transaction_id=uuid.uuid4(), source_started_at=T0,
                                 source_version_hash="h" * 8, revision=1,
                                 event_time=T0 + timedelta(minutes=i),
                                 business_date=T0.date(), transaction_name=name,
                                 method="MMS060MI", status="success",
                                 quantity_classification="pick", attributes={}, created_at=T0))
        await db.commit()


async def _summary():
    async with async_session() as db:
        return await api.registry_summary(customer=CC, db=db)


async def _detail(name):
    async with async_session() as db:
        return await api.transaction_registry_detail(name, customer=CC, db=db)


# =============================================================== 1. the summary

async def test_the_summary_counts_every_registry_block():
    await _seed()
    s = await _summary()
    assert s["transactions"] == {"total": 2, "capture_on": 2, "show_on": 1,
                                 "expand_on": 1, "needs_review": 1}
    assert s["fields"] == {"total": 3, "captured": 1, "needs_review": 3}
    assert s["metrics"] == {"total": 3, "active": 2}


async def test_an_empty_tenant_summarises_to_zeros():
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="empty", timezone="UTC"))
        await db.commit()
    s = await _summary()
    assert s["transactions"]["total"] == 0
    assert s["fields"]["total"] == 0
    assert s["metrics"]["total"] == 0


# =============================================================== 2. the drill-down

async def test_the_detail_reports_switches_and_review_audit():
    await _seed()
    d = await _detail("Pick")
    assert d["transaction_name"] == "Pick"
    assert d["capture"] is True and d["show"] is False and d["expand"] is True
    assert d["needs_review"] is False and d["reviewed_by"] == "amin"


async def test_the_detail_bridges_the_name_to_its_methods_fields():
    """Fields are registered per METHOD; the detail resolves the name to the methods that served
    it and lists only THOSE methods' fields - Count's method must not leak in."""
    await _seed()
    await _seed_bridge_and_facts()
    d = await _detail("Pick")
    assert sorted(d["methods"]) == ["MMS060MI", "PMS420MI"]
    assert sorted(f["field"] for f in d["fields"]) == ["mi.STQT", "resp.BaseUoM", "resp.NewThing"]
    assert d["field_count"] == 3
    captured = {f["field"]: f["captured"] for f in d["fields"]}
    assert captured["resp.BaseUoM"] is True and captured["mi.STQT"] is False


async def test_the_detail_counts_facts_and_their_span():
    await _seed()
    await _seed_bridge_and_facts()
    d = await _detail("Pick")
    assert d["facts"]["count"] == 3
    assert d["facts"]["first_event_at"] == T0.isoformat()
    assert d["facts"]["last_event_at"] == (T0 + timedelta(minutes=2)).isoformat()


async def test_the_detail_splits_metrics_into_referencing_and_apply_to_all():
    await _seed()
    d = await _detail("Pick")
    assert [m["name"] for m in d["metrics"]["referencing"]] == ["picks-by-hour"]
    assert d["metrics"]["referencing"][0]["status"] == "active"
    assert d["metrics"]["apply_to_all"] == 1, "the empty-filter metric covers this name too"


async def test_an_unknown_transaction_name_is_a_404():
    await _seed()
    with pytest.raises(HTTPException) as e:
        await _detail("No Such Thing")
    assert e.value.status_code == 404
