"""Chunk 99: the fields the handheld SENT become reviewable, and both halves default to ticked.

Every transaction is one exchange. The handheld sends a request and the server answers. The fact has
always stored BOTH halves in `attributes` - request keys bare, response keys under `resp.`. But only
the response half was ever written into `analytics_field_registry`, and `definition.validate` refuses
any `attr:` path absent from that registry. So the request half was stored and unusable.

Measured on tmp-live before the change: a stock move carries `Quantity` on all 185 records with 58
distinct values, and a stock count carries `BalanceQuantity` on 713 records with 189 distinct values.
Neither could be measured. Picking and counting worked only because three method names are hardcoded
in `contract.QUANTITY_FIELD`. Proved by running the real definition twice with the same data: refused
with three "not approved" errors, then, with those three names pretended into the registry, 100% field
coverage and correct grouped answers. The gate was the only thing in the way.

**The two halves keep different MEANINGS for the same tick, and that is deliberate.** For a response
field the tick decides whether the value is STORED at all - `payload.select` consults it. For a request
field the value is already on every fact whatever anybody ticks, so the tick only decides whether the
field may be REPORTED on. Defaulting the request half to ticked therefore changes no storage
behaviour; defaulting the response half to ticked does, and that was asked for explicitly.

**What does NOT change.** The credential veto is still checked first and independently, so a
credential-shaped name arrives un-ticked under the new default exactly as under the old one - verified
against every request field on every method on tmp-live: none exist today. Record fields (`rec.`) are
still never seeded, because the record grain is opt-in per transaction and expands to roughly 200k rows
a day. And an un-tick still outlives re-observation.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import capture
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import payload as p
from app.services.analytics.contract import QUANTITY_FIELD as QF

CC = "test_chunk99"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, AnalyticsFieldRegistry, LogTransaction)


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
        db.add(Customer(customer_code=CC, name="request field probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


# ============================================================ 1. the seeding policy, per namespace

@pytest.mark.parametrize("name", ["Quantity", "BalanceQuantity", "FromLocation", "PickListSuffix",
                                  "CustomerName", "Route", "Picker"])
def test_a_field_the_handheld_sent_arrives_ticked(name):
    """These are the names measured as unreachable on tmp-live. Bare, because a request key carries
    no namespace prefix."""
    assert p.seeded(name) is True


@pytest.mark.parametrize("name", ["resp.ErrorCode", "resp.BasicUnitOfMeasure", "resp.PickingSequence",
                                  "resp.SomethingNobodyHasSeenYet"])
def test_a_field_the_server_returned_arrives_ticked_even_off_the_seed_list(name):
    """The decision: both halves default on. `SEED_FIELDS` is now a record of what was chosen before
    the default changed, not the thing that decides."""
    assert p.seeded(name) is True


@pytest.mark.parametrize("name", ["AccessToken", "ApiSecret", "SessionToken", "M3UserCredentials",
                                  "Password", "resp.AccessToken", "resp.M3UserCredentials",
                                  "resp.Cipher", "mi.X.Y.BearerToken"])
def test_a_credential_shaped_name_still_arrives_unticked_on_either_half(name):
    """The veto is checked first and independently, so the new default cannot reach past it. Bare
    spellings included, because the request half is exactly where a new one would appear."""
    assert p.never_auto_approve(name) is True
    assert p.seeded(name) is False


@pytest.mark.parametrize("name", ["rec.ITNO", "rec.STQT", "rec.WHLO"])
def test_a_record_field_is_still_never_seeded(name):
    """Unchanged, and deliberately outside this decision. The record grain is opt-in per transaction
    and expands to roughly 200k rows a day, so it stays a decision somebody makes."""
    assert p.seeded(name) is False


def test_the_bookkeeping_keys_are_never_offered_for_ticking():
    """`__src_fp`, `__norm_v` and `__mi_records` are how the fold proves a fact is current. They are
    not warehouse data and must never appear as something to report on."""
    for name in ("__src_fp", "__norm_v", "__mi_records"):
        assert p.is_bookkeeping(name) is True
        assert p.seeded(name) is False
    assert p.is_bookkeeping("Quantity") is False
    assert p.is_bookkeeping("resp.value") is False


def test_an_untick_still_outlives_re_observation():
    """The oldest rule in the registry, and the new default must not reach past it either. `select`
    consults the registry, never `seeded`."""
    captured, unknown = p.select({"resp.BaseUoM": "KG"}, frozenset())
    assert captured == {}
    assert unknown == ["resp.BaseUoM"]


# ============================================================ 2. discovery registers the request half

def _txn(job_id, *, method, attributes):
    return LogTransaction(
        customer_code=CC, job_id=job_id, sealed=True, started_at=T0, ended_at=T0, date=T0.date(),
        duration_ms=100, method=method, transaction_name="Pick", transaction_type="002001",
        status=LogTransactionStatus.success, item_number="A", user_name="EDA", warehouse="BRI",
        attributes=attributes)


async def _run_one_cycle(attributes, *, method="ConfirmPickLine"):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(_txn(job.id, method=method, attributes=attributes))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        return {r.field: r for r in (await db.execute(select(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == CC))).scalars().all()}


async def test_a_field_the_handheld_sent_is_registered_under_its_own_source():
    rows = await _run_one_cycle({QF["ConfirmPickLine"]: "10.0", "FromLocation": "A01A",
                                 "PickListSuffix": "1"})
    assert "FromLocation" in rows, "a request key must reach the registry at all"
    assert rows["FromLocation"].source == "request", \
        "so the catalog can show the two halves apart, and so a bare name cannot be filed as a response"
    assert rows["FromLocation"].captured is True
    assert rows["PickListSuffix"].source == "request"


async def test_the_bookkeeping_keys_never_reach_the_registry():
    """They are written into `attributes` by the fold itself, AFTER normalise, so a naive
    registration of every attribute key would offer the fold's own plumbing as a dimension."""
    rows = await _run_one_cycle({QF["ConfirmPickLine"]: "10.0", "FromLocation": "A01A"})
    assert not [f for f in rows if f.startswith("__")], sorted(rows)


async def test_a_registered_request_field_is_then_usable_as_a_dimension():
    """The whole point. Before this chunk `validate` refused every bare `attr:` path, because the
    registry could not contain one."""
    await _run_one_cycle({QF["ConfirmPickLine"]: "10.0", "FromLocation": "A01A"})
    async with async_session() as db:
        approved = await capture.approved_attributes(db, CC)
    assert "FromLocation" in approved
    definition = d.MetricDefinition(
        name="picks-by-source-location", dimensions=("attr:FromLocation",),
        measures=(d.Measure("units", d.Aggregation.sum, field="quantity"),),
        grains=("hourly", "daily"), method_filter=("ConfirmPickLine",))
    assert d.validate(definition, known_attributes=approved) == []


async def test_a_credential_shaped_request_field_is_registered_but_not_ticked():
    """Registered so somebody can see it exists and decide; un-ticked so nothing measures it by
    default. The value is still on the fact, because request attributes always were - which is why
    the registry row is the honest place to show it."""
    rows = await _run_one_cycle({QF["ConfirmPickLine"]: "10.0", "SessionToken": "hunter2"})
    assert "SessionToken" in rows
    assert rows["SessionToken"].captured is False


async def test_ticking_a_request_field_needs_no_refold_because_the_value_is_already_there():
    """The difference between the two halves, asserted rather than described. A response value
    appears on the fact only once approved; a request value is on the fact from the first fold, so
    approving it later changes what may be REPORTED and never what is stored."""
    await _run_one_cycle({QF["ConfirmPickLine"]: "10.0", "FromLocation": "A01A"})
    async with async_session() as db:
        # un-tick it, exactly as a person would
        await db.execute(AnalyticsFieldRegistry.__table__.update().where(
            AnalyticsFieldRegistry.customer_code == CC,
            AnalyticsFieldRegistry.field == "FromLocation").values(captured=False))
        await db.commit()
        attributes = (await db.execute(select(AnalyticsFact.attributes).where(
            AnalyticsFact.customer_code == CC))).scalars().all()
    assert attributes, "the fold produced a fact"
    assert all(a.get("FromLocation") == "A01A" for a in attributes), \
        "un-ticking a request field does not remove a value that was never gated on the tick"
