"""Chunk 96: a deliberate one-off cleanup of response-field registry rows that no fact carries.

Observation never deletes (R1b): a field seen once is recorded by name forever, so it can be reviewed.
Chunk 95 found that 47 of the 48 response fields under ConfirmPickLine were foreign - other requests'
answers stitched onto picks by Stage 2 - and once the regroup restates the facts those rows describe
nothing. They stay greyed as "not seen" under the doctrine. The user asked for them gone.

This is NOT discovery deleting. It is a maintenance action a person invokes, dry-run first, and it
keeps its hands off three kinds of row:
    a human decision      `captured` differs from the seeded default, so somebody ticked or un-ticked it
    a referenced field    any metric of the tenant names `attr:<field>`, whatever its status
    a field still carried by some fact of THAT METHOD within the window (60 days, the entry retention:
                          older facts cannot be restated and do not describe current composition)
Record and MI rows are out of scope: only `source = 'response'` rows are considered.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.customer import Customer
from app.services.analytics import capture

CC = "test_chunk96"
NOW = datetime.now(timezone.utc)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsFact, AnalyticsFieldRegistry, AnalyticsMetric):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="prune probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


def _fact(method, attrs, *, days_ago=1):
    t = NOW - timedelta(days=days_ago)
    return AnalyticsFact(id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(),
                         source_started_at=t, source_version_hash="x" * 8, revision=1, event_time=t,
                         business_date=t.date(), transaction_name="Pick", method=method, status="success",
                         quantity_classification="non_quantity", attributes=attrs, created_at=t)


def _row(method, field, *, captured, source="response", reviewed=False):
    """Chunk 99: `reviewed` is how a human decision is now recorded.

    It used to be inferable - under the old default almost nothing arrived ticked, so `captured=True`
    could only mean a person. Both halves of the exchange now default to ticked, so a tick is
    indistinguishable from the default and the stamp the field endpoint already writes is what says a
    person was here."""
    return AnalyticsFieldRegistry(customer_code=CC, method=method, source=source, field=field,
                                  captured=captured,
                                  reviewed_at=NOW if reviewed else None,
                                  reviewed_by="a person" if reviewed else None)


async def _plant():
    async with async_session() as db:
        db.add_all([
            _fact("ConfirmPickLine", {"resp.value": "3.0"}),
            _fact("GetOldestItemBalanceAPI", {"resp.ItemNumber": "100953", "resp.Location": "A03C"}),
            _fact("ConfirmPickLine", {"resp.Old": "x"}, days_ago=70),          # beyond the window
            # the registry, as Stage 2's faults left it
            _row("ConfirmPickLine", "resp.value", captured=True),               # carried: keep
            _row("ConfirmPickLine", "resp.ItemNumber", captured=True),          # foreign: prune
            _row("ConfirmPickLine", "resp.Location", captured=True),            # foreign: prune
            _row("ConfirmPickLine", "resp.Ghost", captured=False),              # unseeded default: prune
            _row("ConfirmPickLine", "resp.Decided", captured=True, reviewed=True),  # a person ticked it: keep
            _row("ConfirmPickLine", "resp.Metric", captured=True),              # a metric names it: keep
            _row("ConfirmPickLine", "resp.Old", captured=False),                # only on an old fact: prune
            _row("ConfirmPickLine", "rec.BANO", captured=False, source="record"),  # out of scope
            _row("GetOldestItemBalanceAPI", "resp.ItemNumber", captured=True),  # carried there: keep
        ])
        db.add(AnalyticsMetric(customer_code=CC, name="by metric field", dimensions=["attr:resp.Metric"],
                               measures=[{"name": "n", "aggregation": "count", "field": None,
                                          "classifications": ["non_quantity"]}],
                               filter={}, grains=["hourly"], status="draft", source="transaction"))
        await db.commit()


async def _fields(method="ConfirmPickLine"):
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.source)
                                 .where(AnalyticsFieldRegistry.customer_code == CC,
                                        AnalyticsFieldRegistry.method == method))).all()
    return sorted(f for f, _ in rows)


async def test_a_dry_run_reports_and_deletes_nothing():
    await _plant()
    async with async_session() as db:
        out = await capture.prune_unseen_fields(db, CC, days=60, dry_run=True)
        await db.commit()
    assert sorted(r["field"] for r in out["candidates"]) == ["resp.Ghost", "resp.ItemNumber", "resp.Location", "resp.Old"]
    assert out["deleted"] == 0 and out["dry_run"] is True
    assert len(await _fields()) == 8


async def test_the_real_run_deletes_only_the_rows_no_fact_carries_and_nobody_decided():
    await _plant()
    async with async_session() as db:
        out = await capture.prune_unseen_fields(db, CC, days=60, dry_run=False)
        await db.commit()
    assert out["deleted"] == 4
    assert await _fields() == ["rec.BANO", "resp.Decided", "resp.Metric", "resp.value"]
    assert await _fields("GetOldestItemBalanceAPI") == ["resp.ItemNumber"], \
        "the same field name under the method that really carries it is untouched"


async def test_the_kept_rows_say_why():
    await _plant()
    async with async_session() as db:
        out = await capture.prune_unseen_fields(db, CC, days=60, dry_run=True)
    kept = {r["field"]: r["reason"] for r in out["kept"]}
    assert kept["resp.Decided"] == "human decision"
    assert kept["resp.Metric"] == "named by a metric"
    assert "resp.value" not in kept, "a carried field is not a candidate at all, so it is not listed"


async def test_a_field_someone_unticked_is_a_decision_too():
    """An un-tick is as much a decision as a tick, and must survive the prune just the same.

    Chunk 99: what identifies it changed. It used to be "differs from the seeded default", which was
    only ever a proxy - and one that stopped working when both halves of the exchange began defaulting
    to ticked, because a legacy row left un-ticked by the OLD default is then indistinguishable from a
    deliberate un-tick. The field endpoint stamps `reviewed_at` whenever `captured` moves, so that is
    what a decision looks like now.
    """
    async with async_session() as db:
        db.add(_row("ConfirmPickLine", "resp.value", captured=False, reviewed=True))
        await db.commit()
    async with async_session() as db:
        out = await capture.prune_unseen_fields(db, CC, days=60, dry_run=False)
        await db.commit()
    assert out["deleted"] == 0
    assert await _fields() == ["resp.value"]


async def test_the_endpoint_defaults_to_a_dry_run():
    await _plant()
    async with async_session() as db:
        out = await api.prune_field_registry(dry_run=True, days=60, customer=CC, db=db)
    assert out["dry_run"] is True and out["deleted"] == 0 and len(out["candidates"]) == 4
    assert len(await _fields()) == 8


async def test_the_endpoint_deletes_when_told_to():
    await _plant()
    async with async_session() as db:
        out = await api.prune_field_registry(dry_run=False, days=60, customer=CC, db=db)
    assert out["deleted"] == 4
    assert len(await _fields()) == 4
