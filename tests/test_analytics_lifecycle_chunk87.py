"""Chunk 87, part 3 of the metric builder: the lifecycle endpoint and the bounded backfill.

A metric is saved as a draft, previewed, then activated. Activation is the one moment with a side
effect beyond the row: it fixes `rollups_from` (now, or the earlier instant the person chose) and, if
that instant is before what analytics has already folded, publishes tickets for exactly that range so
the ordinary fold builds the history. No new worker, no job table: the same queue field approval uses.

Pinned here
-----------
    transitions    draft -> active, active -> inactive, inactive -> active; anything else is a 409
                   naming the current state; the same state again is a no-op
    shape edits    dimensions, measures, filter, grains, source, name: only while draft, else 409;
                   description: any time, it is metadata (chunk 84)
    activation     re-validates against the approved fields; an invalid draft cannot go live
    backfill       tickets cover [rollups_from, source watermark] and nothing else; a rollups_from at
                   or after the watermark publishes none
    progress       `backfilled_through` advances in the fold for every active definition whose
                   `rollups_from` is at or before the run's range end
"""
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import registry

CC = "test_chunk87"
T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)      # the tenant's source watermark

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsPendingWindow, AnalyticsTenantState, AnalyticsMetric, AnalyticsFieldRegistry,
          LogTransaction)


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
        db.add(Customer(customer_code=CC, name="lifecycle probe", timezone="UTC"))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0,
                                    history_starts_at=T0 - timedelta(days=30)))
        await db.commit()
    yield
    await _wipe()


def _body(**over):
    body = {"name": "picks", "description": "Units confirmed as picked",
            "dimensions": ["method", "transaction_name"],
            "measures": [{"name": "quantity", "aggregation": "sum", "field": "quantity"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": []},
            "grains": ["hourly", "daily", "monthly"], "source": "transaction"}
    body.update(over)
    return body


async def _create(**over) -> str:
    async with async_session() as db:
        return (await api.create_metric(payload=_body(**over), customer=CC, db=db))["id"]


async def _patch(metric_id, body):
    async with async_session() as db:
        return await api.update_metric(metric_id, payload=body, customer=CC, db=db)


async def _row(metric_id) -> AnalyticsMetric:
    async with async_session() as db:
        return await db.scalar(select(AnalyticsMetric).where(AnalyticsMetric.id == metric_id))


async def _tickets():
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC)
            .order_by(AnalyticsPendingWindow.range_start))).scalars().all())


# =============================================================== 1. transitions

async def test_a_new_metric_is_a_draft_with_no_bound():
    mid = await _create()
    row = await _row(mid)
    assert row.status == "draft" and row.rollups_from is None


async def test_activating_without_a_start_fixes_rollups_from_at_now_and_backfills_nothing():
    mid = await _create()
    before = datetime.now(timezone.utc)
    out = await _patch(mid, {"status": "active"})
    row = await _row(mid)
    assert out["status"] == "active" and row.status == "active"
    assert row.rollups_from is not None and before <= row.rollups_from <= datetime.now(timezone.utc)
    assert out["tickets_published"] == 0 and await _tickets() == []
    assert row.rollups_from > T0, "now is after the watermark, so there is no history to build"


async def test_activating_with_an_earlier_start_publishes_tickets_for_exactly_that_range():
    mid = await _create()
    start = T0 - timedelta(days=2)
    out = await _patch(mid, {"status": "active", "rollups_from": start.isoformat()})
    row = await _row(mid)
    assert row.rollups_from == start
    tickets = await _tickets()
    assert out["tickets_published"] == len(tickets) >= 2, "one ticket per day, padded"
    assert tickets[0].range_start <= start
    assert tickets[-1].range_end >= T0
    assert tickets[0].range_start > start - timedelta(hours=1), "the pad is Stage 2's, minutes not days"
    assert out["backfill"] == {"from": start.isoformat(), "to": T0.isoformat()}


async def test_a_start_at_or_after_the_watermark_publishes_nothing():
    mid = await _create()
    out = await _patch(mid, {"status": "active", "rollups_from": (T0 + timedelta(hours=1)).isoformat()})
    assert out["tickets_published"] == 0 and out["backfill"] is None


async def test_active_to_inactive_and_back_keeps_the_bound_and_publishes_nothing_new():
    mid = await _create()
    start = T0 - timedelta(days=1)
    await _patch(mid, {"status": "active", "rollups_from": start.isoformat()})
    n = len(await _tickets())
    await _patch(mid, {"status": "inactive"})
    assert (await _row(mid)).status == "inactive"
    out = await _patch(mid, {"status": "active"})
    row = await _row(mid)
    assert row.status == "active" and row.rollups_from == start
    assert out["tickets_published"] == 0 and len(await _tickets()) == n


async def test_the_same_state_again_is_a_no_op():
    mid = await _create()
    out = await _patch(mid, {"status": "draft"})
    assert out["status"] == "draft" and out["changed"] == []


async def test_illegal_transitions_are_409s_naming_the_current_state():
    mid = await _create()
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"status": "inactive"})
    assert exc.value.status_code == 409 and "draft" in str(exc.value.detail)
    await _patch(mid, {"status": "active"})
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"status": "draft"})
    assert exc.value.status_code == 409 and "active" in str(exc.value.detail)


async def test_an_unknown_status_is_a_400_and_an_unknown_id_a_404():
    mid = await _create()
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"status": "live"})
    assert exc.value.status_code == 400
    with pytest.raises(HTTPException) as exc:
        await _patch(str(uuid.uuid4()), {"status": "active"})
    assert exc.value.status_code == 404


# =============================================================== 2. edits

async def test_shape_edits_are_allowed_while_draft_and_revalidated():
    mid = await _create()
    out = await _patch(mid, {"dimensions": ["method", "warehouse"]})
    assert out["dimensions"] == ["method", "warehouse"] and "dimensions" in out["changed"]
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"dimensions": ["no_such_column"]})
    assert exc.value.status_code == 400 and any("no_such_column" in p for p in exc.value.detail)
    assert (await _row(mid)).dimensions == ["method", "warehouse"], "a refused edit changes nothing"


async def test_shape_edits_on_an_active_metric_are_refused():
    mid = await _create()
    await _patch(mid, {"status": "active"})
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"dimensions": ["method"]})
    assert exc.value.status_code == 409
    assert "deactivate" in str(exc.value.detail).lower()


async def test_description_can_change_at_any_time():
    mid = await _create()
    await _patch(mid, {"status": "active"})
    out = await _patch(mid, {"description": "Units picked, by method and screen"})
    assert out["description"] == "Units picked, by method and screen"
    assert (await _row(mid)).status == "active"


async def test_an_invalid_draft_cannot_be_activated():
    # A draft cannot be SAVED invalid (create validates), so the real path to an invalid draft is an
    # approval withdrawn after the save: the field registry row is switched off underneath it.
    async with async_session() as db:
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="record",
                                      field="rec.STQT", captured=True))
        await db.commit()
    mid = await _create(source="record", dimensions=["method"],
                        measures=[{"name": "n", "aggregation": "sum", "field": "attr:rec.STQT"}])
    async with async_session() as db:
        await db.execute(delete(AnalyticsFieldRegistry).where(AnalyticsFieldRegistry.customer_code == CC))
        await db.commit()
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"status": "active"})
    assert exc.value.status_code == 400 and "rec.STQT" in str(exc.value.detail)
    assert (await _row(mid)).status == "draft"


async def test_reactivation_refuses_a_new_start_instead_of_ignoring_it():
    mid = await _create()
    await _patch(mid, {"status": "active"})
    await _patch(mid, {"status": "inactive"})
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {"status": "active", "rollups_from": (T0 - timedelta(days=3)).isoformat()})
    assert exc.value.status_code == 409 and "rollups_from" in str(exc.value.detail)
    assert (await _row(mid)).status == "inactive"


async def test_an_empty_patch_is_a_400():
    mid = await _create()
    with pytest.raises(HTTPException) as exc:
        await _patch(mid, {})
    assert exc.value.status_code == 400


# =============================================================== 3. progress

async def test_backfilled_through_advances_only_for_definitions_the_run_covers():
    unbounded = await _create(name="a")
    later = await _create(name="b")
    await _patch(unbounded, {"status": "active", "rollups_from": (T0 - timedelta(days=5)).isoformat()})
    await _patch(later, {"status": "active", "rollups_from": (T0 + timedelta(days=1)).isoformat()})
    async with async_session() as db:
        n = await registry.advance_backfilled_through(db, CC, range_end=T0)
        await db.commit()
    assert n == 1
    assert (await _row(unbounded)).backfilled_through == T0.date() - timedelta(days=1), \
        "a run ending mid-day has fully covered only the day before"
    assert (await _row(later)).backfilled_through is None


async def test_backfilled_through_never_moves_backwards():
    mid = await _create()
    await _patch(mid, {"status": "active", "rollups_from": (T0 - timedelta(days=9)).isoformat()})
    async with async_session() as db:
        await registry.advance_backfilled_through(db, CC, range_end=T0)
        await registry.advance_backfilled_through(db, CC, range_end=T0 - timedelta(days=5))
        await db.commit()
    assert (await _row(mid)).backfilled_through == T0.date() - timedelta(days=1)


async def test_the_fold_advances_backfilled_through_end_to_end():
    """One real run: a transaction inside the bound, one ticket, `consume_tenant`. Afterwards the
    active metric's `backfilled_through` is the day before the run's range end."""
    mid = await _create()
    start = T0 - timedelta(days=2)
    await _patch(mid, {"status": "active", "rollups_from": start.isoformat()})
    async with async_session() as db:
        # the activation already published tickets; add the transaction they will fold
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        at = T0 - timedelta(days=1)
        db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                              date=at.date(), duration_ms=100, method="ConfirmPickLine",
                              transaction_name="Pick", transaction_type="002001",
                              status=LogTransactionStatus.success, item_number="1", user_name="EDA",
                              warehouse="BRI", attributes={"QuantityPicked": "3.0"}))
        await db.commit()
    stats = await n3.consume_tenant(CC)
    assert stats["failed"] == 0 and stats["consumed"] >= 1
    row = await _row(mid)
    assert row.backfilled_through is not None
    assert row.backfilled_through >= (T0 - timedelta(days=2)).date()


async def test_the_metric_list_and_catalog_report_the_bound_and_the_progress():
    mid = await _create()
    start = T0 - timedelta(days=2)
    await _patch(mid, {"status": "active", "rollups_from": start.isoformat()})
    async with async_session() as db:
        await registry.advance_backfilled_through(db, CC, range_end=T0)
        await db.commit()
        listed = (await api.list_metrics(customer=CC, db=db, limit=50))["metrics"][0]
    assert listed["rollups_from"] == start.isoformat()
    assert listed["backfilled_through"] == str(T0.date() - timedelta(days=1))
