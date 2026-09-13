"""Chunk 84, part 0 of the metric builder: the semantic catalog.

Why a catalog at all
--------------------
Metrics are rows, so the code does not know what exists - and neither does a chat agent or the wizard
that is about to be built. Both need one read that says, for this tenant: which metrics are active,
what each one MEANS, which dimensions it has and which values those take, what unit each measure is
in, and which fields and transaction names exist with their descriptions.

Meaning lives in the data, not in a prompt or a component: `analytics_metrics.description` (already a
column, never filled), `analytics_field_registry.description` and `.unit` (new), and
`analytics_transaction_registry.description` (new).

Three rules pinned here
-----------------------
    drafts and inactive metrics are absent   an agent must never chart a definition nobody activated
    dimension domains are recent and capped  read from the hourly rollups over the last N days, at
                                             most `domain_cap` values, and the truncation is SAID
    describing is not reviewing              editing a description publishes no ticket and does not
                                             touch `reviewed_at`; only the switches do that

Handlers are called directly, the convention chunk 52 documents (TestClient's own event loop breaks
asyncpg under pytest-asyncio).
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import Response
from sqlalchemy import delete, func, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_rollup import AnalyticsHourlyRollup
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.services.analytics import catalog

CC = "test_chunk84"
T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


# =============================================================== fixtures

async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsHourlyRollup, AnalyticsMetric, AnalyticsTransactionRegistry,
                      AnalyticsFieldRegistry, AnalyticsPendingWindow, AnalyticsTenantState):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


def _metric(name, status, *, description=None, dimensions=("method", "transaction_name"),
            measures=None, source="transaction", filt=None):
    return AnalyticsMetric(
        customer_code=CC, name=name, description=description, status=status, source=source,
        dimensions=list(dimensions),
        measures=measures or [{"name": "quantity", "aggregation": "sum", "field": "quantity",
                               "only": ["pick", "attempt"], "statuses": ["success"]}],
        filter=filt or {"methods": ["ConfirmPickLine"], "transactions": []},
        grains=["hourly", "daily", "monthly"], created_by="test")


async def _seed() -> uuid.UUID:
    """One active metric with rollups, one draft, one inactive; described fields and names."""
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="catalog probe", timezone="Europe/London"))
        active = _metric("units_picked", "active", description="Units confirmed as picked")
        db.add(active)
        db.add(_metric("draft_thing", "draft", description="not ready"))
        db.add(_metric("retired_thing", "inactive", description="was useful once"))
        db.add(_metric("stock_seen", "active", source="record",
                       description="Stock units M3 reported during picks",
                       dimensions=("attr:rec.ITNO",),
                       measures=[{"name": "units", "aggregation": "sum",
                                  "field": "attr:rec.STQT", "only": [], "statuses": []}],
                       filt={"methods": [], "transactions": ["Brighton Stock Pick"]}))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Brighton Stock Pick",
                                            description="Picking from the Brighton pick face"))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Quick Stock Count"))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="record",
                                      field="rec.STQT", captured=True,
                                      description="On-hand quantity of one lot", unit="units"))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="record",
                                      field="rec.ITNO", captured=True, description="Item number"))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="response",
                                      field="resp.AccessToken", captured=False))
        db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0,
                                    history_starts_at=T0 - timedelta(days=3)))
        await db.flush()
        did = active.id
        # Domain rows: five methods inside the 7-day window, one older, and two transaction names.
        for i, method in enumerate(("ConfirmPickLine", "ReportCount", "StockMove",
                                    "LogSignOff", "GetAllReasonCodes")):
            db.add(AnalyticsHourlyRollup(
                customer_code=CC, definition_id=did, measure_name="quantity",
                bucket_start=T0 - timedelta(hours=i + 1), dim1=method, dim2="Brighton Stock Pick",
                sum_value=10, count_value=1, computed_at=T0))
        db.add(AnalyticsHourlyRollup(
            customer_code=CC, definition_id=did, measure_name="quantity",
            bucket_start=T0 - timedelta(hours=2), dim1="ConfirmPickLine", dim2="Quick Stock Count",
            sum_value=10, count_value=1, computed_at=T0))
        db.add(AnalyticsHourlyRollup(
            customer_code=CC, definition_id=did, measure_name="quantity",
            bucket_start=T0 - timedelta(days=9), dim1="AncientMethod", dim2="Old Name",
            sum_value=10, count_value=1, computed_at=T0))
        await db.commit()
        return did


async def _catalog(**kwargs):
    response = Response()
    async with async_session() as db:
        body = await api.analytics_catalog(response=response, if_none_match=kwargs.pop("if_none_match", None),
                                           domain_days=kwargs.pop("domain_days", 7),
                                           domain_cap=kwargs.pop("domain_cap", 200),
                                           customer=CC, db=db)
    return body, response


async def _tickets() -> int:
    async with async_session() as db:
        return await db.scalar(select(func.count()).select_from(AnalyticsPendingWindow)
                               .where(AnalyticsPendingWindow.customer_code == CC)) or 0


# =============================================================== 1. what is listed

async def test_only_active_metrics_appear_and_each_carries_its_meaning():
    await _seed()
    body, _ = await _catalog()
    names = [m["name"] for m in body["metrics"]]
    assert names == ["stock_seen", "units_picked"], "sorted by name, drafts and inactive absent"
    picked = next(m for m in body["metrics"] if m["name"] == "units_picked")
    assert picked["description"] == "Units confirmed as picked"
    assert picked["source"] == "transaction"
    assert [dm["name"] for dm in picked["dimensions"]] == ["method", "transaction_name"]
    assert picked["grains"] == ["hourly", "daily", "monthly"]
    assert picked["filter"] == {"methods": ["ConfirmPickLine"], "transactions": []}
    assert picked["rollups_from"] is None, "part 2 adds the column; None means unbounded"
    (measure,) = picked["measures"]
    assert measure == {"name": "quantity", "aggregation": "sum", "field": "quantity",
                       "unit": None, "approximate": False}


async def test_a_measure_on_an_approved_attribute_takes_the_field_registry_unit():
    await _seed()
    body, _ = await _catalog()
    seen = next(m for m in body["metrics"] if m["name"] == "stock_seen")
    (measure,) = seen["measures"]
    assert measure["field"] == "attr:rec.STQT"
    assert measure["unit"] == "units", "the unit lives on the field registry row, once"


async def test_dimension_domains_are_recent_distinct_rollup_values():
    await _seed()
    body, _ = await _catalog()
    picked = next(m for m in body["metrics"] if m["name"] == "units_picked")
    by_name = {dm["name"]: dm for dm in picked["dimensions"]}
    assert by_name["method"]["values"] == sorted(
        ["ConfirmPickLine", "ReportCount", "StockMove", "LogSignOff", "GetAllReasonCodes"])
    assert "AncientMethod" not in by_name["method"]["values"], "older than domain_days"
    assert by_name["transaction_name"]["values"] == ["Brighton Stock Pick", "Quick Stock Count"]
    assert by_name["method"]["truncated"] is False


async def test_dimension_domains_are_capped_and_say_so():
    await _seed()
    body, _ = await _catalog(domain_cap=3)
    picked = next(m for m in body["metrics"] if m["name"] == "units_picked")
    method = next(dm for dm in picked["dimensions"] if dm["name"] == "method")
    assert len(method["values"]) == 3
    assert method["truncated"] is True, "a capped list must never look complete"


async def test_fields_and_transaction_names_carry_descriptions():
    await _seed()
    body, _ = await _catalog()
    fields = {f["field"]: f for f in body["fields"]}
    assert set(fields) == {"rec.STQT", "rec.ITNO"}, "captured fields only; names nobody approved stay out"
    assert fields["rec.STQT"] == {"field": "rec.STQT", "source": "record",
                                 "description": "On-hand quantity of one lot", "unit": "units",
                                 "methods": ["ConfirmPickLine"]}
    assert fields["rec.ITNO"]["unit"] is None
    txns = {t["transaction_name"]: t for t in body["transactions"]}
    assert txns["Brighton Stock Pick"]["description"] == "Picking from the Brighton pick face"
    assert txns["Quick Stock Count"]["description"] is None
    assert txns["Brighton Stock Pick"]["capture"] is True
    assert set(txns["Brighton Stock Pick"]) == {"transaction_name", "description",
                                                "capture", "show", "expand"}


async def test_an_empty_tenant_is_an_empty_catalog_not_an_error():
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="empty", timezone="Europe/London"))
        await db.commit()
    body, response = await _catalog()
    # `aggregations` is the one non-empty list: it describes the builder, not the tenant (chunk 88).
    assert {k: v for k, v in body.items() if k != "aggregations"} == {
        "customer_code": CC, "metrics": [], "fields": [], "transactions": []}
    assert {a["name"] for a in body["aggregations"]} >= {"sum", "count", "distinct"}
    assert response.headers["ETag"]


# =============================================================== 2. describing is not reviewing

async def test_a_transaction_description_edit_publishes_no_ticket_and_is_not_a_review():
    await _seed()
    async with async_session() as db:
        out = await api.set_transaction_switches("Quick Stock Count",
                                                 payload={"description": "Ad-hoc counts by a picker"},
                                                 customer=CC, db=db)
    assert out["description"] == "Ad-hoc counts by a picker"
    assert out["tickets_published"] == 0
    assert await _tickets() == 0
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsTransactionRegistry).where(
            AnalyticsTransactionRegistry.customer_code == CC,
            AnalyticsTransactionRegistry.transaction_name == "Quick Stock Count"))
    assert row.reviewed_at is None, "a description is metadata; only a switch is a review decision"
    body, _ = await _catalog()
    txns = {t["transaction_name"]: t for t in body["transactions"]}
    assert txns["Quick Stock Count"]["description"] == "Ad-hoc counts by a picker"


async def test_a_field_description_and_unit_can_be_set_without_touching_captured():
    await _seed()
    async with async_session() as db:
        fid = await db.scalar(select(AnalyticsFieldRegistry.id).where(
            AnalyticsFieldRegistry.customer_code == CC, AnalyticsFieldRegistry.field == "rec.ITNO"))
        out = await api.set_field_capture(str(fid), payload={"description": "M3 item number",
                                                             "unit": "id"},
                                          customer=CC, db=db)
    assert out["captured"] is True, "untouched"
    assert out["description"] == "M3 item number" and out["unit"] == "id"
    assert out["tickets_published"] == 0, "capture did not move, so nothing to re-fold"
    body, _ = await _catalog()
    fields = {f["field"]: f for f in body["fields"]}
    assert fields["rec.ITNO"]["description"] == "M3 item number"
    assert fields["rec.ITNO"]["unit"] == "id"


async def test_a_field_patch_with_nothing_to_change_is_a_400():
    await _seed()
    from fastapi import HTTPException
    async with async_session() as db:
        fid = await db.scalar(select(AnalyticsFieldRegistry.id).where(
            AnalyticsFieldRegistry.customer_code == CC, AnalyticsFieldRegistry.field == "rec.ITNO"))
        with pytest.raises(HTTPException) as exc:
            await api.set_field_capture(str(fid), payload={}, customer=CC, db=db)
    assert exc.value.status_code == 400


# =============================================================== 3. caching

async def test_etag_is_stable_for_the_same_content_and_moves_when_it_changes():
    await _seed()
    _, r1 = await _catalog()
    _, r2 = await _catalog()
    assert r1.headers["ETag"] == r2.headers["ETag"]
    async with async_session() as db:
        await api.set_transaction_switches("Quick Stock Count", payload={"description": "x"},
                                           customer=CC, db=db)
    _, r3 = await _catalog()
    assert r3.headers["ETag"] != r1.headers["ETag"]


async def test_if_none_match_short_circuits_with_304():
    await _seed()
    _, r1 = await _catalog()
    body, r2 = await _catalog(if_none_match=r1.headers["ETag"])
    assert isinstance(body, Response) and body.status_code == 304


# =============================================================== 4. the metric list learns to describe

async def test_the_metric_list_reports_the_description_too():
    await _seed()
    async with async_session() as db:
        out = await api.list_metrics(customer=CC, db=db, limit=50)
    by_name = {m["name"]: m for m in out["metrics"]}
    assert by_name["units_picked"]["description"] == "Units confirmed as picked"
    assert by_name["draft_thing"]["status"] == "draft", "the list shows everything; the catalog does not"


# =============================================================== 5. the pure shape

def test_shape_is_pure_and_sorted():
    """`catalog.shape` takes rows already in memory and returns the body. No database, so the ordering
    and field selection can be asserted without one."""
    rows = catalog.Rows(
        metrics=[catalog.MetricRow(id=uuid.uuid4(), name="b", description="B", source="transaction",
                                   dimensions=["method"], measures=[{"name": "n", "aggregation": "count"}],
                                   filter={"methods": [], "transactions": []},
                                   grains=["daily"], rollups_from=None, backfilled_through=None),
                 catalog.MetricRow(id=uuid.uuid4(), name="a", description=None, source="transaction",
                                   dimensions=[], measures=[], filter={}, grains=[],
                                   rollups_from=None, backfilled_through=None)],
        domains={},
        fields=[catalog.FieldRow(field="resp.X", source="response", description=None, unit="kg",
                                 methods=["M2", "M1"])],
        transactions=[catalog.TransactionRow(transaction_name="Z", description=None,
                                             capture=True, show=False, expand=False)],
    )
    body = catalog.shape(CC, rows)
    assert [m["name"] for m in body["metrics"]] == ["a", "b"]
    assert body["metrics"][1]["dimensions"] == [{"name": "method", "values": [], "truncated": False}]
    assert body["metrics"][1]["measures"] == [{"name": "n", "aggregation": "count", "field": None,
                                               "unit": None, "approximate": False}]
    assert body["fields"][0]["methods"] == ["M1", "M2"], "methods sorted, never insertion order"
    assert body["transactions"][0]["show"] is False
