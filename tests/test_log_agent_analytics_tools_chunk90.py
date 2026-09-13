"""Chunk 90, part 7 of the metric builder: the chat agent reads analytics through the same service
functions the HTTP endpoints call. Never SQL, never a draft, never another tenant.

Pinned here
-----------
    list_metrics       the catalog's ACTIVE metrics with descriptions, dimensions, measures (with
                       units and the approximate flag), grains and rollups_from; drafts are hidden;
                       an optional name filter narrows it
    query_metric       validates group_by exactly as /series does (unknown field -> the resolver's
                       message; unapproved attr -> the API's wording), refuses a draft or unknown
                       metric with the API's 404 wording, and returns points WITH provenance:
                       grain, from_rollups, live_spans, ad_hoc, approximate, rollups_from
    explain_freshness  read.freshness over the tenant state, plus the plain-words verdict
    scoping            every tool takes the tenant from the loop, never from the model's arguments
    registration       all three are in TOOLS and dispatchable through execute_tool
"""
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

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
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import registry
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.log_agent import analytics_tools as at
from app.services.log_agent import tools

CC = "test_chunk90"
OTHER = "test_chunk90_other"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, AnalyticsFieldRegistry, LogTransaction)


async def _wipe():
    async with async_session() as db:
        for cc in (CC, OTHER):
            for model in MODELS:
                await db.execute(delete(model).where(model.customer_code == cc))
            await db.execute(delete(Job).where(Job.customer_code == cc))
            await db.execute(delete(Customer).where(Customer.customer_code == cc))
        await db.commit()


def _definition(**over) -> d.MetricDefinition:
    base = dict(name="picks", dimensions=("method", "warehouse"),
                measures=(d.Measure(name="quantity", aggregation=d.Aggregation.sum, field="quantity",
                                    unit="units"),
                          d.Measure(name="items", aggregation=d.Aggregation.distinct,
                                    field="item_number")),
                grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",),
                status=d.Status.active)
    base.update(over)
    return d.MetricDefinition(**base)


async def _register(cc: str, definition: d.MetricDefinition, *, description: str | None) -> uuid.UUID:
    async with async_session() as db:
        row = AnalyticsMetric(**registry.to_row(definition, customer_code=cc, created_by="test"))
        row.description = description
        db.add(row)
        await db.commit()
        return row.id


@pytest.fixture(autouse=True)
async def tenant():
    """Two active metrics and one draft, folded once, plus a second tenant that must stay invisible."""
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="agent probe", timezone="UTC"))
        db.add(Customer(customer_code=OTHER, name="other", timezone="UTC"))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="response",
                                      field="resp.Approved", captured=True))
        await db.commit()
    await _register(CC, _definition(), description="Units confirmed as picked, by method and warehouse")
    await _register(CC, _definition(name="attempts", dimensions=("method",),
                                    measures=(d.Measure(name="n", aggregation=d.Aggregation.count),)),
                    description="Every confirmation, picked or not")
    await _register(CC, _definition(name="draft-idea", status=d.Status.draft), description="not live")
    await _register(OTHER, _definition(name="secret"), description="another tenant's")

    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for at_, item, wh in ((T0, "A", "BRI"), (T0, "B", "BRI"), (T0 + timedelta(hours=1), "A", "LON")):
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at_, ended_at=at_,
                date=at_.date(), duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success, item_number=item,
                user_name="EDA", warehouse=wh, attributes={QF["ConfirmPickLine"]: "10.0"}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)
    yield
    await _wipe()


async def _run(name: str, args: dict, cc: str = CC) -> dict:
    async with async_session() as db:
        return json.loads(await tools.execute_tool(name, args, db, cc))


# =============================================================== 1. registration

def test_the_three_tools_are_registered_with_schemas():
    names = {t["name"] for t in tools.TOOLS}
    assert {"list_metrics", "query_metric", "explain_freshness"} <= names
    q = next(t for t in tools.TOOLS if t["name"] == "query_metric")
    assert set(q["input_schema"]["required"]) == {"metric"}
    assert q["input_schema"]["additionalProperties"] is False
    assert "customer" not in json.dumps(tools.TOOLS).lower().replace("customer_code", ""), \
        "the tenant is never a tool argument"


# =============================================================== 2. list_metrics

async def test_list_metrics_returns_active_metrics_with_meaning_and_hides_drafts():
    out = await _run("list_metrics", {})
    names = [m["name"] for m in out["metrics"]]
    # `consumption` is seeded by the first fold for every tenant, so it is legitimately listed.
    assert names == ["attempts", "consumption", "picks"]
    assert "draft-idea" not in names and "secret" not in names
    picks = next(m for m in out["metrics"] if m["name"] == "picks")
    assert picks["description"] == "Units confirmed as picked, by method and warehouse"
    assert [x["name"] for x in picks["dimensions"]] == ["method", "warehouse"]
    quantity = next(x for x in picks["measures"] if x["name"] == "quantity")
    assert quantity["unit"] == "units" and quantity["approximate"] is False
    items = next(x for x in picks["measures"] if x["name"] == "items")
    assert items["approximate"] is True
    assert picks["grains"] == ["hourly", "daily", "monthly"]
    assert "rollups_from" in picks and "backfilled_through" in picks
    assert out["freshness"]["never_folded"] is False


async def test_list_metrics_name_filter_is_a_case_insensitive_substring():
    out = await _run("list_metrics", {"name": "PICK"})
    assert [m["name"] for m in out["metrics"]] == ["picks"]


# =============================================================== 3. query_metric

async def test_query_metric_returns_points_with_full_provenance():
    out = await _run("query_metric", {"metric": "picks", "measure": "quantity",
                                      "start": (T0 - WIDE).isoformat(), "end": (T0 + WIDE).isoformat(),
                                      "group_by": ["warehouse"]})
    assert out["metric"] == "picks" and out["measure"] == "quantity"
    assert out["grain"] in ("hourly", "daily")
    assert out["from_rollups"] is True and out["ad_hoc"] is False
    assert out["approximate"] is False and out["unit"] == "units"
    assert isinstance(out["live_spans"], list) and "rollups_from" in out
    by_wh = {p["dimensions"][0]: Decimal(p["roles"]["sum_value"]) for p in out["points"]}
    assert by_wh == {"BRI": Decimal("20.0"), "LON": Decimal("10.0")}
    assert {t["dimensions"][0]: Decimal(t["roles"]["sum_value"]) for t in out["totals"]} == by_wh
    assert Decimal(out["total"]["sum_value"]) == Decimal("30") and out["total"]["count_value"] == 3
    assert "how_to_read" in out and "two-tier" in out["how_to_read"].lower()


async def test_query_metric_marks_a_distinct_measure_approximate_and_never_returns_bytes():
    out = await _run("query_metric", {"metric": "picks", "measure": "items",
                                      "start": (T0 - WIDE).isoformat(), "end": (T0 + WIDE).isoformat()})
    assert out["approximate"] is True
    assert all("distinct_estimate" in p["roles"] and "distinct_sketch" not in p["roles"]
               for p in out["points"])
    assert out["total"]["distinct_estimate"] == 2, "A in two hours is one item: a union, not a sum"


async def test_query_metric_defaults_measure_and_window():
    out = await _run("query_metric", {"metric": "picks"})
    assert out["measure"] == "quantity", "the first declared measure"
    assert out["window"]["start"] < out["window"]["end"]


async def test_query_metric_refuses_a_draft_or_unknown_metric_with_the_api_wording():
    for name in ("draft-idea", "nope", "secret"):
        out = await _run("query_metric", {"metric": name})
        assert "No ACTIVE metric named" in out["error"], out


async def test_query_metric_refuses_an_undeclared_field_and_an_unapproved_attribute():
    out = await _run("query_metric", {"metric": "picks", "group_by": ["colour"]})
    assert "not a field on the fact row" in out["error"]
    out = await _run("query_metric", {"metric": "picks", "group_by": ["attr:resp.Secret"]})
    assert "not approved for capture" in out["error"]
    out = await _run("query_metric", {"metric": "picks", "measure": "nope"})
    assert "has no measure" in out["error"]


async def test_query_metric_ad_hoc_group_by_is_labelled_as_a_bounded_scan():
    out = await _run("query_metric", {"metric": "picks", "group_by": ["user_name"],
                                      "start": (T0 - WIDE).isoformat(), "end": (T0 + WIDE).isoformat()})
    assert out["ad_hoc"] is True and "fact scan" in out["resolution"]


async def test_query_metric_caps_the_window():
    out = await _run("query_metric", {"metric": "picks",
                                      "start": (T0 - timedelta(days=400)).isoformat(),
                                      "end": T0.isoformat()})
    start = datetime.fromisoformat(out["window"]["start"])
    assert T0 - start <= timedelta(days=at.MAX_WINDOW_DAYS)
    assert any("clamped" in w for w in out["notes"])


async def test_tools_are_scoped_by_the_loop_not_the_arguments():
    out = await _run("list_metrics", {"customer_code": CC}, cc=OTHER)
    assert [m["name"] for m in out["metrics"]] == ["secret"]


# =============================================================== 4. explain_freshness

async def test_explain_freshness_states_lag_and_settledness_in_words():
    out = await _run("explain_freshness", {})
    assert out["configured"] is True
    f = out["freshness"]
    assert f["never_folded"] is False and "lag_seconds" in f and "provisional" in f
    assert isinstance(out["verdict"], str) and out["verdict"]


async def test_explain_freshness_on_an_unconfigured_tenant_says_so():
    out = await _run("explain_freshness", {}, cc=OTHER)
    assert out["configured"] is False and out["freshness"]["never_folded"] is True
    assert "never folded" in out["verdict"]
