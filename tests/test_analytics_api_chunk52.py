"""Chunk 52, Phase 5b: N7, the analytics read API.

Three properties are measurements rather than preferences.

**`/status` must read exactly ONE row** (F5). The browser polls it every 2 seconds per tab across four
gunicorn workers. The original design computed counts over several tables per poll; the worker now writes
every field this endpoint needs into `analytics_tenant_state`. The query count is asserted here, because
the natural way to add a field to this response is to join another table and nothing else would notice.

**No endpoint returns a finished answer.** `/series` returns additive roles and the caller divides.
Invariant 8 does not stop at the rollup table: an endpoint returning an average would be the one place
twelve monthly averages could get averaged into a year.

**There is no `/analytics/backfill`.** The plan lists one; D8 cancelled it. An endpoint that 202s and
folds nothing is worse than its absence.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi import HTTPException, Response
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, select

from app.config.database import async_session, engine
from app.api.v1 import analytics as api
from app.main import app
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
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
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "api-probe"
T0 = datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
HDR = {"X-Customer-Code": CC}

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsMetric, LogTransaction)


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
        db.add(Customer(customer_code=CC, name="api probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


#: TestClient is used ONLY by the synchronous tests below, and deliberately without `with`: that keeps
#: the app lifespan (and the background workers) from starting, and keeps the app's event loop from
#: competing with pytest-asyncio's for the module-level engine. Mixing the two produces
#: "attached to a different loop" from asyncpg -- which is a test-harness fault, not a bug in the app.
#: Everything that needs planted data calls the handler function directly instead.
client = TestClient(app)


async def _plant_and_fold(rows):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for spec in rows:
            method = spec.get("method", "ConfirmPickLine")
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=spec["at"],
                ended_at=spec["at"], date=spec["at"].date(), duration_ms=100, method=method,
                transaction_name="Pick", transaction_type="002001",
                status=spec.get("status", LogTransactionStatus.success), item_number="101978",
                user_name="EDA", warehouse="BRI",
                attributes={QF[method]: spec.get("qty", "10.0")}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    return await n3.consume_tenant(CC)


# ==================================================== /status
async def test_status_on_an_unconfigured_tenant_says_so_rather_than_reporting_zeros():
    """Zeros would render as a healthy, empty chart. The worker ships disabled, so this is the NORMAL
    state until someone switches it on, and saying it plainly is the whole value."""
    resp = Response()
    async with async_session() as db:
        body = await api.analytics_status(resp, customer=CC, db=db)
    assert body["configured"] is False
    assert body["freshness"]["never_folded"] is True
    assert "ETag" in resp.headers


async def test_status_reports_both_freshness_numbers():
    """F4. One number cannot say what the user needs to know."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    async with async_session() as db:
        body = await api.analytics_status(Response(), customer=CC, db=db)
    assert body["configured"] is True
    f = body["freshness"]
    assert "lag_seconds" in f and "unsealed_share" in f
    assert "stale" in f and "provisional" in f


async def test_status_reads_exactly_one_row():
    """F5, asserted rather than trusted. The browser polls this every 2 s per tab across four workers,
    and the natural way to add a field here is to join another table."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])

    seen: list[str] = []

    def record(conn, cursor, statement, params, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            seen.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        async with async_session() as db:
            await api.analytics_status(Response(), customer=CC, db=db)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)

    # Exactly one read of the state row, and nothing else: no counts over the ticket table, no
    # aggregate over the facts.
    analytics_reads = [s for s in seen if "analytics_tenant_state" in s]
    assert len(analytics_reads) == 1, f"{len(analytics_reads)} reads of tenant state:\n" + \
        "\n".join(analytics_reads)
    assert not any("analytics_facts" in s or "analytics_pending_windows" in s for s in seen), \
        "the status card must not touch another analytics table"


async def test_the_etag_is_the_tenant_revision():
    """A5: one authoritative revision, bumped in the same commit as the work. Keying the ETag off
    anything computed in the request would let a 304 be served over changed data."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    resp = Response()
    async with async_session() as db:
        await api.analytics_status(resp, customer=CC, db=db)
        state = (await db.execute(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))).scalar_one()
    assert resp.headers["ETag"] == f'"{state.revision}"'


async def test_status_says_history_is_not_backfilled():
    """D8's consequence surfaced to the interface. Phase 6 must show "no history before <date>" rather
    than an empty chart, and this is the field it reads."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    async with async_session() as db:
        body = await api.analytics_status(Response(), customer=CC, db=db)
    assert body["backfilled"] is False


# ==================================================== tenancy (HTTP concerns only)
#
# These are SYNCHRONOUS and use TestClient, because what they test is routing and validation rather
# than data. Nothing here plants a row, so the app's loop never contends with pytest-asyncio's.

def test_every_endpoint_resolves_the_tenant_through_get_current_customer():
    """Asserted structurally rather than by provoking a 404, and the structural form is the stronger
    claim: it holds for every endpoint including ones added later, and it is what actually guarantees an
    unknown tenant becomes a clean 404 instead of a silent cross-tenant query.

    (Provoking the 404 over HTTP would need a database read on the app's own event loop, which fights
    pytest-asyncio's for the module-level engine -- a harness problem, not an app one.)"""
    from app.api.deps import get_current_customer
    from app.api.v1 import analytics as mod

    for route in mod.router.routes:
        deps = [d.call for d in route.dependant.dependencies]
        assert get_current_customer in deps, f"{route.path} does not resolve the tenant"


def test_a_missing_tenant_header_is_rejected():
    """No endpoint may fall back to a cross-tenant query."""
    assert client.get("/api/v1/analytics/status").status_code == 422


@pytest.mark.parametrize("path", ["/status", "/metrics", "/series", "/breakdown?dimension=method"])
def test_every_read_endpoint_requires_the_tenant(path):
    assert client.get(f"/api/v1/analytics{path}").status_code == 422


def test_breakdown_top_n_is_capped():
    """A chart with 10,000 bars is not a chart. The tenant dependency is overridden so this exercises
    the validation alone and touches no database -- the same technique chunk 13 uses."""
    from app.api.deps import get_current_customer
    from app.config.database import get_session

    app.dependency_overrides[get_current_customer] = lambda: CC
    app.dependency_overrides[get_session] = lambda: None
    try:
        r = client.get("/api/v1/analytics/breakdown",
                       params={"dimension": "method", "top": 10_000})
        assert r.status_code == 422
        detail = str(r.json())
        assert "top" in detail and str(api._MAX_TOP_N) in detail, \
            "the error must name the field and the cap"
    finally:
        app.dependency_overrides.clear()


def test_there_is_no_backfill_endpoint():
    """The plan lists `POST /analytics/backfill`; D8 cancelled it. An endpoint that 202s and folds
    nothing is worse than its absence, and one that DID backfill would contradict a deliberate
    decision."""
    paths = app.openapi()["paths"]
    assert "/api/v1/analytics/backfill" not in paths
    assert "/api/v1/analytics/reconcile" in paths, "the half of Phase 4 that survived"


# ==================================================== /metrics
async def test_metrics_lists_registry_rows_not_hardcoded_names():
    """The whole point of the registry is that the code does not know which metrics exist."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    async with async_session() as db:
        body = await api.list_metrics(customer=CC, db=db, limit=50)
    row = next(m for m in body["metrics"] if m["name"] == "consumption")
    assert sorted(row["measures"]) == ["attempt_count", "pick_count", "quantity"]
    assert row["backfilled_through"] is None, "D8: no history was built"


def test_metrics_is_bounded():
    import inspect
    assert "limit" in inspect.signature(api.list_metrics).parameters


# ==================================================== /series
async def _series(**kw):
    async with async_session() as db:
        return await api.analytics_series(customer=CC, db=db, **kw)


async def test_series_returns_additive_roles_never_a_finished_answer():
    """Invariant 8 does not stop at the rollup table."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"},
                           {"at": T0 + timedelta(minutes=5), "qty": "5.0"}])
    body = await _series(metric="consumption", measure="quantity",
                         start=T0 - WIDE, end=T0 + WIDE, group_by=None)
    assert body["points"], body
    roles = body["points"][0]["roles"]
    assert "sum_value" in roles and "count_value" in roles
    assert "average" not in roles and "rate" not in roles
    assert sum(Decimal(p["roles"]["sum_value"]) for p in body["points"]) == Decimal(15)


async def test_series_states_the_grain_it_chose():
    """So a caller can tell what resolution it got rather than assuming the finest."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    body = await _series(metric="consumption", measure="quantity",
                         start=T0 - WIDE, end=T0 + WIDE, group_by=None)
    assert body["grain"] in ("hourly", "daily", "monthly")
    assert "live_spans" in body and "from_rollups" in body


async def test_series_marks_an_ad_hoc_request():
    """The plan is explicit: the response marks itself "so the interface can show it rather than
    silently running slow"."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    body = await _series(metric="consumption", measure="quantity",
                         start=T0 - WIDE, end=T0 + WIDE, group_by="item_number")
    assert body["ad_hoc"] is True and "item_number" in body["resolution"]


async def test_a_group_by_on_a_nonexistent_field_is_a_400():
    """Not a fallback. A fallback would scan and return nothing, which reads as "no data" rather than
    "that field does not exist"."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    with pytest.raises(HTTPException) as exc:
        await _series(metric="consumption", measure="quantity", start=None, end=None,
                      group_by="no_such_column")
    assert exc.value.status_code == 400 and "not a field" in exc.value.detail


async def test_an_unknown_measure_is_a_400_that_lists_the_real_ones():
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    with pytest.raises(HTTPException) as exc:
        await _series(metric="consumption", measure="nope", start=None, end=None, group_by=None)
    assert exc.value.status_code == 400
    assert "attempt_count" in exc.value.detail, "the error must say what IS available"


async def test_an_unknown_metric_is_a_404():
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    with pytest.raises(HTTPException) as exc:
        await _series(metric="invented", measure="quantity", start=None, end=None, group_by=None)
    assert exc.value.status_code == 404


async def test_an_inverted_window_is_a_400():
    with pytest.raises(HTTPException) as exc:
        await _series(metric="consumption", measure="quantity", start=T0, end=T0 - WIDE,
                      group_by=None)
    assert exc.value.status_code == 400


async def test_the_default_window_is_bounded():
    """An unbounded default is how a read endpoint becomes an outage on the first curious click."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    body = await _series(metric="consumption", measure="quantity", start=None, end=None,
                         group_by=None)
    start = datetime.fromisoformat(body["window"]["start"])
    end = datetime.fromisoformat(body["window"]["end"])
    assert (end - start) <= timedelta(days=1)


# ==================================================== /breakdown
async def test_breakdown_ranks_by_the_measure():
    await _plant_and_fold([{"at": T0, "qty": "10.0", "method": "ConfirmPickLine"},
                           {"at": T0 + timedelta(minutes=1), "qty": "3.0",
                            "method": "ReportCount"}])
    async with async_session() as db:
        body = await api.analytics_breakdown(customer=CC, db=db, metric="consumption",
                                            measure="quantity", dimension="method",
                                            start=T0 - WIDE, end=T0 + WIDE, top=10)
    assert [r["value"] for r in body["rows"]] == ["ConfirmPickLine", "ReportCount"]
    assert Decimal(body["rows"][0]["sum_value"]) == Decimal(10)


# ==================================================== /reconcile
async def test_reconcile_is_report_only_by_default():
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    async with async_session() as db:
        body = await api.trigger_reconcile(customer=CC, db=db, start=T0 - WIDE, end=T0 + WIDE,
                                           repair=False)
    assert body["healthy"] is True
    assert body["tickets_published"] == 0 and body["buckets_recomputed"] == 0


async def test_reconcile_reports_per_check():
    """So an operator can see WHICH check is firing rather than a single healthy bit."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    async with async_session() as db:
        body = await api.trigger_reconcile(customer=CC, db=db, start=None, end=None, repair=False)
    assert set(body["by_check"]) == {"facts_vs_transactions", "rollups_vs_facts",
                                     "entries_vs_assignments",
                                     "record_rollups_vs_record_facts", "records_vs_facts"}


async def test_the_live_half_adds_every_dimension_combo_in_a_bucket():
    """Found in production, from a screenshot: the UNITS tile read 19,144.43 while the
    breakdown table beneath it summed to 19,825.43 -- and the series reported one hour as
    `sum=0 count=0` when the stored rollup for that hour held 681.

    Cause: `_live_points` built a dict COMPREHENSION keyed on the REDUCED dimensions. That
    hour had eight distinct (method, transaction_name) combos, only one of which
    contributes to consumption. With `group_by=()` all eight collapse to a single key, and
    a dict comprehension silently keeps the LAST one -- which was a non-quantity method,
    so the whole bucket reported zero.

    It only showed with no grouping. `group_by=("method",)` keeps the keys distinct, which
    is exactly why the table was right and the tile was wrong, and why every test until
    now passed: they all grouped, or had one combo per bucket.

    Silent under-reporting, in the LIVE half -- the recent tail the two-tier read exists to
    serve.
    """
    from app.services.analytics import read as n6
    from app.services.analytics import registry

    # Two quantity-bearing methods in the SAME hour: two dimension combos, both contributing.
    await _plant_and_fold([
        {"at": T0, "qty": "10.0", "method": "ConfirmPickLine"},
        {"at": T0 + timedelta(minutes=5), "qty": "4.0", "method": "ReportCount"},
    ])

    async with async_session() as db:
        (definition_id, definition), = await registry.active_definitions(db, CC)
        # A watermark BEFORE the data forces the whole window through the live half.
        out = await n6.series(db, CC, definition_id, definition,
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE),
                              measure="quantity", group_by=(),
                              watermark=T0 - WIDE)

    assert out["from_rollups"] is False, "the point of this test is the LIVE path"
    total = sum(Decimal(p["roles"].get("sum_value", "0")) for p in out["points"])
    assert total == Decimal(14), f"expected 10 + 4 from two combos in one bucket, got {total}"

    counts = sum(p["roles"].get("count_value", 0) or 0 for p in out["points"])
    assert counts == 2, f"both confirmations must be counted, got {counts}"


async def test_the_live_half_still_separates_combos_when_grouping_is_asked_for():
    """The other direction, so the fix is not "always merge": with a grouping, each combo
    keeps its own row."""
    from app.services.analytics import read as n6
    from app.services.analytics import registry

    await _plant_and_fold([
        {"at": T0, "qty": "10.0", "method": "ConfirmPickLine"},
        {"at": T0 + timedelta(minutes=5), "qty": "4.0", "method": "ReportCount"},
    ])
    async with async_session() as db:
        (definition_id, definition), = await registry.active_definitions(db, CC)
        out = await n6.series(db, CC, definition_id, definition,
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE),
                              measure="quantity", group_by=("method",),
                              watermark=T0 - WIDE)
    by_method = {p["dimensions"][0]: Decimal(p["roles"]["sum_value"]) for p in out["points"]}
    assert by_method == {"ConfirmPickLine": Decimal(10), "ReportCount": Decimal(4)}


async def test_status_reports_where_history_BEGINS_not_where_it_ends():
    """The production symptom: the card said "No analytics history before 11:07" while the
    chart beneath it plotted data from 09:00. It was reporting the newest folded instant as
    the start of history."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"},
                           {"at": T0 + timedelta(hours=3), "qty": "5.0"}])
    async with async_session() as db:
        body = await api.analytics_status(Response(), customer=CC, db=db)
    start = body["history_starts_at"]
    watermark = body["freshness"]["analytics_watermark"]
    assert start is not None
    assert start < watermark, f"history must begin BEFORE the watermark: {start} vs {watermark}"
    assert start.startswith(T0.isoformat()[:16])


async def test_status_still_reads_exactly_one_row_after_the_history_fix():
    """The fix had to stay inside F5's one-row contract -- min(event_time) over the fact table
    would have been the obvious implementation and would have broken it."""
    await _plant_and_fold([{"at": T0, "qty": "10.0"}])
    seen: list[str] = []

    def record(conn, cursor, statement, params, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            seen.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        async with async_session() as db:
            await api.analytics_status(Response(), customer=CC, db=db)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)
    assert len([s for s in seen if "analytics_tenant_state" in s]) == 1
    assert not any("analytics_facts" in s for s in seen), "no aggregate over the fact table"


# ==================================================== POST /metrics (N7)
ITEM_METRIC = {
    "name": "consumption_by_item",
    "dimensions": ["method", "item_number"],
    "measures": [{"name": "quantity", "aggregation": "sum", "field": "quantity",
                  "only": ["pick", "attempt", "correction"], "statuses": ["success"]}],
    "filter": {"methods": ["ConfirmPickLine"]},
    "grains": ["hourly", "daily", "monthly"],
    "status": "active",
}


async def test_a_metric_can_be_registered_as_data_and_is_then_folded():
    """The registry's whole claim, exercised through the API: a metric nobody wrote code for
    is created as a ROW and the worker folds it."""
    async with async_session() as db:
        created = await api.create_metric(payload=dict(ITEM_METRIC), customer=CC, db=db)
    assert created["name"] == "consumption_by_item"
    assert created["backfilled_through"] is None, "D8: it has no history until a range is re-folded"

    stats = await _plant_and_fold([{"at": T0, "qty": "10.0", "method": "ConfirmPickLine"}])
    assert stats["definitions_rolled"] == 2, "the seed plus the newly registered metric"

    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsHourlyRollup).join(
            AnalyticsMetric, AnalyticsMetric.id == AnalyticsHourlyRollup.definition_id)
            .where(AnalyticsMetric.name == "consumption_by_item"))).scalars().all()
    assert rows, "the new metric must have rollup rows"
    assert {r.dim2 for r in rows} == {"101978"}, "keyed by item_number in slot 2"


async def test_an_invalid_definition_is_rejected_with_every_problem_at_once():
    """Validated through the SAME function the worker applies, so a definition that would fold
    to a silently empty chart is refused at the door rather than skipped later."""
    bad = {**ITEM_METRIC, "name": "bad", "dimensions": ["no_such_field"],
           "measures": [{"name": "q", "aggregation": "sum", "field": "quantity",
                         "only": [], "statuses": ["nope"]}],
           "filter": {"methods": ["ListPickLines"]}}
    async with async_session() as db:
        with pytest.raises(HTTPException) as exc:
            await api.create_metric(payload=bad, customer=CC, db=db)
    assert exc.value.status_code == 400
    detail = " ".join(exc.value.detail)
    assert "no_such_field" in detail          # dimension not on the fact row
    assert "nope" in detail                    # status the projection never emits
    assert "ListPickLines" in detail           # quantity summed over a method that carries none


async def test_a_duplicate_name_is_a_409_not_a_second_row():
    """Two definitions with one name would make "which metric is this chart" unanswerable."""
    async with async_session() as db:
        await api.create_metric(payload=dict(ITEM_METRIC), customer=CC, db=db)
        with pytest.raises(HTTPException) as exc:
            await api.create_metric(payload=dict(ITEM_METRIC), customer=CC, db=db)
    assert exc.value.status_code == 409


async def test_a_nameless_or_measureless_metric_is_refused():
    async with async_session() as db:
        for payload, missing in (({**ITEM_METRIC, "name": "  "}, "name"),
                                 ({**ITEM_METRIC, "name": "m2", "measures": []}, "measure")):
            with pytest.raises(HTTPException) as exc:
                await api.create_metric(payload=payload, customer=CC, db=db)
            assert exc.value.status_code == 400 and missing in str(exc.value.detail)


async def test_creating_a_metric_does_not_promise_a_backfill():
    """The plan specified 202 + a backfill job. D8 cancelled the backfill, so a 202 would
    promise work that never happens."""
    # Asserted against the actual route table, not the source: `or True` in an assertion is
    # a test that cannot fail.
    route = next(r for r in api.router.routes
                 if r.path == "/analytics/metrics" and "POST" in r.methods)
    assert route.status_code == 201, "202 would promise a backfill that D8 cancelled"
    async with async_session() as db:
        out = await api.create_metric(payload=dict(ITEM_METRIC), customer=CC, db=db)
    assert "no history yet" in out["detail"]
    assert "reconcile" in out["detail"], "it must say HOW to get history"
