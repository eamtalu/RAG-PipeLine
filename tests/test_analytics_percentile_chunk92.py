"""Chunk 92: the percentile aggregation, finished end to end.

The design always said percentiles are stored as a 20-band log histogram, because band counts add and
percentiles do not: the median of twelve monthly medians is not the yearly median. The container, the
column and the band-wise merge existed since Phase 3; the two ends did not - nothing put a value INTO
a band, and nothing read a percentile OUT. The wizard offered `percentile` anyway, and a metric using it
would have folded to empty buckets and charted as no data. Found auditing the Station 5 document on
13 Sep 2026.

Pinned here
-----------
    bands          20 bands, powers of two: band 0 holds everything below 1 (zero and negatives), band
                   i holds [2^(i-1), 2^i), the top band clamps everything from 2^18 upwards
    fold           a value lands in exactly one band and the count beside it is exact
    merge          band counts add, so hour -> day -> month composes like every other role
    finish         p50 and p95 are read out of the histogram by walking the cumulative count and
                   interpolating inside the crossing band; they sit inside that band's edges
    public         responses carry p50 and p95 as numbers, never the raw band list
    cascade        a percentile measure survives the whole write and read path with real durations
    catalog        percentile is marked approximate, because a band is a factor of two wide
"""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
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
from app.services.analytics import catalog
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import histogram as hg
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk92"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, LogTransaction)


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
        db.add(Customer(customer_code=CC, name="percentile probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


# =============================================================== 1. the bands, pure

@pytest.mark.parametrize("value, band", [
    (-5, 0), (0, 0), (Decimal("0.5"), 0), (Decimal("0.999"), 0),
    (1, 1), (Decimal("1.99"), 1), (2, 2), (3, 2), (4, 3), (7, 3), (8, 4),
    (100, 7), (127, 7), (128, 8), (1000, 10), (1023, 10), (1024, 11),
    (2 ** 18, 19), (10 ** 9, 19),
])
def test_a_value_lands_in_exactly_one_band(value, band):
    assert hg.band_of(Decimal(value)) == band


def test_there_are_twenty_bands_and_the_edges_are_powers_of_two():
    assert hg.BANDS == 20
    assert hg.EMPTY == (0,) * 20
    assert hg.lower_edge(0) == 0 and hg.upper_edge(0) == 1
    assert hg.lower_edge(1) == 1 and hg.upper_edge(1) == 2
    assert hg.lower_edge(10) == 512 and hg.upper_edge(10) == 1024
    assert hg.lower_edge(19) == 2 ** 18


def test_add_increments_one_band_and_leaves_the_rest():
    h = hg.add(hg.EMPTY, Decimal(40))          # band 6: [32, 64)
    assert h[6] == 1 and sum(h) == 1
    h = hg.add(h, Decimal(50))
    assert h[6] == 2 and sum(h) == 2


def test_histograms_merge_band_by_band_through_add_roles():
    a = hg.add(hg.add(hg.EMPTY, Decimal(40)), Decimal(3))
    b = hg.add(hg.add(hg.EMPTY, Decimal(45)), Decimal(1000))
    merged = d.add_roles({d.Role.histogram: a}, {d.Role.histogram: b})[d.Role.histogram]
    assert tuple(merged) == tuple(x + y for x, y in zip(a, b))
    assert sum(merged) == 4


# =============================================================== 2. the finish, pure

def test_an_empty_histogram_has_no_percentile():
    assert hg.percentile(hg.EMPTY, 0.5) is None
    assert hg.percentile(None, 0.95) is None


def test_the_percentile_sits_inside_the_band_where_the_cumulative_count_crosses():
    h = hg.EMPTY
    for v in range(1, 101):                      # 1..100, one row each
        h = hg.add(h, Decimal(v))
    p50 = hg.percentile(h, 0.5)
    p95 = hg.percentile(h, 0.95)
    # 50 rows are at or below 50, which is in band 6 = [32, 64); 95 rows at or below 95, band 7 = [64, 128)
    assert 32 <= p50 < 64, p50
    assert 64 <= p95 < 128, p95
    assert p50 < p95


def test_a_single_band_interpolates_inside_that_band():
    h = hg.EMPTY
    for _ in range(1000):
        h = hg.add(h, Decimal(40))
    assert 32 <= hg.percentile(h, 0.5) < 64
    assert 32 <= hg.percentile(h, 0.95) < 64


def test_percentile_is_monotonic_in_q():
    h = hg.EMPTY
    for v in (1, 3, 9, 27, 81, 243, 729):
        h = hg.add(h, Decimal(v))
    values = [hg.percentile(h, q) for q in (0.1, 0.5, 0.9, 0.99)]
    assert values == sorted(values)


# =============================================================== 3. the aggregation

def test_percentile_declares_the_histogram_and_a_count():
    assert d.roles_for(d.Aggregation.percentile) == frozenset({d.Role.histogram, d.Role.count_value})


def _definition(**over) -> d.MetricDefinition:
    base = dict(name="pick-time", dimensions=("method",),
                measures=(d.Measure(name="ms", aggregation=d.Aggregation.percentile,
                                    field="duration_ms", unit="ms"),),
                grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",),
                status=d.Status.active)
    base.update(over)
    return d.MetricDefinition(**base)


def _row(ms, method="ConfirmPickLine"):
    return {"method": method, "duration_ms": ms, "transaction_name": "Pick", "status": "success"}


def test_fold_fills_the_histogram_and_counts_exactly_the_rows_that_carried_a_value():
    rows = [_row(40), _row(50), _row(3), _row(None), _row("not a number"), _row(1000, "Other")]
    folded = d.fold(rows, _definition())["ms"]
    h = folded[d.Role.histogram]
    assert sum(h) == 3 and folded[d.Role.count_value] == 3
    assert h[6] == 2 and h[2] == 1


def test_public_roles_finish_the_histogram_into_p50_and_p95_numbers():
    folded = d.fold([_row(v) for v in range(1, 101)], _definition())["ms"]
    public = d.public_roles(folded)
    assert set(public) == {"count_value", "p50", "p95"}
    assert public["count_value"] == 100
    assert isinstance(public["p50"], float) and isinstance(public["p95"], float)
    assert 32 <= public["p50"] < 64 and 64 <= public["p95"] < 128


def test_an_empty_histogram_bucket_is_empty():
    from app.services.analytics import rollups as n5
    assert n5._is_empty(d.fold([], _definition())["ms"])


# =============================================================== 4. the cascade, end to end

async def _plant(rows):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for at, ms in rows:
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=ms, method="ConfirmPickLine", transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success, item_number="A",
                user_name="EDA", warehouse="BRI", attributes={QF["ConfirmPickLine"]: "10.0"}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()


async def _register(definition=None) -> uuid.UUID:
    async with async_session() as db:
        row = AnalyticsMetric(**registry.to_row(definition or _definition(), customer_code=CC,
                                                created_by="test"))
        db.add(row)
        await db.commit()
        return row.id


async def _rows(model, did):
    async with async_session() as db:
        return list((await db.execute(select(model).where(
            model.customer_code == CC, model.definition_id == did,
            model.measure_name == "ms"))).scalars().all())


async def test_a_percentile_metric_folds_through_hourly_daily_and_monthly():
    did = await _register()
    durations = [(T0, 40), (T0, 50), (T0, 45), (T0 + timedelta(hours=1), 3000),
                 (T0 + timedelta(hours=1), 2500)]
    await _plant(durations)
    await n3.consume_tenant(CC)

    hourly = sorted(await _rows(AnalyticsHourlyRollup, did), key=lambda r: r.bucket_start)
    assert [sum(r.histogram) for r in hourly] == [3, 2]
    assert [r.count_value for r in hourly] == [3, 2]
    assert hourly[0].histogram[6] == 3, "40, 50, 45 all sit in [32, 64)"
    assert hourly[1].histogram[12] == 2, "2500 and 3000 sit in [2048, 4096)"

    daily = await _rows(AnalyticsDailyRollup, did)
    assert len(daily) == 1 and sum(daily[0].histogram) == 5 and daily[0].count_value == 5
    assert daily[0].histogram[6] == 3 and daily[0].histogram[12] == 2, "merged band by band"

    monthly = await _rows(AnalyticsMonthlyRollup, did)
    assert len(monthly) == 1 and sum(monthly[0].histogram) == 5


async def test_series_returns_p50_and_p95_and_never_the_band_list():
    did = await _register()
    await _plant([(T0, 40), (T0, 50), (T0, 45), (T0, 3000), (T0, 2500)])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        state = await db.scalar(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))
        out = await n6.series(db, CC, did, _definition(), measure="ms",
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE),
                              watermark=state.analytics_watermark, group_by=())
    assert len(out["points"]) == 1
    roles = out["points"][0]["roles"]
    assert set(roles) == {"count_value", "p50", "p95"}
    assert 32 <= roles["p50"] < 64, "three of five rows are in [32, 64)"
    assert 2048 <= roles["p95"] < 4096
    assert set(out["total"]) == {"count_value", "p50", "p95"}


async def test_the_catalog_marks_percentile_approximate():
    await _register()
    async with async_session() as db:
        body = await catalog.build(db, CC)
    agg = next(a for a in body["aggregations"] if a["name"] == "percentile")
    assert agg["approximate"] is True and agg["needs_field"] is True
    assert sorted(agg["roles"]) == ["count_value", "histogram"]


async def test_a_percentile_metric_previews_with_finished_values():
    await _plant([(T0, 40), (T0, 50), (T0, 3000)])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.preview_metric(payload={
            "name": "pick-time", "dimensions": ["method"], "source": "transaction",
            "measures": [{"name": "ms", "aggregation": "percentile", "field": "duration_ms", "unit": "ms"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": []},
            "grains": ["daily"]}, window_hours=24 * 365, customer=CC, db=db)  # T0 is fixed; the default week window drifts past it
    assert out["ok"], out["problems"] + out["refusals"]
    point = out["sample"]["points"][0]
    assert "p50" in point["roles"] and "p95" in point["roles"] and point["roles"]["count_value"] == 3
