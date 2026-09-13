"""Chunk 88, part 4 of the metric builder: count-distinct as an additive role.

"How many different items were picked this month" cannot be answered from a sum of daily counts: an
item picked on twelve days is one item. Exact sets do not compose within a bounded row either. A
HyperLogLog sketch does both - it unions, and it is a fixed 4 KB - at the price of being an ESTIMATE,
about 1.6 percent at precision 12. The catalog says so (`approximate: true`), and the read path returns
`distinct_estimate`, never the sketch and never a figure dressed as exact.

In-repo, no dependency: the whole thing is a hash, a register array, a harmonic mean and a small-range
correction. Pinned here against known cardinalities so the arithmetic cannot drift.

Pinned here
-----------
    sketch         empty estimates 0; small sets come back exact after rounding; large sets within
                   tolerance; a value added twice is one value; union equals adding both sets
    canonical      "624", 624 and Decimal("624") are one value; None and "" are never added
    aggregation    `distinct` stores {distinct_sketch, count_value}; it requires a field
    fold           the writer's fold builds the sketch and skips absent values
    cascade        hourly -> daily -> monthly UNIONS the sketch: an item in two hours is one item
    read           `/series` returns `distinct_estimate` as an integer, no bytes
    catalog        a distinct measure reports `approximate: true`
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
from app.services.analytics import contract
from app.services.analytics import definition as d
from app.services.analytics import hll
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk88"
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
        db.add(Customer(customer_code=CC, name="distinct probe", timezone="UTC"))
        await db.commit()
    yield
    await _wipe()


# =============================================================== 1. the sketch itself

def test_an_empty_sketch_estimates_zero():
    assert hll.estimate(hll.EMPTY) == 0
    assert len(hll.EMPTY) == hll.REGISTERS == 4096


@pytest.mark.parametrize("n", [1, 2, 3, 5])
def test_tiny_sets_come_back_exact_after_rounding(n):
    """With 4096 registers and a handful of values the linear-counting estimate rounds to the truth;
    the cascade tests below lean on this, so it is pinned rather than assumed."""
    sketch = hll.from_values(f"item-{i}" for i in range(n))
    assert hll.estimate(sketch) == n


@pytest.mark.parametrize("n", [50, 500, 5_000, 50_000])
def test_sets_of_every_size_are_within_three_percent(n):
    """The honest limit: an ESTIMATE, about 1.6 percent standard error at precision 12. The catalog
    says `approximate: true` for exactly this reason."""
    sketch = hll.from_values(f"item-{i}" for i in range(n))
    assert abs(hll.estimate(sketch) - n) <= max(1, n * 0.03), hll.estimate(sketch)


def test_a_value_added_many_times_is_one_value():
    sketch = hll.EMPTY
    for _ in range(1000):
        sketch = hll.add(sketch, "101978")
    assert hll.estimate(sketch) == 1


def test_union_is_the_sketch_of_both_sets_and_commutes():
    a = hll.from_values(f"a-{i}" for i in range(300))
    b = hll.from_values(f"b-{i}" for i in range(300))
    both = hll.from_values([*(f"a-{i}" for i in range(300)), *(f"b-{i}" for i in range(300))])
    assert hll.union(a, b) == both == hll.union(b, a)
    overlap = hll.from_values(f"a-{i}" for i in range(100, 400))
    assert abs(hll.estimate(hll.union(a, overlap)) - 400) <= 12, "the overlap of 200 is counted once"


def test_the_sketch_is_a_pure_function_of_the_values():
    assert hll.from_values(["x", "y"]) == hll.from_values(["y", "x"])


def test_a_value_is_canonicalised_before_hashing():
    assert contract.distinct_key("624") == contract.distinct_key(624) == contract.distinct_key(Decimal("624"))
    assert contract.distinct_key(" 101978 ") == "101978"
    assert contract.distinct_key(None) is None
    assert contract.distinct_key("") is None and contract.distinct_key("   ") is None
    assert contract.distinct_key(True) is None, "a flag is not an identity"


# =============================================================== 2. the aggregation

def test_distinct_declares_a_sketch_and_a_count():
    assert d.roles_for(d.Aggregation.distinct) == frozenset({d.Role.distinct_sketch, d.Role.count_value})


def test_distinct_requires_a_field():
    bad = d.MetricDefinition(name="x", dimensions=("method",),
                             measures=(d.Measure(name="items", aggregation=d.Aggregation.distinct),),
                             grains=("daily",))
    assert any("names no field" in p for p in d.validate(bad))


def _definition(**over) -> d.MetricDefinition:
    base = dict(name="items", dimensions=("method",),
                measures=(d.Measure(name="items", aggregation=d.Aggregation.distinct,
                                    field="item_number"),),
                grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",))
    base.update(over)
    return d.MetricDefinition(**base)


def _row(item, method="ConfirmPickLine"):
    return {"method": method, "item_number": item, "transaction_name": "Pick", "status": "success"}


def test_fold_builds_the_sketch_and_skips_absent_values():
    rows = [_row("A"), _row("B"), _row("A"), _row(None), _row(""), _row("C", method="Other")]
    folded = d.fold(rows, _definition())["items"]
    assert hll.estimate(folded[d.Role.distinct_sketch]) == 2
    assert folded[d.Role.count_value] == 3, "rows that carried a value, not rows seen"


def test_fold_counts_a_non_numeric_identity():
    """The measure field is an identity, not a quantity: a user name must not be dropped for failing
    numeric coercion, which is what every other aggregation does with it."""
    rows = [_row("A"), _row("B")]
    for r in rows:
        r["user_name"] = "EDA"
    rows[1]["user_name"] = "MHT"
    folded = d.fold(rows, _definition(measures=(
        d.Measure(name="users", aggregation=d.Aggregation.distinct, field="user_name"),)))["users"]
    assert hll.estimate(folded[d.Role.distinct_sketch]) == 2


def test_add_roles_unions_sketches():
    a = d.fold([_row("A"), _row("B")], _definition())["items"]
    b = d.fold([_row("B"), _row("C")], _definition())["items"]
    merged = d.add_roles(a, b)
    assert hll.estimate(merged[d.Role.distinct_sketch]) == 3
    assert merged[d.Role.count_value] == 4


def test_public_roles_replace_the_sketch_with_an_integer_estimate():
    folded = d.fold([_row("A"), _row("B")], _definition())["items"]
    public = d.public_roles(folded)
    assert public == {"distinct_estimate": 2, "count_value": 2}
    assert not any(isinstance(v, bytes) for v in public.values())


# =============================================================== 3. the cascade, end to end

async def _plant(rows):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for at, item in rows:
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success, item_number=item,
                user_name="EDA", warehouse="BRI", attributes={QF["ConfirmPickLine"]: "10.0"}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()


async def _register(**over) -> uuid.UUID:
    async with async_session() as db:
        row = AnalyticsMetric(**registry.to_row(
            d.MetricDefinition(**{**_definition().__dict__, "status": d.Status.active, **over}),
            customer_code=CC, created_by="test"))
        db.add(row)
        await db.commit()
        return row.id


async def _rows(model, definition_id):
    async with async_session() as db:
        return list((await db.execute(select(model).where(
            model.customer_code == CC, model.definition_id == definition_id,
            model.measure_name == "items"))).scalars().all())


async def test_the_cascade_unions_rather_than_sums():
    """Item A in two different hours is one item in the day. A sum of hourly distinct counts would say
    two, which is the whole reason the role is a sketch."""
    mid = await _register()
    await _plant([(T0, "A"), (T0, "B"), (T0 + timedelta(hours=1), "A"),
                  (T0 + timedelta(hours=1), "C"), (T0 + timedelta(hours=2), "A")])
    await n3.consume_tenant(CC)

    hourly = sorted(await _rows(AnalyticsHourlyRollup, mid), key=lambda r: r.bucket_start)
    assert [hll.estimate(r.distinct_sketch) for r in hourly] == [2, 2, 1]
    assert [r.count_value for r in hourly] == [2, 2, 1]

    daily = await _rows(AnalyticsDailyRollup, mid)
    assert len(daily) == 1 and hll.estimate(daily[0].distinct_sketch) == 3
    assert daily[0].count_value == 5

    monthly = await _rows(AnalyticsMonthlyRollup, mid)
    assert len(monthly) == 1 and hll.estimate(monthly[0].distinct_sketch) == 3, \
        "monthly is folded from daily rows, so it must union the stored sketch"


async def test_series_returns_an_integer_estimate_and_no_bytes():
    mid = await _register()
    await _plant([(T0, "A"), (T0, "B"), (T0 + timedelta(hours=1), "A")])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        state = await db.scalar(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))
        out = await n6.series(db, CC, mid, _definition(), measure="items",
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE),
                              watermark=state.analytics_watermark, group_by=())
    assert out["grain"] == "hourly" and len(out["points"]) == 2, out
    roles = [p["roles"] for p in out["points"]]
    assert [r["distinct_estimate"] for r in roles] == [2, 1]
    assert sum(r["count_value"] for r in roles) == 3
    assert all("distinct_sketch" not in r for r in roles)
    assert not any(isinstance(v, bytes) for r in roles for v in r.values())


async def test_the_catalog_marks_a_distinct_measure_approximate():
    mid = await _register()
    async with async_session() as db:
        body = await catalog.build(db, CC)
    metric = next(m for m in body["metrics"] if str(m["id"]) == str(mid))
    assert metric["measures"][0]["approximate"] is True
    exact = [a for a in body["aggregations"] if a["name"] == "sum"]
    approx = [a for a in body["aggregations"] if a["name"] == "distinct"]
    assert exact and exact[0]["approximate"] is False
    assert approx and approx[0]["approximate"] is True


async def test_a_distinct_metric_previews_with_an_estimate():
    await _plant([(T0, "A"), (T0, "B"), (T0 + timedelta(hours=1), "A")])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.preview_metric(payload={
            "name": "items", "dimensions": ["method"],
            "measures": [{"name": "items", "aggregation": "distinct", "field": "item_number"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": []},
            "grains": ["daily"], "source": "transaction"}, window_hours=None, customer=CC, db=db)
    assert out["ok"], out["problems"]
    point = out["sample"]["points"][0]
    assert point["roles"]["distinct_estimate"] == 2
