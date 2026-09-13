"""Chunk 85, part 1 of the metric builder: the preview, a dry run of a definition over real facts.

What the wizard needs to know before anyone activates a metric
--------------------------------------------------------------
    is the definition legal                 `definition.validate`, unchanged
    does anything match its filters         zero matching rows is a refusal, with the counts
    does the field it aggregates exist      share of matching rows carrying a numeric value: WARN, allow
    which methods sit behind the filter     per-method counts, so eleven methods behind one name are seen
    how many rollup rows will it produce    distinct dimension combinations per hourly bucket
    what will the chart look like           a sample series folded with the WRITER's own functions

Nothing is written. The pure parts take rows already in memory and are tested without a database; the
endpoint reads a bounded window of facts and hands them over.

Decision pinned here (2026-09-12): a sparse aggregated field is a warning with the percentage, never a
refusal. A quantity summed over a method that carries none is still refused, by the existing rule.
"""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.customer import Customer
from app.services.analytics import definition as d
from app.services.analytics import preview

CC = "test_chunk85"
T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


# =============================================================== pure: rows in memory

def _row(method, name, at, *, quantity=None, attrs=None, warehouse="BRI", status="success"):
    return {"method": method, "transaction_name": name, "event_time": at,
            "business_date": at.date(), "quantity": quantity, "status": status,
            "quantity_classification": "pick" if quantity is not None else "non_quantity",
            "warehouse": warehouse, "attributes": attrs or {}}


def _definition(*, dims=("method",), field="quantity", aggregation=d.Aggregation.sum,
                methods=("ConfirmPickLine",), transactions=(), source="transaction"):
    return d.MetricDefinition(
        name="probe", dimensions=tuple(dims),
        measures=(d.Measure("m", aggregation, field=field),),
        grains=("hourly", "daily", "monthly"), method_filter=tuple(methods),
        transaction_filter=tuple(transactions), source=source)


ROWS = [
    _row("ConfirmPickLine", "Brighton Stock Pick", T0, quantity=Decimal("3.75"),
         attrs={"resp.CustomerNumber": "C1"}),
    _row("ConfirmPickLine", "Brighton Stock Pick", T0 + timedelta(minutes=10), quantity=Decimal("2")),
    _row("ReportCount", "Brighton Stock Pick", T0 + timedelta(minutes=20), quantity=Decimal("4")),
    _row("GetNextDeliveryByRoute", "Brighton Stock Pick", T0 + timedelta(hours=1),
         attrs={"resp.CustomerNumber": "C2"}),
    _row("LogSignOff", "Stock Move", T0 + timedelta(hours=2), warehouse=None),
]


def test_matching_counts_rows_behind_the_filter_per_method_and_name():
    m = preview.matching(ROWS, _definition(methods=(), transactions=("Brighton Stock Pick",)))
    assert m.total == 4
    assert m.per_method == {"ConfirmPickLine": 2, "ReportCount": 1, "GetNextDeliveryByRoute": 1}
    assert m.per_transaction == {"Brighton Stock Pick": 4}
    assert len(m.rows) == 4


def test_zero_matches_is_a_refusal_not_a_zero_chart():
    out = preview.assess(ROWS, _definition(methods=("NeverHappened",)), problems=[])
    assert out["ok"] is False
    assert out["matches"]["total"] == 0
    assert any("no facts match" in r for r in out["refusals"])


def test_field_coverage_is_reported_as_a_percentage_and_only_warns():
    dfn = _definition(methods=(), transactions=("Brighton Stock Pick",),
                      field="attr:resp.CustomerNumber", aggregation=d.Aggregation.count)
    out = preview.assess(ROWS, dfn, problems=[])
    (cov,) = out["field_coverage"]
    assert cov == {"measure": "m", "field": "attr:resp.CustomerNumber", "total": 4,
                   "present": 2, "numeric": 0, "percent_present": 50.0, "percent_numeric": 0.0}
    assert out["ok"] is True, "sparse is a warning, never a refusal"
    assert any("50.0%" in w for w in out["warnings"])


def test_a_numeric_field_counts_only_values_that_parse():
    rows = ROWS + [_row("ConfirmPickLine", "Brighton Stock Pick", T0 + timedelta(minutes=30),
                        quantity=None, attrs={"resp.QuantityOnHand": "abc"})]
    dfn = _definition(field="quantity")
    (cov,) = preview.assess(rows, dfn, problems=[])["field_coverage"]
    assert (cov["total"], cov["present"], cov["numeric"]) == (3, 2, 2)


def test_dimension_coverage_reports_null_shares():
    dfn = _definition(dims=("method", "warehouse"), methods=())
    out = preview.assess(ROWS, dfn, problems=[])
    by_name = {x["name"]: x for x in out["dimension_coverage"]}
    assert by_name["method"]["percent_present"] == 100.0
    assert by_name["warehouse"]["percent_present"] == 80.0, "one of five rows has no warehouse"


def test_budget_counts_distinct_dimension_combinations_per_hour():
    dfn = _definition(dims=("method", "transaction_name"), methods=())
    b = preview.assess(ROWS, dfn, problems=[])["budget"]
    # hour T0: (ConfirmPickLine, BSP), (ReportCount, BSP) -> 2 combos; T0+1h: 1; T0+2h: 1
    assert b["hours_observed"] == 3
    assert b["combos_per_hour_max"] == 2
    assert b["combos_per_hour_mean"] == pytest.approx(4 / 3, rel=1e-3)
    assert b["projected_rollup_rows_per_day"] == pytest.approx(4 / 3 * 24 * 1, rel=1e-3)
    assert b["warning"] is False


def test_budget_warns_above_the_read_planners_assumption():
    rows = [_row("M", f"name-{i}", T0 + timedelta(seconds=i)) for i in range(25)]
    dfn = _definition(dims=("transaction_name",), methods=())
    b = preview.assess(rows, dfn, problems=[])["budget"]
    assert b["combos_per_hour_max"] == 25 and b["warning"] is True


def test_sample_series_is_folded_with_the_writers_functions():
    """Daily grain, one point per (day, dimension tuple), additive roles as strings like /series."""
    dfn = _definition(dims=("method",), methods=("ConfirmPickLine", "ReportCount"))
    out = preview.assess(ROWS, dfn, problems=[])
    points = {(p["bucket"], tuple(p["dimensions"])): p["roles"] for p in out["sample"]["points"]}
    assert points[(str(T0.date()), ("ConfirmPickLine",))]["sum_value"] == "5.75"
    assert points[(str(T0.date()), ("ConfirmPickLine",))]["count_value"] == 2
    assert points[(str(T0.date()), ("ReportCount",))]["sum_value"] == "4"
    assert out["sample"]["grain"] == "daily"
    assert out["sample"]["measure"] == "m"


def test_problems_make_the_preview_not_ok_but_still_describe_the_data():
    out = preview.assess(ROWS, _definition(), problems=["dimension 'nope' is not a field"])
    assert out["ok"] is False
    assert out["problems"] == ["dimension 'nope' is not a field"]
    assert out["matches"]["total"] == 2, "the counts are still computed so the person can fix the shape"


def test_the_preview_says_when_the_sample_was_truncated():
    out = preview.assess(ROWS, _definition(methods=()), problems=[], truncated=True)
    assert out["sample"]["truncated"] is True
    assert any("truncated" in w for w in out["warnings"])


# =============================================================== endpoint: facts in the database

async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsFact, AnalyticsRecordFact, AnalyticsFieldRegistry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="preview probe", timezone="Europe/London"))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="GetNextDeliveryByRoute",
                                      source="response", field="resp.CustomerNumber", captured=True))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine",
                                      source="record", field="rec.STQT", captured=True))
        db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine",
                                      source="record", field="rec.ITNO", captured=True))
        await db.commit()
    yield
    await _wipe()


def _fact(method, name, at, *, quantity=None, attrs=None, warehouse="BRI"):
    return AnalyticsFact(
        customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=at,
        source_version_hash=uuid.uuid4().hex, revision=1, event_time=at, business_date=at.date(),
        duration_ms=100, method=method, transaction_name=name, status="success",
        quantity=quantity, quantity_classification="pick" if quantity is not None else "non_quantity",
        warehouse=warehouse, warehouse_id="1", attributes=attrs or {})


async def _plant(now):
    async with async_session() as db:
        for i in range(6):
            db.add(_fact("ConfirmPickLine", "Brighton Stock Pick", now - timedelta(hours=i + 1),
                         quantity=Decimal("1.5")))
        for i in range(3):
            db.add(_fact("GetNextDeliveryByRoute", "Brighton Stock Pick", now - timedelta(hours=i + 2),
                         attrs={"resp.CustomerNumber": f"C{i}"}))
        db.add(_fact("ListPickLinesByUser", "Brighton Stock Pick", now - timedelta(hours=3)))
        db.add(_fact("ConfirmPickLine", "Brighton Stock Pick", now - timedelta(days=30),
                     quantity=Decimal("99")))                        # outside the preview window
        db.add(AnalyticsRecordFact(
            customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=now - timedelta(hours=1),
            record_index=0, event_time=now - timedelta(hours=1), business_date=(now - timedelta(hours=1)).date(),
            method="ConfirmPickLine", transaction_name="Brighton Stock Pick",
            mi_program="MMS060MI", mi_transaction="LstBalID",
            attributes={"rec.STQT": "79.7", "rec.ITNO": "104609"}))
        await db.commit()


async def _preview(body, **kw):
    async with async_session() as db:
        return await api.preview_metric(payload=body, window_hours=kw.pop("window_hours", 168),
                                        customer=CC, db=db)


def _body(**over):
    body = {"name": "probe", "dimensions": ["method", "transaction_name"],
            "measures": [{"name": "quantity", "aggregation": "sum", "field": "quantity"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": ["Brighton Stock Pick"]},
            "grains": ["hourly", "daily", "monthly"], "source": "transaction"}
    body.update(over)
    return body


async def test_preview_reads_the_window_and_lists_the_methods_behind_a_name():
    now = datetime.now(timezone.utc)
    await _plant(now)
    out = await _preview(_body(filter={"methods": [], "transactions": ["Brighton Stock Pick"]},
                               measures=[{"name": "n", "aggregation": "count"}]))
    assert out["ok"] is True
    assert out["matches"]["total"] == 10, "the 30-day-old fact is outside the 168 h window"
    assert out["matches"]["per_method"] == {"ConfirmPickLine": 6, "GetNextDeliveryByRoute": 3,
                                            "ListPickLinesByUser": 1}
    assert out["window"]["hours"] == 168
    assert out["written"] is False


async def test_preview_warns_on_a_sparse_field_with_the_real_percentage():
    now = datetime.now(timezone.utc)
    await _plant(now)
    out = await _preview(_body(filter={"methods": [], "transactions": ["Brighton Stock Pick"]},
                               measures=[{"name": "customers", "aggregation": "count",
                                          "field": "attr:resp.CustomerNumber"}]))
    assert out["ok"] is True
    (cov,) = out["field_coverage"]
    assert (cov["total"], cov["present"]) == (10, 3)
    assert cov["percent_present"] == 30.0
    assert any("30.0%" in w for w in out["warnings"])


async def test_preview_refuses_when_nothing_matches():
    await _plant(datetime.now(timezone.utc))
    out = await _preview(_body(filter={"methods": ["NoSuchMethod"], "transactions": []}))
    assert out["ok"] is False and out["matches"]["total"] == 0


async def test_preview_returns_every_problem_and_still_counts():
    await _plant(datetime.now(timezone.utc))
    out = await _preview(_body(dimensions=["method", "no_such_column"]))
    assert out["ok"] is False
    assert any("no_such_column" in p for p in out["problems"])
    assert out["matches"]["total"] == 6


async def test_preview_reads_record_facts_for_a_record_source_definition():
    await _plant(datetime.now(timezone.utc))
    out = await _preview(_body(source="record", dimensions=["attr:rec.ITNO"],
                               measures=[{"name": "units", "aggregation": "sum", "field": "attr:rec.STQT"}],
                               filter={"methods": [], "transactions": ["Brighton Stock Pick"]}))
    assert out["ok"] is True
    assert out["matches"]["total"] == 1
    (cov,) = out["field_coverage"]
    assert cov["numeric"] == 1, "'79.7' is a string in JSONB and must still count as numeric"
    (point,) = out["sample"]["points"]
    assert point["dimensions"] == ["104609"] and point["roles"]["sum_value"] == "79.7"


async def test_preview_sample_and_budget_are_present_for_a_healthy_definition():
    await _plant(datetime.now(timezone.utc))
    out = await _preview(_body())
    assert out["budget"]["combos_per_hour_max"] == 1
    assert out["budget"]["warning"] is False
    assert sum(Decimal(p["roles"]["sum_value"]) for p in out["sample"]["points"]) == Decimal("9.0")


async def test_a_malformed_measure_is_a_400_like_create():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        await _preview(_body(measures=[{"name": "x", "aggregation": "not-a-thing"}]))
    assert exc.value.status_code == 400
