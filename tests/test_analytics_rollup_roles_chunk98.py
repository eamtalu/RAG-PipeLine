"""Chunk 98: every role the fold WRITES must survive the read, and no number may leave as `4E+1`.

Two defects, found while measuring the warehouse metric work on live data.

**1. Three roles are written and never read.** `rollups._rows_for` stores `min_value`, `max_value` and
`sum_sq` like any other role, and `_ROLE_COLUMN` maps them, but `read._rollup_points` selected only
`sum_value`, `count_value`, `distinct_sketch` and `histogram`. So an `extent` measure read back EMPTY
from the rollup tier while the live tier answered correctly, and a `stats` measure silently lost its
spread. Nothing in production uses those two aggregations yet, which is the only reason it had not
shown. The rule the fix restores: a role a definition declares is a role the reader returns.

**2. A round number serialised into scientific notation.** `preview._role_json` called
`Decimal.normalize()` to tidy `40.000000` into `40`, but normalize STRIPS trailing zeros by raising the
exponent, so `Decimal("40")` becomes `4E+1` and reaches a chart looking like that. Seen live on a stock
move preview. `format(v, "f")` is the fixed-point spelling that tidies without ever going exponential.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.customer import Customer
from app.services.analytics import definition as d
from app.services.analytics import read as n6
from app.services.analytics import rollups as n5
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk98"
T0 = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsMetric)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="role probe", timezone="UTC"))
        await db.commit()
    yield
    await _wipe()


def _definition(aggregation: d.Aggregation, name: str):
    return d.MetricDefinition(
        name="roles", dimensions=("method",),
        measures=(d.Measure(name, aggregation, field="quantity",
                            unit="each" if aggregation in (d.Aggregation.extent,) else None),),
        grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",))


def _fact(at, qty):
    return AnalyticsFact(
        customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=at,
        source_version_hash=uuid.uuid4().hex, revision=1, event_time=at, business_date=at.date(),
        duration_ms=100, method="ConfirmPickLine", transaction_name="Pick", status="success",
        quantity=Decimal(qty), quantity_classification="pick", warehouse="BRI", attributes={})


async def _fold_and_read(definition, *, measure, quantities, at=T0):
    """Plant facts, fold them into the rollups, then read the SETTLED tier only."""
    async with async_session() as db:
        for q in quantities:
            db.add(_fact(at, q))
        await db.commit()
    did = uuid.uuid4()
    async with async_session() as db:
        await n5.recompute(db, CC, did, definition, hours={n5.hour_of(at)}, dates={at.date()},
                           tz=None)
        await db.commit()
    async with async_session() as db:
        # A watermark past the whole hour, so the live tier contributes nothing and what comes back
        # is exactly what the rollup table can answer.
        return await n6.series(db, CC, did, definition,
                               window=UtcWindow(start=at.replace(minute=0, second=0,
                                                                 microsecond=0),
                                                end=at.replace(minute=0, second=0,
                                                               microsecond=0) + timedelta(hours=1)),
                               measure=measure, group_by=("method",),
                               watermark=at + timedelta(hours=2))


# ================================================ 1. the roles that were written and never read

async def test_an_extent_measure_returns_its_minimum_and_maximum_from_the_rollups():
    """`extent` declares min_value and max_value and NOTHING else, so a reader that skips those two
    returns an empty point for a measure the fold computed correctly."""
    out = await _fold_and_read(_definition(d.Aggregation.extent, "span"), measure="span",
                               quantities=["3", "11", "7"])
    assert out["from_rollups"] is True, "the whole window is settled, so nothing may come from facts"
    assert out["total"].get("min_value") == "3"
    assert out["total"].get("max_value") == "11"


async def test_a_stats_measure_returns_its_sum_of_squares_from_the_rollups():
    """`stats` is sum, count and sum_sq. Without sum_sq the caller can compute a mean but never a
    spread, which is the only reason to ask for `stats` rather than `average`."""
    out = await _fold_and_read(_definition(d.Aggregation.stats, "spread"), measure="spread",
                               quantities=["3", "4"])
    assert out["total"].get("sum_value") == "7"
    assert out["total"].get("count_value") == 2
    assert out["total"].get("sum_sq") == "25", "3 squared plus 4 squared"


async def test_every_role_a_definition_declares_is_a_role_the_reader_can_return():
    """The structural version of the two tests above, so a NEW aggregation cannot reintroduce the
    same gap by declaring a role the reader forgot."""
    readable = n6.READABLE_ROLES
    for aggregation in d.Aggregation:
        missing = set(d.roles_for(aggregation)) - set(readable)
        assert not missing, f"{aggregation.value} declares {missing}, which no reader returns"


# ================================================ 2. numbers never leave in scientific notation

@pytest.mark.parametrize("raw,expected", [
    ("40", "40"),                    # the live case: str(Decimal("40").normalize()) was "4E+1"
    ("40.000000", "40"),             # the tidying the formatter was added for
    ("100", "100"),
    ("1.500000", "1.5"),
    ("0.000040", "0.00004"),
    ("3775.802096", "3775.802096"),
    ("0", "0"),
    ("-40", "-40"),
])
def test_a_role_number_is_written_in_plain_digits(raw, expected):
    assert d.plain_number(Decimal(raw)) == expected


def test_no_role_number_can_contain_an_exponent():
    """The property, not the examples. Any power of ten is what `normalize` turns exponential."""
    for power in range(0, 12):
        text = d.plain_number(Decimal(10) ** power)
        assert "E" not in text.upper(), f"10^{power} serialised as {text}"


def test_the_public_role_serialiser_uses_the_plain_formatter_by_default():
    """The default matters: `/series` never passed a formatter, which is how the exponent reached a
    chart even though `preview` had noticed the problem and passed one of its own."""
    out = d.public_roles({d.Role.sum_value: Decimal("40"),
                          d.Role.min_value: Decimal("100.000")})
    assert out["sum_value"] == "40"
    assert out["min_value"] == "100"
