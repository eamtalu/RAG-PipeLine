"""Step 35 (chunk 213): the hourly forecast (lines and pickers needed) per warehouse, not only for
the whole customer.

Found on 16 November 2026: a site's Overview forecast card showed every shift "quiet" because the
hourly rows were written only for `total` while the card asks for the site's warehouse. Each
warehouse now gets its own hourly rows, cut from its own daily forecast by its own hour-of-day
profile and throughput, and they are scored against that warehouse's own hours. The whole
customer's rows are made exactly as before."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_ml import AnalyticsPrediction
from app.services.analytics_forecast import plan, runner
from tests import forecast_fixtures as fx

CC = "test_chunk213fc"
FIRST = date(2026, 9, 7)  # a Monday
AS_OF = date(2026, 10, 4)  # the Sunday four weeks later
UTC = timezone.utc
CFG = plan.ForecastConfig(
    settlement=fx.SETTLEMENT, units_value="picked", history_days=120, min_history_days=14, min_days_ets=21,
    item_daily_min_active_days=10, max_items=200, backtest_folds=4, score_lag_hours=2,
    accuracy_window_days=28, staffing_buffer_pct=0.10)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _day_rows(day: date) -> list:
    """Brighton 60 lines by 6 pickers every day; London 12 lines by 2 pickers."""
    at = datetime(day.year, day.month, day.day)
    return (fx.shift_rows(CC, at, lines=60, pickers=6, warehouse="BRI")
            + fx.shift_rows(CC, at, lines=12, pickers=2, warehouse="LON"))


async def _plant_weeks(start: date, days: int) -> None:
    rows = []
    for i in range(days):
        rows.extend(_day_rows(start + timedelta(days=i)))
    await fx.plant(rows)


async def _rows(**where):
    async with async_session() as db:
        q = select(AnalyticsPrediction).where(AnalyticsPrediction.customer_code == CC)
        for k, v in where.items():
            q = q.where(getattr(AnalyticsPrediction, k) == v)
        return (await db.execute(q.order_by(AnalyticsPrediction.target_at))).scalars().all()


async def test_each_warehouse_gets_hourly_lines_and_pickers_from_its_own_daily_forecast():
    await _plant_weeks(FIRST, 28)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    assert out["status"] == "completed", out
    for wh in ("BRI", "LON"):
        hours = await _rows(metric="lines", grain="hour", subject_kind="warehouse", subject=wh)
        pickers = await _rows(metric="pickers", grain="hour", subject_kind="warehouse", subject=wh)
        assert len(hours) == 8 * 24 and len(pickers) == 8 * 24, wh
        day0 = (await _rows(metric="lines", grain="day", subject_kind="warehouse", subject=wh))[0]
        first_day = [r for r in hours if r.target_at < datetime(2026, 10, 5, 23, 0, tzinfo=UTC)]
        # the hours add back up to the warehouse's own day, as the total's do
        assert sum(float(r.value) for r in first_day) == pytest.approx(float(day0.value), rel=1e-6), wh
    bri = max(int(r.value) for r in await _rows(metric="pickers", grain="hour", subject_kind="warehouse", subject="BRI"))
    lon = max(int(r.value) for r in await _rows(metric="pickers", grain="hour", subject_kind="warehouse", subject="LON"))
    # London is a fifth of Brighton's volume, so it never needs more pickers than Brighton
    assert 1 <= lon <= bri


async def test_the_whole_customers_hourly_rows_are_unchanged_by_the_warehouses():
    await _plant_weeks(FIRST, 28)
    await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    hours = await _rows(metric="lines", grain="hour", subject_kind="total", subject="total")
    total_day0 = (await _rows(metric="lines", grain="day", subject_kind="total", subject="total"))[0]
    first_day = [r for r in hours if r.target_at < datetime(2026, 10, 5, 23, 0, tzinfo=UTC)]
    assert len(hours) == 8 * 24
    assert sum(float(r.value) for r in first_day) == pytest.approx(float(total_day0.value), rel=1e-6)
    # the total is cut by the whole customer's profile, not by summing the warehouses
    assert all(r.detail.get("subject_scope") is None for r in hours)


async def test_a_warehouses_hours_are_scored_against_that_warehouses_own_hours():
    await _plant_weeks(FIRST, 28)
    await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    await fx.plant(_day_rows(AS_OF + timedelta(days=1)))
    await runner.run_tenant(CC, as_of_date=AS_OF + timedelta(days=1), trigger="manual", cfg=CFG,
                            now=datetime(2026, 10, 6, 3, 0, tzinfo=UTC))
    lon = [r for r in await _rows(metric="lines", grain="hour", subject_kind="warehouse", subject="LON") if r.scored_at]
    total = {r.target_at: r for r in await _rows(metric="lines", grain="hour", subject_kind="total", subject="total")
             if r.scored_at}
    assert len(lon) == 24
    assert sum(r.actual for r in lon) == Decimal(12)  # London's 12 lines, not the 72 of the whole customer
    assert sum(r.actual for r in total.values()) == Decimal(72)


def test_actuals_read_a_warehouses_hour_from_its_own_hours():
    hour = datetime(2026, 10, 5, 15, 0)
    bundle = type("B", (), {})()
    bundle.start, bundle.daily, bundle.items, bundle.items_truncated = AS_OF - timedelta(days=30), [], [], False
    bundle.hourly = [plan.hourly.HourRow(start=hour, lines=10.0, pickers=3)]
    bundle.hourly_by_warehouse = {"LON": [plan.hourly.HourRow(start=hour, lines=2.0, pickers=1)]}
    actuals = plan.Actuals(bundle, as_of=AS_OF + timedelta(days=2), cfg=CFG)
    at = hour.replace(tzinfo=fx.LONDON)
    assert actuals.value(metric="lines", grain="hour", subject_kind="total", subject="total", target_at=at, tz=fx.LONDON) == Decimal("10")
    assert actuals.value(metric="pickers", grain="hour", subject_kind="warehouse", subject="LON", target_at=at, tz=fx.LONDON) == Decimal(1)
    # a warehouse the run read no hours for has no actual, rather than the whole customer's
    assert actuals.value(metric="lines", grain="hour", subject_kind="warehouse", subject="BIC", target_at=at, tz=fx.LONDON) is None
