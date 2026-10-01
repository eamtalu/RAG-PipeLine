"""Chunk 139: one tenant's run, end to end against the tables.

Plant four weeks of a shift-shaped operation, run the forecast as of the last planted day, and pin
what lands: daily, weekly and monthly rows for the headline series, per-transaction rows, hourly
lines and pickers for the heatmap, daily rows only for the items with enough active days, and a
`detail` that names the model and its backtest. Then the next day's actuals arrive and the first
out-of-sample score appears. A re-run changes nothing; too little history is a skip, not a guess.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_forecast import AnalyticsForecastSeries
from app.persistence.models.analytics_ml import AnalyticsPrediction
from app.services.analytics_forecast import accuracy_store, plan, run_store, runner
from tests import forecast_fixtures as fx

CC = "test_chunk139fc"
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
    """Weekdays 60 lines by 6 pickers over two items; weekends 30 lines by 3. A third item picked only
    on Mondays. A JIT transaction on top on every day."""
    weekend = day.weekday() >= 5
    at = datetime(day.year, day.month, day.day)
    rows = fx.shift_rows(CC, at, lines=30 if weekend else 60, pickers=3 if weekend else 6)
    rows += fx.shift_rows(CC, at, lines=10, pickers=2, items=("200001",), tx="JIT and Shorts Pick (Brighton)")
    if day.weekday() == 0:
        rows += fx.shift_rows(CC, at, lines=4, pickers=1, items=("300777",))
    return rows


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


async def test_a_run_writes_every_grain_for_the_headline_and_the_right_grains_for_items():
    await _plant_weeks(FIRST, 28)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    assert out["status"] == "completed", out

    total_day = await _rows(metric="lines", grain="day", subject_kind="total", subject="total")
    assert [r.horizon for r in total_day] == [f"{k}d" for k in range(1, 15)]
    assert total_day[0].target_at == datetime(2026, 10, 4, 23, 0, tzinfo=UTC)  # London midnight 5 Oct
    # Monday 5 Oct: 60 + 10 + 4 lines on every past Monday, so the forecast is close to 74
    assert 60 <= float(total_day[0].value) <= 90
    assert float(total_day[0].p10) <= float(total_day[0].value) <= float(total_day[0].p90)

    assert [r.horizon for r in await _rows(metric="lines", grain="week", subject_kind="total", subject="total")] \
        == ["0w", "1w", "2w", "3w", "4w"]
    months = await _rows(metric="lines", grain="month", subject_kind="total", subject="total")
    assert [r.horizon for r in months] == ["0m", "1m", "2m", "3m"]
    assert months[0].detail["partial"] is True and months[0].detail["actual_to_date"] is not None

    assert len(await _rows(metric="units", grain="day", subject_kind="transaction_name",
                           subject="JIT and Shorts Pick (Brighton)")) == 14
    assert len(await _rows(metric="lines", grain="day", subject_kind="warehouse", subject="BRI")) == 14

    hours = await _rows(metric="lines", grain="hour", subject_kind="total", subject="total")
    pickers = await _rows(metric="pickers", grain="hour", subject_kind="total", subject="total")
    assert len(hours) == 8 * 24 and len(pickers) == 8 * 24
    first_day = [r for r in hours if r.target_at < datetime(2026, 10, 5, 23, 0, tzinfo=UTC)]
    assert sum(float(r.value) for r in first_day) == pytest.approx(float(total_day[0].value), rel=1e-6)
    assert all(float(r.value) == 0 for r in hours if 8 <= r.target_at.astimezone(fx.LONDON).hour < 14)
    assert max(int(r.value) for r in pickers) >= 1

    # items: the two busy items get daily rows, the Monday-only item gets weekly and monthly rows only
    assert len(await _rows(metric="units", grain="day", subject_kind="item_number", subject="104568")) == 14
    assert len(await _rows(metric="units", grain="day", subject_kind="item_number", subject="300777")) == 0
    assert len(await _rows(metric="units", grain="week", subject_kind="item_number", subject="300777")) == 5

    detail = total_day[0].detail
    assert detail["model"] in {"seasonal_naive", "weekday_mean", "ets_add_weekly", "blend_top2", "moving_average"}
    assert detail["backtest"]["wape"] is not None and detail["backtest"]["folds"] >= 1
    assert detail["classification"] == "smooth"
    assert detail["history"]["start"] == FIRST.isoformat() and detail["history"]["end"] == AS_OF.isoformat()

    async with async_session() as db:
        series_rows = (await db.execute(select(AnalyticsForecastSeries).where(
            AnalyticsForecastSeries.customer_code == CC))).scalars().all()
        run = await run_store.latest(db, CC)
    by_key = {(s.metric, s.grain, s.subject_kind, s.subject): s for s in series_rows}
    assert by_key[("units", "day", "item_number", "300777")].classification == "intermittent"
    assert by_key[("lines", "day", "total", "total")].model == detail["model"]
    assert run.status == "completed" and run.points_written == out["points_written"] > 0
    assert run.detail["history"]["days"] == 28 and run.detail["series"]["items"] == 4  # 104568, 100944, 200001, 300777


async def test_running_the_same_night_twice_changes_nothing():
    await _plant_weeks(FIRST, 28)
    first = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    second = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    async with async_session() as db:
        n = await db.scalar(select(func.count()).select_from(AnalyticsPrediction).where(AnalyticsPrediction.customer_code == CC))
    assert first["points_written"] == second["points_written"] == n


async def test_the_next_days_actuals_produce_the_first_out_of_sample_score():
    await _plant_weeks(FIRST, 28)
    await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    await fx.plant(_day_rows(AS_OF + timedelta(days=1)))
    out = await runner.run_tenant(CC, as_of_date=AS_OF + timedelta(days=1), trigger="manual", cfg=CFG,
                                  now=datetime(2026, 10, 6, 3, 0, tzinfo=UTC))
    assert out["scored"] >= 1
    scored = [r for r in await _rows(metric="lines", grain="day", subject_kind="total", subject="total") if r.scored_at]
    assert len(scored) == 1 and scored[0].horizon == "1d"
    monday = AS_OF + timedelta(days=1)
    assert scored[0].actual == Decimal(await fx.lines_between(CC, monday, monday))
    async with async_session() as db:
        acc = await accuracy_store.read_series(db, CC, metric="lines", grain="day", subject_kind="total", subject="total")
    assert len(acc) == 1 and acc[0].horizon == "1d" and acc[0].n == 1
    # the heatmap's hours for 5 Oct have closed too, so they are scored against the hourly actuals
    hour_scored = [r for r in await _rows(metric="pickers", grain="hour", subject_kind="total", subject="total") if r.scored_at]
    assert len(hour_scored) == 24


async def test_too_little_history_is_a_skip_with_no_rows():
    await _plant_weeks(AS_OF - timedelta(days=6), 7)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    assert out["status"] == "skipped" and "insufficient_history" in out["reason"]
    assert await _rows() == []
    async with async_session() as db:
        assert (await run_store.latest(db, CC)).status == "skipped"


async def test_a_rollout_ramp_is_trimmed_and_the_trim_is_visible():
    ramp = []
    for i in range(10):
        day = FIRST + timedelta(days=i)
        ramp += fx.shift_rows(CC, datetime(day.year, day.month, day.day), lines=4, pickers=1)
    await fx.plant(ramp)
    await _plant_weeks(FIRST + timedelta(days=10), 18)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG)
    assert out["status"] == "completed"
    async with async_session() as db:
        run = await run_store.latest(db, CC)
    assert run.detail["history"]["steady_from"] == (FIRST + timedelta(days=10)).isoformat()
    assert run.detail["history"]["ramp_trimmed_days"] == 10
    assert any("ets" in w for w in run.detail["warnings"])  # 18 steady days < 21
