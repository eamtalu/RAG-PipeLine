"""Chunk 142: the forecast block of the Teams Home snapshot.

The tab on the edge draws, never computes, so everything it shows about the forecast is decided
here: four tiles as text, fifteen days (seven back, today, seven ahead) with actual and forecast
side by side, the next seven shifts' peak pickers, and the confidence note in plain words. The
block comes from the same tables the web page reads, so the two can never disagree.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from app.config.database import async_session
from app.services.analytics_forecast import plan, runner
from app.services.teams import home_forecast, home_snapshot
from tests import forecast_fixtures as fx
from tests.test_analytics_forecast_runner_chunk139 import _day_rows

CC = "test_chunk142fc"
FIRST = date(2026, 9, 7)
AS_OF = date(2026, 10, 4)
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=timezone.utc)  # Monday 5 Oct, 15:30 London
UTC = timezone.utc

CFG = plan.ForecastConfig(
    settlement=fx.SETTLEMENT, units_value="picked", history_days=120, min_history_days=14, min_days_ets=21,
    item_daily_min_active_days=10, max_items=200, backtest_folds=3, score_lag_hours=2,
    accuracy_window_days=28, staffing_buffer_pct=0.10)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


async def _plant_and_run(days: int = 28) -> None:
    rows = []
    for i in range(days):
        for r in _day_rows(FIRST + timedelta(days=i)):
            r.customer_code = CC
            rows.append(r)
    await fx.plant(rows)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG, now=NOW)
    assert out["status"] == "completed", out


async def test_without_a_run_the_block_says_so_and_nothing_else():
    async with async_session() as db:
        block = await home_forecast.compute(db, CC, NOW, fx.LONDON)
    assert block == {"available": False, "note": "no forecast run yet · the model runs nightly after the pick window closes"}


async def test_the_block_carries_tiles_days_shifts_and_the_confidence_note():
    await _plant_and_run()
    async with async_session() as db:
        block = await home_forecast.compute(db, CC, NOW, fx.LONDON)
    assert block["available"] is True
    assert block["as_of_text"] == "as of Sun 4 Oct" and block["model"]
    assert block["confidence"] == "low"
    assert block["note"].startswith("indicative only · 28 days of steady history")

    tiles = {t["id"]: t for t in block["tiles"]}
    assert list(tiles) == ["tomorrow", "week", "accuracy", "tonight"]
    assert tiles["tomorrow"]["label"] == "Lines tomorrow" and tiles["tomorrow"]["value"].isdigit() or "," in tiles["tomorrow"]["value"]
    assert tiles["tomorrow"]["caption"].startswith("p10 – p90 ")
    assert tiles["week"]["label"] == "Lines next 7 days"
    assert tiles["accuracy"]["value"].endswith("%") and "backtest" in tiles["accuracy"]["caption"]
    assert "not enough data yet" in tiles["accuracy"]["caption"]
    assert tiles["tonight"]["label"] == "Pickers · tonight's peak" and tiles["tonight"]["value"].isdigit()
    assert "–" in tiles["tonight"]["caption"] and "lines" in tiles["tonight"]["caption"]

    days = block["days"]
    assert len(days) == 15
    assert [d["kind"] for d in days] == ["past"] * 7 + ["today"] + ["ahead"] * 7
    assert days[7]["label"] == "Mon 5" and days[7]["actual_to_date_text"] is not None and days[7]["p50"] is not None
    assert days[0]["label"] == "Mon 28" and days[0]["actual"] is not None and days[0]["p50"] is None  # no forecast existed yet
    assert days[8]["actual"] is None and days[8]["p50"] is not None and days[8]["p10"] <= days[8]["p50"] <= days[8]["p90"]
    assert block["axis"]["top"] >= max(d["p90"] or 0 for d in days)
    assert all(isinstance(d["p50_text"], str) for d in days)

    shifts = block["shifts"]
    assert len(shifts) == 7 and shifts[0]["label"] == "Mon 5" and shifts[0]["quiet"] is False
    assert shifts[0]["peak_text"].endswith("pickers") and shifts[0]["peak_hour"].endswith(":00")
    assert all(s["picker_hours_text"].endswith("picker-hours") for s in shifts)
    assert block["caption"] == "last 7 days against the forecast made before each · then 7 days ahead"


async def test_the_snapshot_carries_the_forecast_block():
    await _plant_and_run()
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    assert snap is not None and snap["forecast"]["available"] is True
    assert len(snap["forecast"]["days"]) == 15
