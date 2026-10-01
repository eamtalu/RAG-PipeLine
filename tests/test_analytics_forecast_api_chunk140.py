"""Chunk 140: the forecast endpoints, called the way chunk 117 calls the settlement ones.

What a screen sees: three grains in one `/series` call with the actuals merged in from the same
read the model trained on, the week in progress marked partial, a horizon that can be pinned to
"what we said a week out", an accuracy answer that says plainly when nothing is scorable yet, a
heatmap of fixed shape, an items page that pages without overlap, and a trigger that answers 202
and refuses to double-run.
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from app.api.v1 import analytics_forecast as api
from app.config.database import async_session
from app.services.analytics_forecast import plan, runner
from tests import forecast_fixtures as fx
from tests.test_analytics_forecast_runner_chunk139 import _day_rows

CC = "test_chunk140fc"
FIRST = date(2026, 9, 7)
AS_OF = date(2026, 10, 4)
NOW = datetime(2026, 10, 5, 3, 0, tzinfo=timezone.utc)
UTC = timezone.utc

CFG = plan.ForecastConfig(
    settlement=fx.SETTLEMENT, units_value="picked", history_days=120, min_history_days=14, min_days_ets=21,
    item_daily_min_active_days=10, max_items=200, backtest_folds=3, score_lag_hours=2,
    accuracy_window_days=28, staffing_buffer_pct=0.10)


@pytest.fixture(autouse=True)
async def clean(monkeypatch):
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    monkeypatch.setattr(runner, "config_from_settings", lambda cc: CFG)
    monkeypatch.setattr(api, "_now", lambda: NOW)
    yield
    await fx.wipe(CC)


async def _plant_and_run(days: int = 28, as_of: date = AS_OF) -> None:
    rows = []
    # reuse chunk 139's day shape but under this chunk's tenant code
    for i in range(days):
        for r in _day_rows(FIRST + timedelta(days=i)):
            r.customer_code = CC
            rows.append(r)
    await fx.plant(rows)
    out = await runner.run_tenant(CC, as_of_date=as_of, trigger="manual", cfg=CFG, now=NOW)
    assert out["status"] == "completed", out


# ==================================================== 1. series

async def test_series_returns_three_grains_in_one_call_with_actuals_merged():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.read_series(metric="lines", subject_kind="total", subject="total", grains="day,week,month",
                                    days_back=14, weeks_back=4, months_back=3, horizon="latest", customer=CC, db=db)
    assert out["timezone"] == "Europe/London" and out["latest_run"]["as_of_date"] == AS_OF.isoformat()
    day = out["grains"]["day"]
    assert day["history_from"] == FIRST.isoformat() and day["steady_from"] == FIRST.isoformat()
    points = day["points"]
    past = [p for p in points if p["actual"] is not None]
    future = [p for p in points if p["actual"] is None]
    assert len(past) == 14 and len(future) == 14
    assert past[-1]["target"] == AS_OF.isoformat()
    assert past[-1]["actual"] == str(await fx.lines_between(CC, AS_OF, AS_OF))
    assert future[0]["target"] == (AS_OF + timedelta(days=1)).isoformat() and future[0]["horizon"] == "1d"
    assert future[0]["p10"] is not None and future[0]["p50"] is not None and future[0]["p90"] is not None
    assert all(p["p50"] is None for p in past)  # no prediction existed for the past yet

    week = out["grains"]["week"]["points"]
    current = next(p for p in week if p["target"] == "2026-10-05")
    assert current["partial"] is True and current["horizon"] == "0w" and current["actual"] is None
    last_full = next(p for p in week if p["target"] == "2026-09-28")
    assert last_full["actual"] == str(await fx.lines_between(CC, date(2026, 9, 28), date(2026, 10, 4)))
    assert last_full["partial"] is False
    assert len(out["grains"]["month"]["points"]) >= 2


async def test_series_can_be_pinned_to_a_horizon():
    await _plant_and_run()
    for r in _day_rows(AS_OF + timedelta(days=1)):
        r.customer_code = CC
        await fx.plant([r])
    await runner.run_tenant(CC, as_of_date=AS_OF + timedelta(days=1), trigger="manual", cfg=CFG,
                            now=NOW + timedelta(days=1))
    async with async_session() as db:
        latest = await api.read_series(metric="lines", subject_kind="total", subject="total", grains="day",
                                       days_back=3, weeks_back=1, months_back=1, horizon="latest", customer=CC, db=db)
        pinned = await api.read_series(metric="lines", subject_kind="total", subject="total", grains="day",
                                       days_back=3, weeks_back=1, months_back=1, horizon="2d", customer=CC, db=db)
    oct6 = (AS_OF + timedelta(days=2)).isoformat()
    assert next(p for p in latest["grains"]["day"]["points"] if p["target"] == oct6)["horizon"] == "1d"
    assert next(p for p in pinned["grains"]["day"]["points"] if p["target"] == oct6)["horizon"] == "2d"
    yesterday = next(p for p in latest["grains"]["day"]["points"] if p["target"] == (AS_OF + timedelta(days=1)).isoformat())
    monday = AS_OF + timedelta(days=1)
    assert yesterday["actual"] == str(await fx.lines_between(CC, monday, monday))
    assert yesterday["p50"] is not None  # a scored day shows both


async def test_series_rejects_a_bad_metric():
    async with async_session() as db:
        with pytest.raises(HTTPException) as exc:
            await api.read_series(metric="money", subject_kind="total", subject="total", grains="day", days_back=7,
                                  weeks_back=1, months_back=1, horizon="latest", customer=CC, db=db)
    assert exc.value.status_code == 422


# ==================================================== 2. accuracy

async def test_accuracy_says_when_nothing_is_scorable_yet_and_scores_once_it_is():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.read_accuracy(metric="lines", subject_kind="total", subject="total", grain="day", customer=CC, db=db)
    assert out["status"] == "no_out_of_sample_yet" and out["horizons"] == []
    assert out["first_scorable_on"] == (AS_OF + timedelta(days=1)).isoformat()
    assert out["backtest"]["model"] and out["backtest"]["wape"] is not None

    for r in _day_rows(AS_OF + timedelta(days=1)):
        r.customer_code = CC
        await fx.plant([r])
    await runner.run_tenant(CC, as_of_date=AS_OF + timedelta(days=1), trigger="manual", cfg=CFG,
                            now=NOW + timedelta(days=1))
    async with async_session() as db:
        out = await api.read_accuracy(metric="lines", subject_kind="total", subject="total", grain="day", customer=CC, db=db)
    assert out["status"] == "ok"
    assert [h["horizon"] for h in out["horizons"]] == ["1d"] and out["horizons"][0]["n"] == 1
    assert out["window"]["days"] == 28


# ==================================================== 3. heatmap

async def test_heatmap_is_eight_days_by_twenty_four_hours_with_whole_pickers():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.read_heatmap(week="next", customer=CC, db=db)
    assert len(out["days"]) == 8 and all(len(d["hours"]) == 24 for d in out["days"])
    assert out["days"][0]["date"] == (AS_OF + timedelta(days=1)).isoformat()
    cell = out["days"][0]["hours"][15]
    assert isinstance(cell["pickers_p50"], int) and cell["lines_p50"] is not None
    assert out["throughput"]["lines_per_picker_hour"] is not None and out["throughput"]["buffer_pct"] == 0.1
    assert all(d["hours"][10]["pickers_p50"] == 0 for d in out["days"])  # the lull


# ==================================================== 4. items

async def test_items_page_by_volume_without_overlap_and_carry_the_next_bucket():
    await _plant_and_run()
    async with async_session() as db:
        first = await api.list_items(metric="units", horizon="1w", sort="volume", limit=2, after=None, q=None,
                                     customer=CC, db=db)
        second = await api.list_items(metric="units", horizon="1w", sort="volume", limit=2, after=first["next_after"],
                                      q=None, customer=CC, db=db)
        searched = await api.list_items(metric="units", horizon="1w", sort="volume", limit=10, after=None, q="3007",
                                        customer=CC, db=db)
    assert first["truncated"] is True and len(first["items"]) == 2
    assert {i["item_number"] for i in first["items"]} == {"104568", "100944"}
    assert second["truncated"] is False and {i["item_number"] for i in second["items"]} == {"200001", "300777"}
    assert first["items"][0]["next"]["p50"] is not None and first["items"][0]["next"]["horizon"] == "1w"
    assert first["items"][0]["accuracy"] is None
    assert [i["item_number"] for i in searched["items"]] == ["300777"]
    assert searched["items"][0]["classification"] == "intermittent" and searched["items"][0]["grains"] == ["week", "month"]


# ==================================================== 5. runs

async def test_trigger_answers_202_and_refuses_to_double_run():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.trigger_run(body={"as_of_date": AS_OF.isoformat()}, customer=CC, db=db)
    assert out["status"] == "queued" and out["poll"].endswith(out["run_id"])
    async with async_session() as db:
        with pytest.raises(HTTPException) as exc:
            await api.trigger_run(body={}, customer=CC, db=db)
    assert exc.value.status_code == 409
    await api._run_tasks[out["run_id"]]
    async with async_session() as db:
        run = await api.get_run(run_id=out["run_id"], customer=CC, db=db)
        status = await api.read_status(customer=CC, db=db)
        runs = await api.list_runs(limit=5, customer=CC, db=db)
    assert run["status"] == "completed" and run["trigger"] == "manual" and run["points_written"] > 0
    assert status["latest_run"]["run_id"] == out["run_id"] and status["next_as_of_date"] == AS_OF.isoformat()
    assert len(runs["runs"]) == 2


# ==================================================== 6. subjects

async def test_subjects_lists_what_has_been_forecast_for_a_kind():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.list_subjects(subject_kind="transaction_name", metric="lines", customer=CC, db=db)
        bad = None
        try:
            await api.list_subjects(subject_kind="colour", metric="lines", customer=CC, db=db)
        except HTTPException as exc:
            bad = exc.status_code
    assert [s["subject"] for s in out["subjects"]] == ["Brighton Stock Pick", "JIT and Shorts Pick (Brighton)"]
    assert out["subjects"][0]["model"] and out["subjects"][0]["classification"] == "smooth"
    assert bad == 422


# ==================================================== 7. the hour grain

async def test_series_at_the_hour_grain_puts_actual_pickers_next_to_the_forecast_per_hour():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.read_series(metric="pickers", subject_kind="total", subject="total", grains="hour", days_back=1,
                                    weeks_back=1, months_back=1, horizon="latest", hours_back=48, customer=CC, db=db)
    pts = out["grains"]["hour"]["points"]
    assert len(pts) == 48 + 8 * 24                       # 48 hours back, the heatmap's eight days ahead
    assert pts[0]["target"] == "2026-10-03T00:00" and pts[0]["start"] == pts[0]["target"]   # 48 h before today
    assert pts[0]["end"] == "2026-10-03T01:00"
    # Saturday 3 Oct 15:00 London: lines were planted 14:00 onwards by 3 pickers
    sat_15 = next(p for p in pts if p["target"] == "2026-10-03T15:00")
    assert sat_15["actual"] is not None and int(sat_15["actual"]) >= 1 and sat_15["p50"] is None
    # the hour in progress at NOW (03:00Z = 04:00 London on 5 Oct) is partial, with a forecast
    now_hour = next(p for p in pts if p["target"] == "2026-10-05T04:00")
    assert now_hour["partial"] is True and now_hour["actual"] is None and now_hour["p50"] is not None
    ahead = next(p for p in pts if p["target"] == "2026-10-05T15:00")
    assert ahead["actual"] is None and ahead["p50"] is not None and ahead["horizon"] == "1d"
    async with async_session() as db:
        out2 = await api.read_series(metric="lines", subject_kind="total", subject="total", grains="hour", days_back=1,
                                     weeks_back=1, months_back=1, horizon="latest", hours_back=24, customer=CC, db=db)
    assert len(out2["grains"]["hour"]["points"]) == 24 + 8 * 24


async def test_the_hour_grain_is_bounded_and_only_for_the_total():
    await _plant_and_run()
    async with async_session() as db:
        out = await api.read_series(metric="pickers", subject_kind="total", subject="total", grains="hour", days_back=1,
                                    weeks_back=1, months_back=1, horizon="latest", hours_back=10_000, customer=CC, db=db)
        assert len(out["grains"]["hour"]["points"]) == api.MAX_BACK["hour"] + 8 * 24
        with pytest.raises(HTTPException) as exc:
            await api.read_series(metric="pickers", subject_kind="item_number", subject="104568", grains="hour", days_back=1,
                                  weeks_back=1, months_back=1, horizon="latest", hours_back=24, customer=CC, db=db)
    assert exc.value.status_code == 422
