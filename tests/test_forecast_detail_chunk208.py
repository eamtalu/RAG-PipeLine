"""Step 30 (chunk 208): the Teams forecast page's row, the web Forecast page's figures.

The owner asked (16 November 2026) for the web app's Forecast page in Teams, opened from the
forecast card on Overview. The edge cannot reach this server, so the consumer mirrors one row per
site (and for the whole customer) every five minutes. The row is built by calling the very
functions behind `/analytics/forecast/*`, so the two pages can never disagree; each point keeps
only the fields the charts read, so the row fits a DynamoDB item."""

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from app.api.v1 import analytics_forecast as web
from app.config.database import async_session
from app.persistence.repositories.site_repository import SiteRepository
from app.services.analytics_forecast import plan, runner
from app.services.teams import forecast_detail
from app.services.teams.home_forecast import NO_RUN_NOTE
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.sites import Site
from tests import forecast_fixtures as fx
from tests.test_analytics_forecast_runner_chunk139 import _day_rows

CC = "test_chunk208fc"
FIRST = date(2026, 9, 7)
AS_OF = date(2026, 10, 4)
NOW = datetime(2026, 10, 5, 14, 30, tzinfo=timezone.utc)
CFG = plan.ForecastConfig(
    settlement=fx.SETTLEMENT, units_value="picked", history_days=120, min_history_days=14, min_days_ets=21,
    item_daily_min_active_days=10, max_items=200, backtest_folds=3, score_lag_hours=2,
    accuracy_window_days=28, staffing_buffer_pct=0.10)
BRIGHTON = Site("BRI", "Brighton")


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    """The web endpoints read the clock: pin it to the planted Monday, so today's hours have data."""
    monkeypatch.setattr(web, "_now", lambda: NOW)


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)
    async with async_session() as db:
        await SiteRepository(db).remove(CC, "BRI")
        await db.commit()


async def _plant_and_run(days: int = 28) -> None:
    rows = []
    for i in range(days):
        for r in _day_rows(FIRST + timedelta(days=i)):
            r.customer_code = CC
            rows.append(r)
    await fx.plant(rows)
    out = await runner.run_tenant(CC, as_of_date=AS_OF, trigger="manual", cfg=CFG, now=NOW)
    assert out["status"] == "completed", out


async def _web_series(db, metric: str, kind: str = "total", subject: str = "total") -> dict:
    return await web.read_series(metric=metric, subject_kind=kind, subject=subject, grains="day,week,month",
                                 days_back=28, weeks_back=12, months_back=6, hours_back=72, horizon="latest",
                                 customer=CC, db=db)


async def _status() -> dict:
    async with async_session() as db:
        return await web.read_status(customer=CC, db=db)


# ==================================================== 1. no run yet


async def test_without_a_run_the_row_says_so():
    async with async_session() as db:
        row = await forecast_detail.compute(db, CC)
    assert row["available"] is False and row["note"] == NO_RUN_NOTE


# ==================================================== 2. the same figures as the web page


async def test_the_whole_customer_row_carries_the_web_pages_figures():
    await _plant_and_run()
    async with async_session() as db:
        row = await forecast_detail.compute(db, CC)
        lines, units = await _web_series(db, "lines"), await _web_series(db, "units")
        accuracy = await web.read_accuracy(metric="lines", subject_kind="total", subject="total", grain="day", customer=CC, db=db)
        heat = await web.read_heatmap(week="next", customer=CC, db=db)
        items = await web.list_items(metric="units", horizon="1w", sort="volume", limit=100, after=None, q=None, customer=CC, db=db)
        subjects = await web.list_subjects(subject_kind="transaction_name", metric="lines", limit=100, customer=CC, db=db)
        hourly = await web.read_series(metric="pickers", subject_kind="total", subject="total", grains="hour", days_back=28,
                                       weeks_back=12, months_back=6, hours_back=72, horizon="latest", customer=CC, db=db)
    assert row["available"] is True and row["site_id"] == "all" and row["scope"] == "all sites"
    for grain in ("day", "week", "month"):
        assert row["series"]["lines"][grain] == [forecast_detail.slim(p) for p in lines["grains"][grain]["points"]]
        assert row["series"]["units"][grain] == [forecast_detail.slim(p) for p in units["grains"][grain]["points"]]
    assert row["today"] == lines["today"] and row["as_of_date"] == lines["as_of_date"]
    assert row["accuracy"] == accuracy
    status = await _status()
    run = status["latest_run"]
    assert row["status"]["latest_run"]["history_days"] == run["detail"]["history"]["days"]
    assert row["status"]["latest_run"]["warnings"] == run["detail"].get("warnings", [])[:3]
    assert row["heatmap"] == heat
    assert row["items"] == items["items"]
    assert [g["key"] for g in row["by_transaction"]["lines"]] == [s["subject"] for s in subjects["subjects"]]
    assert row["hourly_pickers"] == [forecast_detail.slim(p) for p in hourly["grains"]["hour"]["points"]]
    assert len(row["hourly_pickers"]) > 72 and set(row["hourly_pickers"][0]) == set(forecast_detail.POINT_FIELDS)
    assert row["by_transaction"]["lines"][0]["series"]["day"]
    # the fields a chart never reads are left out of every point
    assert set(row["series"]["lines"]["day"][0]) == set(forecast_detail.POINT_FIELDS)


async def test_a_sites_row_reads_that_warehouses_series_and_names_what_is_for_all_sites():
    await _plant_and_run()
    async with async_session() as db:
        row = await forecast_detail.compute(db, CC, BRIGHTON)
        site_lines = await _web_series(db, "lines", "warehouse", "BRI")
    assert row["site_id"] == "BRI" and row["scope"] == "Brighton"
    assert row["series"]["lines"]["day"] == [forecast_detail.slim(p) for p in site_lines["grains"]["day"]["points"]]
    # pickers per hour, the shifts, the transactions, the items and the accuracy exist for the whole
    # customer only, and the row says so rather than letting them pass for the site's
    assert row["all_sites_panels"] == ["accuracy", "hourly_pickers", "heatmap", "by_transaction", "items"]


# ==================================================== 3. the row fits a DynamoDB item


async def test_the_row_fits_a_dynamodb_item():
    await _plant_and_run()
    async with async_session() as db:
        row = await forecast_detail.compute(db, CC)
    assert len(json.dumps(row, default=str).encode()) < forecast_detail.MAX_ROW_BYTES


def test_an_oversized_row_drops_the_items_then_the_transactions_and_says_so():
    row = {"available": True, "items": [{"x": "a" * 1000}] * 30, "by_transaction": {"lines": [{"x": "b" * 1000}] * 30}, "series": {"lines": {}}}
    fitted = forecast_detail.fit(row, max_bytes=40_000)
    assert fitted["items"] == [] and fitted["by_transaction"] == {"lines": [{"x": "b" * 1000}] * 30}
    assert fitted["trimmed"] == ["items"]
    smaller = forecast_detail.fit(row, max_bytes=5_000)
    assert smaller["items"] == [] and smaller["by_transaction"] == {} and smaller["trimmed"] == ["items", "by_transaction"]
    assert forecast_detail.fit({"available": True, "items": []}, max_bytes=5_000) == {"available": True, "items": []}


# ==================================================== 4. the sweep writes one row per site and for all


async def test_the_sweep_mirrors_the_whole_customer_and_each_site():
    await _plant_and_run()
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BRI", "Brighton", sort_order=1)
        await db.commit()
    mirror = InMemoryMirrorWriter()
    written = await forecast_detail.sweep_once(mirror, only=[CC], max_sites=3)
    assert written == [CC]
    keys = sorted(mirror.items)
    assert keys == [(f"EYE#{CC}", "SITE#BRI#FORECAST"), (f"EYE#{CC}", "SITE#all#FORECAST")]
    assert json.loads(mirror.items[(f"EYE#{CC}", "SITE#BRI#FORECAST")].payload_json)["scope"] == "Brighton"


async def test_a_failing_customer_does_not_stop_the_sweep_and_writes_nothing_for_it():
    mirror = InMemoryMirrorWriter(fail=True)
    assert await forecast_detail.sweep_once(mirror, only=[CC], max_sites=3) == []
