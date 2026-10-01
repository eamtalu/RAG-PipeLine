"""Chunk 138: the forecast's store modules against the real tables.

History is read through the same grouped read the pick-releases screen uses, so what the model
learns from is exactly what the screen shows. Predictions upsert in place for a re-run and keep
one row per night per target otherwise. Scoring fills the actual in after the bucket has closed,
and the accuracy table is a straight recomputation from scored rows.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_ml import AnalyticsPrediction
from app.services.analytics import settle_query
from app.services.analytics import settle_store
from app.services.analytics_forecast import accuracy_store, history_store, prediction_store, run_store
from tests import forecast_fixtures as fx

CC = "test_chunk138fc"
LONDON = fx.LONDON
UTC = timezone.utc


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


# ==================================================== 1. history

async def test_read_daily_counts_lines_and_sums_units_per_day_warehouse_and_transaction():
    d = datetime(2026, 9, 14, 15, 0, tzinfo=UTC)
    await fx.plant([fx.settled(CC, d, picked="3"), fx.settled(CC, d + timedelta(hours=1), picked="4"),
                    fx.settled(CC, d, picked="10", tx="JIT and Shorts Pick (Brighton)"),
                    fx.settled(CC, d + timedelta(days=1), picked="1")])
    async with async_session() as db:
        rows = await history_store.read_daily(db, CC, fx.SETTLEMENT, since=d - timedelta(days=1),
                                              until=d + timedelta(days=3), tz=LONDON, units_value="picked")
    got = {(r.day, r.warehouse, r.transaction_name): (r.lines, r.units) for r in rows}
    assert got == {
        (date(2026, 9, 14), "BRI", "Brighton Stock Pick"): (2, 7.0),
        (date(2026, 9, 14), "BRI", "JIT and Shorts Pick (Brighton)"): (1, 10.0),
        (date(2026, 9, 15), "BRI", "Brighton Stock Pick"): (1, 1.0)}


async def test_a_line_at_half_past_eleven_utc_in_summer_belongs_to_the_next_local_day():
    when = datetime(2026, 9, 14, 23, 30, tzinfo=UTC)  # 00:30 on the 15th in London
    await fx.plant([fx.settled(CC, when)])
    async with async_session() as db:
        rows = await history_store.read_daily(db, CC, fx.SETTLEMENT, since=when - timedelta(days=1),
                                              until=when + timedelta(days=1), tz=LONDON, units_value="picked")
    assert [r.day for r in rows] == [date(2026, 9, 15)]


async def test_read_hourly_counts_lines_and_distinct_pickers_per_local_hour():
    h = datetime(2026, 9, 14, 15, 0, tzinfo=UTC)  # 16:00 London
    await fx.plant([fx.settled(CC, h, user="A"), fx.settled(CC, h + timedelta(minutes=10), user="A"),
                    fx.settled(CC, h + timedelta(minutes=20), user="B"),
                    fx.settled(CC, h + timedelta(hours=1), user="C")])
    async with async_session() as db:
        rows = await history_store.read_hourly(db, CC, fx.SETTLEMENT, since=h - timedelta(days=1),
                                               until=h + timedelta(days=1), tz=LONDON)
    got = {(r.start.hour, r.start.tzinfo is None): (r.lines, r.pickers) for r in rows}
    assert got == {(16, True): (3.0, 2), (17, True): (1.0, 1)}


async def test_read_daily_items_reports_when_it_hit_the_cap():
    d = datetime(2026, 9, 14, 15, 0, tzinfo=UTC)
    await fx.plant([fx.settled(CC, d, item=f"10{n}") for n in range(5)])
    async with async_session() as db:
        rows, truncated = await history_store.read_daily_items(
            db, CC, fx.SETTLEMENT, since=d - timedelta(days=1), until=d + timedelta(days=1), tz=LONDON,
            units_value="picked", cap=3)
        all_rows, not_truncated = await history_store.read_daily_items(
            db, CC, fx.SETTLEMENT, since=d - timedelta(days=1), until=d + timedelta(days=1), tz=LONDON,
            units_value="picked", cap=50)
    assert truncated is True and len(rows) == 3
    assert not_truncated is False and {r.item_number for r in all_rows} == {f"10{n}" for n in range(5)}


# ==================================================== 2. the month bucket

async def test_month_is_a_valid_bucket_and_groups_to_the_first_of_the_local_month():
    assert settle_query.validate(fx.PICK_RELEASE, group_by=("month",)) == []
    sep = datetime(2026, 9, 30, 23, 30, tzinfo=UTC)  # 1 Oct 00:30 London
    await fx.plant([fx.settled(CC, sep), fx.settled(CC, sep - timedelta(days=1))])
    async with async_session() as db:
        rows = await settle_store.read_grouped(db, CC, fx.PICK_RELEASE, group_by=("month",), since=None,
                                               until=None, tz=LONDON)
    assert sorted(r["dimensions"][0] for r in rows) == ["2026-09-01", "2026-10-01"]


# ==================================================== 3. predictions

def _pred(target: datetime, *, horizon="1d", value="100", predicted_at=None, grain="day", subject="total",
          subject_kind="total", metric="lines", run_id=None) -> prediction_store.PredictionRow:
    return prediction_store.PredictionRow(
        metric=metric, grain=grain, subject_kind=subject_kind, subject=subject, horizon=horizon,
        model_version="forecast-v1", target_at=target, predicted_at=predicted_at or target - timedelta(days=1),
        value=Decimal(value), p10=Decimal(value) * Decimal("0.8"), p90=Decimal(value) * Decimal("1.2"),
        detail={"model": "weekday_mean"}, run_id=run_id)


T = datetime(2026, 9, 29, 23, 0, tzinfo=UTC)  # London midnight 30 Sep


async def test_upserting_the_same_prediction_twice_keeps_one_row_with_the_new_value():
    async with async_session() as db:
        assert await prediction_store.upsert(db, CC, [_pred(T, value="100")]) == 1
        await db.commit()
        await prediction_store.upsert(db, CC, [_pred(T, value="120")])
        await db.commit()
        rows = (await db.execute(select(AnalyticsPrediction).where(AnalyticsPrediction.customer_code == CC))).scalars().all()
    assert len(rows) == 1 and rows[0].value == Decimal("120") and rows[0].metric == "lines" and rows[0].grain == "day"


async def test_latest_per_target_takes_the_newest_prediction_and_can_be_pinned_to_a_horizon():
    async with async_session() as db:
        await prediction_store.upsert(db, CC, [
            _pred(T, horizon="3d", value="90", predicted_at=T - timedelta(days=3)),
            _pred(T, horizon="1d", value="110", predicted_at=T - timedelta(days=1)),
            _pred(T + timedelta(days=1), horizon="2d", value="130", predicted_at=T - timedelta(days=1))])
        await db.commit()
        latest = await prediction_store.latest_per_target(
            db, CC, metric="lines", grain="day", subject_kind="total", subject="total",
            start=T - timedelta(days=1), end=T + timedelta(days=5))
        pinned = await prediction_store.latest_per_target(
            db, CC, metric="lines", grain="day", subject_kind="total", subject="total",
            start=T - timedelta(days=1), end=T + timedelta(days=5), horizon="3d")
    assert [(r.target_at, r.value) for r in latest] == [(T, Decimal("110")), (T + timedelta(days=1), Decimal("130"))]
    assert [(r.target_at, r.value) for r in pinned] == [(T, Decimal("90"))]


async def test_scoring_fills_the_actual_for_closed_buckets_only():
    async with async_session() as db:
        await prediction_store.upsert(db, CC, [_pred(T, value="100"), _pred(T + timedelta(days=1), value="100")])
        await db.commit()
        rows = await prediction_store.scorable(db, CC, since=T - timedelta(days=30), until=T + timedelta(days=1, hours=6), tz=LONDON)
        assert [r.target_at for r in rows] == [T]
        await prediction_store.write_scores(db, [(rows[0].id, Decimal("90"))], scored_at=T + timedelta(days=2))
        await db.commit()
        got = (await db.execute(select(AnalyticsPrediction).where(AnalyticsPrediction.customer_code == CC)
                                .order_by(AnalyticsPrediction.target_at))).scalars().all()
    assert got[0].actual == Decimal("90") and got[0].abs_error == Decimal("10") and got[0].scored_at is not None
    assert got[1].actual is None


# ==================================================== 4. accuracy

async def test_recompute_matches_a_hand_calculation_per_horizon():
    async with async_session() as db:
        preds = []
        for k, (value, actual) in enumerate([("100", "90"), ("200", "220"), ("50", "50")]):
            preds.append(_pred(T + timedelta(days=k), value=value))
        await prediction_store.upsert(db, CC, preds)
        await db.commit()
        rows = await prediction_store.scorable(db, CC, since=T - timedelta(days=30), until=T + timedelta(days=10), tz=LONDON)
        await prediction_store.write_scores(db, [(r.id, Decimal(a)) for r, a in zip(rows, ["90", "220", "50"])],
                                            scored_at=T + timedelta(days=10))
        await db.commit()
        n = await accuracy_store.recompute(db, CC, window_start=date(2026, 9, 1), window_end=date(2026, 10, 31),
                                           model_version="forecast-v1")
        await db.commit()
        acc = await accuracy_store.read_series(db, CC, metric="lines", grain="day", subject_kind="total", subject="total")
    assert n == 1
    assert len(acc) == 1 and acc[0].horizon == "1d" and acc[0].n == 3
    assert acc[0].mae == Decimal("10")                                   # (10 + 20 + 0) / 3
    assert acc[0].wape == pytest.approx(Decimal(30) / Decimal(360), abs=Decimal("0.000001"))
    assert acc[0].mape == pytest.approx((Decimal(10) / 90 + Decimal(20) / 220 + 0) / 3, abs=Decimal("0.000001"))
    assert acc[0].mape_n == 3
    assert acc[0].bias == pytest.approx(Decimal(10 - 20 + 0) / 3, abs=Decimal("0.000001"))


# ==================================================== 5. runs

async def test_claiming_a_run_has_one_winner():
    async with async_session() as db:
        run = await run_store.create(db, CC, as_of_date=date(2026, 10, 1), trigger="manual", model_version="forecast-v1")
        await db.commit()
    async with async_session() as a, async_session() as b:
        first = await run_store.claim(a, run.id)
        await a.commit()
        second = await run_store.claim(b, run.id)
        await b.commit()
    assert first is True and second is False
    async with async_session() as db:
        latest = await run_store.latest(db, CC)
        assert latest.status == "running" and latest.started_at is not None
        assert (await run_store.running(db, CC)).id == run.id
        await run_store.finish(db, run.id, status="completed", points_written=7, scored=2, detail={"ok": True})
        await db.commit()
        done = await run_store.latest(db, CC)
    assert done.status == "completed" and done.points_written == 7 and done.finished_at is not None
    assert done.detail == {"ok": True}
