"""Chunk 233, route changed in chunk 235: stored rollups are rebuilt straight from stored facts.

Chunk 232 gave every rollup row its place, and the rows stored before it keep an empty place
until their buckets are folded again. The rebuild folds one local day at a time from the facts
already stored, under the fact writer's own tenant lock, and looks at the disk before each day,
because this server's disk stalls under load. It queues no ticket: a ticket makes the writer
compare the facts with a source that is only kept 60 days (chunk 235).

Pinned here
-----------
    a day      folding a day gives that day's rows their place back and queues no ticket
    no source  folding a day whose source has aged out keeps its facts and its totals
    old days   a day older than hourly rows are kept is folded at the daily level only
    one by one a range is folded oldest first
    the disk   the run stops before a day when a write waits longer than the limit, and says where
"""
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select, update

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_rollup import AnalyticsDailyRollup, AnalyticsHourlyRollup
from app.persistence.models.log_transaction import LogTransaction
from app.services.analytics import consume as n3
from app.services.analytics import rollup_refold as refold
from tests import test_rollup_place_chunk232 as base

CC = base.CC
TZ = "Europe/London"
DAY = base.DAY


@pytest.fixture(autouse=True)
async def clean():
    await base._wipe()
    async with async_session() as db:
        db.add(base.Customer(customer_code=CC, name="refold probe", timezone=TZ))
        await db.commit()
    yield
    await base._wipe()


async def _old_shape() -> None:
    """Put the stored rows back to how chunk 232 found them: no place on any of them."""
    async with async_session() as db:
        for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup):
            await db.execute(update(model).where(model.customer_code == CC)
                             .values(company=None, warehouse=None))
        await db.commit()


async def _count(model, *where) -> int:
    async with async_session() as db:
        return int(await db.scalar(select(func.count()).select_from(model).where(
            model.customer_code == CC, *where)) or 0)


async def _healthy() -> float:
    return 1.0


async def _folded_day() -> None:
    await base._register()
    await base._plant((0, "10.0", "915", "BRI"))
    await n3.consume_tenant(CC)
    await _old_shape()
    assert await base._places(AnalyticsHourlyRollup) == {(None, None): Decimal("10")}


async def test_folding_a_day_gives_its_rows_their_place_back_and_queues_no_ticket():
    await _folded_day()
    tickets = await _count(AnalyticsPendingWindow)
    stats = await refold.fold_day(async_session, CC, DAY, TZ)
    assert stats["rows_written"] >= 2
    for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup):
        assert await base._places(model) == {("915", "BRI"): Decimal("10")}
    assert await _count(AnalyticsPendingWindow) == tickets


async def test_folding_a_day_whose_source_has_aged_out_keeps_facts_and_totals():
    await _folded_day()
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
        await db.commit()
    await refold.fold_day(async_session, CC, DAY, TZ)
    assert await _count(AnalyticsFact) == 1
    assert await _count(AnalyticsFactLedger, AnalyticsFactLedger.reason == "reverse") == 0
    for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup):
        assert await base._places(model) == {("915", "BRI"): Decimal("10")}


async def test_a_day_past_the_hourly_retention_keeps_its_hourly_rows_untouched():
    await _folded_day()
    later = datetime.combine(DAY + timedelta(days=200), time.min, tzinfo=timezone.utc)
    await refold.fold_day(async_session, CC, DAY, TZ, now=later)
    # the hourly level is left as it was; the day is folded from the facts
    assert await base._places(AnalyticsHourlyRollup) == {(None, None): Decimal("10")}
    assert await base._places(AnalyticsDailyRollup) == {("915", "BRI"): Decimal("10")}


async def test_a_range_is_folded_one_day_at_a_time_oldest_first():
    await _folded_day()
    seen = []
    progress = await refold.refold_range(
        async_session, CC, start=DAY - timedelta(days=1), end=DAY + timedelta(days=1), tz=TZ,
        max_write_wait_ms=200, read_write_wait=_healthy,
        on_day=lambda day, waited: seen.append((day, waited)))
    assert progress.days_done == [DAY - timedelta(days=1), DAY, DAY + timedelta(days=1)]
    assert progress.stopped_at is None and [day for day, _w in seen] == progress.days_done
    assert await base._places(AnalyticsHourlyRollup) == {("915", "BRI"): Decimal("10")}


async def test_the_run_stops_before_a_day_when_the_disk_is_slow():
    await _folded_day()
    readings = iter([1.0, 350.0])

    async def disk() -> float:
        return next(readings)

    progress = await refold.refold_range(
        async_session, CC, start=DAY, end=DAY + timedelta(days=2), tz=TZ, max_write_wait_ms=200,
        read_write_wait=disk)
    assert progress.days_done == [DAY] and progress.stopped_at == DAY + timedelta(days=1)
    assert "350 ms" in progress.reason and "200 ms" in progress.reason


async def test_a_machine_without_disk_figures_is_not_stopped():
    async def unknown() -> None:
        return None

    progress = await refold.refold_range(
        async_session, CC, start=DAY, end=DAY, tz=TZ, max_write_wait_ms=200,
        read_write_wait=unknown)
    assert progress.days_done == [DAY]


def test_the_script_takes_two_dates_and_safe_defaults():
    from scripts import refold_rollups_by_day as script

    args = script.build_parser().parse_args(["tmp-live", "2026-09-03", "2026-09-05"])
    assert (args.start, args.end) == (date(2026, 9, 3), date(2026, 9, 5))
    assert (args.pause, args.max_write_wait_ms, args.device) == (5.0, 200.0, "sda")
    assert not hasattr(args, "timeout")  # nothing waits for the fact writer any more
