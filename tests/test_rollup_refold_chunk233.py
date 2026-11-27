"""Chunk 233: stored rollups are rebuilt from facts one local day at a time (step 54; 1d stage 4).

Chunk 232 gave every rollup row its place, and the rows stored before it keep an empty place
until their buckets are folded again. The rebuild queues one refold ticket per day, lets the
fact writer do the folding, and looks at the disk before each day, because this server's disk
stalls under load.

Pinned here
-----------
    a day      queueing a day and letting the writer run gives that day's rows their place back
    one by one the next day is queued only after the writer has consumed the one before
    the disk   the run stops before a day when a write waits longer than the limit, and says where
    stuck      the run stops when the writer does not consume a ticket in time
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import update

from app.config.database import async_session
from app.persistence.models.analytics_rollup import AnalyticsDailyRollup, AnalyticsHourlyRollup
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


async def _writer_runs() -> bool:
    await n3.consume_tenant(CC)
    return await refold.open_tickets(async_session, CC) == 0


async def _healthy() -> float:
    return 1.0


async def test_refolding_a_day_gives_its_rows_their_place_back():
    await base._register()
    await base._plant((0, "10.0", "915", "BRI"))
    await n3.consume_tenant(CC)
    await _old_shape()
    assert await base._places(AnalyticsHourlyRollup) == {(None, None): Decimal("10")}
    assert await refold.queue_day(async_session, CC, DAY, TZ) >= 1
    assert await refold.open_tickets(async_session, CC) >= 1
    assert await _writer_runs()
    for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup):
        assert await base._places(model) == {("915", "BRI"): Decimal("10")}


async def test_a_range_is_refolded_one_day_at_a_time_oldest_first():
    await base._register()
    await base._plant((0, "10.0", "915", "BRI"))
    await n3.consume_tenant(CC)
    await _old_shape()
    seen, open_when_queued = [], []

    async def writer() -> bool:
        open_when_queued.append(await refold.open_tickets(async_session, CC))
        return await _writer_runs()

    progress = await refold.refold_range(
        async_session, CC, start=DAY - timedelta(days=1), end=DAY + timedelta(days=1), tz=TZ,
        max_write_wait_ms=200, read_write_wait=_healthy, wait_until_consumed=writer,
        on_day=lambda day, waited: seen.append((day, waited)))
    assert progress.days_done == [DAY - timedelta(days=1), DAY, DAY + timedelta(days=1)]
    assert progress.stopped_at is None and [day for day, _w in seen] == progress.days_done
    # never more than one day's tickets waiting: the day before was consumed first
    assert max(open_when_queued) <= 2
    assert await base._places(AnalyticsHourlyRollup) == {("915", "BRI"): Decimal("10")}


async def test_the_run_stops_before_a_day_when_the_disk_is_slow():
    readings = iter([1.0, 350.0])

    async def disk() -> float:
        return next(readings)

    progress = await refold.refold_range(
        async_session, CC, start=DAY, end=DAY + timedelta(days=2), tz=TZ, max_write_wait_ms=200,
        read_write_wait=disk, wait_until_consumed=_writer_runs)
    assert progress.days_done == [DAY] and progress.stopped_at == DAY + timedelta(days=1)
    assert "350 ms" in progress.reason and "200 ms" in progress.reason
    assert await refold.open_tickets(async_session, CC) == 0  # nothing queued for the day refused


async def test_the_run_stops_when_the_writer_does_not_consume_the_ticket():
    async def never() -> bool:
        return False

    progress = await refold.refold_range(
        async_session, CC, start=DAY, end=DAY + timedelta(days=1), tz=TZ, max_write_wait_ms=200,
        read_write_wait=_healthy, wait_until_consumed=never)
    assert progress.days_done == [] and progress.stopped_at == DAY
    assert "did not consume" in progress.reason


async def test_a_machine_without_disk_figures_is_not_stopped():
    async def unknown() -> None:
        return None

    progress = await refold.refold_range(
        async_session, CC, start=DAY, end=DAY, tz=TZ, max_write_wait_ms=200,
        read_write_wait=unknown, wait_until_consumed=_writer_runs)
    assert progress.days_done == [DAY]


def test_the_script_takes_two_dates_and_safe_defaults():
    from scripts import refold_rollups_by_day as script

    args = script.build_parser().parse_args(["tmp-live", "2026-09-03", "2026-09-05"])
    assert (args.start, args.end) == (date(2026, 9, 3), date(2026, 9, 5))
    assert (args.pause, args.max_write_wait_ms, args.device) == (5.0, 200.0, "sda")
