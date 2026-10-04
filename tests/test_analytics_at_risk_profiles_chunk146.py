"""Chunk 146: what a route's closed days teach, and how that becomes the clock.

Once a day the worker reads the closed rows of the last `window_days` for each route and writes one
profile row: the time of day by which the route's van was ready on nine days in ten (the coverage
quantile of the dock's last scan, over days), the median and the latest, when loading usually starts,
and the same for the last pick on a route without a loading step. Below `min_days` the usual time is
unknown and the WMS departure stands in. `clock_for` turns the latest profiles into the clock each
delivery is judged against on its own departure day.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import RULE_VERSION, model, profile_store, settings_store
from tests import at_risk_fixtures as fx

CC = "test_chunk146ar"
LONDON = fx.LONDON
UTC = timezone.utc
AS_OF = date(2026, 10, 1)
NOW = datetime(2026, 10, 2, 2, 5, tzinfo=UTC)  # 03:05 London on the 2nd


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _closed(route: str, day: date, van_hhmm: str | None, *, number: str, pick_hhmm: str | None = None, loading: bool = True):
    """One closed delivery whose route's van was ready at `van_hhmm` that day (local)."""
    dep = fx.local_day(day, 11, 30)
    van = None if van_hhmm is None else fx.local_day(day, int(van_hhmm[:2]), int(van_hhmm[2:]))
    pick = fx.local_day(day, int(pick_hhmm[:2]), int(pick_hhmm[2:])) if pick_hhmm else (van - timedelta(minutes=30) if van else dep - timedelta(hours=5))
    if not loading:
        return fx.closed_delivery(CC, number, route=route, departure_at=dep, last_load_at=None, last_pick_at=pick, outcome="picked_in_time")
    return fx.closed_delivery(CC, number, route=route, departure_at=dep, last_load_at=None if van is None else van - timedelta(minutes=5),
                              last_pick_at=pick, outcome="never_loaded" if van is None else "loaded_in_time",
                              route_loaded_at=van, route_loading_from=None if van is None else van - timedelta(minutes=90))


async def _compute(**over):
    async with async_session() as db:
        settings = settings_store.DEFAULTS if not over else settings_store.Settings(**{**settings_store.DEFAULTS.__dict__, **over})
        rows = await profile_store.compute(db, CC, as_of=AS_OF, settings=settings, rule_version=RULE_VERSION, now=NOW, tz=LONDON)
        await db.commit()
    return {r.route: r for r in rows}


def _state(route="BRI03", day=AS_OF, loading=True) -> model.DeliveryState:
    return model.DeliveryState(delivery_number="x", route=route, customer_name=None, customer_number=None,
                               departure_at=fx.local_day(day, 11, 30), lines_expected=1, lines_confirmed=0, lines_picked=0, lines_short=0,
                               packages_created=0, packages_loaded=0, last_pick_at=None, last_load_at=None, loading_expected=loading)


async def test_fewer_than_min_days_of_van_history_leaves_the_usual_time_unknown_and_the_wms_departure_stands_in():
    # four days (three deliveries each): under the five-day floor
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=d), "0700", number=f"d{d}{i}") for d in range(4) for i in range(3)])
    profiles = await _compute()
    p = profiles["BRI03"]
    assert (p.sample, p.loaded_sample, p.van_days, p.van_ready_usual_min) == (12, 12, 4, None)
    # the median and the latest are still reported, so a reader can see the rhythm forming
    assert (p.van_ready_p50_min, p.van_ready_latest_min, p.loading_from_p50_min) == (Decimal("420"), Decimal("420"), Decimal("330"))
    clock = profile_store.clock_for(profiles, settings_store.DEFAULTS, LONDON)(_state())
    assert (clock.usual_ready_at, clock.source) == (fx.local_day(AS_OF, 11, 30), "wms_departure")
    assert (clock.warn_before, clock.gone_after) == (timedelta(minutes=30), timedelta(minutes=20))
    # a route nobody has learned anything about gets the WMS departure too
    assert profile_store.clock_for(profiles, settings_store.DEFAULTS, LONDON)(_state(route="BRI99")).source == "wms_departure"


async def test_five_days_teach_the_time_the_van_was_ready_on_nine_days_in_ten():
    # the van was ready at 06:40, 06:50, 07:00, 07:10 and 07:20 on five days: the 90th percentile by
    # linear interpolation is 07:16 (position 3.6 between 07:10 and 07:20), the median 07:00
    times = ["0640", "0650", "0700", "0710", "0720"]
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=d), t, number=f"d{d}{i}") for d, t in enumerate(times) for i in range(2)])
    profiles = await _compute()
    p = profiles["BRI03"]
    assert (p.sample, p.van_days) == (10, 5)
    assert (p.van_ready_usual_min, p.van_ready_p50_min, p.van_ready_latest_min) == (Decimal("436"), Decimal("420"), Decimal("440"))
    assert (p.as_of_date, p.window_days, p.coverage, p.rule_version, p.departure_time_mode) == (AS_OF, 28, Decimal("0.900"), RULE_VERSION, "1130")
    assert p.computed_at == NOW
    clock = profile_store.clock_for(profiles, settings_store.DEFAULTS, LONDON)(_state(day=date(2026, 10, 2)))
    assert (clock.usual_ready_at, clock.source) == (fx.local_day(date(2026, 10, 2), 7, 16), "learned")
    # a tenant that wants the median instead sets the coverage to a half
    assert (await _compute(coverage=Decimal("0.5")))["BRI03"].van_ready_usual_min == Decimal("420")


async def test_a_route_without_a_loading_step_learns_from_its_last_pick_of_the_day():
    await fx.plant([_closed("BRILA1", AS_OF - timedelta(days=d), None, pick_hhmm=f"10{10 + 5 * d:02d}", number=f"g{d}", loading=False)
                    for d in range(6)])
    profiles = await _compute()
    p = profiles["BRILA1"]
    assert (p.van_days, p.van_ready_usual_min, p.pick_days) == (0, None, 6)
    assert p.pick_done_usual_min == Decimal("632.5")  # 10:32:30: the 90th percentile of 10:10 .. 10:35
    clock = profile_store.clock_for(profiles, settings_store.DEFAULTS, LONDON)(_state(route="BRILA1", loading=False))
    assert (clock.usual_ready_at, clock.source) == (fx.local_day(AS_OF, 10, 32) + timedelta(seconds=30), "learned")


async def test_a_day_the_van_never_loaded_teaches_nothing_about_the_van_but_counts_in_the_sample():
    await fx.plant([_closed("BRI01", AS_OF - timedelta(days=d), "0700", number=f"ok{d}") for d in range(5)]
                   + [_closed("BRI01", AS_OF - timedelta(days=5), None, number="never")])
    p = (await _compute())["BRI01"]
    assert (p.sample, p.loaded_sample, p.van_days, p.van_ready_usual_min) == (6, 5, 5, Decimal("420"))


async def test_the_window_is_the_last_window_days_up_to_as_of_and_other_tenants_are_invisible():
    await fx.plant([_closed("BRI02", AS_OF - timedelta(days=27), "0700", number="in"),
                    _closed("BRI02", AS_OF - timedelta(days=28), "0700", number="out"),
                    _closed("BRI02", AS_OF + timedelta(days=1), "0700", number="future")])
    await fx.wipe("test_chunk146other")
    await fx.seed_tenant("test_chunk146other")
    other = fx.closed_delivery("test_chunk146other", "x", route="BRI02", departure_at=fx.local_day(AS_OF, 11, 30),
                               last_load_at=fx.local_day(AS_OF, 7, 0), outcome="loaded_in_time")
    await fx.plant([other])
    try:
        assert (await _compute())["BRI02"].sample == 1
    finally:
        await fx.wipe("test_chunk146other")


async def test_one_row_per_route_per_day_and_the_latest_is_read_as_of_a_day():
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=d), "0700", number=f"d{d}") for d in range(6)])
    await _compute()
    await _compute()  # the same day again: upsert, not a second row
    async with async_session() as db:
        yesterday = await profile_store.compute(db, CC, as_of=AS_OF - timedelta(days=1), settings=settings_store.DEFAULTS,
                                                rule_version=RULE_VERSION, now=NOW - timedelta(days=1), tz=LONDON)
        await db.commit()
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC))
        latest = await profile_store.latest(db, CC)
        earlier = await profile_store.latest(db, CC, as_of=AS_OF - timedelta(days=1))
    assert n == 2 and len(yesterday) == 1
    assert latest["BRI03"].as_of_date == AS_OF and earlier["BRI03"].as_of_date == AS_OF - timedelta(days=1)
