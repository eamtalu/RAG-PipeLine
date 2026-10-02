"""Chunk 146: what a route's closed deliveries teach, and how that becomes a threshold.

Once a day the worker reads the closed rows of the last `window_days` for each route and writes one
profile row: the lead nine in ten loaded deliveries kept before departure (the coverage quantile),
the median, the tightest, and how many deliveries that rests on. Below the sample floor the learned
lead is unknown and the floor alone applies. The effective threshold a delivery is judged against is
the larger of the learned lead and the configured floor.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import RULE_VERSION, profile_store, settings_store
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


def _closed(route: str, day: date, load_lead_min: int | None, *, pick_lead_min: int | None = None, number: str) :
    dep = fx.local_day(day, 11, 30)
    last_load = None if load_lead_min is None else dep - timedelta(minutes=load_lead_min)
    last_pick = dep - timedelta(minutes=pick_lead_min if pick_lead_min is not None else (load_lead_min or 0) + 30)
    outcome = "never_loaded" if load_lead_min is None else ("loaded_late" if load_lead_min < 0 else "loaded_in_time")
    return fx.closed_delivery(CC, number, route=route, departure_at=dep, last_load_at=last_load, last_pick_at=last_pick,
                              outcome=outcome)


async def _compute(**over):
    async with async_session() as db:
        settings = settings_store.DEFAULTS if not over else settings_store.Settings(**{**settings_store.DEFAULTS.__dict__, **over})
        rows = await profile_store.compute(db, CC, as_of=AS_OF, settings=settings, rule_version=RULE_VERSION, now=NOW, tz=LONDON)
        await db.commit()
    return {r.route: r for r in rows}


async def test_fewer_than_min_sample_deliveries_yields_floor_only():
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=i), 240 + i, number=f"d{i}") for i in range(19)])
    profiles = await _compute()
    p = profiles["BRI03"]
    assert (p.sample, p.loaded_sample, p.learned_load_min, p.learned_pick_min) == (19, 19, None, None)
    # the median and the tightest are still reported, so a reader can see the route forming
    assert p.load_lead_min == Decimal("240")
    assert p.load_lead_p50 == Decimal("249")
    async with async_session() as db:
        settings = await settings_store.effective(db, CC)
    th = profile_store.thresholds_for(profiles, settings)("BRI03")
    assert (th.load_min, th.load_source, th.pick_min, th.pick_source) == (Decimal(120), "floor", Decimal(180), "floor")
    # a route nobody has learned anything about gets the floor too
    th = profile_store.thresholds_for(profiles, settings)("BRI99")
    assert (th.load_min, th.load_source) == (Decimal(120), "floor")


async def test_twenty_closed_deliveries_teach_the_coverage_quantile():
    # leads 200, 210, ..., 390: the 10th percentile by linear interpolation is 219
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=i % 10), 200 + 10 * i, pick_lead_min=300 + 10 * i, number=f"d{i}")
                    for i in range(20)])
    profiles = await _compute()
    p = profiles["BRI03"]
    assert (p.sample, p.loaded_sample) == (20, 20)
    assert p.learned_load_min == Decimal("219")
    assert p.learned_pick_min == Decimal("319")
    assert (p.load_lead_p50, p.load_lead_min, p.pick_lead_p50, p.pick_lead_min) == (Decimal("295"), Decimal("200"), Decimal("395"), Decimal("300"))
    assert (p.as_of_date, p.window_days, p.coverage, p.rule_version, p.departure_time_mode) == (AS_OF, 28, Decimal("0.900"), RULE_VERSION, "1130")
    assert p.computed_at == NOW


async def test_leads_below_zero_and_over_a_day_are_dropped_and_never_loaded_rows_count_in_the_sample_only():
    rows = [_closed("BRI01", AS_OF - timedelta(days=i % 7), 300, number=f"ok{i}") for i in range(19)]
    rows += [_closed("BRI01", AS_OF, -15, number="late"),       # loaded after departure: outside the band
             _closed("BRI01", AS_OF, 1500, number="early"),     # a day early: outside the band
             _closed("BRI01", AS_OF, None, number="never")]     # never loaded: in the sample, no lead
    await fx.plant(rows)
    profiles = await _compute()
    p = profiles["BRI01"]
    assert (p.sample, p.loaded_sample) == (22, 21)
    assert p.learned_load_min is None  # 19 usable leads, under the floor of 20
    assert await _compute(min_sample=19) and (await _compute(min_sample=19))["BRI01"].learned_load_min == Decimal("300")


async def test_the_window_is_the_last_window_days_up_to_as_of_and_other_tenants_are_invisible():
    await fx.plant([_closed("BRI02", AS_OF - timedelta(days=27), 250, number="in"),
                    _closed("BRI02", AS_OF - timedelta(days=28), 250, number="out"),
                    _closed("BRI02", AS_OF + timedelta(days=1), 250, number="future")])
    await fx.wipe("test_chunk146other")
    await fx.seed_tenant("test_chunk146other")
    other = fx.closed_delivery("test_chunk146other", "x", route="BRI02", departure_at=fx.local_day(AS_OF, 11, 30),
                               last_load_at=fx.local_day(AS_OF, 7, 0), outcome="loaded_in_time")
    await fx.plant([other])
    try:
        assert (await _compute())["BRI02"].sample == 1
    finally:
        await fx.wipe("test_chunk146other")


async def test_one_row_per_route_per_day_and_the_latest_is_read():
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=i % 10), 200 + 10 * i, number=f"d{i}") for i in range(20)])
    await _compute()
    await _compute()  # the same day again: upsert, not a second row
    async with async_session() as db:
        yesterday = await profile_store.compute(db, CC, as_of=AS_OF - timedelta(days=1), settings=settings_store.DEFAULTS,
                                                rule_version=RULE_VERSION, now=NOW - timedelta(days=1), tz=LONDON)
        await db.commit()
        n = await db.scalar(select(func.count()).select_from(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC))
        latest = await profile_store.latest(db, CC)
    assert n == 2 and len(yesterday) == 1
    assert latest["BRI03"].as_of_date == AS_OF


async def test_the_effective_threshold_is_the_learned_lead_once_it_beats_the_floor():
    await fx.plant([_closed("BRI03", AS_OF - timedelta(days=i % 10), 200 + 10 * i, number=f"d{i}") for i in range(20)])
    profiles = await _compute()
    for_route = profile_store.thresholds_for(profiles, settings_store.DEFAULTS)
    th = for_route("BRI03")
    assert (th.load_min, th.load_source) == (Decimal("219"), "learned")
    # pick lead learned here is 249 (load lead + 30), above the 180 floor
    assert (th.pick_min, th.pick_source) == (Decimal("249"), "learned")
    high_floor = settings_store.Settings(**{**settings_store.DEFAULTS.__dict__, "load_floor_min": 300})
    th = profile_store.thresholds_for(profiles, high_floor)("BRI03")
    assert (th.load_min, th.load_source) == (Decimal(300), "floor")
