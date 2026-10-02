"""Chunk 151: the backfill, so the history is not empty on day one.

The worker can only judge a delivery whose routing call was folded after the departure fields were
approved, and approving a field never rewrites old facts. The backfill reads the departures of past
days straight from the routing calls' response text in `log_transactions`, reuses the live reads for
picks, lines and loads, replays the tier rule over the clocks, and writes CLOSED rows marked
`reconstructed`. The pins: the replay flags at the same instants the minute pass would have; the
universe is the latest routing call per delivery, kept to the day asked for; a live row is never
overwritten; a day that has not closed is refused; the accuracy score ignores reconstructed rows,
while the history and the route learning use them.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile
from app.services.analytics_at_risk import backfill, delivery_store, model
from tests import at_risk_fixtures as fx

CC = "test_chunk151ar"
LONDON = fx.LONDON
UTC = timezone.utc
DAY = date(2026, 10, 1)
DEP = datetime(2026, 10, 1, 10, 30, tzinfo=UTC)  # 11:30 BST on 1 Oct
GRACE = timedelta(minutes=180)
TH = model.Thresholds(load_min=Decimal(120), load_source="floor", pick_min=Decimal(180), pick_source="floor")


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


def _state(**over) -> model.DeliveryState:
    base = dict(delivery_number="29616", route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=DEP, lines_expected=2, lines_confirmed=2, lines_picked=2, lines_short=0,
                packages_created=1, packages_loaded=1, last_pick_at=DEP - timedelta(hours=5), last_load_at=DEP - timedelta(hours=4))
    base.update(over)
    return model.DeliveryState(**base)


# ============================================================== the replay (pure)

def test_a_delivery_loaded_well_ahead_is_never_flagged():
    replay = model.replay_tiers(_state(), TH, close_at=DEP + GRACE)
    assert replay.changes == () and replay.max_tier is model.Tier.none and replay.first_flagged_at is None
    assert replay.final_tier is model.Tier.none


def test_a_load_inside_the_lead_is_flagged_at_the_minute_the_lead_was_crossed_and_cleared_at_the_load():
    load = DEP - timedelta(minutes=30)
    replay = model.replay_tiers(_state(last_load_at=load), TH, close_at=DEP + GRACE)
    assert [(c.tier, c.at) for c in replay.changes] == [
        (model.Tier.at_risk, DEP - timedelta(minutes=120) + timedelta(minutes=1)),
        (model.Tier.none, load),
    ]
    assert replay.first_flagged_at == DEP - timedelta(minutes=119) and replay.first_flagged_tier is model.Tier.at_risk
    assert replay.max_tier is model.Tier.at_risk and replay.final_tier is model.Tier.none
    assert replay.changes[0].minutes_to_departure == Decimal(119)


def test_a_package_never_loaded_goes_at_risk_then_late_and_stays_late():
    replay = model.replay_tiers(_state(packages_loaded=0, last_load_at=None), TH, close_at=DEP + GRACE)
    assert [(c.tier, c.at) for c in replay.changes] == [
        (model.Tier.at_risk, DEP - timedelta(minutes=119)),
        (model.Tier.late, DEP + timedelta(minutes=1)),
    ]
    assert replay.final_tier is model.Tier.late


def test_slow_picking_is_watched_then_at_risk_then_cleared():
    pick, load = DEP - timedelta(minutes=150), DEP - timedelta(minutes=100)
    replay = model.replay_tiers(_state(last_pick_at=pick, last_load_at=load), TH, close_at=DEP + GRACE)
    assert [(c.tier, c.at) for c in replay.changes] == [
        (model.Tier.watch, DEP - timedelta(minutes=179)),
        (model.Tier.none, pick),
        (model.Tier.at_risk, DEP - timedelta(minutes=119)),
        (model.Tier.none, load),
    ]
    assert replay.max_tier is model.Tier.at_risk


def test_a_route_without_a_loading_step_is_judged_on_its_last_pick():
    pick = DEP + timedelta(minutes=10)
    replay = model.replay_tiers(_state(loading_expected=False, packages_loaded=0, last_load_at=None, last_pick_at=pick),
                                TH, close_at=DEP + GRACE)
    assert [(c.tier, c.at) for c in replay.changes] == [
        (model.Tier.watch, DEP - timedelta(minutes=179)),
        (model.Tier.late, DEP + timedelta(minutes=1)),
        (model.Tier.none, pick),
    ]


# ============================================================== the routing universe from the raw calls

async def _plant_calls(rows) -> None:
    await fx.plant_calls(CC, rows)


async def test_the_universe_is_the_latest_routing_call_per_delivery_kept_to_the_day_asked_for():
    t = DEP - timedelta(hours=16)
    await _plant_calls([
        fx.routing_call("29616", route="BRI03", dep_date="20260930", dep_time="1130", when=t),
        fx.routing_call("29616", route="BRI03", dep_date="20261001", dep_time="1130", when=t + timedelta(hours=1)),  # moved: latest wins
        fx.routing_call("29617", route="BRILAT", dep_date="20261001", dep_time="1200", when=t, customer_name="SHAKE SHACK", status="soft"),
        fx.routing_call("29618", route="BRI01", dep_date="20261002", dep_time="1130", when=t),  # tomorrow: not this day
        fx.routing_call("29619", route="BRI01", dep_date="garbage", dep_time="1130", when=t),
    ])
    async with async_session() as db:
        routes, unreadable = await backfill.routing_universe(db, CC, day=DAY, tz=LONDON)
    assert sorted(routes) == ["29616", "29617"]
    assert routes["29616"].departure_at == DEP and routes["29616"].route == "BRI03"
    assert routes["29616"].customer_name == "BOK SHOP HORSHAM" and routes["29616"].customer_number == "10567"
    assert routes["29617"].departure_at == datetime(2026, 10, 1, 11, 0, tzinfo=UTC) and routes["29617"].customer_name == "SHAKE SHACK"
    assert unreadable == 1


# ============================================================== writing the day

async def _plant_day() -> None:
    t = DEP - timedelta(hours=16)
    await _plant_calls([
        fx.routing_call("29616", route="BRI03", dep_date="20261001", dep_time="1130", when=t),
        fx.routing_call("29700", route="BRI03", dep_date="20261001", dep_time="1130", when=t, customer_name="HILTON"),
        fx.routing_call("29800", route="BRI03", dep_date="20261001", dep_time="1130", when=t, customer_name="LIVE ROW"),
    ])
    await fx.plant(fx.pick_line_values(CC, "29616", ["600001", "600002"]) + fx.pick_line_values(CC, "29700", ["600003"]))
    p = DEP - timedelta(hours=5)
    await fx.plant([
        fx.pick_fact(CC, "29616", "600001", p, expected="2", picked="2", package="29616/1-1"),
        fx.pick_fact(CC, "29616", "600002", p + timedelta(minutes=2), expected="1", picked="1", package="29616/1-1"),
        fx.load_fact(CC, "29616", "29616/1-1", DEP - timedelta(minutes=30), dock="BRI03"),  # inside the 120 lead
        fx.pick_fact(CC, "29700", "600003", p, expected="1", picked="1", package="29700/1-1"),  # picked, never loaded
        fx.load_fact(CC, "29900", "29900/1-1", DEP - timedelta(hours=4), dock="BRI03"),  # the dock's last scan is 29616's
        fx.load_fact(CC, "29616", "29616/1-1", DEP + timedelta(hours=4), dock="BRI03"),  # after the close: must not count
    ])
    await fx.settle(CC)
    await fx.plant([fx.closed_delivery(CC, "29800", route="BRI03", departure_at=DEP, last_load_at=DEP - timedelta(hours=4),
                                       outcome="loaded_in_time")])


async def _rows() -> dict[str, AnalyticsAtRiskDelivery]:
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))).scalars().all()
    return {r.delivery_number: r for r in rows}


async def test_backfill_writes_closed_reconstructed_rows_and_leaves_the_live_row_alone():
    await _plant_day()
    out = await backfill.backfill_tenant(CC, start=DAY, end=DAY, now=DEP + timedelta(days=1))
    assert out["status"] == "completed"
    assert out["days"] == [{"date": "2026-10-01", "deliveries": 3, "written": 2, "existing": 1, "unreadable": 0,
                            "missed": 1, "delayed": 1, "fine": 0}]
    rows = await _rows()
    a = rows["29616"]
    assert (a.status, a.outcome, a.reconstructed, a.closed_at) == ("closed", "loaded_in_time", True, DEP + GRACE)
    assert (a.max_tier, a.tier, a.first_flagged_tier, a.first_flagged_at) == ("at_risk", "none", "at_risk", DEP - timedelta(minutes=119))
    assert a.last_load_at == DEP - timedelta(minutes=30) and a.route_loaded_at == DEP - timedelta(minutes=30)
    assert (a.lines_expected, a.lines_picked, a.packages_created, a.packages_loaded) == (2, 2, 1, 1)
    assert a.transaction_names == ["Brighton Stock Pick"] and a.customer_name == "BOK SHOP HORSHAM"
    assert (a.load_threshold_min, a.load_threshold_source) == (Decimal("120"), "floor")
    assert [h["tier"] for h in a.tier_history] == ["at_risk", "none"]
    b = rows["29700"]
    assert (b.outcome, b.max_tier, b.tier, b.reconstructed) == ("never_loaded", "late", "late", True)
    live = rows["29800"]
    assert live.reconstructed is False and live.rule_version == fx.RULE_VERSION and live.max_tier == "none"
    async with async_session() as db:
        profile = await db.scalar(select(AnalyticsAtRiskRouteProfile).where(
            AnalyticsAtRiskRouteProfile.customer_code == CC, AnalyticsAtRiskRouteProfile.as_of_date == DAY))
    assert profile is not None and profile.route == "BRI03" and profile.sample == 3  # the live row and both reconstructed rows teach
    # a second run changes nothing
    again = await backfill.backfill_tenant(CC, start=DAY, end=DAY, now=DEP + timedelta(days=1))
    assert again["days"][0]["written"] == 0 and again["days"][0]["existing"] == 3


async def test_a_day_whose_deliveries_have_not_closed_is_refused():
    await _plant_day()
    out = await backfill.backfill_tenant(CC, start=DAY, end=DAY, now=DEP + timedelta(hours=1))
    assert out["days"] == [{"date": "2026-10-01", "skipped": "not closed yet"}]
    assert await _rows() == {} or all(r.delivery_number == "29800" for r in (await _rows()).values())


async def test_the_accuracy_score_ignores_reconstructed_rows_but_the_history_shows_them():
    await _plant_day()
    await backfill.backfill_tenant(CC, start=DAY, end=DAY, now=DEP + timedelta(days=1))
    async with async_session() as db:
        agg = await delivery_store.accuracy(db, CC, start=DAY, end=DAY)
        rows, _ = await delivery_store.history_rows(db, CC, start=DAY, end=DAY)
    assert agg["routes"]["BRI03"]["departures"] == 1 and agg["by_tier"] == {}
    assert sorted(r.delivery_number for r in rows) == ["29616", "29700", "29800"]


async def test_the_api_row_says_when_it_was_reconstructed():
    from app.api.v1 import analytics_at_risk as api
    await _plant_day()
    await backfill.backfill_tenant(CC, start=DAY, end=DAY, now=DEP + timedelta(days=1))
    async with async_session() as db:
        out = await api.read_history(start="2026-10-01", end="2026-10-01", customer=CC, db=db)
    by = {r["delivery_number"]: r for r in out["rows"]}
    assert by["29616"]["reconstructed"] is True and by["29800"]["reconstructed"] is False
    assert by["29616"]["category"] == "delayed" and by["29700"]["category"] == "missed"
