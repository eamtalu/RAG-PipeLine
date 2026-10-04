"""Chunk 143: the pure rules of "Deliveries at risk".

No database, no clock, no settings. The pins: a WMS departure (date `20261002`, time `1130`, in the
tenant's zone, the time sometimes arriving as the number 930 once a settlement has read it) becomes
one instant; the milk load's `PackagesToLoad` string becomes (delivery, package) pairs and garbage
becomes nothing; the tiers are judged against the VAN's usual ready time, left behind over at risk
over watch, and nothing is flagged before the warning window; a lookup gap never flags every
delivery; the coverage quantile is the time nine days in ten met and is unknown below the day floor;
and an outcome is decided by whether every package was on the van before the WMS departure, while the
plain word reads the last load against the van's usual time.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from app.services.analytics_at_risk import model as m

LONDON = ZoneInfo("Europe/London")
UTC = timezone.utc


def _state(**over) -> m.DeliveryState:
    base = dict(delivery_number="29616", route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=datetime(2026, 10, 2, 10, 30, tzinfo=UTC), lines_expected=5, lines_confirmed=0,
                lines_picked=0, lines_short=0, packages_created=0, packages_loaded=0, last_pick_at=None,
                last_load_at=None, loading_expected=True)
    base.update(over)
    if "lines_confirmed" not in over:  # the helper confirms what it picks unless a test says otherwise
        base["lines_confirmed"] = base["lines_picked"]
    return m.DeliveryState(**base)


USUAL = datetime(2026, 10, 2, 6, 0, tzinfo=UTC)  # the BRI03 van is usually ready at 07:00 BST
CLOCK = m.RouteClock(usual_ready_at=USUAL, source="learned", warn_before=timedelta(minutes=30), gone_after=timedelta(minutes=20))


def test_a_short_line_counts_as_confirmed_so_picking_closes():
    """A line declared short moved no stock. Judging on `lines_picked` flagged one closed delivery in
    four as behind although every package was on the van."""
    short = _state(lines_expected=20, lines_confirmed=20, lines_picked=19, lines_short=1, packages_created=2, packages_loaded=2)
    assert m.picking_open(short) is False
    assert m.tier_for(short, USUAL, CLOCK) is m.Tier.none
    assert m.picking_open(_state(lines_expected=20, lines_confirmed=19, lines_picked=19)) is True
    # unknown expected count: any confirmation closes it, a short one included
    assert m.picking_open(_state(lines_expected=None, lines_confirmed=1, lines_picked=0, lines_short=1)) is False


# ----------------------------------------------------------------- the three plain words

def test_category_is_missed_held_or_fine():
    dep = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    before, after = USUAL - timedelta(minutes=5), USUAL + timedelta(minutes=5)
    # missed: the van went without it
    assert m.category_for(outcome="never_loaded", last_at=None, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "missed"
    assert m.category_for(outcome="picked_late", last_at=before, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=3) == "missed"
    assert m.category_for(outcome="picked_late", last_at=None, usual_ready_at=USUAL, lines_expected=None, lines_confirmed=0) == "missed"
    # held: on the van, but after the van's usual ready time, or after the WMS departure
    assert m.category_for(outcome="loaded_in_time", last_at=after, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "held"
    assert m.category_for(outcome="loaded_late", last_at=dep + timedelta(minutes=5), usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "held"
    assert m.category_for(outcome="picked_late", last_at=dep + timedelta(minutes=5), usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "held"
    # fine: on the van before the usual time, whatever the tiers said on the way
    assert m.category_for(outcome="loaded_in_time", last_at=before, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "fine"
    assert m.category_for(outcome="picked_in_time", last_at=before, usual_ready_at=USUAL, lines_expected=None, lines_confirmed=4) == "fine"
    # no clock written (an old row): loaded in time is fine
    assert m.category_for(outcome="loaded_in_time", last_at=after, usual_ready_at=None, lines_expected=5, lines_confirmed=5) == "fine"
    # the board lost sight of it
    assert m.category_for(outcome="unknown", last_at=None, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "unknown"
    assert m.category_for(outcome=None, last_at=None, usual_ready_at=USUAL, lines_expected=5, lines_confirmed=5) == "open"


# ----------------------------------------------------------------- routes without a loading step

def test_a_route_that_never_loads_is_judged_on_picking_only():
    # BRILA routes (the Gatwick run) are picked and packed but never scanned onto a van, measured over 30 days.
    in_window = USUAL - timedelta(minutes=10)
    assert m.tier_for(_state(lines_picked=5, packages_created=2, loading_expected=False), in_window, CLOCK) is m.Tier.none
    assert m.tier_for(_state(lines_picked=2, loading_expected=False), in_window, CLOCK) is m.Tier.watch
    gone = USUAL + timedelta(minutes=21)
    assert m.tier_for(_state(lines_picked=5, loading_expected=False), gone, CLOCK) is m.Tier.none
    assert m.tier_for(_state(lines_picked=4, loading_expected=False), gone, CLOCK) is m.Tier.left_behind


def test_outcome_without_a_loading_step_is_decided_by_the_last_pick():
    dep = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    done = _state(lines_picked=5, loading_expected=False, last_pick_at=dep - timedelta(hours=2))
    assert m.outcome_for(done) == ("picked_in_time", Decimal("120"))
    late = _state(lines_picked=5, loading_expected=False, last_pick_at=dep + timedelta(minutes=10))
    assert m.outcome_for(late) == ("picked_late", Decimal("-10"))
    short = _state(lines_picked=3, loading_expected=False, last_pick_at=dep - timedelta(hours=2))
    assert m.outcome_for(short) == ("picked_late", Decimal("120"))
    assert m.outcome_for(_state(loading_expected=False)) == ("picked_late", None)
    assert done.last_at == done.last_pick_at and _state(last_load_at=dep).last_at == dep


def test_loading_is_open_only_while_fewer_packages_are_loaded_than_are_known():
    assert m.loading_open(_state(packages_created=0, packages_loaded=0)) is True
    assert m.loading_open(_state(packages_created=0, packages_loaded=1)) is False
    assert m.loading_open(_state(packages_created=3, packages_loaded=2)) is True
    assert m.loading_open(_state(packages_created=3, packages_loaded=4)) is False


# ----------------------------------------------------------------- departure

def test_departure_at_combines_wms_date_and_time_in_the_tenant_zone():
    # British Summer Time: 11:30 local is 10:30Z.
    assert m.departure_at("20261002", "1130", LONDON) == datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    # A settlement's `last` rule reads "0930" as the number 930; the model pads it back.
    assert m.departure_at("20261002", 930, LONDON) == datetime(2026, 10, 2, 8, 30, tzinfo=UTC)
    assert m.departure_at(Decimal("20261002"), Decimal("1130"), LONDON) == datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    # Winter: 11:30 local is 11:30Z.
    assert m.departure_at("20261202", "1130", LONDON) == datetime(2026, 12, 2, 11, 30, tzinfo=UTC)


@pytest.mark.parametrize("date_text,time_text", [(None, "1130"), ("20261002", None), ("", "1130"),
                                                 ("2026-10-02", "1130"), ("20261002", "2460"), ("20261399", "1130"),
                                                 ("abc", "1130"), ("20261002", "11:30")])
def test_departure_at_is_unknown_when_either_part_is_unreadable(date_text, time_text):
    assert m.departure_at(date_text, time_text, LONDON) is None


# ----------------------------------------------------------------- packages to load

def test_parse_packages_to_load_reads_the_live_string_shape_and_ignores_garbage():
    live = ('[{"DeliveryNumber":"29616","PackageNumber":"13702"},{"DeliveryNumber":"29627","PackageNumber":"13703"},'
            '{"DeliveryNumber":"29627","PackageNumber":"13704"}]')
    assert m.parse_packages_to_load(live) == [("29616", "13702"), ("29627", "13703"), ("29627", "13704")]
    # already a list (a fixture may hand the parsed value)
    assert m.parse_packages_to_load([{"DeliveryNumber": "1", "PackageNumber": "2"}]) == [("1", "2")]
    for garbage in (None, "", "not json", "{}", '[{"DeliveryNumber":"1"}]', '[1, 2]', 42, '[{"DeliveryNumber":"","PackageNumber":"x"}]'):
        assert m.parse_packages_to_load(garbage) == []


# ----------------------------------------------------------------- tiers against the van

def test_tier_is_none_before_the_warning_window_whatever_the_progress():
    early = USUAL - timedelta(minutes=31)
    assert m.tier_for(_state(), early, CLOCK) is m.Tier.none
    assert m.tier_for(_state(lines_picked=2), early, CLOCK) is m.Tier.none


def test_watch_when_picking_is_open_inside_the_window_and_at_risk_when_packages_are_off_the_van():
    in_window = USUAL - timedelta(minutes=30)  # the window edge counts
    # picking open, the one package it has is on the van: watch
    assert m.tier_for(_state(lines_picked=2, packages_created=1, packages_loaded=1), in_window, CLOCK) is m.Tier.watch
    # picking done, a package still off the van: at risk
    assert m.tier_for(_state(lines_picked=5, packages_created=2, packages_loaded=1), in_window, CLOCK) is m.Tier.at_risk
    # picking open AND packages off the van: at risk outranks watch
    assert m.tier_for(_state(lines_picked=3), in_window, CLOCK) is m.Tier.at_risk
    # nothing created at all counts as loading open
    assert m.tier_for(_state(lines_picked=5), in_window, CLOCK) is m.Tier.at_risk
    # everything picked and loaded: fine
    assert m.tier_for(_state(lines_picked=5, packages_created=2, packages_loaded=2), in_window, CLOCK) is m.Tier.none


def test_past_the_usual_time_the_tier_holds_until_the_dock_goes_quiet_then_left_behind():
    state = _state(lines_picked=5, packages_created=2, packages_loaded=1, route_loaded_at=USUAL + timedelta(minutes=10))
    # the usual time has passed but the dock was scanning 10 minutes ago: still at risk, the van has not gone
    assert m.tier_for(state, USUAL + timedelta(minutes=25), CLOCK) is m.Tier.at_risk
    assert m.van_gone(state, USUAL + timedelta(minutes=25), CLOCK) is False
    # 20 minutes of quiet after the last scan: the van is taken as gone
    assert m.tier_for(state, USUAL + timedelta(minutes=30), CLOCK) is m.Tier.left_behind
    # a dock that never scanned anything: gone once the usual time plus the window has passed
    never = _state(lines_picked=5, packages_created=2, packages_loaded=0)
    assert m.tier_for(never, USUAL + timedelta(minutes=19), CLOCK) is m.Tier.at_risk
    assert m.tier_for(never, USUAL + timedelta(minutes=20), CLOCK) is m.Tier.left_behind
    # fully loaded: never left behind, whatever the clock says
    assert m.tier_for(_state(lines_picked=5, packages_created=2, packages_loaded=2), USUAL + timedelta(hours=2), CLOCK) is m.Tier.none
    assert m.TIER_RANK[m.Tier.left_behind] > m.TIER_RANK[m.Tier.at_risk] > m.TIER_RANK[m.Tier.watch] > m.TIER_RANK[m.Tier.none]


def test_unknown_expected_lines_with_picks_recorded_is_not_picking_open():
    in_window = USUAL - timedelta(minutes=10)
    # The lookup never saw this delivery's lines. Picks exist, so we do not pretend it is behind.
    assert m.tier_for(_state(lines_expected=None, lines_picked=4, packages_created=1, packages_loaded=1), in_window, CLOCK) is m.Tier.none
    # ...but with no picks at all it is still something to watch.
    assert m.tier_for(_state(lines_expected=None, lines_picked=0, packages_created=1, packages_loaded=1), in_window, CLOCK) is m.Tier.watch


def test_the_replay_lands_on_the_minute_pass_instants():
    load = USUAL - timedelta(minutes=5)
    replay = m.replay_tiers(_state(lines_picked=5, packages_created=1, packages_loaded=1, last_load_at=load, route_loaded_at=load),
                            CLOCK, close_at=USUAL + timedelta(hours=4))
    assert [(c.tier, c.at) for c in replay.changes] == [(m.Tier.at_risk, USUAL - timedelta(minutes=29)), (m.Tier.none, load)]
    assert replay.first_flagged_at == USUAL - timedelta(minutes=29) and replay.max_tier is m.Tier.at_risk and replay.final_tier is m.Tier.none
    assert replay.changes[0].minutes_to_usual_ready == Decimal(29)
    # never loaded, the dock's last scan at usual+3: at risk, then left behind 20 minutes after that scan
    never = _state(lines_picked=5, packages_created=1, packages_loaded=0, route_loaded_at=USUAL + timedelta(minutes=3))
    replay = m.replay_tiers(never, CLOCK, close_at=USUAL + timedelta(hours=4))
    assert [(c.tier, c.at) for c in replay.changes] == [(m.Tier.at_risk, USUAL - timedelta(minutes=29)),
                                                        (m.Tier.left_behind, USUAL + timedelta(minutes=24))]
    # loaded well ahead: nothing ever
    calm = m.replay_tiers(_state(lines_picked=5, packages_created=1, packages_loaded=1, last_load_at=USUAL - timedelta(hours=2)),
                          CLOCK, close_at=USUAL + timedelta(hours=4))
    assert calm.changes == () and calm.first_flagged_at is None


def test_minutes_to_departure_is_exact_and_signed():
    dep = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    assert m.minutes_to_departure(dep, dep - timedelta(minutes=42, seconds=30)) == Decimal("42.5")
    assert m.minutes_to_departure(dep, dep + timedelta(minutes=12)) == Decimal("-12")


# ----------------------------------------------------------------- outcome

def test_outcome_is_decided_by_the_last_load_against_the_departure_instant():
    dep = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    on_time = _state(packages_created=3, packages_loaded=3, last_load_at=dep - timedelta(hours=4))
    assert m.outcome_for(on_time) == ("loaded_in_time", Decimal("240"))
    late = _state(packages_created=3, packages_loaded=3, last_load_at=dep + timedelta(minutes=5))
    assert m.outcome_for(late) == ("loaded_late", Decimal("-5"))
    partial = _state(packages_created=3, packages_loaded=2, last_load_at=dep - timedelta(hours=4))
    assert m.outcome_for(partial) == ("never_loaded", Decimal("240"))
    nothing = _state()
    assert m.outcome_for(nothing) == ("never_loaded", None)


# ----------------------------------------------------------------- learning

def test_coverage_quantile_is_unknown_below_the_sample_floor():
    assert m.coverage_quantile([Decimal(200)] * 19, coverage=Decimal("0.9"), min_sample=20) is None
    assert m.coverage_quantile([], coverage=Decimal("0.9"), min_sample=20) is None


def test_coverage_quantile_is_the_lead_nine_in_ten_deliveries_met():
    # 20 leads from 200 to 390 in steps of 10. Nine in ten met at least the 10th percentile.
    leads = [Decimal(200 + 10 * i) for i in range(20)]
    # linear interpolation at p=0.1 over 20 points: position 1.9 -> 200+10*1.9 = 219
    assert m.coverage_quantile(leads, coverage=Decimal("0.9"), min_sample=20) == Decimal("219")
    # coverage 0.5 is the median
    assert m.coverage_quantile(leads, coverage=Decimal("0.5"), min_sample=20) == Decimal("295")


def test_coverage_quantile_drops_negative_and_over_a_day_leads_before_counting_the_sample():
    leads = [Decimal(-30), Decimal(1500)] + [Decimal(300)] * 19
    # 19 usable values: below the floor of 20
    assert m.coverage_quantile(leads, coverage=Decimal("0.9"), min_sample=20) is None
    assert m.coverage_quantile(leads, coverage=Decimal("0.9"), min_sample=19) == Decimal("300")
