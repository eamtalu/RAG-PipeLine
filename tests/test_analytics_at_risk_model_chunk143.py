"""Chunk 143: the pure rules of "Deliveries at risk".

No database, no clock, no settings. The pins: a WMS departure (date `20261002`, time `1130`, in the
tenant's zone, the time sometimes arriving as the number 930 once a settlement has read it) becomes
one instant; the milk load's `PackagesToLoad` string becomes (delivery, package) pairs and garbage
becomes nothing; the three tiers rank late over at-risk over watch; a lookup gap never flags every
delivery; the learned lead is the lead nine in ten deliveries met, which is the TENTH percentile of
lead minutes, and it is unknown below the sample floor; the floor only ever tightens; and an outcome
is decided by whether every package was on the van before the departure instant.
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
    return m.DeliveryState(**base)


# ----------------------------------------------------------------- the three plain categories

def test_category_is_missed_delayed_or_fine():
    # missed: the van left without it
    assert m.category_for(outcome="never_loaded", max_tier="late", lines_expected=5, lines_picked=5) == "missed"
    assert m.category_for(outcome="picked_late", max_tier="late", lines_expected=5, lines_picked=3) == "missed"
    assert m.category_for(outcome="picked_late", max_tier="late", lines_expected=None, lines_picked=0) == "missed"
    # delayed: it got away, but behind the rhythm or after the departure
    assert m.category_for(outcome="loaded_late", max_tier="none", lines_expected=5, lines_picked=5) == "delayed"
    assert m.category_for(outcome="loaded_in_time", max_tier="at_risk", lines_expected=5, lines_picked=5) == "delayed"
    assert m.category_for(outcome="loaded_in_time", max_tier="watch", lines_expected=5, lines_picked=5) == "delayed"
    assert m.category_for(outcome="picked_late", max_tier="late", lines_expected=5, lines_picked=5) == "delayed"
    assert m.category_for(outcome="picked_in_time", max_tier="watch", lines_expected=5, lines_picked=5) == "delayed"
    # fine: in time and never flagged
    assert m.category_for(outcome="loaded_in_time", max_tier="none", lines_expected=5, lines_picked=5) == "fine"
    assert m.category_for(outcome="picked_in_time", max_tier="none", lines_expected=None, lines_picked=4) == "fine"
    # the board lost sight of it
    assert m.category_for(outcome="unknown", max_tier="none", lines_expected=5, lines_picked=5) == "unknown"
    assert m.category_for(outcome=None, max_tier="at_risk", lines_expected=5, lines_picked=5) == "open"


# ----------------------------------------------------------------- routes without a loading step

def test_a_route_that_never_loads_is_judged_on_picking_only():
    # BRILA routes (the Gatwick run) are picked and packed but never scanned onto a van, measured over 30 days.
    inside_load = datetime(2026, 10, 2, 9, 0, tzinfo=UTC)  # 90 min left: inside the load lead
    assert m.tier_for(_state(lines_picked=5, packages_created=2, loading_expected=False), inside_load, THRESHOLDS) is m.Tier.none
    assert m.tier_for(_state(lines_picked=2, loading_expected=False), inside_load, THRESHOLDS) is m.Tier.watch
    after = datetime(2026, 10, 2, 10, 31, tzinfo=UTC)
    assert m.tier_for(_state(lines_picked=5, loading_expected=False), after, THRESHOLDS) is m.Tier.none
    assert m.tier_for(_state(lines_picked=4, loading_expected=False), after, THRESHOLDS) is m.Tier.late


def test_outcome_without_a_loading_step_is_decided_by_the_last_pick():
    dep = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)
    done = _state(lines_picked=5, loading_expected=False, last_pick_at=dep - timedelta(hours=2))
    assert m.outcome_for(done) == ("picked_in_time", Decimal("120"))
    late = _state(lines_picked=5, loading_expected=False, last_pick_at=dep + timedelta(minutes=10))
    assert m.outcome_for(late) == ("picked_late", Decimal("-10"))
    short = _state(lines_picked=3, loading_expected=False, last_pick_at=dep - timedelta(hours=2))
    assert m.outcome_for(short) == ("picked_late", Decimal("120"))
    assert m.outcome_for(_state(loading_expected=False)) == ("picked_late", None)


def test_loading_is_open_only_while_fewer_packages_are_loaded_than_are_known():
    # The known packages come from the pick confirmations plus any extra package created by hand.
    # Loads of packages nobody "created" through the app still count: nothing loaded is open, anything
    # loaded that covers every known package is closed.
    assert m.loading_open(_state(packages_created=0, packages_loaded=0)) is True
    assert m.loading_open(_state(packages_created=0, packages_loaded=1)) is False
    assert m.loading_open(_state(packages_created=3, packages_loaded=2)) is True
    assert m.loading_open(_state(packages_created=3, packages_loaded=4)) is False


THRESHOLDS = m.Thresholds(load_min=Decimal("120"), load_source="floor", pick_min=Decimal("180"), pick_source="floor")


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


# ----------------------------------------------------------------- tiers

def test_tier_is_none_with_plenty_of_time_left():
    now = datetime(2026, 10, 2, 3, 0, tzinfo=UTC)  # 450 min before departure
    assert m.tier_for(_state(), now, THRESHOLDS) is m.Tier.none


def test_watch_when_picking_is_open_inside_the_pick_lead():
    now = datetime(2026, 10, 2, 8, 0, tzinfo=UTC)  # 150 min left: inside pick lead (180), outside load lead (120)
    assert m.tier_for(_state(lines_picked=3), now, THRESHOLDS) is m.Tier.watch
    # picking done, nothing loaded yet, but loading is not yet inside its lead
    assert m.tier_for(_state(lines_picked=5, lines_confirmed=5), now, THRESHOLDS) is m.Tier.none


def test_at_risk_when_loading_is_open_inside_the_load_lead():
    now = datetime(2026, 10, 2, 9, 0, tzinfo=UTC)  # 90 min left
    assert m.tier_for(_state(lines_picked=5, lines_confirmed=5, packages_created=2, packages_loaded=1), now, THRESHOLDS) is m.Tier.at_risk
    # no package created at all counts as loading open
    assert m.tier_for(_state(lines_picked=5, lines_confirmed=5), now, THRESHOLDS) is m.Tier.at_risk
    # everything loaded: fine
    assert m.tier_for(_state(lines_picked=5, lines_confirmed=5, packages_created=2, packages_loaded=2), now, THRESHOLDS) is m.Tier.none


def test_at_risk_outranks_watch_and_late_outranks_both():
    inside_both = datetime(2026, 10, 2, 9, 0, tzinfo=UTC)
    assert m.tier_for(_state(lines_picked=3), inside_both, THRESHOLDS) is m.Tier.at_risk
    after = datetime(2026, 10, 2, 10, 31, tzinfo=UTC)
    assert m.tier_for(_state(lines_picked=3), after, THRESHOLDS) is m.Tier.late
    assert m.tier_for(_state(lines_picked=5, packages_created=2, packages_loaded=1), after, THRESHOLDS) is m.Tier.late
    # fully picked and loaded before departure: never late
    assert m.tier_for(_state(lines_picked=5, packages_created=2, packages_loaded=2), after, THRESHOLDS) is m.Tier.none
    assert m.TIER_RANK[m.Tier.late] > m.TIER_RANK[m.Tier.at_risk] > m.TIER_RANK[m.Tier.watch] > m.TIER_RANK[m.Tier.none]


def test_unknown_expected_lines_with_picks_recorded_is_not_picking_open():
    now = datetime(2026, 10, 2, 8, 0, tzinfo=UTC)  # inside the pick lead only
    # The lookup never saw this delivery's lines. Picks exist, so we do not pretend it is behind.
    assert m.tier_for(_state(lines_expected=None, lines_picked=4), now, THRESHOLDS) is m.Tier.none
    # ...but with no picks at all it is still something to watch.
    assert m.tier_for(_state(lines_expected=None, lines_picked=0), now, THRESHOLDS) is m.Tier.watch


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


def test_effective_threshold_is_the_larger_of_learned_and_floor():
    assert m.effective_threshold(Decimal("219"), Decimal("120")) == (Decimal("219"), "learned")
    assert m.effective_threshold(Decimal("90"), Decimal("120")) == (Decimal("120"), "floor")
    assert m.effective_threshold(None, Decimal("120")) == (Decimal("120"), "floor")
    # a tie is the learned value: the floor did not change anything
    assert m.effective_threshold(Decimal("120"), Decimal("120")) == (Decimal("120"), "learned")
