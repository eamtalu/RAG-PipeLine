"""Chunk 144: reading what the warehouse has done to each delivery, from the real tables.

The board is assembled from four bounded reads: the `delivery_route` settlement (departure, route,
customer), `pick_release` grouped by delivery (lines confirmed, picked, short), the `pick line`
lookup (lines expected), and one facts read for packages created and loaded, including the milk
load's list call whose deliveries live inside a JSON string. The pins: the latest routing call's
departure wins; a milk delivery shows as loaded; a delivery two days out is not on the board; the
facts read stops at its cap and says so.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.config.database import async_session
from app.services.analytics_at_risk import board_store
from tests import at_risk_fixtures as fx

CC = "test_chunk144ar"
LONDON = fx.LONDON
UTC = timezone.utc
#: 06:00 London on 2 Oct 2026, mid-way through the morning wave.
NOW = datetime(2026, 10, 2, 5, 0, tzinfo=UTC)
DEP_TODAY = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)  # 11:30 BST


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


async def _read(**over) -> board_store.BoardRead:
    async with async_session() as db:
        return await board_store.read_states(db, CC, now=over.pop("now", NOW), tz=LONDON, **over)


def _by_number(read: board_store.BoardRead) -> dict:
    return {s.delivery_number: s for s in read.states}


async def test_board_reads_departure_from_the_latest_route_call():
    await fx.plant([
        fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=12)),
        fx.route_fact(CC, "29616", route="BRI03", dep_date="20261003", dep_time="1130", when=NOW - timedelta(hours=1)),
    ])
    await fx.settle(CC)
    states = _by_number(await _read())
    assert states["29616"].departure_at == datetime(2026, 10, 3, 10, 30, tzinfo=UTC)
    assert states["29616"].route == "BRI03"
    assert states["29616"].customer_name == "BOK SHOP HORSHAM"
    assert states["29616"].customer_number == "10567"


async def test_pick_progress_comes_from_the_pick_release_rows_per_delivery():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(hours=2)
    await fx.plant([
        fx.pick_fact(CC, "29616", "600001", t, expected="4", picked="4"),
        fx.pick_fact(CC, "29616", "600002", t + timedelta(minutes=5), expected="2", picked="1"),  # short
        fx.pick_fact(CC, "29616", "600003", t + timedelta(minutes=9), expected="3", picked="0"),  # zero pick
        fx.pick_fact(CC, "29999", "600004", t, expected="1", picked="1"),  # another delivery, no route row
    ])
    await fx.settle(CC)
    states = _by_number(await _read())
    s = states["29616"]
    # three lines confirmed; two moved stock; two fell short of expected (the zero pick is short too,
    # which is how pick_release defines `is_short`)
    assert (s.lines_confirmed, s.lines_picked, s.lines_short) == (3, 2, 2)
    assert s.last_pick_at == t + timedelta(minutes=9)
    assert s.transaction_names == ("Brighton Stock Pick",)
    assert "29999" not in states  # no routing call ever named it, so it has no departure to be late for


async def test_expected_lines_come_from_the_pick_line_lookup_and_are_unknown_without_it():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3)),
                    fx.route_fact(CC, "29617", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    await fx.plant(fx.pick_line_values(CC, "29616", ["600001", "600002", "600003", "600004", "600005"]))
    await fx.settle(CC)
    states = _by_number(await _read())
    assert states["29616"].lines_expected == 5
    assert states["29617"].lines_expected is None


async def test_standard_loads_count_packages_created_and_loaded_by_delivery():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(hours=1)
    await fx.plant([
        fx.pick_fact(CC, "29616", "600001", t, expected="2", picked="2", package="29616/1-1"),
        fx.pick_fact(CC, "29616", "600002", t + timedelta(minutes=2), expected="1", picked="1", package="29616/1-2"),
        fx.load_fact(CC, "29616", "29616/1-1", t + timedelta(minutes=20), dock="BRI03"),
        fx.load_fact(CC, "29616", "29616/1-1", t + timedelta(minutes=21), dock="BRI03"),  # a retry of the same package
    ])
    await fx.settle(CC)
    s = _by_number(await _read())["29616"]
    assert (s.packages_created, s.packages_loaded) == (2, 1)
    assert s.last_load_at == t + timedelta(minutes=21)
    # the route's "van ready" moment is the last scan on its dock that day, whichever delivery it was for
    await fx.plant([fx.load_fact(CC, "29999", "29999/1-1", t + timedelta(minutes=50), dock="BRI03")])
    s = _by_number(await _read())["29616"]
    assert s.route_loaded_at == t + timedelta(minutes=50)
    assert s.last_load_at == t + timedelta(minutes=21)


async def test_milk_loads_are_parsed_out_of_the_list_call_so_a_milk_delivery_shows_as_loaded():
    await fx.plant([fx.route_fact(CC, "29625", route="BRI05", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3)),
                    fx.route_fact(CC, "29426", route="BRI05", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(minutes=30)
    await fx.plant([
        fx.pick_fact(CC, "29625", "600001", t, expected="1", picked="1", package="13716"),
        fx.pick_fact(CC, "29426", "600002", t, expected="1", picked="1", package="13714"),
        fx.pick_fact(CC, "29426", "600003", t, expected="1", picked="1", package="13715"),
        fx.load_list_fact(CC, [("29625", "13716"), ("29426", "13714")], t + timedelta(minutes=10)),
    ])
    await fx.settle(CC)
    states = _by_number(await _read())
    assert (states["29625"].packages_created, states["29625"].packages_loaded) == (1, 1)
    assert (states["29426"].packages_created, states["29426"].packages_loaded) == (2, 1)
    assert states["29625"].last_load_at == t + timedelta(minutes=10)


async def test_packages_known_are_the_ones_the_pick_confirmations_filled():
    """Not a hand-made package nobody filled (an empty box: 2 of 11 were loaded over a live week) and
    not the package number on a short line (noise, sometimes another delivery's number)."""
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(hours=2)
    await fx.plant([
        fx.pick_fact(CC, "29616", "600001", t, expected="4", picked="4", package="29616/1-1"),
        fx.pick_fact(CC, "29616", "600002", t + timedelta(minutes=5), expected="2", picked="2", package="29616/1-1"),
        fx.pick_fact(CC, "29616", "600003", t + timedelta(minutes=9), expected="3", picked="3", package="29616/2-1"),
        fx.pick_fact(CC, "29616", "600004", t + timedelta(minutes=12), expected="1", picked="1", package=""),  # 7% carry none
        fx.pick_fact(CC, "29616", "600005", t + timedelta(minutes=14), expected="2", picked="0", package="28874/3-1"),  # short: not a package
        fx.package_fact(CC, "29616", "29616/3-1", t + timedelta(minutes=20)),  # hand-made, never filled: an empty box
        fx.load_fact(CC, "29616", "29616/1-1", t + timedelta(minutes=30), dock="BRI03"),
    ])
    await fx.settle(CC)
    s = _by_number(await _read())["29616"]
    assert (s.packages_created, s.packages_loaded) == (2, 1)


async def test_a_route_that_loaded_nothing_in_the_lookback_has_no_loading_step():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3)),
                    fx.route_fact(CC, "28518", route="BRILAT", dep_date="20261002", dep_time="1200", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(hours=1)
    await fx.plant([fx.package_fact(CC, "29616", "29616/1-1", t), fx.load_fact(CC, "29616", "29616/1-1", t + timedelta(minutes=5), dock="BRI03"),
                    fx.package_fact(CC, "28518", "28518/1-1", t, route="BRILAT")])
    await fx.settle(CC)
    states = _by_number(await _read())
    assert states["29616"].loading_expected is True
    assert states["28518"].loading_expected is False
    # the caller can widen it with what the route's history says
    read = await _read(routes_that_load={"BRILAT"})
    assert _by_number(read)["28518"].loading_expected is True


async def test_a_delivery_two_days_out_or_two_days_gone_is_not_on_the_board():
    await fx.plant([
        fx.route_fact(CC, "1", route="BRI01", dep_date="20261004", dep_time="1130", when=NOW - timedelta(hours=1)),  # two days out
        fx.route_fact(CC, "2", route="BRI01", dep_date="20260930", dep_time="1130", when=NOW - timedelta(hours=1)),  # two days gone
        fx.route_fact(CC, "3", route="BRI01", dep_date="20261003", dep_time="1130", when=NOW - timedelta(hours=1)),  # tomorrow
        fx.route_fact(CC, "4", route="BRI01", dep_date="20261001", dep_time="1130", when=NOW - timedelta(hours=1)),  # yesterday, still closable
        fx.route_fact(CC, "5", route="BRI01", dep_date="garbage", dep_time="1130", when=NOW - timedelta(hours=1)),
    ])
    await fx.settle(CC)
    read = await _read()
    assert sorted(_by_number(read)) == ["3", "4"]
    assert read.unreadable_departures == 1


async def test_the_facts_read_stops_at_its_cap_and_reports_overflow():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=3))])
    t = NOW - timedelta(hours=1)
    await fx.plant([fx.pick_fact(CC, "29616", f"60000{i}", t + timedelta(seconds=i), expected="1", picked="1", package=f"29616/1-{i}")
                    for i in range(6)])
    await fx.settle(CC)
    read = await _read(facts_cap=4)
    assert read.overflow is True
    assert _by_number(read)["29616"].packages_created == 4
    assert (await _read(facts_cap=6)).overflow is False
