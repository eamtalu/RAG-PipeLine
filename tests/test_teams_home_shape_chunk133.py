"""Chunk 133: "Shape of the day" on the Teams Home tab.

The pick releases page draws lines per hour for the last 24 hours, stacked by transaction, with the
pickers active each hour beneath. The Teams tab shows the same chart under its stat tiles, so the
consumer adds it to the Home snapshot: 24 clock hours on the tenant's clock, every label and number
pre-formatted, the axis and the peaks decided here so the page only draws.
"""

import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.services.teams import home_shape, home_snapshot
from tests.test_analytics_agent_chunk124 import BODY
from tests.test_teams_home_snapshot_chunk127 import CC, _fact, clean  # noqa: F401  (clean is the fixture)

TZ = ZoneInfo("Europe/London")


# ==================================================== 1. transactions by kind

@pytest.mark.parametrize("name, code", [
    ("JIT and Shorts Pick (Brighton)", "jit"),
    ("Milk Pick (Brighton)", "milk"),
    ("Freezer Pick (Brighton)", "frz"),
    ("Frozen Pick", "frz"),
    ("Stock Pick (Brighton)", "stock"),
    ("Bulk Pick Pack", "other"),
    (None, "other"),
])
def test_a_transaction_name_is_read_as_its_kind_the_way_the_pick_releases_page_reads_it(name, code):
    assert home_shape.tx_code(name) == code


# ==================================================== 2. the axis

@pytest.mark.parametrize("peak, top, ticks", [
    (112, 150, ["0", "50", "100", "150"]),
    (0, 1, ["0", "1"]),
    (3, 3, ["0", "1", "2", "3"]),
    (10, 10, ["0", "5", "10"]),
    (25, 30, ["0", "10", "20", "30"]),
    (90, 100, ["0", "25", "50", "75", "100"]),
    (7, 8, ["0", "2", "4", "6", "8"]),
    (1800, 2000, ["0", "500", "1,000", "1,500", "2,000"]),
])
def test_the_axis_rounds_the_peak_up_to_a_nice_whole_step(peak, top, ticks):
    """The page's steps (1, 2, 2.5, 5, 10 times a power of ten), but lines are whole: a step of 2.5
    or 0.25 would label a tick "3" that sits at 2.5, so below 10 the 2.5 step is skipped."""
    axis = home_shape.axis(peak)
    assert axis["top"] == top and [t["text"] for t in axis["ticks"]] == ticks


# ==================================================== 3. the hours, pure

def _hours_at(local_now: datetime) -> list[datetime]:
    return home_shape.hour_starts(local_now)


def test_the_window_is_24_clock_hours_ending_with_the_current_one():
    hours = _hours_at(datetime(2026, 9, 30, 19, 59))
    assert len(hours) == 24
    assert hours[0] == datetime(2026, 9, 29, 20, 0) and hours[-1] == datetime(2026, 9, 30, 19, 0)


def test_hours_are_stacked_by_kind_with_pickers_and_peaks():
    hours = _hours_at(datetime(2026, 9, 30, 19, 59))
    lines = [
        {"dimensions": ["2026-09-29T23:00:00", "Stock Pick (Brighton)"], "rows": 112},
        {"dimensions": ["2026-09-30T06:00:00", "JIT and Shorts Pick (Brighton)"], "rows": 88},
        {"dimensions": ["2026-09-30T06:00:00", "Freezer Pick (Brighton)"], "rows": 4},
        {"dimensions": ["2026-09-30T06:00:00", "Stock Pick (Brighton)"], "rows": 2},
        {"dimensions": ["2026-09-30T02:00:00", "Milk Pick (Brighton)"], "rows": 33},
        {"dimensions": ["2026-09-28T06:00:00", "Stock Pick (Brighton)"], "rows": 999},  # outside the window
    ]
    pickers = [
        {"dimensions": ["2026-09-29T23:00:00"], "distinct_user_name": 2},
        {"dimensions": ["2026-09-30T06:00:00"], "distinct_user_name": 7},
    ]
    shape = home_shape.build(hours, lines, pickers)

    assert shape["caption"] == "29 Sep 20:00 → 30 Sep 20:00"
    assert [x["tx"] for x in shape["legend"]] == ["stock", "jit", "milk", "frz"]
    six = shape["hours"][10]
    assert six["label"] == "06:00" and six["range"] == "06:00 – 07:00"
    assert six["total"] == 94 and six["total_text"] == "94"
    # bottom to top in the page's order: stock, jit, milk, frz
    assert [(s["tx"], s["lines"]) for s in six["segments"]] == [("stock", 2), ("jit", 88), ("frz", 4)]
    assert six["pickers"] == 7 and six["pickers_text"] == "7"
    assert shape["peak"] == 3 and shape["hours"][3]["total_text"] == "112"
    assert shape["peak_pickers"] == 10 and shape["peak_pickers_text"] == "7 pickers"
    assert shape["pickers_top"] == 7 and shape["pickers_top_text"] == "7"
    assert shape["axis"]["top"] == 150
    assert sum(h["total"] for h in shape["hours"]) == 239  # the row from the day before is not counted


def test_a_quiet_day_has_no_peaks_and_one_picker_reads_singular():
    hours = _hours_at(datetime(2026, 9, 30, 9, 5))
    empty = home_shape.build(hours, [], [])
    assert empty["peak"] is None and empty["peak_pickers"] is None and empty["legend"] == []
    assert empty["pickers_top_text"] == "1" and all(h["segments"] == [] for h in empty["hours"])
    one = home_shape.build(hours, [{"dimensions": ["2026-09-30T09:00:00", "Stock Pick"], "rows": 1}],
                           [{"dimensions": ["2026-09-30T09:00:00"], "distinct_user_name": 1}])
    assert one["peak_pickers_text"] == "1 picker"


# ==================================================== 4. in the snapshot, from settled rows

async def _plant_across_kinds(now: datetime):
    """An hour ago: two stock lines by two pickers and a milk line. Three hours ago: a JIT line. A
    day and a half ago: a stock line the 24-hour window must leave out."""
    hour_ago, three_ago, long_ago = now - timedelta(minutes=55), now - timedelta(hours=3), now - timedelta(hours=36)

    def fact(when, rep, tx, user):
        f = _fact(when, 5, 5, rep=rep, delivery="D" + rep, user=user)
        f.transaction_name = tx
        f.id = uuid.uuid4()
        return f

    facts = [
        fact(hour_ago, "S1", "Stock Pick (Brighton)", "BCHAM"),
        fact(hour_ago, "S2", "Stock Pick (Brighton)", "DBOBOC"),
        fact(hour_ago, "M1", "Milk Pick (Brighton)", "DBOBOC"),
        fact(three_ago, "J1", "JIT and Shorts Pick (Brighton)", "BCHAM"),
        fact(long_ago, "OLD", "Stock Pick (Brighton)", "BCHAM"),
    ]
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)


def _hour_of(shape: dict, when: datetime) -> dict:
    label = when.astimezone(TZ).strftime("%H:00")
    return next(h for h in shape["hours"] if h["label"] == label)


async def test_the_snapshot_carries_the_last_24_hours_by_kind_and_pickers():
    now = datetime.now(timezone.utc)
    await _plant_across_kinds(now)
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    shape = snap["shape"]
    assert len(shape["hours"]) == 24
    assert shape["hours"][-1]["label"] == now.astimezone(TZ).strftime("%H:00")
    recent = _hour_of(shape, now - timedelta(minutes=55))
    assert [(s["tx"], s["lines"]) for s in recent["segments"]] == [("stock", 2), ("milk", 1)]
    assert recent["pickers"] == 2
    assert [(s["tx"], s["lines"]) for s in _hour_of(shape, now - timedelta(hours=3))["segments"]] == [("jit", 1)]
    assert sum(h["total"] for h in shape["hours"]) == 4  # the line from 36 hours ago is outside


async def test_an_empty_window_is_a_flat_chart_not_an_error():
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        snap = await home_snapshot.compute(db, CC)
    shape = snap["shape"]
    assert len(shape["hours"]) == 24 and shape["peak"] is None and all(h["total"] == 0 for h in shape["hours"])
