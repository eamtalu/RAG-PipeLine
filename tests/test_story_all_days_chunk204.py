"""Step 26 (chunk 204): Teams' story of one delivery covers every day it appears.

On 16 November 2026 "give me the details of this delivery number 33330" got two different answers:
the log explorer searched its page filter (15 November) and found the picking, 30 transactions;
Teams searched today first, found the loading of 16 November (13 transactions) and stopped, so
the picking was left out. Delivery 33330 is one delivery over two days (order 1000016095, route
BRI01). With no day named, the story now gathers every day the delivery appears in the 7 days up
to today; a day named still searches that day first."""

import json
from datetime import datetime, timezone

import pytest

from app.agent.presentation import blocks
from app.config.database import async_session
from app.services.analytics_agent import story_tools as st
from tests.golden import plant

CC = "test_chunk204"
NEXT_NOON = datetime(2026, 9, 19, 11, 0, tzinfo=timezone.utc)  # the morning after the picking, London noon


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    await plant.plant_delivery_story(CC)
    await plant.plant_delivery_loading(CC)
    yield
    await plant.wipe_analytics(CC)


async def _story(args, now=NEXT_NOON):
    async with async_session() as db:
        return await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY, **args}, now=now)


# ==================================================== 1. no day named: every day it appears


async def test_with_no_day_named_the_story_holds_the_picking_and_the_next_mornings_loading():
    out = await _story({})
    assert out["found"] is True and out["total"] == 8 and out["outside_filter"] is False
    assert "2026-09-13 to 2026-09-19" in out["looked_in"]
    assert [(d["date"], d["transactions"]) for d in out["days"]] == [("2026-09-18", 6), ("2026-09-19", 2)]
    assert out["days"][1]["first"] == "05:00:00" and out["days"][1]["users"] == ["PEVANS"]
    assert out["picks"]["lines"] == 3 and out["packages"] == 1  # the picking of the 18th is in the digest
    assert out["summary"]["users"] == ["BCHAM", "PEVANS"]
    assert out["summary"]["first"] == "2026-09-18 07:10:00" and out["summary"]["last"] == "2026-09-19 05:05:00"


async def test_a_one_day_story_keeps_its_plain_times():
    out = await _story({}, now=datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc))
    assert out["total"] == 6 and [d["date"] for d in out["days"]] == ["2026-09-18"]
    assert out["summary"]["first"] == "07:10:00"


# ==================================================== 2. a day named still searches that day first


@pytest.mark.parametrize("day", ["2026-09-19", "today"])
async def test_a_day_named_searches_that_day_first(day):
    out = await _story({"day": day})
    assert out["total"] == 2 and [t["method"] for t in out["transactions"]] == ["LoadDeliveryPackage"] * 2
    assert out["outside_filter"] is False


# ==================================================== 3. the table says the day when there are two


async def test_the_story_table_shows_the_date_when_the_story_spans_days():
    out = await _story({})
    drawn = blocks.blocks_from_trace([{"tool": "delivery_story", "input": {"value": plant.STORY_DELIVERY},
                                       "result": json.dumps(out, default=str)}])
    table = next(b for b in drawn if b["type"] == "table")
    assert table["rows"][0][0] == "2026-09-18 07:10:00" and table["rows"][-1][0] == "2026-09-19 05:05:00"


# ==================================================== 4. counts that cover only part of the rows say so


def test_a_cut_off_story_says_its_counts_cover_only_the_rows_returned():
    assert st.coverage_note(200, 150) == "picks, issues, packages and photos count the first 150 of 200 transactions"
    assert st.coverage_note(43, 43) is None


def test_the_tool_description_says_every_day_when_none_is_named():
    text = st.STORY_TOOLS[0]["description"]
    assert "every day it appears in the 7 days up to today" in text
