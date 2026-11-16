"""Step 27 (chunk 205): the release tools read the window the model meant.

Found in the live golden run of 16 November 2026 after D26, both older than steps 24 to 26:

1. "how many picked on 18 September 2026" answered "I could not get that" in 2 of 4 reruns: the
   model filtered `event_time>=2026-09-18T00:00:00` with no start or end, and the tools fell back
   to the last 24 hours, so every row was outside the window and nothing said why.
2. "Which items were short on several releases in the week to 18 September 2026?" answered "No
   items were short" in 2 of 4 reruns: `end: "2026-09-18"` was read as midnight at the START of
   the 18th, so the whole last day was left out (11 units were short on it).
3. A quoted value, `event_time>='2026-09-18T00:00:00'`, came back as a raw ValueError."""

import pytest

from app.config.database import async_session
from app.services.analytics_agent import tools as agent_tools
from tests.golden import plant

CC = "test_chunk205"
SHORT = {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short"}


@pytest.fixture(autouse=True)
async def clean():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


async def _aggregate(args):
    async with async_session() as db:
        return await agent_tools.aggregate_releases(db, dict(args), CC)


async def _reference():
    """The planted day read the way the scripted cases read it."""
    return await _aggregate({**plant.window()})


# ==================================================== 1. a date-only end is the whole of that day


async def test_a_week_ending_on_a_date_includes_that_whole_day():
    out = await _aggregate({**SHORT, "start": "2026-09-11", "end": "2026-09-18"})
    assert out["overall"]["shortfall"] == "-11" and out["groups"] == 2
    assert out["window"]["end"].startswith("2026-09-19T00:00:00")
    assert any("whole of 2026-09-18" in n for n in out.get("notes", []))


async def test_an_end_with_a_time_is_kept_as_it_is():
    out = await _aggregate({"start": "2026-09-18T00:00:00", "end": "2026-09-18T05:00:00"})
    assert out["window"]["end"].startswith("2026-09-18T05:00:00")
    assert not any("whole of" in n for n in out.get("notes", []))


# ==================================================== 2. a where on the time sets the window when start and end are not given


@pytest.mark.parametrize("where", [
    ["event_time>=2026-09-18T00:00:00", "event_time<2026-09-19T00:00:00"],
    ["event_time>='2026-09-18T00:00:00'", "event_time<'2026-09-19T00:00:00'"],
    ["business_date==2026-09-18"],
    ["event_time>=2026-09-18"],
])
async def test_a_time_filter_without_start_or_end_sets_the_window(where):
    reference = await _reference()
    out = await _aggregate({"where": where})
    assert out["rows"][0]["rows"] == reference["rows"][0]["rows"] == 4
    assert out["rows"][0]["picked"] == reference["rows"][0]["picked"]
    assert any("window follows the where" in n for n in out.get("notes", []))


async def test_start_and_end_given_still_win_over_a_time_filter():
    out = await _aggregate({"start": "2026-09-01T00:00:00", "end": "2026-09-02T00:00:00", "where": ["event_time>=2026-09-18"]})
    assert out["window"]["start"].startswith("2026-09-01") and out["rows"][0]["rows"] == 0


async def test_with_no_time_named_anywhere_the_window_is_still_the_last_day():
    out = await _aggregate({"where": ["shortfall<0"]})
    assert not any("window follows the where" in n for n in out.get("notes", []))


async def test_the_list_tool_reads_the_same_window():
    async with async_session() as db:
        out = await agent_tools.list_releases(db, {"where": ["event_time>=2026-09-18", "event_time<2026-09-19"]}, CC)
    assert out["total"] == 4 and len(out["rows"]) == 4
    assert any("window follows the where" in n for n in out.get("notes", []))
