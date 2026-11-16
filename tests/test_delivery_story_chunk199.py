"""Step 21 (chunk 199): Teams Ask answers "details of a delivery" from the delivery's own log story.

On 15 November the same question, "give me the details of this delivery number 33330", got the
full transaction story in the log explorer (logspace profile, `trace`) and only the at-risk status
in Teams (analytics profile, `explain_at_risk_delivery`), because the analytics profile had no
story tool and routed "what happened to one delivery" to the at-risk tools. `delivery_story` gives
the analytics profile the same story, without the LAN links Teams users cannot open."""

import json
from datetime import datetime, timezone

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks
from app.agent.profiles import load_profile
from app.config.database import async_session
from app.services.analytics_agent import story_tools as st
from tests.agent_helpers import analytics_runtime, ask, tool_registry
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk199"
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)  # noon on the planted day


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    await plant.plant_delivery_story(CC)
    yield
    await plant.wipe_analytics(CC)


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result, default=str)}


# ==================================================== 1. the story


async def test_the_story_of_one_delivery_counts_the_picks_names_the_soft_error_and_carries_no_link():
    async with async_session() as db:
        out = await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY}, now=NOW)
    assert out["found"] is True and out["key"] == "delivery_number" and out["total"] == 6
    assert out["summary"]["users"] == ["BCHAM"] and out["summary"]["first"] == "07:10:00" and out["summary"]["last"] == "07:20:00"
    assert out["picks"] == {"lines": 3, "exact": 2, "over": 1, "partial": 0, "zero_pick": 0}
    # step 24: the difference travels with it, so "over by 0.08" is the tool's figure
    assert out["not_exact"] == [{"item": "104441", "reporting_number": "889666", "picked": "4.08", "expected": "4",
                                 "difference": "0.08", "outcome": "over"}]
    assert out["issues"] == [{"time": "07:10:00", "method": "NewDeliveryPackage", "status": "soft",
                              "message": "No packages exist for delivery number " + plant.STORY_DELIVERY}]
    assert out["packages"] == 1 and out["photos"] == 1
    first = out["transactions"][0]
    assert first["method"] == "NewDeliveryPackage" and first["reqid"].startswith("STORY-") and "link" not in first
    assert [t["time"] for t in out["transactions"]] == sorted(t["time"] for t in out["transactions"])


async def test_a_delivery_not_in_the_log_says_where_it_looked():
    async with async_session() as db:
        out = await st.story_rows(db, CC, {"value": "99999"}, now=NOW)
    assert out["found"] is False and out["total"] == 0 and "nothing for delivery_number 99999" in out["looked_in"]


async def test_the_story_widens_to_the_week_before_the_day_asked():
    later = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)  # three days after the planted day
    async with async_session() as db:
        out = await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY}, now=later)
        named = await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY, "day": "today"}, now=later)
    # step 26: with no day named the 7 days are the search itself, not a widening
    assert out["found"] is True and out["outside_filter"] is False and "2026-09-15 to 2026-09-21" in out["looked_in"]
    # a day named that has nothing still widens to the week before it
    assert named["found"] is True and named["outside_filter"] is True and "2026-09-15 to 2026-09-21" in named["looked_in"]
    async with async_session() as db:
        pinned = await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY, "day": "2026-09-18"}, now=later)
    assert pinned["found"] is True and pinned["outside_filter"] is False


async def test_a_key_or_value_the_tool_cannot_read_comes_back_as_a_problem():
    async with async_session() as db:
        bad = json.loads(await st.run_story_tool(db, CC, "delivery_story", {"key": "route", "value": "BRI01"}))
        empty = json.loads(await st.run_story_tool(db, CC, "delivery_story", {}))
    assert bad["problems"][0].startswith("key is one of delivery_number")
    assert empty["problems"] == ["value is required: the delivery, order, item, request id or reporting number"]


# ==================================================== 2. the lift draws the story


async def test_the_lift_draws_the_tiles_and_the_story_table_from_the_tools_own_rows():
    async with async_session() as db:
        out = await st.story_rows(db, CC, {"value": plant.STORY_DELIVERY}, now=NOW)
    drawn = blocks.blocks_from_trace([_call("delivery_story", out, value=plant.STORY_DELIVERY)])
    assert [b["type"] for b in drawn] == ["kpis", "table"]
    kpis = {i["label"]: i["value"] for i in drawn[0]["items"]}
    assert kpis == {"Lines picked": "3", "Exact": "2", "Over": "1", "Packages": "1", "Issues": "1"}
    table = drawn[1]
    assert table["title"] == f"Delivery {plant.STORY_DELIVERY} · 6 transactions"
    assert [c["name"] for c in table["columns"]] == ["time", "method", "status", "user", "item", "picked", "expected", "outcome", "from", "request"]
    assert table["rows"][1] == ["07:12:00", "ConfirmPickLine", "success", "BCHAM", "100944", "6", "6", "exact", "D02A", "STORY-2"]
    # the model's own table wins
    own = blocks.blocks_from_trace([_call("delivery_story", out), _call("show_table", {"shown": "table"}, title="Mine", columns=["a"], rows=[["1"]])])
    assert [b["title"] for b in own if b["type"] == "table"] == ["Mine"]


# ==================================================== 3. the profile routes one delivery to its story


def test_the_analytics_profile_offers_the_story_and_routes_one_delivery_to_it():
    offered = [s.name for s in tool_registry().offered(load_profile("analytics").namespaces)]
    assert "delivery_story" in offered
    prompt = load_profile("analytics").system_prompt
    assert "delivery_story" in prompt and "The details of ONE delivery" in prompt
    assert "or what happened to one delivery" not in prompt  # no longer routed to the at-risk tools


async def test_a_scripted_run_answers_the_details_of_a_delivery_from_its_story():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "delivery_story", "args": {"value": plant.STORY_DELIVERY, "day": "2026-09-18"}}]),
        AIMessage(content=f"Delivery {plant.STORY_DELIVERY} was picked by BCHAM between 07:10 and 07:20: 3 lines, 2 exact and 1 over (item 104441, 4.08 against 4)."),
    )
    result = await ask(analytics_runtime(CC, model=model), f"give me the details of this delivery number {plant.STORY_DELIVERY}")
    assert result["retries"] == 0 and result["stop_reason"] == "end_turn"
    assert [b["type"] for b in result["blocks"]] == ["kpis", "table"]


def test_quantities_read_as_a_person_reads_them():
    digest = st.story_digest([{"method": "ConfirmPickLine", "status": "success", "pick_outcome": "over", "item_number": "104441",
                               "reporting_number": "889666", "QuantityPicked": "4.08", "ExpectedQuantity": "4.000000"}])
    assert digest["not_exact"][0]["expected"] == "4" and digest["not_exact"][0]["picked"] == "4.08"
    table = blocks.blocks_from_trace([_call("delivery_story", {"found": True, "key": "delivery_number", "value": "1", "total": 1, "picks": {"lines": 1}, "summary": {"users": ["B"]},
                                                               "transactions": [{"time": "t", "method": "ConfirmPickLine", "status": "success",
                                                                                 "QuantityPicked": "4.08", "ExpectedQuantity": "4.000000"}]})])
    assert next(b for b in table if b["type"] == "table")["rows"][0][5:7] == ["4.08", "4"]
