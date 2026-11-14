"""Step 19 tuning (D19c) chunk 193: a withheld answer keeps the blocks drawn from the data itself,
so a chart the model never typed survives a total it worked out by hand."""

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted
from tests.test_chart_from_data_chunk191 import BY_ITEM, _call

CC = "test_chunk193"


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


def test_only_the_grounded_blocks_survive_the_filter():
    trace = [
        _call("aggregate_releases", BY_ITEM, group_by=["item_number"]),
        _call("chart_from_data", {"shown": "chart"}, measure="units_short"),
        _call("show_table", {"shown": "table"}, title="t", columns=["item"], rows=[["100230"]]),
        _call("show_kpis", {"shown": "kpis"}, labels=["Short"], values=["11"]),
        _call("suggest_followups", {"shown": "followups"}, items=["Why?"]),
    ]
    assert [b["type"] for b in blocks.blocks_from_trace(trace)] == ["entities", "kpis", "chart", "table", "followups"]
    assert [b["type"] for b in blocks.grounded_blocks_from_trace(trace)] == ["entities", "chart"]
    assert blocks.grounded_blocks_from_trace(trace)[1]["series"][0]["values"] == [10.0, 1.0]


async def test_a_withheld_answer_keeps_the_chart_from_the_data_and_says_so():
    call = {"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short", **plant.window()}}
    model = _scripted(
        AIMessage(content="", tool_calls=[call]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "chart_from_data", "args": {"measure": "units_short"}}]),
        AIMessage(content="Two items were short, 11 units in all, so 5.5 on average per item."),
        # the nudged second draft repeats the arithmetic, so the answer is withheld
        AIMessage(content="", tool_calls=[{**call, "id": "c3"}]),
        AIMessage(content="", tool_calls=[{"id": "c4", "name": "chart_from_data", "args": {"measure": "units_short"}}]),
        AIMessage(content="Two items were short, 11 units in all, so 5.5 on average per item."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, as a chart?")
    assert result["stop_reason"] == "withheld" and result["retries"] == 1
    assert result["answer"].startswith("I could not verify these figures")
    assert "The chart below is drawn from the data itself." in result["answer"]
    assert "Ask again more narrowly" not in result["answer"]
    assert [b["type"] for b in result["blocks"]] == ["entities", "chart"]
    assert result["blocks"][1]["series"][0]["values"] == [10.0, 1.0]


async def test_a_withheld_answer_without_a_data_chart_keeps_no_block():
    call = {"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}
    model = _scripted(
        AIMessage(content="", tool_calls=[call]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "show_kpis", "args": {"labels": ["Units short"], "values": ["9999"]}}]),
        AIMessage(content="See the cards."),
        AIMessage(content="", tool_calls=[{**call, "id": "c3"}]),
        AIMessage(content="", tool_calls=[{"id": "c4", "name": "show_kpis", "args": {"labels": ["Units short"], "values": ["9999"]}}]),
        AIMessage(content="See the cards."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert result["stop_reason"] == "withheld"
    assert [b["type"] for b in result["blocks"]] == ["entities"]
    assert "Ask again more narrowly, or ask me to show the rows." in result["answer"]
