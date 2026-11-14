"""Step 19 (D19e) chunk 194: a presentation call the model wrote out as text is lifted into a real
block (and guarded as one), and the lift draws the chart or the key figures itself when the model
drew none and the data has a shape."""

import json

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks, charts
from app.agent.presentation.written_calls import lift_written_calls
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted
from tests.test_chart_from_data_chunk191 import BY_DAY, BY_ITEM, _call

CC = "test_chunk194"


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


# the words the live model wrote on 14 November 2026 at 20:13 (names are sample pickers)
WRITTEN = """DBARNETT picked the most units today with 1,373 units across 308 releases.

chart_from_data title: "Top 10 Pickers by Units Picked Today" kind: "bar" measure: "picked"

state_findings
findings: ["DBARNETT picked the most units today with 1,373.4 units.", "SGIAMPORCA picked 264 units with no shortfall."]
reasoning: ["DBARNETT's 1,373.4 units is the highest among all pickers today.", "SGIAMPORCA's zero shortfall indicates all expected units were picked."]
sources: ["aggregate_releases", "aggregate_releases"]

suggest_followups
items: ["Why did ISZABO have such a high shortfall today?", "How many zero-picks did each picker have today?"]

From the data: 8 of 8 group(s)."""


# ==================================================== 1. the written-out calls


def test_the_written_calls_are_lifted_off_the_answer_with_their_arguments():
    text, calls = lift_written_calls(WRITTEN)
    assert text == "DBARNETT picked the most units today with 1,373 units across 308 releases.\n\nFrom the data: 8 of 8 group(s)."
    assert [c["tool"] for c in calls] == ["chart_from_data", "state_findings", "suggest_followups"]
    assert calls[0]["input"] == {"title": "Top 10 Pickers by Units Picked Today", "kind": "bar", "measure": "picked"}
    assert calls[1]["input"]["findings"][1] == "SGIAMPORCA picked 264 units with no shortfall."
    assert calls[1]["input"]["sources"] == ["aggregate_releases", "aggregate_releases"]
    assert calls[2]["input"] == {"items": ["Why did ISZABO have such a high shortfall today?", "How many zero-picks did each picker have today?"]}
    assert all(c["written"] is True for c in calls)


def test_words_that_only_look_like_a_call_stay_words():
    text, calls = lift_written_calls("Call show_table when you need the rows.\nshow_kpis\n\nThe rest.")
    assert calls == [] and "show_kpis" in text
    text, calls = lift_written_calls("state_findings: findings: [\"x\"]")
    assert [c["tool"] for c in calls] == ["state_findings"] and calls[0]["input"] == {"findings": ["x"]} and text == ""


async def test_a_written_findings_call_becomes_the_findings_block_and_is_guarded():
    call = {"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["user_name"], "sort": "picked", **plant.window()}}
    model = _scripted(
        AIMessage(content="", tool_calls=[call]),
        AIMessage(content=(
            "FNACHONLEO picked the most units.\n\n"
            "state_findings\nfindings: [\"FNACHONLEO picked 9 units on 1 release\", \"DBOBOC picked 8 over 3\"]\n"
            "sources: [\"aggregate_releases\", \"aggregate_releases\"]\n\n"
            "suggest_followups\nitems: [\"Which releases were short?\"]"
        )),
    )
    result = await ask(analytics_runtime(CC, model=model), "which pickers picked the most?")
    assert result["retries"] == 0 and result["stop_reason"] == "end_turn"
    assert result["answer"].startswith("FNACHONLEO picked the most units.\n\nFrom the data: 2 of 2 group(s), sorted by picked desc")
    assert "state_findings" not in result["answer"] and "suggest_followups" not in result["answer"]
    assert result["answer"].endswith("| FNACHONLEO | 1 | 9 |\n| DBOBOC | 3 | 8 |")
    kinds = [b["type"] for b in result["blocks"]]
    assert kinds == ["chart", "findings", "followups"], kinds  # the chart is the automatic one
    assert result["blocks"][1]["findings"][0]["source"] == "aggregate_releases"
    assert result["blocks"][2]["items"] == ["Which releases were short?"]
    assert [t["tool"] for t in result["tool_calls"]] == ["aggregate_releases", "state_findings", "suggest_followups"]


async def test_a_written_call_with_a_figure_no_tool_returned_is_withheld_like_a_real_one():
    call = {"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}
    model = _scripted(
        AIMessage(content="", tool_calls=[call]),
        AIMessage(content="Two items were short.\n\nshow_kpis\nlabels: [\"Units short\"]\nvalues: [\"9999\"]"),
        AIMessage(content="", tool_calls=[{**call, "id": "c3"}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert result["retries"] == 1 and "9999" not in result["answer"]
    assert result["answer"].startswith("Two items were short: 100230")


# ==================================================== 2. the automatic shape


def test_the_lift_draws_a_chart_for_a_trend_or_a_ranking_when_the_model_drew_none():
    trend = blocks.blocks_from_trace([_call("aggregate_releases", {**BY_DAY, "sort": {"by": "day", "dir": "asc"}})])
    assert [b["type"] for b in trend] == ["chart"] and trend[0]["kind"] == "line" and trend[0]["series"][0]["name"] == "deliveries"
    ranked = blocks.blocks_from_trace([_call("aggregate_releases", BY_ITEM, sort="units_short")])
    assert [b["type"] for b in ranked] == ["entities", "chart"] and ranked[1]["kind"] == "bar"
    assert ranked[1]["series"][0] == {"name": "units short", "values": [10.0, 1.0]}
    # not for a list the model did not ask to rank, nor for a trend of two points, nor when a table was drawn
    assert [b["type"] for b in blocks.blocks_from_trace([_call("aggregate_releases", BY_ITEM)])] == ["entities"]
    two = {**BY_DAY, "rows": BY_DAY["rows"][:2]}
    assert blocks.blocks_from_trace([_call("aggregate_releases", two)]) == []
    with_table = [_call("aggregate_releases", BY_ITEM, sort="units_short"), _call("show_table", "{}", title="t", columns=["item"], rows=[["100230"]])]
    assert [b["type"] for b in blocks.blocks_from_trace(with_table)] == ["entities", "table"]
    # never for a withheld answer's grounded blocks
    assert [b["type"] for b in blocks.grounded_blocks_from_trace([_call("aggregate_releases", BY_ITEM, sort="units_short")])] == ["entities"]


def test_the_lift_draws_key_figures_for_a_single_total():
    total = {"group_by": [], "rows": [{"rows": 4, "picked": 17, "expected": 28, "shortfall": -11, "deliveries": 2}], "window": {"start": "2026-09-18T04:44:42+00:00", "end": "2026-09-18T07:44:42+00:00"}}
    out = blocks.blocks_from_trace([_call("aggregate_releases", total)])
    assert [b["type"] for b in out] == ["kpis"]
    assert [(i["label"], i["value"]) for i in out[0]["items"]] == [("Releases", "4"), ("Deliveries", "2"), ("Units picked", "17"), ("Units expected", "28"), ("Shortfall", "-11")]
    assert out[0]["items"][0]["caption"] == "2026-09-18"
    # the model's own kpis win
    own = blocks.blocks_from_trace([_call("aggregate_releases", total), _call("show_kpis", "{}", labels=["Lines"], values=["4"])])
    assert [i["label"] for i in own[0]["items"]] == ["Lines"]
    assert charts.auto_kpis("at_risk_board", {"summary": {"open": 3}}) is None


async def test_a_scripted_run_gets_the_chart_without_asking_for_one():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short", **plant.window()}}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert [b["type"] for b in result["blocks"]] == ["entities", "chart"]
    assert result["blocks"][1]["labels"] == ["100230", "104568"] and result["blocks"][1]["series"][0]["values"] == [10.0, 1.0]
