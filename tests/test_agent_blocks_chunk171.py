"""Chunk 171 (stage D, step 5): typed answer blocks from the presentation tools.

The model asks for a chart, a table or follow-ups with flat tool calls; the calls are validated
against the wire contract, lifted off the trace into blocks, and held to the same grounding rule
as written figures: a number the data tools never returned is withheld, drawn or written.
"""

import json

import pytest
from langchain_core.messages import AIMessage
from pydantic import ValidationError

from app.agent.presentation import blocks
from app.agent.runtime.pipeline import FabricationGuard, UngroundedGuard, Draft
from app.services.analytics_agent import presentation_tools as pt
from app.services.teams.contracts import ChartBlock, TableBlock
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk171"


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


CHART_ARGS = {"kind": "bar", "title": "Units short by item", "labels": ["100230", "104568"],
              "series_names": ["units short"], "series_values": [[10, 1]], "unit": "units"}


# ==================================================== 1. the contract and the tool calls

def test_the_contract_holds_a_chart_to_its_shape():
    ChartBlock(**pt.chart_from_args(CHART_ARGS).model_dump())
    with pytest.raises(ValidationError):
        pt.chart_from_args({**CHART_ARGS, "series_values": [[10]]})
    with pytest.raises(ValueError):
        pt.chart_from_args({**CHART_ARGS, "series_names": ["a", "b"]})
    with pytest.raises(ValidationError):
        pt.chart_from_args({**CHART_ARGS, "kind": "pie", "series_values": [[-1, 2]]})
    with pytest.raises(ValidationError):
        pt.chart_from_args({**CHART_ARGS, "kind": "scatter"})


def test_a_table_call_right_aligns_its_number_columns():
    table = pt.table_from_args({"title": "t", "columns": ["item", "short"], "rows": [["GINGER", "8.98"], ["PEAR", "7.5"]]})
    assert [c.align for c in table.columns] == ["left", "right"]
    with pytest.raises(ValidationError):
        TableBlock(title="t", columns=table.columns, rows=[["only one"]])


async def test_the_tool_answers_an_acknowledgement_or_the_problems():
    ok = json.loads(await pt.run_presentation_tool("show_chart", CHART_ARGS))
    assert ok == {"shown": "chart", "points": 2}
    bad = json.loads(await pt.run_presentation_tool("show_chart", {**CHART_ARGS, "series_values": [[1]]}))
    assert "problems" in bad and bad["problems"]
    assert json.loads(await pt.run_presentation_tool("suggest_followups", {"items": ["Why?"]})) == {"shown": "followups", "items": 1}


# ==================================================== 2. blocks from the trace

def _call(tool: str, args: dict, result: str = '{"shown": "x"}') -> dict:
    return {"tool": tool, "input": args, "result": result, "result_preview": result[:200]}


def test_blocks_come_off_the_trace_in_a_fixed_order_and_a_refused_call_is_not_a_block():
    trace = [
        _call("aggregate_releases", {"group_by": ["item_number"]}, '{"rows": []}'),
        _call("suggest_followups", {"items": ["Why was 100230 short?"]}),
        _call("show_chart", {**CHART_ARGS, "series_values": [[1]]}, '{"problems": ["bad"]}'),
        _call("show_chart", CHART_ARGS),
        _call("show_table", {"title": "Rows", "columns": ["item"], "rows": [["100230"]]}),
    ]
    out = blocks.blocks_from_trace(trace)
    assert [b["type"] for b in out] == ["table", "chart", "followups"]
    assert out[1]["series"] == [{"name": "units short", "values": [10.0, 1.0]}]
    assert blocks.presentation_figures(trace) == "1 10 1 100230"
    assert blocks.blocks_from_trace([_call("show_chart", {"kind": "bar"})]) == []


def test_the_guards_hold_drawn_figures_to_the_data():
    data = _call("aggregate_releases", {}, json.dumps({"rows": [{"dimensions": ["100230"], "shortfall": -10}]}))
    good = Draft(answer="100230 was short.", trace=(data, _call("show_chart", CHART_ARGS)), question="short?",
                 given=("",), schema_tools=frozenset())
    assert UngroundedGuard().inspect(good) is None and FabricationGuard().inspect(good) is None
    invented = Draft(answer="100230 was short.",
                     trace=(data, _call("show_chart", {**CHART_ARGS, "series_values": [[9999, 1]]})),
                     question="short?", given=("",), schema_tools=frozenset())
    problem = UngroundedGuard().inspect(invented)
    assert problem is not None and problem.figures == ("9999",)
    # a chart with no data call behind it is fabricated, however plausible the numbers
    drawn_only = Draft(answer="Here is the chart.", trace=(_call("show_chart", {**CHART_ARGS, "series_values": [[4500, 120]]}),),
                       question="chart?", given=("",), schema_tools=frozenset())
    assert FabricationGuard().inspect(drawn_only) is not None


# ==================================================== 3. a scripted run end to end

def _window():
    return plant.window()


async def test_a_scripted_run_draws_a_chart_from_the_fetched_numbers():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "show_chart", "args": CHART_ARGS},
                                          {"id": "c3", "name": "suggest_followups", "args": {"items": ["Which customers were short?"]}}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, as a chart?")
    assert result["retries"] == 0
    # the entities the data call named lead (step 14c), then the chart and the follow-ups
    assert [b["type"] for b in result["blocks"]] == ["entities", "chart", "followups"]
    assert [i["id"] for i in result["blocks"][0]["items"]] == ["100230", "104568"]
    assert result["blocks"][1]["labels"] == ["100230", "104568"]
    assert result["blocks"][1]["series"][0]["values"] == [10.0, 1.0]
    assert [t["tool"] for t in result["tool_calls"]] == ["aggregate_releases", "show_chart", "suggest_followups"]


async def test_a_chart_with_a_number_no_tool_returned_is_withheld():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "show_chart",
                                           "args": {**CHART_ARGS, "series_values": [[9999, 1]]}}]),
        AIMessage(content="Two items were short, see the chart."),
        # the nudged second draft answers in words, without a chart
        AIMessage(content="", tool_calls=[{"id": "c4", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="Two items were short: 100230 and 104568."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, as a chart?")
    assert result["retries"] == 1 and [b["type"] for b in result["blocks"]] == ["entities"], "the chart is withheld"
    assert result["answer"].startswith("Two items were short: 100230")
