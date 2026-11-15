"""Step 19 (D19) chunk 191: `chart_from_data` draws from the data call's own result, so a chart can
never carry a number no tool returned; the kind follows the shape of the data."""

import json

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks, charts
from app.services.analytics_agent import presentation_tools as pt
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk191"


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


BY_DAY = {
    "group_by": ["day"],
    "rows": [
        {"dimensions": ["2026-09-16"], "rows": 3, "deliveries": 1, "shortfall": 0},
        {"dimensions": ["2026-09-17"], "rows": 5, "deliveries": 1, "shortfall": -2},
        {"dimensions": ["2026-09-18"], "rows": 4, "deliveries": 2, "shortfall": -11},
    ],
    "window": {"start": "2026-09-11T00:00:00+00:00", "end": "2026-09-19T00:00:00+00:00"},
}
BY_ITEM = {
    "group_by": ["item_number", "lookup:item description.ItemDescription"],
    "sort": {"by": "units_short", "dir": "desc"},
    "rows": [{"dimensions": ["100230", "GINGER"], "shortfall": -10, "rows": 2}, {"dimensions": ["104568", "BASIL"], "shortfall": -1, "rows": 1}],
    "window": {"start": "2026-09-18T00:00:00+00:00", "end": "2026-09-19T00:00:00+00:00"},
}


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result) if isinstance(result, dict) else result}


# ==================================================== 1. the chooser


def test_choose_kind_follows_the_decision_table():
    assert charts.choose_kind("time", [1, 2, 3]) == "line"
    assert charts.choose_kind("group", [5, 2]) == "bar"
    assert charts.choose_kind("share", [3, 1, 2]) == "pie"
    assert charts.choose_kind("share", [1] * 9) == "bar"  # too many parts for a pie
    assert charts.choose_kind("time", [4]) == "bar"  # one point is no trend
    assert charts.choose_kind("group", [-3, 2], "pie") == "bar"  # the contract refuses negatives
    assert charts.choose_kind("time", [1, 2], "bar") == "bar"  # a named kind wins


def test_a_trend_by_day_becomes_a_line_with_the_tools_numbers():
    chart = charts.chart_from_result("aggregate_releases", BY_DAY, {"measure": "deliveries"})
    assert chart.kind == "line" and chart.labels == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert chart.series[0].values == [1.0, 1.0, 2.0] and chart.series[0].name == "deliveries"
    assert chart.title == "Deliveries per day · 2026-09-11 to 2026-09-19" and chart.unit == "deliveries"
    lines = charts.chart_from_result("aggregate_releases", BY_DAY, {"measure": "lines", "title": "Lines"})
    assert lines.series[0].values == [3.0, 5.0, 4.0] and lines.title == "Lines" and lines.unit == "releases"


def test_units_short_flips_the_sign_and_counts_become_a_pie():
    chart = charts.chart_from_result("aggregate_releases", BY_ITEM, {"measure": "units_short"})
    assert chart.kind == "bar" and chart.labels == ["100230", "104568"] and chart.series[0].values == [10.0, 1.0]
    assert chart.title == "Units short by item number · 2026-09-18 to 2026-09-19" and chart.unit == "units"
    history = {"range": {"start": "2026-09-01", "end": "2026-09-18"}, "counts": {"missed": 2, "held": 1, "fine": 9}, "rows": [{"delivery": "1"}]}
    pie = charts.chart_from_result("at_risk_history", history, {"measure": "counts"})
    assert pie.kind == "pie" and pie.labels == ["missed", "held", "fine"] and pie.series[0].values == [2.0, 1.0, 9.0]
    assert pie.title == "Deliveries by outcome · 2026-09-01 to 2026-09-18"
    vans = {"vans": [{"day": "2026-09-16", "route": "BRI04", "late_min": 12}, {"day": "2026-09-17", "route": "BRI04", "late_min": -3}]}
    trend = charts.chart_from_result("at_risk_vans", vans, {"measure": "late_min"})
    assert trend.kind == "line" and trend.labels == ["2026-09-16", "2026-09-17"] and trend.series[0].values == [12.0, -3.0]
    assert trend.unit == "min"


def test_a_measure_the_rows_lack_or_a_single_total_is_refused_by_name():
    with pytest.raises(ValueError, match="carry no 'nope'; they carry"):
        charts.chart_from_result("aggregate_releases", BY_DAY, {"measure": "nope"})
    with pytest.raises(ValueError, match="show_kpis"):
        charts.chart_from_result("aggregate_releases", {"group_by": [], "rows": [{"rows": 4, "picked": 17}]}, {"measure": "picked"})
    with pytest.raises(ValueError, match="measure is required"):
        charts.chart_from_result("aggregate_releases", BY_DAY, {})


# ==================================================== 2. the lift binds the chart to the last usable data call


def test_the_chart_binds_to_the_last_usable_data_call_not_describe_releases():
    trace = [
        _call("aggregate_releases", BY_ITEM, group_by=["item_number"]),
        _call("describe_releases", {"fields": ["x"]}),
        _call("chart_from_data", {"shown": "chart", "measure": "units_short"}, measure="units_short"),
    ]
    out = blocks.blocks_from_trace(trace)
    assert [b["type"] for b in out] == ["entities", "chart"]
    assert out[1]["labels"] == ["100230", "104568"] and out[1]["series"][0]["values"] == [10.0, 1.0]
    # the later data call wins when two were made
    trace = [_call("aggregate_releases", BY_ITEM), _call("aggregate_releases", BY_DAY, group_by=["day"]), trace[2]]
    out = blocks.blocks_from_trace(trace)
    assert [b["type"] for b in out] == ["entities", "chart"]  # the entities come from the item call
    assert out[1]["kind"] == "line" and out[1]["labels"] == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert out[1]["series"][0]["values"] == [0.0, 2.0, 11.0]  # units short per day, sign flipped
    # no data call at all: the chart is dropped, nothing raised
    assert blocks.blocks_from_trace([trace[2]]) == []
    assert blocks.is_presentation_call({"tool": "chart_from_data"})


async def test_the_tool_acknowledges_and_refuses_a_bad_shape_at_call_time():
    ok = json.loads(await pt.run_presentation_tool("chart_from_data", {"measure": "deliveries"}))
    assert ok["shown"] == "chart" and ok["measure"] == "deliveries"
    assert "problems" in json.loads(await pt.run_presentation_tool("chart_from_data", {}))
    assert "problems" in json.loads(await pt.run_presentation_tool("chart_from_data", {"measure": "x", "kind": "donut"}))
    assert pt.CHART_FROM_DATA in pt.PRESENTATION_TOOL_NAMES
    assert [t["name"] for t in pt.PRESENTATION_TOOLS][0] == "chart_from_data"


# ==================================================== 3. a scripted run end to end


def _window():
    return plant.window()


async def test_a_scripted_run_draws_the_chart_from_the_fetched_numbers_without_retyping():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short", **_window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "chart_from_data", "args": {"measure": "units_short"}}, {"id": "c3", "name": "suggest_followups", "args": {"items": ["Which customers were short?"]}}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, as a chart?")
    assert result["retries"] == 0
    assert [b["type"] for b in result["blocks"]] == ["entities", "chart", "followups"]
    chart = result["blocks"][1]
    assert chart["kind"] == "bar" and chart["labels"] == ["100230", "104568"]
    assert chart["series"] == [{"name": "units short", "values": [10.0, 1.0]}]
    assert chart["title"].startswith("Units short by item number")
    assert [t["tool"] for t in result["tool_calls"]] == ["aggregate_releases", "chart_from_data", "suggest_followups"]


async def test_an_unknown_measure_drops_the_chart_and_keeps_the_answer():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "chart_from_data", "args": {"measure": "nope"}}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, as a chart?")
    assert result["retries"] == 0 and result["stop_reason"] == "end_turn"
    assert [b["type"] for b in result["blocks"]] == ["entities"]
    assert result["answer"].startswith("Two items were short")


async def test_show_chart_still_works_as_before():
    # two points since step 23: a one-point chart is refused (tests/test_answer_polish_chunk201.py)
    two = {"kind": "bar", "title": "t", "labels": ["a", "b"], "series_names": ["s"], "series_values": [[1, 2]]}
    assert pt.chart_from_args(two).kind == "bar"
    ok = json.loads(await pt.run_presentation_tool("show_chart", two))
    assert ok == {"shown": "chart", "points": 2}
