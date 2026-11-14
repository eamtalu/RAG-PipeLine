"""Step 19 (D19) chunk 192: `show_kpis` and `state_findings` become the kpis and findings blocks,
in the reading order, held to the data by the same guards as the text; a finding sourced to a
tool that did not run is dropped."""

import json

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks
from app.agent.runtime.pipeline import Draft, FabricationGuard, UngroundedGuard
from app.services.analytics_agent import presentation_tools as pt
from app.services.teams.contracts import FindingsBlock, KpisBlock
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk192"


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


KPIS = {"labels": ["Releases", "Units picked"], "values": ["4", "17"], "captions": ["on 2026-09-18"], "tones": ["neutral", "good"], "title": "Today"}
FINDINGS = {
    "findings": ["100230 was short by 10 units on 2 releases", "104568 by 1 unit"],
    "reasoning": ["the biggest gap of the day"],
    "sources": ["aggregate_releases", "aggregate_releases"],
    "recommendations": ["Check 100230's stock before tomorrow's releases"],
    "reasons": ["the same item was short on both releases"],
    "limitations": ["One day of data"],
}
AGG = {"group_by": ["item_number"], "rows": [{"dimensions": ["100230"], "shortfall": -10, "rows": 2}, {"dimensions": ["104568"], "shortfall": -1, "rows": 1}]}


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result) if isinstance(result, dict) else result}


# ==================================================== 1. the contract and the two tools


def test_kpis_and_findings_validate_against_the_contract():
    kpis = pt.kpis_from_args(KPIS)
    assert isinstance(kpis, KpisBlock) and kpis.title == "Today"
    assert [(i.label, i.value, i.caption, i.tone) for i in kpis.items] == [("Releases", "4", "on 2026-09-18", "neutral"), ("Units picked", "17", None, "good")]
    with pytest.raises(ValueError, match="2 labels for 3 values"):
        pt.kpis_from_args({**KPIS, "values": ["1", "2", "3"]})
    with pytest.raises(ValueError, match="captions"):
        pt.kpis_from_args({**KPIS, "captions": ["a", "b", "c"]})
    findings = pt.findings_from_args(FINDINGS)
    assert isinstance(findings, FindingsBlock) and len(findings.findings) == 2
    assert findings.findings[0].reasoning == "the biggest gap of the day" and findings.findings[1].reasoning is None
    assert findings.recommendations[0].reason == "the same item was short on both releases"
    assert findings.limitations == ["One day of data"]
    assert pt.SHOW_KPIS in pt.PRESENTATION_TOOL_NAMES and pt.STATE_FINDINGS in pt.PRESENTATION_TOOL_NAMES
    assert [t["name"] for t in pt.PRESENTATION_TOOLS] == ["chart_from_data", "show_chart", "show_table", "show_kpis", "state_findings", "suggest_followups"]


async def test_the_tools_acknowledge_or_name_the_problem():
    assert json.loads(await pt.run_presentation_tool("show_kpis", KPIS)) == {"shown": "kpis"}
    assert json.loads(await pt.run_presentation_tool("state_findings", FINDINGS)) == {"shown": "findings"}
    assert "problems" in json.loads(await pt.run_presentation_tool("show_kpis", {"labels": ["a"], "values": []}))
    assert "problems" in json.loads(await pt.run_presentation_tool("state_findings", {"findings": []}))


# ==================================================== 2. the lift: order, cap, the source rule


def test_the_blocks_come_in_the_reading_order_with_at_most_eight():
    trace = [
        _call("aggregate_releases", AGG),
        _call("suggest_followups", {"shown": "followups"}, items=["Why?"]),
        _call("state_findings", {"shown": "findings"}, **FINDINGS),
        _call("show_table", {"shown": "table"}, title="t", columns=["item"], rows=[["100230"]]),
        _call("chart_from_data", {"shown": "chart"}, measure="units_short"),
        _call("show_kpis", {"shown": "kpis"}, **KPIS),
    ]
    out = blocks.blocks_from_trace(trace)
    assert [b["type"] for b in out] == ["entities", "kpis", "chart", "table", "findings", "followups"]
    assert blocks.MAX_BLOCKS == 8 and blocks.ORDER[-1] == "followups"


def test_a_finding_sourced_to_an_uncalled_tool_is_dropped_and_the_block_survives_when_another_remains():
    args = {**FINDINGS, "sources": ["at_risk_board", "aggregate_releases"]}
    out = blocks.blocks_from_trace([_call("aggregate_releases", AGG), _call("state_findings", "{}", **args)])
    findings = next(b for b in out if b["type"] == "findings")
    assert [f["text"] for f in findings["findings"]] == ["104568 by 1 unit"]
    # every finding sourced elsewhere: the block is dropped, the answer is not
    args = {**FINDINGS, "sources": ["at_risk_board", "at_risk_board"]}
    out = blocks.blocks_from_trace([_call("aggregate_releases", AGG), _call("state_findings", "{}", **args)])
    assert [b["type"] for b in out] == ["entities"]
    # a failed data call does not count as called
    out = blocks.blocks_from_trace([_call("at_risk_board", '{"error": "x"}'), _call("aggregate_releases", AGG), _call("state_findings", "{}", **{**FINDINGS, "sources": ["at_risk_board", ""]})])
    assert [f["text"] for f in next(b for b in out if b["type"] == "findings")["findings"]] == ["104568 by 1 unit"]


# ==================================================== 3. the guards read the words


def _draft(answer: str, trace: list[dict]) -> Draft:
    return Draft(answer=answer, trace=tuple(trace), question="how many?", given=("",), schema_tools=frozenset())


def test_a_kpi_value_no_tool_returned_is_ungrounded_and_a_date_is_not_a_figure():
    data = _call("aggregate_releases", {"rows": [{"rows": 4, "picked": 17, "deliveries": 2}]})
    good = [data, _call("show_kpis", "{}", labels=["Units picked"], values=["17"], captions=["on 2026-09-18"])]
    assert UngroundedGuard().inspect(_draft("Picked today.", good)) is None
    bad = [data, _call("show_kpis", "{}", labels=["Units picked"], values=["9,999"])]
    problem = UngroundedGuard().inspect(_draft("Picked today.", bad))
    assert problem is not None and problem.figures == ("9,999",)
    # the findings' words are read the same way; a date in them is not a figure
    text = [data, _call("state_findings", "{}", findings=["Picked 17 units on 2026-09-18"], limitations=["one day"])]
    assert UngroundedGuard().inspect(_draft("Picked today.", text)) is None
    invented = [data, _call("state_findings", "{}", findings=["Picked 1,375 units"])]
    assert UngroundedGuard().inspect(_draft("Picked today.", invented)).figures == ("1,375",)
    assert blocks.presentation_figures(invented).endswith("\nPicked 1,375 units")


def test_findings_with_no_data_call_behind_them_look_fabricated():
    trace = [_call("state_findings", "{}", findings=["Picked 312 units"])]
    assert FabricationGuard().inspect(_draft("Picked today.", trace)) is not None


# ==================================================== 4. a scripted run end to end


async def test_a_scripted_run_shows_the_key_figures_and_the_findings_from_the_fetched_numbers():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short", **plant.window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "show_kpis", "args": {"labels": ["Items short"], "values": ["2"], "captions": ["this morning"]}},
                                          {"id": "c3", "name": "state_findings", "args": FINDINGS}]),
        AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert result["retries"] == 0
    assert [b["type"] for b in result["blocks"]] == ["entities", "kpis", "findings"]
    assert result["blocks"][1]["items"][0] == {"label": "Items short", "value": "2", "caption": "this morning", "tone": None}
    assert result["blocks"][2]["findings"][0]["source"] == "aggregate_releases"
    assert result["blocks"][2]["recommendations"][0]["reason"] == "the same item was short on both releases"


async def test_a_kpi_value_no_tool_returned_is_withheld_then_the_retry_answers_in_words():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "show_kpis", "args": {"labels": ["Units short"], "values": ["9999"]}}]),
        AIMessage(content="Two items were short, see the cards."),
        AIMessage(content="", tool_calls=[{"id": "c4", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}]),
        AIMessage(content="Two items were short: 100230 and 104568."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert result["retries"] == 1 and [b["type"] for b in result["blocks"]] == ["entities"]
    assert "9999" not in result["answer"]
