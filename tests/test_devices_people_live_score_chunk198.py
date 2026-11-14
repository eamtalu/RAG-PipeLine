"""Fix after the live check of 14 November (chunk 198):

- "How many devices were in use per day in the last month?": the live model sent
  {"days": 30, "grain": "day", "by": "device"} twice, and the tool dropped the grain, so the
  answer was a total with no days. A time grain asked for now keeps the series by time; the cut
  comes back beside it as the breakdown, and the lift draws the trend.
- "Who was active today, and when did each person start and finish?": when the model writes the
  people as bullets, the lift draws the people table from the tool's own list.
- The live score judges a case that names its blocks by the blocks a person sees, not by the
  model's route to them.
"""

import json
from datetime import datetime, timezone

import pytest

from app.agent.presentation import blocks
from app.config.database import async_session
from app.services.analytics_agent import activity_tools as at
from tests.golden import plant
from tests.golden.harness import Case, mismatches

CC = "test_chunk198"
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
WEEK = {"start": "2026-09-11", "end": "2026-09-18"}


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC, week=True)
    yield
    await plant.wipe_analytics(CC)


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result, default=str)}


# ==================================================== 1. a grain and a cut together keep the days


async def test_the_live_arguments_return_the_days_with_the_device_breakdown_beside_them():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {**WEEK, "grain": "day", "by": "device"}, now=NOW)
    assert out["group_by"] == ["day"] and out["grain"] == "day" and out["by"] == "device"
    assert [(r["dimensions"][0], r["devices"]) for r in out["rows"]] == [("2026-09-16", 2), ("2026-09-17", 2), ("2026-09-18", 3)]
    assert out["breakdown_by"] == "device"
    assert {r["dimensions"][0]: r["calls"] for r in out["breakdown"]} == {"HH-01": 4, "HH-02": 3, "HH-03": 3}


async def test_a_cut_without_a_grain_is_still_a_comparison():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {**WEEK, "by": "device"}, now=NOW)
    assert out["group_by"] == ["device"] and "breakdown" not in out
    assert {r["dimensions"][0] for r in out["rows"]} == {"HH-01", "HH-02", "HH-03"}


async def test_the_lift_draws_devices_per_day_from_the_live_arguments():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {**WEEK, "grain": "day", "by": "device"}, now=NOW)
    drawn = blocks.blocks_from_trace([
        _call("wms_activity", out, days=30, grain="day", by="device"),
        _call("show_kpis", {"shown": "kpis"}, labels=["Devices in use"], values=["3"]),
        _call("chart_from_data", {"shown": "chart"}, measure="devices"),
    ])
    chart = next(b for b in drawn if b["type"] == "chart")
    assert chart["kind"] == "line" and chart["labels"] == ["2026-09-16", "2026-09-17", "2026-09-18"]
    assert chart["series"] == [{"name": "devices", "values": [2.0, 2.0, 3.0]}]
    # even when the model draws only tiles, the trend is drawn by the lift
    tiles_only = blocks.blocks_from_trace([_call("wms_activity", out), _call("show_kpis", {"shown": "kpis"}, labels=["Devices"], values=["3"])])
    assert [b["type"] for b in tiles_only] == ["kpis", "chart"]


def test_the_tool_description_says_how_to_ask_for_devices_per_day():
    text = at.ACTIVITY_TOOLS[0]["description"]
    assert "devices per day" in text and "grain 'day'" in text


# ==================================================== 2. the people table from the tool's own list


async def test_who_was_active_today_gets_the_people_table_when_the_model_writes_bullets():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {"by": "user", "day": "2026-09-18"}, now=NOW)
    drawn = blocks.blocks_from_trace([_call("wms_activity", out, by="user", day="today"),
                                      _call("state_findings", {"shown": "findings"}, findings=["Three people were active"], sources=["wms_activity"])])
    table = next(b for b in drawn if b["type"] == "table")
    assert [c["name"] for c in table["columns"]] == ["user", "name", "first call", "last call", "calls", "devices"]
    rows = {r[0]: r for r in table["rows"]}
    assert rows["DBOBOC"] == ["DBOBOC", "Dee Boboc", "09:15", "09:15", "2", "1"]
    assert rows["BCHAM"][2:4] == ["06:15", "07:15"]
    assert table["title"] == "Who was active · 2026-09-18"


async def test_the_models_own_table_wins_and_a_trend_gets_no_people_table():
    async with async_session() as db:
        people = await at.activity_rows(db, CC, {"by": "user", "day": "2026-09-18"}, now=NOW)
        daily = await at.activity_rows(db, CC, {**WEEK, "grain": "day"}, now=NOW)
    own = blocks.blocks_from_trace([_call("wms_activity", people), _call("show_table", {"shown": "table"}, title="Mine", columns=["user"], rows=[["BCHAM"]])])
    assert [b["title"] for b in own if b["type"] == "table"] == ["Mine"]
    assert "table" not in [b["type"] for b in blocks.blocks_from_trace([_call("wms_activity", daily)])]


# ==================================================== 3. the live score judges what a person sees


def _case(expect):
    return Case(profile="analytics", name="c", question="q", history=(), gate=False, replies=(), expect={"stop_reason": "end_turn", **expect})


class _Result:
    def __init__(self, tools, kinds):
        self.stop_reason, self.declined, self.answer, self.retries = "end_turn", None, "a", 0
        self.tool_calls = [type("T", (), {"tool": t})() for t in tools]
        self.blocks = [{"type": k} for k in kinds]
        self.evidence, self.withheld_figures, self.trace = None, (), None


def test_a_live_case_with_blocks_is_judged_by_the_blocks_not_the_route():
    case = _case({"tools": ["wms_activity", "show_table"], "blocks": ["chart", "table"]})
    # the model drew tiles instead of a table: the table is missing, so it is a miss
    assert mismatches(case, _Result(["wms_activity", "show_kpis"], ["kpis", "chart"]), scripted=False) == [
        "blocks: ['table'] were not drawn; drawn ['kpis', 'chart']"]
    # another route to the same picture is a pass
    assert mismatches(case, _Result(["wms_activity", "chart_from_data", "show_table"], ["chart", "table", "findings"]), scripted=False) == []
    # the data tool behind the picture must still be called
    assert mismatches(case, _Result(["aggregate_releases", "show_table"], ["chart", "table"]), scripted=False) == [
        "tools: ['wms_activity'] were not called; called ['aggregate_releases', 'show_table']"]


def test_a_case_without_blocks_and_every_scripted_run_keep_the_strict_tool_check():
    case = _case({"tools": ["aggregate_releases"]})
    assert mismatches(case, _Result(["list_releases"], []), scripted=False) == [
        "tools: ['aggregate_releases'] were not called; called ['list_releases']"]
    strict = _case({"tools": ["wms_activity", "show_table"], "blocks": ["table"]})
    assert mismatches(strict, _Result(["wms_activity", "show_table"], ["table"]), scripted=True) == []
