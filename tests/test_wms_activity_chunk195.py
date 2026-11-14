"""Step 20 (D20) chunk 195: `wms_activity` counts who was active on the WMS, when and on which
devices, on the tenant's clock; the lift draws the heat map from its matrix, the line from its
days and the tiles from its totals; the "People and devices" topic is in the catalogue."""

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import blocks, charts
from app.config.database import async_session
from app.services.analytics_agent import activity_tools as at
from app.services.ask import catalogue
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk195"
LONDON = ZoneInfo("Europe/London")
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)  # noon on the planted day
WEEK = {"start": "2026-09-11", "end": "2026-09-18"}


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC, week=True)
    yield
    await plant.wipe_analytics(CC)


# ==================================================== 1. the window


def test_the_window_is_read_on_the_tenants_clock_and_capped():
    start, end, notes = at.resolve_window({"day": "2026-09-18"}, LONDON, NOW)
    assert start.isoformat() == "2026-09-18T00:00:00+01:00" and end.isoformat() == "2026-09-19T00:00:00+01:00"
    start, end, notes = at.resolve_window({"days": 400}, LONDON, NOW)
    assert start.date().isoformat() == "2026-06-19" and end == NOW and notes == ["window capped at 92 days"]
    start, end, _ = at.resolve_window({"start": "2026-09-11", "end": "2026-09-18"}, LONDON, NOW)
    assert start.isoformat() == "2026-09-11T00:00:00+01:00" and end.isoformat() == "2026-09-19T00:00:00+01:00"
    start, end, _ = at.resolve_window({}, LONDON, NOW)
    assert start.isoformat() == "2026-09-05T00:00:00+01:00" and end == NOW


# ==================================================== 2. the counts


async def test_users_per_day_and_the_totals_come_from_the_log_on_the_tenants_clock():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {"grain": "day", **WEEK}, now=NOW)
    assert out["group_by"] == ["day"] and out["grain"] == "day" and out["by"] == "none"
    assert [(r["dimensions"][0], r["users"], r["devices"], r["calls"], r["errors"]) for r in out["rows"]] == [
        ("2026-09-16", 2, 2, 3, 1), ("2026-09-17", 2, 2, 2, 0), ("2026-09-18", 3, 3, 5, 1)]
    assert out["rows"][2]["first_at"] == "2026-09-18 06:15" and out["rows"][2]["last_at"] == "2026-09-18 09:15"
    assert out["total"] == {"users": 3, "devices": 3, "calls": 10, "errors": 2}
    assert "matrix" not in out and "people" not in out
    assert out["grain_note"].startswith("a user counts as active")


async def test_the_hour_grain_carries_the_day_by_hour_matrix_with_the_cells_outside_the_window():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {"grain": "hour", "start": "2026-09-16", "end": "2026-09-18"}, now=NOW)
    matrix = out["matrix"]
    assert matrix["rows"] == ["Wed 16 Sep", "Thu 17 Sep", "Fri 18 Sep"] and matrix["columns"][6] == "06"
    # London is an hour ahead of UTC in September: the 05:15 UTC calls sit in the 06 column
    assert matrix["values"][0][6] == 1 and matrix["values"][0][7] == 2 and sum(matrix["values"][0]) == 3
    assert matrix["values"][2][6] == 2 and matrix["values"][2][7] == 1 and matrix["values"][2][9] == 1
    assert matrix["unit"] == "users" and matrix["scale"] == [1, 2, 4, 9] and matrix["outside"] is None
    assert [r["dimensions"][0] for r in out["rows"]][:2] == ["2026-09-16 06:00", "2026-09-16 07:00"]
    # a window that starts mid-day marks the hours before it as outside
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {"grain": "hour", "start": "2026-09-18T08:00:00+01:00", "end": "2026-09-18T12:00:00+01:00"}, now=NOW)
    assert out["matrix"]["outside"][0][:8] == [True] * 8 and out["matrix"]["outside"][0][8] is False
    assert out["matrix"]["outside"][0][12] is True


async def test_a_user_cut_and_a_short_window_name_the_people_with_their_first_and_last_call():
    async with async_session() as db:
        out = await at.activity_rows(db, CC, {"by": "user", "day": "2026-09-18"}, now=NOW)
    assert out["group_by"] == ["user"]
    assert [(r["dimensions"][0], r["calls"]) for r in out["rows"]] == [("BCHAM", 2), ("DBOBOC", 2), ("PEVANS", 1)] or \
        {r["dimensions"][0] for r in out["rows"]} == {"BCHAM", "DBOBOC", "PEVANS"}
    people = {p["user"]: p for p in out["people"]}
    assert people["DBOBOC"]["employee"] == "Dee Boboc" and people["DBOBOC"]["first_at"] == "2026-09-18 09:15"
    assert people["BCHAM"]["first_at"] == "2026-09-18 06:15" and people["BCHAM"]["last_at"] == "2026-09-18 07:15"
    assert people["PEVANS"]["devices"] == 1
    async with async_session() as db:
        narrowed = await at.activity_rows(db, CC, {"grain": "day", **WEEK, "user": "PEVANS"}, now=NOW)
    assert [r["users"] for r in narrowed["rows"]] == [1, 1, 1] and narrowed["total"]["calls"] == 4
    async with async_session() as db:
        by_device = await at.activity_rows(db, CC, {"by": "device", **WEEK}, now=NOW)
    assert {r["dimensions"][0] for r in by_device["rows"]} == {"HH-01", "HH-02", "HH-03"}


async def test_the_tool_answers_json_or_the_problem():
    async with async_session() as db:
        out = json.loads(await at.run_activity_tool(db, CC, "wms_activity", {"grain": "day", **WEEK}))
    assert out["total"]["users"] == 3
    assert "problems" in json.loads(await at.run_activity_tool(None, CC, "nope", {}))


# ==================================================== 3. the lift: heat map, line, tiles


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result) if isinstance(result, dict) else result}


async def test_the_lift_draws_the_heat_map_the_line_or_the_tiles_from_the_activity_result():
    async with async_session() as db:
        hourly = await at.activity_rows(db, CC, {"grain": "hour", **WEEK}, now=NOW)
        daily = await at.activity_rows(db, CC, {"grain": "day", **WEEK}, now=NOW)
        people = await at.activity_rows(db, CC, {"by": "user", "day": "2026-09-18"}, now=NOW)
    heat = blocks.blocks_from_trace([_call("wms_activity", hourly)])
    assert [b["type"] for b in heat] == ["heatmap"]
    assert heat[0]["rows"][-1] == "Fri 18 Sep" and len(heat[0]["columns"]) == 24 and heat[0]["values"][-1][6] == 2
    assert heat[0]["title"] == "Active users by hour · 2026-09-11 to 2026-09-19"
    line = blocks.blocks_from_trace([_call("wms_activity", daily)])
    assert [b["type"] for b in line] == ["chart"] and line[0]["kind"] == "line"
    assert line[0]["series"] == [{"name": "users", "values": [2.0, 2.0, 3.0]}] and line[0]["unit"] == "users"
    assert line[0]["title"] == "Users per day · 2026-09-11 to 2026-09-19"
    tiles = blocks.blocks_from_trace([_call("wms_activity", people)])
    assert [b["type"] for b in tiles] == ["kpis", "table"]  # the people table since chunk 198
    assert [(i["label"], i["value"]) for i in tiles[0]["items"]] == [("People active", "3"), ("Devices", "3"), ("Transactions", "5"), ("Errors", "1")]
    # the model's own chart over the days wins, and devices is a measure the rows carry
    devices = charts.chart_from_result("wms_activity", daily, {"measure": "devices"})
    assert devices.series[0].values == [2.0, 2.0, 3.0] and devices.unit == "devices"
    # a withheld answer keeps the heat map, which comes from the data
    assert [b["type"] for b in blocks.grounded_blocks_from_trace([_call("wms_activity", hourly)])] == ["heatmap"]


# ==================================================== 4. the catalogue and a scripted run


def test_the_people_topic_is_open_to_everybody_and_offered_first_among_the_usage_topics():
    topic = next(t for t in catalogue.TOPICS if t.id == "people")
    assert topic.needs is None and topic.label == "People and devices" and len(topic.questions) == 6
    assert [t.id for t in catalogue.TOPICS].index("people") < [t.id for t in catalogue.TOPICS].index("usage")
    assert catalogue.CATALOGUE_VERSION == "20.1"


async def test_a_scripted_run_gets_the_heat_map_without_asking_for_a_chart():
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "wms_activity", "args": {"grain": "hour", **WEEK}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "state_findings", "args": {"findings": ["3 people were active on 2026-09-18, the busiest of the three days"], "sources": ["wms_activity"]}}]),
        AIMessage(content="3 different users were active over the three days with activity, at most 3 on 2026-09-18."),
    )
    result = await ask(analytics_runtime(CC, model=model), "show active users per hour in the week to 18 September 2026")
    assert result["retries"] == 0 and result["stop_reason"] == "end_turn"
    assert [b["type"] for b in result["blocks"]] == ["heatmap", "findings"]
    assert result["blocks"][0]["values"][-1][6] == 2
