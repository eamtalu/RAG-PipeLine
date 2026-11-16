"""Step 25 (chunk 203), the two open items found in the step 24 live runs of 16 November 2026:

1. "Which products were short today?" lost its chart whenever the model read the release list
   after the ranked aggregate (golden runs 5, 7 and 8, and every D23 run): the lift drew the chart
   only from the last data result, and a list cannot be drawn as a ranking. A read that cannot be
   drawn no longer replaces the result the chart is drawn from.
2. "How long does a line take per picker?" once called one picker's median (59.831 s) the median
   across all pickers; the real one was 51.103 s and no result carried it. A grouped read now
   returns each statistic it was asked for over ALL groups in `overall`."""

import json

import pytest

from app.agent.presentation.blocks import blocks_from_trace
from app.config.database import async_session
from app.services.analytics_agent import tools as agent_tools
from tests.golden import plant

CC = "test_chunk203"

RANKING = {"group_by": ["item_number"], "sort": {"by": "units_short", "dir": "desc"},
           "rows": [{"dimensions": ["100230"], "rows": 2, "units_short": 10, "deliveries": 1},
                    {"dimensions": ["104568"], "rows": 1, "units_short": 1, "deliveries": 1}]}
LIST = {"rows": [{"release": "B1", "item_number": "100230", "expected": 7, "picked": 4}], "total": 1}
TOTAL = {"group_by": [], "rows": [{"rows": 3, "shortfall": -11, "deliveries": 1}]}
FINDINGS = {"tool": "state_findings", "input": {"findings": ["Two products were short"], "sources": ["aggregate_releases"]},
            "result": '{"shown": "findings"}'}


def _call(tool, result, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result)}


@pytest.fixture(autouse=True)
async def clean():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    yield
    await plant.wipe_analytics(CC)


# ==================================================== 1. a list read keeps the ranking's chart


def test_the_ranking_keeps_its_chart_when_the_release_list_is_read_after_it():
    ranking = _call("aggregate_releases", RANKING, group_by=["item_number"], sort="units_short")
    for after in ([_call("list_releases", LIST)], [_call("list_releases", LIST), _call("explain_release", {"rows": [{"release": "B1"}]})]):
        drawn = blocks_from_trace([ranking, *after, FINDINGS])
        chart = next(b for b in drawn if b["type"] == "chart")
        assert chart["labels"] == ["100230", "104568"]


def test_a_later_aggregate_still_replaces_the_ranking_as_the_source():
    # a total read after the ranking is what the answer is about: tiles, no ranking chart (unchanged)
    drawn = blocks_from_trace([_call("aggregate_releases", RANKING, group_by=["item_number"], sort="units_short"),
                               _call("aggregate_releases", TOTAL)])
    assert [b["type"] for b in drawn if b["type"] in ("chart", "kpis")] == ["kpis"]


def test_a_list_read_alone_still_draws_no_chart():
    assert "chart" not in [b["type"] for b in blocks_from_trace([_call("list_releases", LIST), FINDINGS])]


# ==================================================== 2. a statistic over all groups is the tool's


async def test_a_grouped_median_also_comes_back_over_all_groups_as_an_ungrouped_read_gives_it():
    await plant.plant_analytics(CC)
    async with async_session() as db:
        per = await agent_tools.aggregate_releases(db, {"group_by": ["user_name"], "stat": ["median:picked"], **plant.window()}, CC)
        whole = await agent_tools.aggregate_releases(db, {"stat": ["median:picked"], **plant.window()}, CC)
    assert per["groups"] >= 2
    assert per["overall"]["median_picked"] == whole["rows"][0]["median_picked"]
    assert "deliveries" in per["overall"] and "distinct_delivery_number" not in per["overall"]
    assert "stats asked for" in per["overall_note"]


async def test_a_grouped_read_with_no_stat_asked_has_no_extra_overall_keys():
    await plant.plant_analytics(CC)
    async with async_session() as db:
        per = await agent_tools.aggregate_releases(db, {"group_by": ["user_name"], **plant.window()}, CC)
    assert not [k for k in per["overall"] if k.startswith(("median_", "avg_", "max_", "min_"))]
