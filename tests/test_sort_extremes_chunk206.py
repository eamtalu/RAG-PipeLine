"""Step 28 (chunk 206): a sorted read names its lowest and highest row.

Golden run of 16 November 2026 after D27: "Which picker picked the fewest units on 18 September
2026?" named FNACHONLEO (9) the fewest while DBOBOC had 8 (2 of 6 reruns on D27, 1 of 6 on D26).
The model sent `sort: picked` with no `dir`, the tool sorted largest first, and the model read the
first row as the fewest. The tool never sees the question, so it names both ends instead."""

import pytest

from app.config.database import async_session
from app.services.analytics_agent import tools as agent_tools
from tests.golden import plant

CC = "test_chunk206"


@pytest.fixture(autouse=True)
async def clean():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    await plant.plant_analytics(CC)
    yield
    await plant.wipe_analytics(CC)


async def _aggregate(args):
    async with async_session() as db:
        return await agent_tools.aggregate_releases(db, {**args, **plant.window()}, CC)


@pytest.mark.parametrize("direction", [None, "asc", "desc"])
async def test_a_sorted_read_names_the_lowest_and_the_highest_whatever_the_order(direction):
    args = {"group_by": ["user_name"], "sort": "picked", **({"dir": direction} if direction else {})}
    out = await _aggregate(args)
    assert out["extremes"]["by"] == "picked"
    assert out["extremes"]["lowest"] == {"dimensions": ["DBOBOC"], "picked": out["extremes"]["lowest"]["picked"]}
    assert float(out["extremes"]["lowest"]["picked"]) == 8 and out["extremes"]["highest"]["dimensions"] == ["FNACHONLEO"]
    assert float(out["extremes"]["highest"]["picked"]) == 9


async def test_no_extremes_without_a_sort_with_one_group_or_with_a_cut_off_list():
    assert "extremes" not in await _aggregate({"group_by": ["user_name"]})
    assert "extremes" not in await _aggregate({"group_by": [], "sort": "picked"})
    # a list cut at its limit may not hold the true lowest, so it names none
    assert "extremes" not in await _aggregate({"group_by": ["user_name"], "sort": "picked", "limit": 1})


async def test_the_rows_and_their_order_are_unchanged():
    out = await _aggregate({"group_by": ["user_name"], "sort": "picked"})
    assert [r["dimensions"][0] for r in out["rows"]] == ["FNACHONLEO", "DBOBOC"]
