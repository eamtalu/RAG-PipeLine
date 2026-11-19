"""Step 41 (D41) chunk 217: `wms_activity` answers "logins per user per day" with a grid of people
down and days across, in two layers (logins, transactions) the page switches between, and narrows
to one company: a call tagged with it, or an untagged call (a login) by someone who worked for it."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from app.agent.presentation import blocks, charts
from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics_agent import activity_grid as grid
from app.services.analytics_agent import activity_tools as at
from app.services.teams.contracts import HeatmapBlock
from tests.golden import plant

CC = "test_chunk217"
NOW = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)  # London is on UTC in November
WINDOW = {"grain": "day", "by": "user", "start": "2026-11-17", "end": "2026-11-19"}
OK, SOFT, ERROR = LogTransactionStatus.success, LogTransactionStatus.soft, LogTransactionStatus.error

#: (day of November, hour, minute, user, employee, method, company, status)
CALLS = [
    (17, 6, 0, "UA", None, "CheckOperator", None, OK),  # a login
    (17, 6, 5, "UA", None, "CheckOperator", None, ERROR),  # a failed login: a call, not a login
    (17, 6, 10, "UA", "User A", "ConfirmPickLine", "920", OK),
    (17, 6, 11, "UA", "User A", "ConfirmPickLine", "920", OK),
    (17, 6, 20, "UA", None, "GetAccessToken", None, OK),  # background: neither
    (19, 5, 0, "UA", None, "CheckOperator", None, SOFT),  # a login with M3's location warning
    (19, 5, 10, "UA", "User A", "ReportCount", "920", OK),
    (18, 22, 0, "UB", None, "CheckOperator", None, OK),
    (18, 22, 30, "UB", "User B", "LoadDeliveryPackage", "930", OK),
    (18, 23, 0, "UB", None, "CheckServer", None, OK),  # background: neither
    (18, 10, 0, "UC", None, "CheckOperator", None, OK),  # no name and no tagged work
]


@pytest.fixture(autouse=True)
async def tenant():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    async with async_session() as db:
        job = Job(customer_code=CC, filename="grid.log", storage_key="k")
        db.add(job)
        await db.flush()
        for i, (day, hour, minute, user, employee, method, company, status) in enumerate(CALLS):
            when = datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)
            db.add(LogTransaction(
                customer_code=CC, job_id=job.id, started_at=when, ended_at=when, date=when.date(),
                user_name=user, employee_name=employee, device_name="HH-01", device_id="HH-01",
                warehouse="BRI", company=company, method=method, reqid=f"GRID-{i}", status=status,
            ))
        await db.commit()
    yield
    await plant.wipe_analytics(CC)


async def _rows(**args):
    async with async_session() as db:
        return await at.activity_rows(db, CC, {**WINDOW, **args}, now=NOW)


def _layer(user_day: dict, key: str) -> list[list[int]]:
    return next(layer["values"] for layer in user_day["layers"] if layer["key"] == key)


# ==================================================== 1. the grid


async def test_a_day_grain_by_user_carries_people_down_and_every_day_across_in_two_layers():
    out = await _rows()
    user_day = out["user_day"]
    assert user_day["rows"] == ["User A (UA)", "User B (UB)", "UC"] and user_day["users"] == ["UA", "UB", "UC"]
    assert user_day["columns"] == ["Tu 17/11", "We 18/11", "Th 19/11"]
    assert user_day["days"] == ["2026-11-17", "2026-11-18", "2026-11-19"]
    assert [layer["key"] for layer in user_day["layers"]] == ["logins", "transactions"]
    assert [layer["label"] for layer in user_day["layers"]] == ["Logins", "Transactions"]
    # a login is a CheckOperator call that did not fail; a soft one (M3's warning) still counts
    assert _layer(user_day, "logins") == [[1, 0, 1], [0, 1, 0], [0, 1, 0]]
    # every call but the background ones (GetAccessToken, CheckServer)
    assert _layer(user_day, "transactions") == [[4, 0, 2], [0, 2, 0], [0, 1, 0]]
    assert user_day["totals"] == {"logins": 4, "transactions": 9}
    # the rows, the breakdown and the people of a day grain by user are still there
    assert out["group_by"] == ["day"] and out["breakdown_by"] == "user" and out["people"]


async def test_no_grid_without_the_user_cut_or_with_another_grain():
    assert "user_day" not in await _rows(by="none")
    assert "user_day" not in await _rows(grain="hour")
    assert "user_day" not in await _rows(by="device")
    # "who was active today" is a user cut with no grain asked: it keeps its people table
    assert "user_day" not in await _rows(grain="")


async def test_more_people_than_the_grid_holds_keeps_the_busiest_and_says_so(monkeypatch):
    monkeypatch.setattr(grid, "MAX_USERS", 2)
    out = await _rows()
    assert out["user_day"]["users"] == ["UA", "UB"]
    assert "the grid shows the 2 people with the most logins; 1 more not shown" in out["notes"]


async def test_a_window_longer_than_the_grid_holds_returns_no_grid_and_says_so():
    out = await _rows(start="2026-09-01", end="2026-11-19")
    assert "user_day" not in out
    assert f"the people-by-day grid holds up to {grid.MAX_DAYS} days; ask for a shorter window to see it" in out["notes"]


def test_the_bands_rise_from_one_and_follow_the_counts():
    assert grid.bands_of([[4, 0, 2], [0, 2, 0], [0, 1, 0]]) == [1, 2, 4]
    assert grid.bands_of([[0, 0]]) == [1, 2]
    assert grid.bands_of([[5, 5, 5]]) == [1, 5]
    spread = grid.bands_of([[n] for n in range(1, 101)])
    assert spread == [1, 50, 75, 90] and all(b > a for a, b in zip(spread, spread[1:]))


def test_a_label_names_the_person_and_keeps_the_user_id_within_forty_characters():
    assert grid.user_label("UA", "User A") == "User A (UA)"
    assert grid.user_label("UC", None) == "UC"
    long = grid.user_label("FNACHONLEO", "Francisco Alejandro Nachon-Leon de la Vega")
    assert len(long) == 40 and long.endswith(" (FNACHONLEO)")


# ==================================================== 2. the company


async def test_a_company_keeps_its_tagged_calls_and_the_untagged_calls_of_the_people_who_worked_for_it():
    out = await _rows(company="920")
    user_day = out["user_day"]
    # UA worked for 920: their logins (untagged) count; UB worked for 930 and UC for nobody
    assert user_day["users"] == ["UA"]
    assert _layer(user_day, "logins") == [[1, 0, 1]] and _layer(user_day, "transactions") == [[4, 0, 2]]
    assert out["company"] == "920" and out["total"]["users"] == 1
    assert any(n.startswith("company 920: an untagged call, such as a login,") for n in out["notes"])
    other = (await _rows(company="930"))["user_day"]
    assert other["users"] == ["UB"] and _layer(other, "transactions") == [[0, 2, 0]]


async def test_the_company_narrows_every_count_the_tool_returns():
    out = await _rows(company="920", by="none")
    assert [(r["dimensions"][0], r["users"]) for r in out["rows"]] == [("2026-11-17", 1), ("2026-11-19", 1)]
    nobody = await _rows(company="999")
    assert "user_day" not in nobody and nobody["total"]["users"] == 0


# ==================================================== 3. the block


async def test_the_lift_draws_the_grid_as_a_heat_map_with_both_layers_and_totals():
    out = await _rows(company="920")
    call = {"tool": "wms_activity", "input": {}, "result": json.dumps(out)}
    drawn = blocks.blocks_from_trace([call])
    assert [b["type"] for b in drawn] == ["heatmap"]  # the grid names the people: no people table
    heat = HeatmapBlock(**drawn[0])
    assert heat.title == "Logins and transactions per user per day · 2026-11-17 to 2026-11-20 · company 920"
    assert heat.rows == ["User A (UA)"] and heat.columns == ["Tu 17/11", "We 18/11", "Th 19/11"]
    assert heat.totals is True and [layer.label for layer in heat.layers] == ["Logins", "Transactions"]
    assert heat.values == [[1.0, 0.0, 1.0]] and heat.unit == "logins" and heat.scale == [1, 2, 4, 8]
    assert heat.layers[1].values == [[4.0, 0.0, 2.0]] and heat.layers[1].unit == "transactions"
    # a withheld answer keeps it, because it is drawn from the data
    assert [b["type"] for b in blocks.grounded_blocks_from_trace([call])] == ["heatmap"]


async def test_the_day_by_hour_heat_map_is_unchanged():
    async with async_session() as db:
        hourly = await at.activity_rows(db, CC, {"grain": "hour", "start": "2026-11-17", "end": "2026-11-19"}, now=NOW)
    heat = charts.auto_heatmap("wms_activity", hourly)
    assert heat.title.startswith("Active users by hour") and heat.layers is None and heat.totals is False


def test_the_tool_offers_the_company_and_a_recipe_for_logins_per_user_per_day():
    schema = at.ACTIVITY_TOOLS[0]["input_schema"]["properties"]
    assert "company" in schema
    recipe = next(r for r in at.RECIPES if r["question"].startswith("logins per user per day"))
    assert recipe["call"] == {"tool": "wms_activity", "grain": "day", "by": "user", "days": 30}
    assert "people down and days across" in at.ACTIVITY_TOOLS[0]["description"]


def test_a_month_fits_the_grid():
    assert grid.MAX_DAYS >= 32 and grid.MAX_DAYS <= 48 and grid.MAX_USERS <= 62
    assert timedelta(days=grid.MAX_DAYS) >= timedelta(days=31)
