"""Chunk 152: the assistant's tools over deliveries at risk.

The five tools read the same stores the board, the history page and the Teams card read, so the
assistant's figures are the page's figures. The pins: dates resolve on the warehouse's clock and a
missing range means the last seven days; the history carries the counts per word and the rows carry
the van clocks; the board lists flagged rows only unless asked; the vans aggregate one row per route
per day; one delivery's story carries its tiers and checks; a bad argument comes back as a readable
problem; every result that is a list carries a `table` the evidence layer draws unchanged; and the
agent's prompt and tool list know the vocabulary.
"""

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from app.config.database import async_session
from app.services.analytics_agent import agent as agent_module
from app.services.analytics_agent import at_risk_tools as t
from app.services.analytics_agent import evidence
from app.services.analytics_agent.tools import TOOL_NAMES, build_tools
from app.services.analytics_at_risk import RULE_VERSION, check_store, delivery_store, model
from tests import at_risk_fixtures as fx

CC = "test_chunk152ar"
LONDON = fx.LONDON
UTC = timezone.utc
DAY = date(2026, 10, 1)
DEP = fx.local_day(DAY, 11, 30)
USUAL = DEP - timedelta(minutes=270)  # the van is usually ready at 07:00


@pytest.fixture(autouse=True)
async def clean():
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    yield
    await fx.wipe(CC)


async def _run(name, **args) -> dict:
    async with async_session() as db:
        return json.loads(await t.run_at_risk_tool(name, args, db, CC))


async def _plant_week():
    rows = []
    for i in range(7):
        day = DAY - timedelta(days=i)
        dep, usual = fx.local_day(day, 11, 30), fx.local_day(day, 7, 0)
        rows += [
            fx.closed_delivery(CC, f"f{i}", route="BRI04", departure_at=dep, last_load_at=usual - timedelta(minutes=20), outcome="loaded_in_time",
                               usual_ready_at=usual, route_loaded_at=usual - timedelta(minutes=10), route_loading_from=usual - timedelta(hours=2),
                               customer_name="BOK SHOP HORSHAM", transaction_names=("Brighton Stock Pick",)),
            fx.closed_delivery(CC, f"m{i}", route="BRI06", departure_at=dep, last_load_at=None, outcome="never_loaded", max_tier="left_behind",
                               first_flagged_at=usual - timedelta(minutes=29), usual_ready_at=usual, route_loaded_at=usual + timedelta(minutes=5),
                               customer_name="HILTON BRIGHTON METROPOLE", transaction_names=("JIT and Shorts Pick (Brighton)",)),
        ]
    # one delivery that held a van 95 minutes late, on the first day
    rows.append(fx.closed_delivery(CC, "h0", route="BRI04", departure_at=DEP, last_load_at=USUAL + timedelta(minutes=95), outcome="loaded_in_time",
                                   usual_ready_at=USUAL, route_loaded_at=USUAL + timedelta(minutes=95), route_loading_from=USUAL - timedelta(hours=2),
                                   max_tier="at_risk", first_flagged_at=USUAL - timedelta(minutes=29), customer_name="OCKENDEN MANOR"))
    await fx.plant(rows)


# ============================================================== describe

async def test_describe_names_the_words_the_tiers_the_settings_and_each_routes_usual_time():
    await _plant_week()
    async with async_session() as db:
        from app.services.analytics_at_risk import profile_store, settings_store
        await profile_store.compute(db, CC, as_of=DAY, settings=settings_store.DEFAULTS, rule_version=RULE_VERSION,
                                    now=datetime.now(UTC), tz=LONDON)
        await db.commit()
    out = await _run("describe_at_risk")
    assert set(out["words"]) == {"missed", "held", "fine", "unknown"} and set(out["tiers"]) == {"watch", "at_risk", "left_behind"}
    assert out["settings"] == {"warn_before_min": 30, "gone_after_min": 20, "min_days": 5, "held_after_min": 60, "window_days": 28, "coverage": "0.900"}
    routes = {r["route"]: r for r in out["routes"]}
    # six days ready at 06:50 and one at 08:35: the time met on nine days in ten lands at 07:32
    assert routes["BRI04"]["van_usually_ready"] == "07:32" and routes["BRI04"]["clock_source"] == "learned" and routes["BRI04"]["days_learned_from"] == 7
    assert "the van is the clock" in out["how_to_read"].lower().replace("the clock is the van", "the van is the clock")
    assert any(r["question"].startswith("how many deliveries were missed") for r in out["recipes"])


# ============================================================== history

async def test_history_counts_per_word_over_the_range_and_shapes_the_rows_and_the_table():
    await _plant_week()
    out = await _run("at_risk_history", start=(DAY - timedelta(days=6)).isoformat(), end=DAY.isoformat(), category=["missed"])
    assert out["counts"] == {"missed": 7, "held": 1, "fine": 7, "unknown": 0}  # counts cover the range under every filter but the word
    assert len(out["rows"]) == 7 and all(r["word"] == "missed" for r in out["rows"])
    m0 = next(r for r in out["rows"] if r["delivery"] == "m0")
    assert (m0["route"], m0["customer"], m0["day"]) == ("BRI06", "HILTON BRIGHTON METROPOLE", "2026-10-01")
    assert (m0["van_usually_ready"], m0["van_ready"], m0["van_late_min"], m0["on_van"], m0["highest_tier"]) == ("07:00", "07:05", 5, None, "left_behind")
    assert m0["picking"] == ["JIT and Shorts Pick (Brighton)"] and m0["wms_departure"] == "11:30"
    assert out["table"]["columns"][:5] == ["day", "delivery", "customer", "route", "word"]
    assert out["table"]["rows"][0][8] == "never"
    assert "missed 7" in out["table"]["facts"]["counts over the range"]
    assert out["held_after_min"] == 60


async def test_history_filters_by_customer_route_picking_kind_and_delivery_and_defaults_to_the_last_seven_days():
    await _plant_week()
    hilton = await _run("at_risk_history", start=(DAY - timedelta(days=6)).isoformat(), end=DAY.isoformat(), customer_name="hilton")
    assert hilton["counts"] == {"missed": 7, "held": 0, "fine": 0, "unknown": 0}
    held = await _run("at_risk_history", day=DAY.isoformat(), category=["held"])
    assert [r["delivery"] for r in held["rows"]] == ["h0"]
    assert held["rows"][0]["van_late_min"] == 95 and held["rows"][0]["before_van_min"] == 0 and held["table"]["rows"][0][9] == "last one on"
    jit = await _run("at_risk_history", day=DAY.isoformat(), transaction="jit")
    assert [r["delivery"] for r in jit["rows"]] == ["m0"]
    one = await _run("at_risk_history", start=(DAY - timedelta(days=6)).isoformat(), end=DAY.isoformat(), delivery="f3")
    assert [r["delivery"] for r in one["rows"]] == ["f3"]
    bri06 = await _run("at_risk_history", day=DAY.isoformat(), route="bri06")
    assert [r["delivery"] for r in bri06["rows"]] == ["m0"]
    # no dates: the last seven days on the warehouse's clock, said in a note
    default = await _run("at_risk_history")
    assert default["notes"][0].startswith("no dates given: the last 7 days")
    assert (date.fromisoformat(default["range"]["end"]) - date.fromisoformat(default["range"]["start"])).days == 6


async def test_a_bad_day_or_range_comes_back_as_a_readable_problem():
    out = await _run("at_risk_history", day="tomorrow-ish")
    assert "error" in out and "today, yesterday or YYYY-MM-DD" in out["error"] and "hint" in out
    out = await _run("at_risk_history", start="2026-10-05", end="2026-10-01")
    assert "must not be after" in out["error"]
    out = await _run("at_risk_board", window="next week")
    assert "today, tomorrow or both" in out["error"]
    out = await _run("explain_at_risk_delivery")
    assert "delivery_number" in out["error"]


# ============================================================== board

def _state(number, **over) -> model.DeliveryState:
    base = dict(delivery_number=number, route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=DEP, lines_expected=5, lines_confirmed=5, lines_picked=5, lines_short=0,
                packages_created=2, packages_loaded=2, last_pick_at=DEP - timedelta(hours=4), last_load_at=DEP - timedelta(hours=3))
    base.update(over)
    if "lines_confirmed" not in over:
        base["lines_confirmed"] = base["lines_picked"]
    return model.DeliveryState(**base)


async def test_board_lists_flagged_rows_worst_first_with_the_van_clock_and_counts_the_fine_ones():
    today = datetime.now(LONDON).date()
    dep = fx.local_day(today, 11, 30)
    now = dep - timedelta(minutes=90)
    async with async_session() as db:
        await delivery_store.apply(db, CC, [
            _state("fine", departure_at=dep),
            _state("watch", departure_at=dep, lines_picked=2, packages_created=1, packages_loaded=1),
            _state("risk", departure_at=dep, packages_loaded=1, customer_name="Tesco Hove"),
            _state("behind", departure_at=dep - timedelta(days=1), packages_loaded=1),
        ], now=now, tz=LONDON, clock_for=fx.clock_before_departure(60), rule_version=RULE_VERSION)
        await db.commit()
    out = await _run("at_risk_board", window="today")
    assert out["summary"] == {"open": 4, "left_behind": 1, "at_risk": 1, "watch": 1, "fine": 1, "checked": 0}
    assert [r["delivery"] for r in out["rows"]] == ["behind", "risk", "watch"]
    risk = out["rows"][1]
    assert (risk["tier"], risk["customer"], risk["lines"], risk["packages"]) == ("at_risk", "Tesco Hove", "5 / 5", "1 / 2")
    assert risk["why"].startswith("packages still off the van · van usually ready by") and risk["clock_source"] == "learned"
    assert out["table"]["columns"] == ["tier", "delivery", "route", "customer", "van usually ready", "until van", "lines", "packages", "why"]
    assert out["table"]["rows"][0][0] == "Left behind"
    everything = await _run("at_risk_board", window="today", include_fine=True)
    assert len(everything["rows"]) == 4
    only = await _run("at_risk_board", window="today", tier="watch")
    assert [r["delivery"] for r in only["rows"]] == ["watch"] and only["summary"]["open"] == 1


# ============================================================== vans

async def test_vans_aggregate_one_row_per_route_per_day_and_can_keep_only_the_late_ones():
    await _plant_week()
    out = await _run("at_risk_vans", day=DAY.isoformat())
    by = {v["route"]: v for v in out["vans"]}
    assert (by["BRI04"]["van_ready"], by["BRI04"]["van_usually_ready"], by["BRI04"]["late_min"]) == ("08:35", "07:00", 95)
    assert (by["BRI04"]["deliveries"], by["BRI04"]["held"], by["BRI04"]["missed"]) == (2, 1, 0)
    assert (by["BRI06"]["late_min"], by["BRI06"]["missed"]) == (5, 1)
    late = await _run("at_risk_vans", start=(DAY - timedelta(days=6)).isoformat(), end=DAY.isoformat(), late_only=True, route="BRI04")
    assert [(v["day"], v["late_min"]) for v in late["vans"]] == [("2026-10-01", 95)]
    assert late["table"]["columns"] == ["day", "route", "loading from", "van ready", "usually by", "late by", "deliveries", "held", "missed"]
    assert late["table"]["rows"][0][5] == "95 min late"


# ============================================================== one delivery

async def test_explaining_a_delivery_carries_its_clocks_tier_story_and_checks():
    await _plant_week()
    async with async_session() as db:
        await delivery_store.apply(db, CC, [_state("29616", lines_picked=5, packages_created=2, packages_loaded=1)],
                                   now=USUAL - timedelta(minutes=20), tz=LONDON, clock_for=fx.clock_before_departure(270), rule_version=RULE_VERSION)
        await check_store.check(db, CC, "29616", actor="Amin", note="loader called", now=USUAL - timedelta(minutes=15))
        await db.commit()
    out = await _run("explain_at_risk_delivery", delivery_number="29616")
    assert out["found"] is True and out["status"] == "open" and out["tier"] == "at_risk"
    assert out["tier_story"] == [{"tier": "At risk", "at": "06:40", "minutes_to_usual_ready": "20", "clock_source": "learned"}]
    assert out["checks"] == [{"action": "checked", "tier": "at_risk", "by": "Amin", "note": "loader called", "at": "06:45", "day": "2026-10-01"}]
    assert out["why"].startswith("packages still off the van")
    missing = await _run("explain_at_risk_delivery", delivery_number="nope")
    assert missing["found"] is False
    closed = await _run("explain_at_risk_delivery", delivery_number="m0")
    assert (closed["word"], closed["outcome"], closed["van_ready"]) == ("missed", "never_loaded", "07:05")


# ============================================================== the agent knows

async def test_the_agent_carries_the_tools_the_vocabulary_and_draws_the_shaped_table():
    assert {"describe_at_risk", "at_risk_history", "at_risk_board", "at_risk_vans", "explain_at_risk_delivery"} <= set(TOOL_NAMES)
    async with async_session() as db:
        names = {tool.name for tool in build_tools(db, CC)}
    assert "at_risk_history" in names and "describe_at_risk" in agent_module.SCHEMA_TOOLS
    assert "DELIVERIES AT RISK" in agent_module.SYSTEM_PROMPT and "held the van" in agent_module.SYSTEM_PROMPT
    assert "deliveries at risk" in agent_module.DOMAIN and "at_risk_history" in agent_module._TOOL_TEXT
    await _plant_week()
    result = await _run("at_risk_vans", day=DAY.isoformat())
    trace = [{"tool": "describe_at_risk", "input": {}, "result": "{}"}, {"tool": "at_risk_vans", "input": {"day": "2026-10-01"}, "result": json.dumps(result)}]
    text = evidence.render(trace)
    assert text.startswith("From the data: Vans 2026-10-01 to 2026-10-01") and "| BRI04 |" in text and "95 min late" in text
    data = evidence.structured(trace, at_risk_link="https://x/matrix/at-risk")
    assert data["title"].startswith("Vans") and data["link"] == "https://x/matrix/at-risk"
    assert [c["name"] for c in data["columns"]][:3] == ["day", "route", "loading from"] and data["columns"][6] == {"name": "deliveries", "align": "right"}
    # the figures in the table are grounded: nothing the model could be accused of inventing
    assert evidence.ungrounded("BRI04's van was 95 min late on 1 October, carrying 2 deliveries.", [json.dumps(result)]) == []
