"""Chunk 148: the deliveries-at-risk endpoints, called the way chunk 140 calls the forecast ones.

What a screen sees: the board sorted late first then by minutes to departure, with measures as
strings and counts as integers; a check that records a person's word and an uncheck that clears it;
history that pages by keyset without overlap; the ledger newest first; an accuracy answer with
precision and recall over the closed rows; settings that refuse a bad range and persist a good one;
a status row whose readiness flags say what is still missing on a tenant; and `stale` when the worker
has stopped writing.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi import HTTPException

from app.api.v1 import analytics_at_risk as api
from app.config.database import async_session
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.services.analytics_at_risk import RULE_VERSION, delivery_store, model, state_store
from tests import at_risk_fixtures as fx

CC = "test_chunk148ar"
LONDON = fx.LONDON
UTC = timezone.utc
DEP = datetime(2026, 10, 2, 10, 30, tzinfo=UTC)  # 11:30 BST on 2 Oct
NOW = DEP - timedelta(minutes=90)
TH = model.Thresholds(load_min=Decimal("120"), load_source="floor", pick_min=Decimal("180"), pick_source="floor")


@pytest.fixture(autouse=True)
async def clean(monkeypatch):
    await fx.wipe(CC)
    await fx.seed_tenant(CC)
    monkeypatch.setattr(api, "_now", lambda: NOW)
    yield
    await fx.wipe(CC)


def _state(number, **over) -> model.DeliveryState:
    base = dict(delivery_number=number, route="BRI03", customer_name="BOK SHOP HORSHAM", customer_number="10567",
                departure_at=DEP, lines_expected=5, lines_confirmed=5, lines_picked=5, lines_short=0,
                packages_created=2, packages_loaded=2, last_pick_at=DEP - timedelta(hours=4), last_load_at=DEP - timedelta(hours=3))
    base.update(over)
    if "lines_confirmed" not in over:  # the helper confirms what it picks unless a test says otherwise
        base["lines_confirmed"] = base["lines_picked"]
    return model.DeliveryState(**base)


async def _apply(states, now=NOW):
    async with async_session() as db:
        await delivery_store.apply(db, CC, states, now=now, tz=LONDON, thresholds_for=lambda r: TH, rule_version=RULE_VERSION)
        await state_store.touch(db, CC, last_evaluated_at=now, open_rows=len(states))
        await db.commit()


async def _board_states():
    """Four open deliveries: one fine, one watch, one at risk, one late (yesterday's departure)."""
    yesterday = DEP - timedelta(days=1)
    return [
        _state("fine"),
        _state("watch", departure_at=DEP + timedelta(minutes=60), lines_picked=2, packages_created=0, packages_loaded=0),  # 150 min: inside pick lead
        _state("risk", packages_loaded=1, last_load_at=DEP - timedelta(hours=3), route_loaded_at=DEP - timedelta(hours=2)),  # 90 min: inside load lead
        _state("late", departure_at=yesterday, packages_loaded=1),
    ]


# ==================================================== 1. board

async def test_board_lists_open_deliveries_sorted_by_tier_then_minutes():
    await _apply(await _board_states())
    async with async_session() as db:
        out = await api.read_board(window="both", customer=CC, db=db)
    assert out["timezone"] == "Europe/London" and out["stale"] is False
    assert out["evaluated_at"] == NOW.isoformat() and out["now"] == NOW.isoformat()
    assert out["counts"] == {"open": 4, "watch": 1, "at_risk": 1, "late": 1, "checked": 0}
    assert out["settings"] == {"load_floor_min": 120, "pick_floor_min": 180}
    assert [d["delivery_number"] for d in out["deliveries"]] == ["late", "risk", "watch", "fine"]
    risk = out["deliveries"][1]
    assert risk["tier"] == "at_risk" and risk["max_tier"] == "at_risk"
    assert risk["minutes_to_departure"] == "90" and isinstance(risk["lines"]["expected"], int)
    assert risk["threshold"] == {"load_min": "120", "load_source": "floor", "pick_min": "180", "pick_source": "floor"}
    assert risk["lines"] == {"expected": 5, "confirmed": 5, "picked": 5, "short": 0}
    assert risk["packages"] == {"created": 2, "loaded": 1}
    assert risk["check"] is None and risk["reopened"] is False
    assert risk["departure_at"] == DEP.isoformat() and risk["departure_date"] == "2026-10-02"
    assert risk["last_load_at"] == (DEP - timedelta(hours=3)).isoformat()
    assert risk["route_loaded_at"] == (DEP - timedelta(hours=2)).isoformat()
    assert out["deliveries"][3]["route_loaded_at"] is None
    assert risk["first_flagged_at"] == NOW.isoformat()
    late = out["deliveries"][0]
    assert late["tier"] == "late" and late["minutes_to_departure"].startswith("-")


async def test_board_windows_and_tier_filter():
    await _apply(await _board_states() + [_state("tomorrow", departure_at=DEP + timedelta(days=1))])
    async with async_session() as db:
        today = await api.read_board(window="today", customer=CC, db=db)
        tomorrow = await api.read_board(window="tomorrow", customer=CC, db=db)
        risky = await api.read_board(window="both", tier="at_risk", customer=CC, db=db)
    # yesterday's departure that is still open is overdue, so it stays on today's board until it closes
    assert sorted(d["delivery_number"] for d in today["deliveries"]) == ["fine", "late", "risk", "watch"]
    assert [d["delivery_number"] for d in tomorrow["deliveries"]] == ["tomorrow"]
    assert [d["delivery_number"] for d in risky["deliveries"]] == ["risk"]
    with pytest.raises(HTTPException) as e:
        async with async_session() as db:
            await api.read_board(window="yesterday", customer=CC, db=db)
    assert e.value.status_code == 422


async def test_board_is_stale_when_the_worker_has_stopped_writing():
    await _apply(await _board_states(), now=NOW - timedelta(minutes=10))
    async with async_session() as db:
        out = await api.read_board(window="both", customer=CC, db=db)
    assert out["stale"] is True and out["evaluated_at"] == (NOW - timedelta(minutes=10)).isoformat()


# ==================================================== 2. checks

async def test_check_and_uncheck_round_trip_with_the_default_actor():
    await _apply(await _board_states())
    async with async_session() as db:
        checked = await api.check_delivery("risk", body={"note": "loader called"}, customer=CC, db=db)
        board = await api.read_board(window="both", customer=CC, db=db)
    assert checked["check"] == {"checked_at": NOW.isoformat(), "checked_by": "api", "note": "loader called",
                                "tier": "at_risk", "reopened_count": 0}
    assert board["counts"]["checked"] == 1
    async with async_session() as db:
        named = await api.check_delivery("watch", body={"checked_by": "Amin Talukder"}, customer=CC, db=db)
        cleared = await api.uncheck_delivery("risk", body={"checked_by": "mark"}, customer=CC, db=db)
        ledger = await api.read_checks(customer=CC, db=db)
    assert named["check"]["checked_by"] == "Amin Talukder"
    assert cleared["check"] is None
    # the clock is frozen in this test, so the three actions share an instant and only the set is pinned
    assert sorted((c["delivery_number"], c["action"], c["actor"]) for c in ledger["checks"]) == sorted([
        ("risk", "unchecked", "mark"), ("watch", "checked", "Amin Talukder"), ("risk", "checked", "api")])


async def test_checking_an_unknown_delivery_is_404():
    with pytest.raises(HTTPException) as e:
        async with async_session() as db:
            await api.check_delivery("nope", body={}, customer=CC, db=db)
    assert e.value.status_code == 404


async def test_a_reopened_check_is_visible_on_the_board():
    await _apply(await _board_states())
    async with async_session() as db:
        await api.check_delivery("watch", body={"checked_by": "amin"}, customer=CC, db=db)
    # the watch delivery's loading is now behind too
    await _apply([_state("watch", departure_at=DEP + timedelta(minutes=60), lines_picked=5, packages_created=0, packages_loaded=0)],
                 now=DEP - timedelta(minutes=30))  # 90 min left: at_risk
    async with async_session() as db:
        out = await api.read_board(window="both", customer=CC, db=db)
    row = next(d for d in out["deliveries"] if d["delivery_number"] == "watch")
    assert row["tier"] == "at_risk" and row["reopened"] is True
    assert row["check"]["tier"] == "watch" and row["check"]["reopened_count"] == 1 and row["check"]["checked_by"] == "amin"


# ==================================================== 3. history and accuracy

async def _close_all(now):
    async with async_session() as db:
        await delivery_store.close_due(db, CC, {}, now=now, grace=timedelta(0), tz=LONDON)
        await db.commit()


async def test_history_pages_closed_rows_by_keyset_without_overlap():
    rows = [fx.closed_delivery(CC, f"d{i:02d}", route="BRI03" if i % 2 else "BRI01",
                               departure_at=fx.local_day(date(2026, 9, 20) + timedelta(days=i), 11, 30),
                               last_load_at=fx.local_day(date(2026, 9, 20) + timedelta(days=i), 7, 0),
                               outcome="loaded_in_time" if i % 3 else "loaded_late", max_tier="at_risk" if i % 3 == 0 else "none",
                               first_flagged_at=fx.local_day(date(2026, 9, 20) + timedelta(days=i), 9, 0) if i % 3 == 0 else None)
            for i in range(7)]
    await fx.plant(rows)
    seen = []
    after = None
    async with async_session() as db:
        for _ in range(5):
            page = await api.read_history(start="2026-09-20", end="2026-09-26", limit=3, after=after, customer=CC, db=db)
            seen += [r["delivery_number"] for r in page["rows"]]
            if not page["truncated"]:
                break
            after = page["next_after"]
    assert seen == [f"d{i:02d}" for i in range(6, -1, -1)]  # newest first, nothing twice, nothing missing
    async with async_session() as db:
        late = await api.read_history(start="2026-09-20", end="2026-09-26", outcome="loaded_late", customer=CC, db=db)
        route = await api.read_history(start="2026-09-20", end="2026-09-26", route="BRI01", customer=CC, db=db)
        flagged = await api.read_history(start="2026-09-20", end="2026-09-26", tier="at_risk", customer=CC, db=db)
    assert [r["delivery_number"] for r in late["rows"]] == ["d06", "d03", "d00"]
    assert all(r["route"] == "BRI01" for r in route["rows"]) and len(route["rows"]) == 4
    assert [r["delivery_number"] for r in flagged["rows"]] == ["d06", "d03", "d00"]
    assert flagged["rows"][0]["outcome"] == "loaded_late" and flagged["rows"][0]["status"] == "closed"
    with pytest.raises(HTTPException) as e:
        async with async_session() as db:
            await api.read_history(start="2026-01-01", end="2026-09-26", customer=CC, db=db)
    assert e.value.status_code == 422


async def test_history_filters_by_category_picking_screen_and_delivery_and_carries_the_counts():
    day = date(2026, 9, 30)
    dep = fx.local_day(day, 11, 30)
    await fx.plant([
        fx.closed_delivery(CC, "fine1", route="BRI03", departure_at=dep, last_load_at=dep - timedelta(hours=4), outcome="loaded_in_time"),
        fx.closed_delivery(CC, "slow1", route="BRI03", departure_at=dep, last_load_at=dep - timedelta(hours=1), outcome="loaded_in_time",
                           max_tier="at_risk", first_flagged_at=dep - timedelta(hours=2), transaction_names=("Brighton Stock Pick", "JIT and Shorts Pick (Brighton)")),
        fx.closed_delivery(CC, "gone1", route="BRI06", departure_at=dep, last_load_at=dep - timedelta(hours=3), outcome="never_loaded",
                           max_tier="late", transaction_names=("Milk Pick (Brighton)",), customer_name="HILTON"),
    ])
    async with async_session() as db:
        default = await api.read_history(start="2026-09-30", end="2026-09-30", category="missed,delayed", customer=CC, db=db)
        jit = await api.read_history(start="2026-09-30", end="2026-09-30", transaction="JIT and Shorts Pick (Brighton)", customer=CC, db=db)
        one = await api.read_history(start="2026-09-30", end="2026-09-30", delivery="gone", customer=CC, db=db)
    assert sorted(r["delivery_number"] for r in default["rows"]) == ["gone1", "slow1"]
    assert {r["delivery_number"]: r["category"] for r in default["rows"]} == {"gone1": "missed", "slow1": "delayed"}
    # the counts cover the whole range, not just the category filter, so the bar keeps its shape as pills change
    assert default["counts"] == {"missed": 1, "delayed": 1, "fine": 1, "unknown": 0}
    assert default["transactions"] == ["Brighton Stock Pick", "JIT and Shorts Pick (Brighton)", "Milk Pick (Brighton)"]
    assert [r["delivery_number"] for r in jit["rows"]] == ["slow1"] and jit["counts"] == {"missed": 0, "delayed": 1, "fine": 0, "unknown": 0}
    assert [r["delivery_number"] for r in one["rows"]] == ["gone1"] and one["rows"][0]["transaction_names"] == ["Milk Pick (Brighton)"]
    assert one["rows"][0]["customer_name"] == "HILTON"
    with pytest.raises(HTTPException) as e:
        async with async_session() as db:
            await api.read_history(start="2026-09-30", end="2026-09-30", category="bad", customer=CC, db=db)
    assert e.value.status_code == 422


async def test_accuracy_counts_flagged_against_actually_late():
    day = date(2026, 9, 30)
    plant = []
    # 10 departures on one route: 4 flagged, 3 of those late; 1 late never flagged; 5 fine and unflagged
    for i in range(10):
        flagged = i < 4
        late = i < 3 or i == 9
        plant.append(fx.closed_delivery(
            CC, f"a{i}", route="BRI03", departure_at=fx.local_day(day, 11, 30),
            last_load_at=None if late and i == 9 else fx.local_day(day, 12, 0) if late else fx.local_day(day, 7, 0),
            outcome="never_loaded" if late and i == 9 else "loaded_late" if late else "loaded_in_time",
            max_tier="at_risk" if flagged else "none", first_flagged_at=fx.local_day(day, 9, 0) if flagged else None))
    plant.append(fx.closed_delivery(CC, "u", route="BRI01", departure_at=fx.local_day(day, 11, 30), last_load_at=None, outcome="unknown"))
    await fx.plant(plant)
    async with async_session() as db:
        out = await api.read_accuracy(days=28, customer=CC, db=db)
    total = out["total"]
    assert out["window"] == {"start": "2026-09-04", "end": "2026-10-01", "days": 28}
    assert (total["departures"], total["flagged"], total["flagged_late"], total["late_not_flagged"], total["flagged_not_late"]) == (11, 4, 3, 1, 1)
    assert total["precision"] == "0.75" and total["recall"] == "0.75"
    assert total["outcomes"] == {"loaded_in_time": 6, "loaded_late": 3, "never_loaded": 1, "picked_in_time": 0, "picked_late": 0, "unknown": 1}
    assert total["by_tier"] == {"at_risk": {"flagged": 4, "late": 3}}
    assert [r["route"] for r in out["routes"]] == ["BRI01", "BRI03"]
    assert out["routes"][1]["flagged"] == 4 and out["routes"][0]["departures"] == 1


async def test_accuracy_with_nothing_closed_says_so_with_nulls_not_zeros():
    async with async_session() as db:
        out = await api.read_accuracy(days=14, customer=CC, db=db)
    assert out["total"]["departures"] == 0 and out["total"]["precision"] is None and out["total"]["recall"] is None
    assert out["routes"] == []


# ==================================================== 4. settings, routes, status

async def test_settings_default_then_persist_and_refuse_a_bad_range():
    async with async_session() as db:
        before = await api.read_settings(customer=CC, db=db)
    assert before["defaulted"] is True and before["load_floor_min"] == 120 and before["coverage"] == "0.9"
    with pytest.raises(HTTPException) as e:
        async with async_session() as db:
            await api.put_settings(body={"load_floor_min": -5, "coverage": 2}, customer=CC, db=db)
    assert e.value.status_code == 422 and "load_floor_min" in str(e.value.detail) and "coverage" in str(e.value.detail)
    async with async_session() as db:
        after = await api.put_settings(body={"load_floor_min": 90, "updated_by": "amin"}, customer=CC, db=db)
    assert (after["defaulted"], after["load_floor_min"], after["pick_floor_min"], after["updated_by"]) == (False, 90, 180, "amin")


async def test_routes_read_the_latest_profile_with_the_effective_threshold():
    rows = [fx.closed_delivery(CC, f"d{i}", route="BRI03", departure_at=fx.local_day(date(2026, 10, 1) - timedelta(days=i % 10), 11, 30),
                               last_load_at=fx.local_day(date(2026, 10, 1) - timedelta(days=i % 10), 11, 30) - timedelta(minutes=200 + 10 * i),
                               outcome="loaded_in_time") for i in range(20)]
    await fx.plant(rows)
    from app.services.analytics_at_risk import runner
    assert (await runner.profile_tenant(CC, as_of=date(2026, 10, 1), now=NOW))["status"] == "completed"
    async with async_session() as db:
        out = await api.read_routes(customer=CC, db=db)
    assert len(out["routes"]) == 1
    r = out["routes"][0]
    assert (r["route"], r["as_of_date"], r["sample"], r["loaded_sample"]) == ("BRI03", "2026-10-01", 20, 20)
    assert r["learned_load_min"] == "219" and r["effective_load_min"] == "219" and r["load_source"] == "learned"
    assert r["departure_time_mode"] == "1130"


async def test_status_readiness_flags_say_what_a_tenant_still_lacks():
    async with async_session() as db:
        out = await api.read_status(customer=CC, db=db)
    assert out["rule_version"] == RULE_VERSION and out["last_evaluated_at"] is None
    assert out["readiness"] == {"settlement_declared": True, "departure_fields_captured": False, "pick_line_has_delivery": True}
    async with async_session() as db:
        for field in ("resp.DeparatureDate", "resp.DeparatureTime"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="GetNextDeliveryByRoute", source="response", field=field, captured=True))
        await db.commit()
        out = await api.read_status(customer=CC, db=db)
    assert out["readiness"]["departure_fields_captured"] is True


async def test_evaluate_runs_inline_and_returns_the_pass():
    await fx.plant([fx.route_fact(CC, "29616", route="BRI03", dep_date="20261002", dep_time="1130", when=NOW - timedelta(hours=5))])
    await fx.settle(CC)
    async with async_session() as db:
        out = await api.evaluate_now(customer=CC, db=db)
    assert out["status"] == "completed" and out["evaluated"] == 1
