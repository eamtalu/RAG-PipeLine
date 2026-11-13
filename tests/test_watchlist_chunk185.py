"""Chunk 185: the watchlist (stage D, step 14, backend half).

A person watches a delivery, a route, a location, a zone, a product or a measure. The tick reads
each kind's data once, keeps the live value, and writes one event per status change: the first
status after adding is silent, an upgrade lands at once, a downgrade settles, the day's resting
status never wakes anyone, a closed delivery ends its watch. Ten watches at most; the eleventh
answers with the quietest watches to swap; nothing outside the person's sites; the same thing
twice is one watch. The person's `USER#<id>#WATCHES` row and the customer's `WATCHABLES` row are
what the edge reads. Conditions ("when fill rate drops below") are watches now, not schedules.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_lookup import AnalyticsLookupValue
from app.persistence.models.customer import Customer
from app.persistence.models.tenant_site import TenantSite
from app.persistence.models.watch import Watch, WatchEvent
from app.services.access import commands as access_commands, records, users
from app.services.analytics_at_risk import RULE_VERSION
from app.services.teams import home_snapshot
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.watchlist import commands, registry, runner, search, store
from app.services.watchlist import mirror as watch_mirror

CC = "test_c185"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 15, 10, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        for model_ in (AnalyticsAtRiskDelivery, AnalyticsFact, AnalyticsLookupValue):
            await s.execute(delete(model_).where(model_.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()
    home_snapshot._LATEST_FILL_RATES.clear()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C185", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        await users.create_user(s, CC, "u-sup", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await users.create_user(s, CC, "u-ops", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Olu Ops", role_code="ops_manager", all_sites=True))
        await s.commit()
    yield
    await _wipe()


def _at_risk(delivery: str, tier: str = "none", *, route="R14", customer="Hove Deli", warehouse="BRI",
             lines_expected=36, lines_picked=31, status="open", outcome=None) -> AnalyticsAtRiskDelivery:
    return AnalyticsAtRiskDelivery(
        customer_code=CC, delivery_number=delivery, departure_date=NOW.astimezone(TZ).date(),
        departure_at=NOW + timedelta(hours=3), route=route, customer_name=customer, warehouse=warehouse,
        tier=tier, max_tier=tier, first_flagged_at=NOW - timedelta(minutes=5), first_flagged_tier=tier,
        lines_expected=lines_expected, lines_confirmed=lines_picked, lines_picked=lines_picked, packages_created=3,
        packages_loaded=1, last_pick_at=NOW - timedelta(minutes=2), usual_ready_at=NOW + timedelta(hours=2),
        usual_ready_source="learned", status=status, outcome=outcome, rule_version=RULE_VERSION, last_evaluated_at=NOW)


def _pick(item: str, location: str, *, zero: bool = False, when: datetime = NOW) -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.astimezone(TZ).date(),
        transaction_name="JIT and Shorts Pick", method="ConfirmPickLine", status="success",
        quantity_classification="attempt" if zero else "pick", warehouse="BRI", item_number=item, user_name="PICKER1",
        attributes={"FromLocation": location}, created_at=when)


def _count(item: str, location: str, counted: str, balance: str, when: datetime = NOW) -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.astimezone(TZ).date(),
        transaction_name="Quick Stock Count", method="ReportCount", status="success", quantity_classification="pick",
        warehouse="BRI", item_number=item, user_name="PICKER1",
        attributes={"Location": location, "CountedQuantity": counted, "BalanceQuantity": balance}, created_at=when)


def _lookup(lookup: str, key: str, attribute: str, value: str) -> AnalyticsLookupValue:
    return AnalyticsLookupValue(customer_code=CC, lookup=lookup, key=key, attribute=attribute, value=value,
                                valid_from=NOW - timedelta(days=30), valid_to=None)


def _command(sender: str, kind: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question=f"watch: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by=name, payload=payload))


async def _record(db, object_id: str) -> records.AccessRecord:
    record = await records.access_record_for(db, CC, object_id)
    assert record is not None
    return record


async def _tick(now: datetime, mirror=None) -> dict:
    return await runner.sweep_once(mirror, only=[CC], now=now)


# ==================================================== 1. the rules

def test_the_pure_rules():
    assert registry.default_alert("product") == {"rule": "status_change"}
    assert registry.clean_alert("location", {"rule": "status_change", "count": "2"}) == {"rule": "status_change", "count": 2}
    with pytest.raises(ValueError, match="rule must be"):
        registry.clean_alert("delivery", {"rule": "below"})
    assert registry.delivery_status("closed", "none", "never_loaded") == "never_loaded"
    assert registry.route_status({"watch": 2}, 1) == "needs_look"
    assert registry.is_alert("route", {"rule": "tier_from", "tier": "at_risk"}, "on_track", "done", {"late_today": 1})
    assert not registry.is_alert("route", {"rule": "tier_from", "tier": "at_risk"}, "on_track", "done", {"late_today": 0})
    assert registry.level_of("below") == "high" and registry.status_text("needs_look") == "Needs a look"


# ==================================================== 2. adding, the limit, the swap, access

async def test_a_watch_is_added_once_within_the_persons_sites_and_the_eleventh_offers_a_swap():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add_all([_at_risk("D-1"), _at_risk("D-2", route="R3", customer="Lewes Grocer", warehouse="LON")])
        db.add_all([_lookup("item description", f"10{n:04d}", "ItemDescription", f"Item {n}") for n in range(12)])
        await db.commit()
    # Step 1: the eye button adds a delivery with the default alert; the same tap again finds it
    outcome = await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-1", "via": "eye"}, "Sam"), mirror=mirror, now=NOW)
    assert outcome.text.startswith("Watching · D-1 · Hove Deli · Tells you if it is likely to leave short or late · 1 of 10 places used")
    receipt = outcome.blocks[0]
    assert receipt["type"] == "receipt" and receipt["undo"]["action"] == "stop" and receipt["undo_until"]
    watch_id = receipt["reference"]
    again = await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-1"}, "Sam"), mirror=mirror, now=NOW)
    assert again.text == "Already watching · D-1 · Hove Deli" and again.blocks[0]["reference"] == watch_id
    # Step 2: another site is refused in plain words; an unknown thing too
    with pytest.raises(access_commands.Refused, match="belongs to LON, and your access covers BRI only"):
        await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-2"}, "Sam"), mirror=mirror, now=NOW)
    with pytest.raises(access_commands.Refused, match="no open delivery D-9"):
        await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-9"}, "Sam"), mirror=mirror, now=NOW)
    # Step 3: nine products fill the list; the eleventh thing comes back as a choice of swaps
    for n in range(9):
        await commands.run(_command("u-sup", "watch_add", {"kind": "product", "entity_id": f"10{n:04d}"}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=n))
    full = await commands.run(_command("u-sup", "watch_add", {"kind": "product", "entity_id": "100011"}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=20))
    assert full.text == "Your watchlist is full (10 of 10)"
    choice = full.blocks[0]
    assert choice["type"] == "choice" and choice["options"][-1]["label"] == "Never mind" and choice["options"][-1]["command"] is None
    assert choice["options"][0]["suggested"] and choice["options"][0]["command"]["kind"] == "watch_add"
    assert choice["options"][0]["command"]["payload"]["entity_id"] == "100011" and "swap_watch_id" in choice["options"][0]["command"]["payload"]
    assert len(choice["options"]) == 11  # ten watches, then Never mind
    # Step 4: the swap ends one and adds the other in one go; the row counts ten still
    swapped = await commands.run(_command("u-sup", "watch_add", choice["options"][0]["command"]["payload"], "Sam"), mirror=mirror, now=NOW + timedelta(minutes=21))
    assert swapped.text.startswith("Swapped · Watching · Item 11 · 100011")
    row = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    assert row["used"] == 10 and row["limit"] == 10
    assert any(w["entity_id"] == "100011" for w in row["watches"]) and not any(w["id"] == choice["options"][0]["id"] for w in row["watches"])
    async with async_session() as db:
        ended = await db.scalar(select(Watch).where(Watch.id == uuid.UUID(choice["options"][0]["id"])))
        assert ended.status == "ended" and ended.ended_reason == "swapped"
        actions = sorted(e.action for e in (await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC))).scalars()
                         if e.action.startswith("watch"))
        assert actions.count("watch_add") == 13  # 1 + 1 again + 9 + the full attempt + the swap
    # the queue routes watch commands here
    assert (await run_command(_command("u-sup", "watch_seen", {}, "Sam"))).text == "marked seen · 0 changes"


# ==================================================== 3. the tick: deliveries

async def test_a_delivery_watch_alerts_on_a_tier_change_once_settles_a_downgrade_and_ends_when_it_has_gone():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add(_at_risk("D-1"))
        await db.commit()
    await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-1"}, "Sam"), mirror=mirror, now=NOW)
    # Step 1: the first check sets the status silently, no event
    assert await _tick(NOW + timedelta(seconds=5), mirror) == {CC: 0}
    async with async_session() as db:
        w = (await db.execute(select(Watch).where(Watch.customer_code == CC))).scalar_one()
        assert w.last_status == "on_track" and w.last_value["lines_picked"] == 31 and w.next_check_at == NOW + timedelta(seconds=65)
        assert w.last_summary.startswith("31 of 36 lines picked · leaves ")
    # Step 2: the worker flags it: one event, an alert; the same state again: nothing
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))).scalar_one()
        row.tier, row.lines_short = "at_risk", 5
        await db.commit()
    assert await _tick(NOW + timedelta(minutes=2), mirror) == {CC: 1}
    assert await _tick(NOW + timedelta(minutes=4), mirror) == {CC: 0}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    assert row_payload["unseen"] == 1 and row_payload["counts"] == {"needs_look": 1, "keep_an_eye": 0, "fine": 0}
    event = row_payload["events_today"][0]
    assert event["old_status"] == "on_track" and event["new_status"] == "at_risk" and event["is_alert"] and not event["seen"]
    assert row_payload["watches"][0]["status_text"] == "At risk" and row_payload["watches"][0]["level"] == "high"
    # Step 3: the tier eases: not an event until it has held for the settle time
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))).scalar_one()
        row.tier = "watch"
        await db.commit()
    assert await _tick(NOW + timedelta(minutes=6), mirror) == {CC: 0}
    assert await _tick(NOW + timedelta(minutes=7), mirror) == {CC: 0}
    assert await _tick(NOW + timedelta(minutes=9), mirror) == {CC: 1}
    async with async_session() as db:
        events = (await db.execute(select(WatchEvent).where(WatchEvent.customer_code == CC).order_by(WatchEvent.seq))).scalars().all()
        assert [(e.old_status, e.new_status, e.is_alert) for e in events] == [("on_track", "at_risk", True), ("at_risk", "needs_look", False)]
    # Step 4: marked seen; then the van goes late: a last event and the watch ends, its place free
    assert (await commands.run(_command("u-sup", "watch_seen", {}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=10))).text == "marked seen · 2 changes"
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))).scalar_one()
        row.status, row.outcome = "closed", "loaded_late"
        await db.commit()
    assert await _tick(NOW + timedelta(minutes=12), mirror) == {CC: 1}
    async with async_session() as db:
        w = (await db.execute(select(Watch).where(Watch.customer_code == CC))).scalar_one()
        assert w.status == "ended" and w.ended_reason == "delivered" and w.last_status == "gone_late"
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    assert row_payload["used"] == 0 and row_payload["unseen"] == 1 and row_payload["events_today"][0]["new_status"] == "gone_late"
    assert await _tick(NOW + timedelta(minutes=14), mirror) == {}


# ==================================================== 4. the tick: routes and measures

async def test_a_route_rolls_up_its_deliveries_and_a_measure_crosses_its_line_with_a_band():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add_all([_at_risk("D-1", route="R14"), _at_risk("D-2", route="R14", customer="Brighton Bakes", lines_picked=10)])
        await db.commit()
    home_snapshot._LATEST_FILL_RATES[(CC, "BRI")] = {"fill_rate": 0.98, "as_of": NOW.isoformat(), "provisional": False, "expected": 400}
    await commands.run(_command("u-sup", "watch_add", {"kind": "route", "entity_id": "R14"}, "Sam"), mirror=mirror, now=NOW)
    measure = await commands.run(_command("u-sup", "watch_add", {"kind": "measure", "entity_id": "fill_rate:BRI", "alert": {"rule": "below", "level": 97}}, "Sam"), mirror=mirror, now=NOW)
    assert "Watching · Fill rate · Brighton · Tells you if it drops below 97%" in measure.text
    assert await _tick(NOW + timedelta(seconds=5), mirror) == {CC: 0}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    by_kind = {w["kind"]: w for w in row_payload["watches"]}
    assert by_kind["route"]["status"] == "on_track" and by_kind["route"]["summary"].startswith("2 open · 41 of 72 lines picked")
    assert by_kind["measure"]["status"] == "above" and by_kind["measure"]["summary"] == "98.0% · your line 97%"
    # Step 1: one delivery falls behind: the route is behind, one alert
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.delivery_number == "D-2"))).scalar_one()
        row.tier = "left_behind"
        await db.commit()
    home_snapshot._LATEST_FILL_RATES[(CC, "BRI")]["fill_rate"] = 0.965
    assert await _tick(NOW + timedelta(minutes=2), mirror) == {CC: 2}
    # Step 2: the figure hovers back over the line but under the band: still below; over the band: above, a quiet event
    home_snapshot._LATEST_FILL_RATES[(CC, "BRI")]["fill_rate"] = 0.972
    assert await _tick(NOW + timedelta(minutes=4), mirror) == {CC: 0}
    home_snapshot._LATEST_FILL_RATES[(CC, "BRI")]["fill_rate"] = 0.976
    assert await _tick(NOW + timedelta(minutes=6), mirror) == {CC: 1}
    async with async_session() as db:
        events = (await db.execute(select(WatchEvent).where(WatchEvent.customer_code == CC).order_by(WatchEvent.at, WatchEvent.new_status))).scalars().all()
        assert sorted((e.new_status, e.is_alert) for e in events) == [("above", False), ("below", True), ("left_behind", True)]
    # the whole-customer measure needs every site; a supervisor of one site is refused
    with pytest.raises(access_commands.Refused, match="every site"):
        await commands.run(_command("u-sup", "watch_add", {"kind": "measure", "entity_id": "fill_rate:all"}, "Sam"), mirror=mirror, now=NOW)


# ==================================================== 5. the tick: products, locations, zones

async def test_products_locations_and_zones_read_todays_picks_and_the_latest_counts():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add_all([_lookup("item description", "104568", "ItemDescription", "Ginger"),
                    _lookup("location", "A-12-03", "StockZone", "C"), _lookup("location", "A-12-04", "StockZone", "C"),
                    _lookup("location", "B-01-01", "StockZone", "D")])
        db.add_all([_pick("104568", "A-12-03", when=NOW - timedelta(hours=2)), _pick("104568", "A-12-03", when=NOW - timedelta(hours=1))])
        await db.commit()
    for kind, entity_id in (("product", "104568"), ("location", "A-12-03"), ("zone", "C")):
        await commands.run(_command("u-sup", "watch_add", {"kind": kind, "entity_id": entity_id}, "Sam"), mirror=mirror, now=NOW)
    assert await _tick(NOW + timedelta(seconds=5), mirror) == {CC: 0}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    by_kind = {w["kind"]: w for w in row_payload["watches"]}
    assert by_kind["product"]["label"] == "Ginger · 104568" and by_kind["product"]["status"] == "picking"
    assert by_kind["location"]["status"] == "picking" and by_kind["zone"]["summary"] == "2 picks today across 2 locations"
    # Step 1: three picks find the location empty: the location and its zone alert; the product too
    async with async_session() as db:
        db.add_all([_pick("104568", "A-12-03", zero=True, when=NOW + timedelta(minutes=n)) for n in range(3)])
        await db.commit()
    assert await _tick(NOW + timedelta(minutes=6), mirror) == {CC: 3}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    by_kind = {w["kind"]: w for w in row_payload["watches"]}
    assert by_kind["product"]["status"] == "zero_picked" and by_kind["location"]["status"] == "empty_picks"
    assert by_kind["zone"]["status"] == "empty_picks" and by_kind["zone"]["value"]["empty_locations"] == ["A-12-03"]
    assert row_payload["unseen"] == 3 and all(e["is_alert"] for e in row_payload["events_today"])
    # Step 2: a count says the slot is empty: the product reads counted empty, the location a disagreeing count; the same again: no event
    async with async_session() as db:
        db.add(_count("104568", "A-12-03", "0", "13.33", when=NOW + timedelta(minutes=7)))
        await db.commit()
    assert await _tick(NOW + timedelta(minutes=12), mirror) == {CC: 3}
    assert await _tick(NOW + timedelta(minutes=18), mirror) == {CC: 0}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    by_kind = {w["kind"]: w for w in row_payload["watches"]}
    assert by_kind["product"]["status"] == "counted_empty" and by_kind["product"]["summary"].startswith("counted empty at A-12-03")
    assert by_kind["location"]["status"] == "count_variance"
    # Step 3: the next day starts quiet, silently (a count stands for 24 hours, as the stock agent reads it)
    tomorrow = NOW + timedelta(hours=25)
    assert await _tick(tomorrow, mirror) == {CC: 0}
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    assert {w["status"] for w in row_payload["watches"]} == {"no_activity", "quiet"} and row_payload["events_today"] == []


# ==================================================== 6. stop and undo, the alert, Not now, the rows

async def test_stop_undo_change_alert_not_now_and_the_rows_the_edge_reads():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add(_at_risk("D-1"))
        db.add(Advisory(id=uuid.uuid4(), customer_code=CC, agent="stock", dedup_key="count:I9:A-01", severity="high",
                        title="Item I9 at A-01 counted empty, system has 4", status="open", site_id="BRI",
                        source={"kind": "count", "item": "I9", "location": "A-01"}, opened_at=NOW, updated_at=NOW, last_seen_at=NOW))
        db.add(_lookup("item description", "I9", "ItemDescription", "Nutmeg"))
        await db.commit()
    added = await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-1"}, "Sam"), mirror=mirror, now=NOW)
    watch_id = added.blocks[0]["reference"]
    # Step 1: the alert changes with an undo back to the old one
    changed = await commands.run(_command("u-sup", "watch_update", {"watch_id": watch_id, "alert": {"rule": "tier_from", "tier": "watch"}}, "Sam"), mirror=mirror, now=NOW)
    assert changed.text == "Alert changed · D-1 · Hove Deli · Tells you if it needs a look, or is likely to leave short or late"
    assert changed.blocks[0]["undo"]["alert"] == {"rule": "tier_from", "tier": "at_risk"}
    # Step 2: stopped, then undone within the window; too late, refused
    stopped = await commands.run(_command("u-sup", "watch_stop", {"watch_id": watch_id}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=1))
    assert stopped.text == "Stopped watching · D-1 · Hove Deli" and stopped.blocks[0]["undo"]["action"] == "resume"
    assert json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)["used"] == 0
    resumed = await commands.run(_command("u-sup", "watch_update", {"watch_id": watch_id, "action": "resume", "via": "undo"}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=2))
    assert resumed.text == "Watching again · D-1 · Hove Deli"
    await commands.run(_command("u-sup", "watch_stop", {"watch_id": watch_id}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=3))
    with pytest.raises(access_commands.Refused, match="undo window has passed"):
        await commands.run(_command("u-sup", "watch_update", {"watch_id": watch_id, "action": "resume"}, "Sam"), mirror=mirror, now=NOW + timedelta(minutes=30))
    with pytest.raises(access_commands.Refused, match="not yours"):
        await commands.run(_command("u-ops", "watch_stop", {"watch_id": watch_id}, "Olu"), mirror=mirror, now=NOW)
    # Step 3: the Suggested box offers the HIGH advisory's item; Not now hides it for a day
    row_payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)
    assert row_payload["suggestion"]["kind"] == "product" and row_payload["suggestion"]["entity_id"] == "I9"
    await commands.run(_command("u-sup", "watch_not_now", {"kind": "product", "entity_id": "I9"}, "Sam"), mirror=mirror, now=NOW)
    assert json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#WATCHES")].payload_json)["suggestion"] is None
    async with async_session() as db:
        assert await store.is_not_now(db, CC, "u-sup", "product", "I9", now=NOW + timedelta(hours=23))
        assert not await store.is_not_now(db, CC, "u-sup", "product", "I9", now=NOW + timedelta(hours=25))
        # Step 4: the WATCHABLES row lists what can be watched, with the site that decides who may
        watchables = await watch_mirror.watchables_payload(db, CC, now=NOW)
        assert [d["entity_id"] for d in watchables["deliveries"]] == ["D-1"] and watchables["deliveries"][0]["site_id"] == "BRI"
        assert watchables["routes"][0] == {"entity_id": "R14", "label": "Route R14", "detail": "1 open · 31 of 36 lines picked", "site_id": "BRI", "site_ids": ["BRI"]}
        assert [m["entity_id"] for m in watchables["measures"]] == ["fill_rate:BRI", "fill_rate:LON"]
        assert watchables["products"] == [{"entity_id": "I9", "label": "Nutmeg · I9", "detail": "", "site_id": None}]
        # the search finds things by their words, within the person's sites
        record = await _record(db, "u-sup")
        found = await search.find(db, CC, record, "hove", tz=TZ)
        assert [(c.kind, c.entity_id) for c in found] == [("delivery", "D-1")]
        assert [(c.kind, c.entity_id) for c in await search.find(db, CC, record, "route 14", tz=TZ)] == [("route", "R14")]
        assert [(c.kind, c.entity_id) for c in await search.find(db, CC, record, "nutm", tz=TZ)] == [("product", "I9")]
        assert [c.entity_id for c in await search.find(db, CC, record, "fill rate", tz=TZ)] == ["fill_rate:BRI"]
    # the sweep writes the WATCHABLES row and every row with a watch or an event today; this person
    # stopped theirs and had no change, so only the customer's row is written
    sweep = InMemoryMirrorWriter()
    assert await watch_mirror.sweep_once(sweep, only=[CC], now=NOW) == {CC: 1}
    assert {k[1] for k in sweep.items} == {"WATCHABLES"}
    await commands.run(_command("u-ops", "watch_add", {"kind": "delivery", "entity_id": "D-1"}, "Olu"), mirror=mirror, now=NOW)
    assert await watch_mirror.sweep_once(sweep, only=[CC], now=NOW) == {CC: 2}  # no tracker: both rows written
    assert {k[1] for k in sweep.items} == {"WATCHABLES", "USER#u-ops#WATCHES"}


async def test_a_watch_ends_quietly_when_its_time_is_up_or_the_person_lost_the_site():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add(_at_risk("D-1"))
        await db.commit()
    await commands.run(_command("u-sup", "watch_add", {"kind": "delivery", "entity_id": "D-1", "until": "end_of_day"}, "Sam"), mirror=mirror, now=NOW)
    await commands.run(_command("u-sup", "watch_add", {"kind": "route", "entity_id": "R14"}, "Sam"), mirror=mirror, now=NOW)
    assert await _tick(NOW + timedelta(seconds=5), mirror) == {CC: 0}
    async with async_session() as db:
        await users.update_user(db, CC, "u-sup", actor=SUPER, now=NOW, change=users.UserChange(site_ids=("LON",)))
        await db.commit()
    assert await _tick(NOW + timedelta(hours=12), mirror) == {CC: 0}
    async with async_session() as db:
        reasons = sorted((w.kind, w.ended_reason) for w in (await db.execute(select(Watch).where(Watch.customer_code == CC))).scalars())
        assert reasons == [("delivery", "access"), ("route", "access")]
