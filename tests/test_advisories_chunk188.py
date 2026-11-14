"""Chunk 188: the Advisories tab becomes a grouped list (stage D, step 17, backend half).

Every finding says what kind of thing it is and what it is about, in words the agents write
(app/services/advisories/kinds.py), so the edge groups the feed without parsing titles; count
findings name the item from the description lookup where it is known; the ADVISORIES row lists
the kinds with their words and counts; one `advisory_act` may act on up to fifty advisories at
once, each checked and audited on its own.
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
from app.services.access import commands as access_commands, users
from app.services.advisories import actions, kinds, registry, store
from app.services.advisories import mirror as advisories_mirror
from app.services.advisories.agents import location as location_agent
from app.services.advisories.agents import stock as stock_agent
from app.services.advisories.registry import Finding
from app.services.analytics_at_risk import RULE_VERSION
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c188"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        for model_ in (AnalyticsAtRiskDelivery, AnalyticsFact, AnalyticsLookupValue):
            await s.execute(delete(model_).where(model_.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C188", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        await users.create_user(s, CC, "u-sup", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await users.create_user(s, CC, "u-pick", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Pat Picker", role_code="picker", site_ids=("BRI",)))
        await s.commit()
    yield
    await _wipe()


def _fact(method: str, when: datetime, *, item: str, attributes: dict, classification: str | None = None) -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.astimezone(TZ).date(),
        transaction_name="Quick Stock Count" if method == "ReportCount" else "JIT and Shorts Pick", method=method,
        status="success", quantity_classification=classification, warehouse="BRI", item_number=item,
        user_name="FNACHONLEO", attributes=attributes, created_at=when)


def _command(sender: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question="advisory: act", source="tab",
                       site_id="BRI", command=Command(kind="advisory_act", by=name, payload=payload))


def _finding(key: str, site: str = "BRI", kind: str = "count_gap", severity: str = "high") -> Finding:
    return Finding(dedup_key=key, severity=severity, title=f"Finding {key}", site_id=site, kind=kind,
                   about=kinds.about_item(key.split(":")[-1], where="M02C"))


# ==================================================== 1. the agents say what a finding is and is about
async def test_the_stock_agent_names_the_item_in_words_where_the_lookup_knows_it():
    async with async_session() as db:
        db.add_all([
            _fact("ReportCount", NOW - timedelta(hours=1), item="104568",
                  attributes={"Location": "A-01-02", "CountedQuantity": "2", "BalanceQuantity": "4"}),
            _fact("ReportCount", NOW - timedelta(minutes=30), item="200100",
                  attributes={"Location": "B-07-01", "CountedQuantity": "0", "BalanceQuantity": "13.33"}),
            AnalyticsLookupValue(customer_code=CC, lookup="item description", key="104568", attribute="ItemDescription",
                                 value="Ginger", valid_from=NOW - timedelta(days=30)),
        ])
        await db.commit()
        findings = {f.dedup_key: f for f in await stock_agent.run(db, CC, NOW, TZ)}
    named, numbered = findings["count:104568:A-01-02"], findings["count:200100:B-07-01"]
    assert named.title == "Ginger (104568) at A-01-02: counted 2, system 4"
    assert named.kind == "count_gap"
    assert named.about == {"kind": "item", "id": "104568", "label": "Ginger (104568)", "where": "A-01-02", "area": "A"}
    # an item the lookup does not know keeps its number, and the words of chunk 179
    assert numbered.title == "Item 200100 at B-07-01 counted empty, system has 13.33"
    assert numbered.about["label"] == "Item 200100" and numbered.about["area"] == "B"


async def test_the_location_and_delivery_agents_name_their_kind_and_subject():
    async with async_session() as db:
        for i in range(6):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=10 * i), item=f"10{i % 2}", classification="attempt",
                         attributes={"FromLocation": "M02C", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        for i in range(3):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=5 * i), item="777", classification="attempt",
                         attributes={"FromLocation": "JIT", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        db.add(AnalyticsAtRiskDelivery(
            customer_code=CC, delivery_number="D-1", departure_date=NOW.astimezone(TZ).date(),
            departure_at=NOW + timedelta(minutes=45), route="R14", customer_name="Hove Deli", warehouse="BRI",
            tier="at_risk", max_tier="at_risk", first_flagged_at=NOW - timedelta(minutes=5), first_flagged_tier="at_risk",
            lines_expected=84, lines_confirmed=60, lines_picked=60, packages_created=3, packages_loaded=1,
            last_pick_at=NOW - timedelta(minutes=2), usual_ready_at=NOW + timedelta(minutes=25),
            usual_ready_source="learned", status="open", rule_version=RULE_VERSION, last_evaluated_at=NOW))
        await db.commit()
        places = {f.dedup_key: f for f in await location_agent.run(db, CC, NOW, TZ)}
        deliveries = await registry.spec("delivery").run(db, CC, NOW, TZ)
    assert places["location:M02C:2026-11-14"].kind == "empty_location"
    assert places["location:M02C:2026-11-14"].about == {"kind": "location", "id": "M02C", "label": "Location M02C", "where": None, "area": "M"}
    assert places["location:JIT:2026-11-14"].about["label"] == "JIT staging"
    assert deliveries[0].kind == "delivery_at_risk"
    assert deliveries[0].about == {"kind": "delivery", "id": "D-1", "label": "Route R14", "where": "Hove Deli", "area": ""}


def test_the_words_of_each_kind_and_the_backfill_mapping():
    assert [k.kind for k in kinds.KINDS] == ["count_gap", "chronic_short", "empty_location", "delivery_at_risk", "other"]
    assert kinds.spec_of("empty_location").meaning == "Pickers found this location empty more than once today."
    assert kinds.kind_from_source({"kind": "count"}) == "count_gap"
    assert kinds.kind_from_source({"kind": "item"}) == "chronic_short"
    assert kinds.kind_from_source({"kind": "location"}) == "empty_location"
    assert kinds.kind_from_source({"kind": "delivery"}) == "delivery_at_risk"
    assert kinds.kind_from_source(None) == "other"
    assert kinds.about_from_source({"kind": "count", "item": "104697", "location": "M03B"}) == {
        "kind": "item", "id": "104697", "label": "Item 104697", "where": "M03B", "area": "M"}
    assert kinds.about_from_source({"kind": "location", "location": "JIT"})["label"] == "JIT staging"
    assert kinds.about_from_source({"kind": "delivery", "delivery_number": "D-9"})["label"] == "D-9"


# ==================================================== 2. the row the edge reads lists the kinds
async def test_the_row_lists_the_kinds_with_their_words_and_counts():
    async with async_session() as db:
        await store.reconcile(db, CC, "stock", [
            _finding("count:1"), _finding("count:2", severity="medium"), _finding("chronic:3", kind="chronic_short", severity="medium")],
            now=NOW)
        await store.reconcile(db, CC, "location", [_finding("location:4", kind="empty_location")], now=NOW)
        await db.commit()
        # one acknowledged: still live, not open
        row = await db.scalar(select(Advisory).where(Advisory.customer_code == CC, Advisory.dedup_key == "location:4"))
        row.status = "acknowledged"
        await db.commit()
        payload = await advisories_mirror.advisories_payload(db, CC, now=NOW)
    assert payload["kinds"] == [
        {"kind": "count_gap", "label": "Stock count gaps",
         "meaning": "A count found a different amount from what the system expects.", "open": 2, "high": 1},
        {"kind": "chronic_short", "label": "Items short this week",
         "meaning": "The same item was short on several releases this week.", "open": 1, "high": 0},
        {"kind": "empty_location", "label": "Empty locations",
         "meaning": "Pickers found this location empty more than once today.", "open": 0, "high": 0},
    ]
    live = {a["title"]: a for a in payload["live"]}
    assert live["Finding count:1"]["kind"] == "count_gap"
    assert live["Finding count:1"]["about"] == {"kind": "item", "id": "1", "label": "Item 1", "where": "M02C", "area": "M"}


# ==================================================== 3. one command, many advisories
async def test_a_batch_acts_on_each_advisory_within_reach_and_counts_the_rest():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await store.reconcile(db, CC, "stock", [_finding("count:1"), _finding("count:2"), _finding("count:3", site="LON")], now=NOW)
        await db.commit()
        ids = {r.dedup_key: str(r.id) for r in (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars()}
    outcome = await actions.run(_command("u-sup", {"advisory_ids": [ids["count:1"], ids["count:2"], ids["count:3"], str(uuid.uuid4())],
                                                   "action": "acknowledge"}, "Sam"), mirror=mirror, now=NOW)
    assert outcome.text == "acknowledged 2 · 2 not yours to change"
    assert outcome.blocks[0]["type"] == "receipt" and outcome.blocks[0]["reference"] == "2 advisories"
    async with async_session() as db:
        rows = {r.dedup_key: r for r in (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars()}
        assert rows["count:1"].status == rows["count:2"].status == "acknowledged" and rows["count:3"].status == "open"
        events = list((await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC, AuditEvent.action == "advisory.acknowledge"))).scalars())
        assert sorted(e.subject_id for e in events) == sorted([ids["count:1"], ids["count:2"]])
    payload = json.loads(mirror.items[(f"EYE#{CC}", "ADVISORIES")].payload_json)
    assert payload["counts"] == {"open": 1, "high": 1, "medium": 0, "low": 0, "acknowledged": 2, "snoozed": 0}
    # nothing within reach is a refusal with the first reason; too many at once is a bad command
    with pytest.raises(access_commands.Refused, match="not allowed for Picker"):
        await actions.run(_command("u-pick", {"advisory_ids": [ids["count:1"], ids["count:2"]], "action": "dismiss"}), mirror=mirror, now=NOW)
    with pytest.raises(ValueError, match="at most 50"):
        await actions.run(_command("u-sup", {"advisory_ids": [str(uuid.uuid4()) for _ in range(51)], "action": "acknowledge"}), mirror=mirror, now=NOW)
    # a single id keeps the step 9 answer, and a batch of one reads the same
    outcome = await actions.run(_command("u-sup", {"advisory_ids": [ids["count:1"]], "action": "snooze", "minutes": 30}, "Sam"),
                                mirror=mirror, now=NOW)
    assert outcome.text == "snoozed until 09:30 UTC · Finding count:1" and outcome.blocks[0]["reference"] == ids["count:1"]
