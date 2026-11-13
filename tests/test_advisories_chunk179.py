"""Chunk 179: the advisories of the eSmart Eye Teams app (stage D, step 9, backend half).

Three agents read what the pipeline already holds: the Delivery agent lifts the at-risk board,
the Stock controller reads the handhelds' counts against the system balance, the Location agent
counts the picks that found a slot empty. The runner keeps one row per finding (open while it
persists, cleared when it stops, reopened when it returns), announces HIGH ones once, and writes
the ADVISORIES row the edge reads; people acknowledge, assign, snooze or dismiss with an audit
line each, inside their own site.
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
from app.persistence.models.advisory import Advisory, AdvisoryAgent, AdvisoryAgentRun
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.tenant_site import TenantSite
from app.services.access import commands as access_commands, users
from app.services.advisories import actions, registry, runner, store
from app.services.advisories import mirror as advisories_mirror
from app.services.advisories.agents import location as location_agent
from app.services.advisories.agents import stock as stock_agent
from app.services.advisories.registry import Finding
from app.services.analytics_at_risk import RULE_VERSION
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c179"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        for model_ in (AnalyticsAtRiskDelivery, AnalyticsFact):
            await s.execute(delete(model_).where(model_.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C179", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        await users.create_user(s, CC, "u-sup", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await users.create_user(s, CC, "u-pick", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Pat Picker", role_code="picker", site_ids=("BRI",)))
        await users.create_user(s, CC, "u-lon", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Lee London", role_code="supervisor", site_ids=("LON",)))
        await s.commit()
    yield
    await _wipe()


def _at_risk(delivery: str, tier: str, *, lines_expected=84, lines_picked=60, route="R14", customer="Hove Deli",
             warehouse="BRI") -> AnalyticsAtRiskDelivery:
    return AnalyticsAtRiskDelivery(
        customer_code=CC, delivery_number=delivery, departure_date=NOW.astimezone(TZ).date(),
        departure_at=NOW + timedelta(minutes=45), route=route, customer_name=customer, warehouse=warehouse,
        tier=tier, max_tier=tier, first_flagged_at=NOW - timedelta(minutes=5), first_flagged_tier=tier,
        lines_expected=lines_expected, lines_confirmed=lines_picked, lines_picked=lines_picked, packages_created=3,
        packages_loaded=1, last_pick_at=NOW - timedelta(minutes=2), usual_ready_at=NOW + timedelta(minutes=25),
        usual_ready_source="learned", status="open", rule_version=RULE_VERSION, last_evaluated_at=NOW)


def _fact(method: str, when: datetime, *, item: str, attributes: dict, classification: str | None = None,
          warehouse: str = "BRI") -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.astimezone(TZ).date(),
        transaction_name="Quick Stock Count" if method == "ReportCount" else "JIT and Shorts Pick", method=method,
        status="success", quantity_classification=classification, warehouse=warehouse, item_number=item,
        user_name="FNACHONLEO", attributes=attributes, created_at=when)


def _command(sender: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question="advisory: act", source="tab",
                       site_id="BRI", command=Command(kind="advisory_act", by=name, payload=payload))


# ==================================================== 1. the agents read real signals
async def test_the_delivery_agent_lifts_the_at_risk_board_with_its_own_words():
    async with async_session() as db:
        db.add_all([_at_risk("D-1", "at_risk"), _at_risk("D-2", "watch", lines_expected=40, lines_picked=39, route="R2"),
                    _at_risk("D-3", "none")])
        await db.commit()
        findings = await registry.spec("delivery").run(db, CC, NOW, TZ)
    assert [(f.severity, f.site_id) for f in findings] == [("high", "BRI"), ("medium", "BRI")]
    high = findings[0]
    assert high.title == "Route R14 (Hove Deli) likely to leave 24 lines behind"
    assert high.evidence == "Picking progress · 60 of 84 lines · 1 of 3 packages loaded · van usually ready 09:25 · 25 min to go"
    assert high.impact == "24 lines not yet picked for Hove Deli"
    assert high.dedup_key == "delivery:D-1:2026-11-13" and high.source["delivery_number"] == "D-1"
    assert high.confidence is None  # nothing closed in the last fortnight yet
    assert high.due_at == NOW + timedelta(minutes=25)


async def test_the_stock_agent_flags_counts_that_disagree_with_the_system():
    async with async_session() as db:
        db.add_all([
            # an early count says 4, a later one says 2 against a balance of 4: the latest counts
            _fact("ReportCount", NOW - timedelta(hours=3), item="104568",
                  attributes={"Location": "A-01-02", "CountedQuantity": "4", "BalanceQuantity": "4"}),
            _fact("ReportCount", NOW - timedelta(hours=1), item="104568",
                  attributes={"Location": "A-01-02", "CountedQuantity": "2", "BalanceQuantity": "4"}),
            # counted empty against 13.33: HIGH
            _fact("ReportCount", NOW - timedelta(minutes=30), item="200100",
                  attributes={"Location": "B-07-01", "CountedQuantity": "0", "BalanceQuantity": "13.33"}),
            # agrees: nothing
            _fact("ReportCount", NOW - timedelta(minutes=20), item="300300",
                  attributes={"Location": "C-01-01", "CountedQuantity": "97.5", "BalanceQuantity": "97.5"}),
            # a flag value, not a count: nothing
            _fact("ReportCount", NOW - timedelta(minutes=10), item="400400",
                  attributes={"Location": "C-02-01", "OnHandBalanceToCompare": "2"}),
            # too old
            _fact("ReportCount", NOW - timedelta(hours=30), item="500500",
                  attributes={"Location": "D-01-01", "CountedQuantity": "0", "BalanceQuantity": "9"}),
        ])
        await db.commit()
        findings = await stock_agent.run(db, CC, NOW, TZ)
    by_key = {f.dedup_key: f for f in findings}
    assert set(by_key) == {"count:104568:A-01-02", "count:200100:B-07-01"}
    assert by_key["count:104568:A-01-02"].severity == "high"  # half the balance is gone
    assert by_key["count:104568:A-01-02"].title == "Item 104568 at A-01-02: counted 2, system 4"
    assert by_key["count:104568:A-01-02"].evidence == "Quick Stock Count at 08:00 · gap -2 units · 50% of the balance"
    assert by_key["count:200100:B-07-01"].title == "Item 200100 at B-07-01 counted empty, system has 13.33"
    assert by_key["count:200100:B-07-01"].recommended == "Recount B-07-01 and adjust the balance for item 200100"


async def test_the_location_agent_counts_the_picks_that_found_a_slot_empty_today():
    async with async_session() as db:
        for i in range(6):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=10 * i), item=f"10{i % 2}", classification="attempt",
                         attributes={"FromLocation": "A-03-04", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        for i in range(3):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=5 * i), item="777", classification="attempt",
                         attributes={"FromLocation": "JIT", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=1), item="888", classification="attempt",
                     attributes={"FromLocation": "B-01-01", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=1), item="999", classification="pick",
                     attributes={"FromLocation": "A-03-04", "ExpectedQuantity": "5", "QuantityPicked": "5"}))
        # yesterday's zero-picks do not count
        db.add(_fact("ConfirmPickLine", NOW - timedelta(hours=20), item="100", classification="attempt",
                     attributes={"FromLocation": "A-03-04", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        await db.commit()
        findings = await location_agent.run(db, CC, NOW, TZ)
    assert [(f.dedup_key, f.severity) for f in findings] == [
        ("location:A-03-04:2026-11-13", "high"), ("location:JIT:2026-11-13", "medium")]
    assert findings[0].title == "Location A-03-04 turned up empty 6 times today"
    assert findings[0].evidence == "6 zero-picks since midnight · items 100, 101"
    assert findings[1].title == "3 zero-picks at the JIT staging today across 1 items"


def test_the_thresholds_and_the_registry():
    assert stock_agent.variance_of("2", "4") == (-2, 0.5)
    assert stock_agent.variance_of("97.5", "97.5") is None
    assert stock_agent.variance_of("4", "4.5") is None  # under a unit
    assert stock_agent.variance_of("95", "100") is None  # under a tenth
    assert location_agent.severity_for(6) == "high" and location_agent.severity_for(2) is None
    assert [a.name for a in registry.AGENTS if a.enabled_by_default] == ["delivery", "stock", "location"]
    assert registry.spec("forecast").off_reason and registry.spec("forecast").run is None
    with pytest.raises(ValueError):
        registry.spec("weather")


# ==================================================== 2. the runner keeps one row per finding
async def test_findings_open_stay_clear_and_come_back_on_one_row():
    f1 = Finding(dedup_key="count:1:A", severity="medium", title="Item 1 at A: counted 2, system 4", site_id="BRI")
    f2 = Finding(dedup_key="count:2:B", severity="high", title="Item 2 at B counted empty", site_id="BRI")
    async with async_session() as db:
        assert await store.reconcile(db, CC, "stock", [f1, f2], now=NOW) == (2, 0)
        # the same findings again: nothing new, the words refresh, the row stays
        again = Finding(dedup_key="count:1:A", severity="high", title="Item 1 at A: counted 0, system 4", site_id="BRI")
        assert await store.reconcile(db, CC, "stock", [again, f2], now=NOW + timedelta(minutes=15)) == (0, 0)
        # one gone: cleared by the agent, with the reason
        assert await store.reconcile(db, CC, "stock", [f2], now=NOW + timedelta(minutes=30)) == (0, 1)
        rows = {r.dedup_key: r for r in (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars()}
        assert rows["count:1:A"].status == "resolved" and rows["count:1:A"].resolved_reason == "cleared"
        assert rows["count:1:A"].title == "Item 1 at A: counted 0, system 4" and rows["count:1:A"].severity == "high"
        assert rows["count:2:B"].status == "open" and rows["count:2:B"].opened_at == NOW
        # it comes back: the same row reopens rather than a second one
        assert await store.reconcile(db, CC, "stock", [f1, f2], now=NOW + timedelta(hours=1)) == (1, 0)
        rows = list((await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars())
        assert len(rows) == 2
        back = next(r for r in rows if r.dedup_key == "count:1:A")
        assert back.status == "open" and back.opened_at == NOW + timedelta(hours=1) and back.resolved_at is None
        await db.commit()


async def test_a_snooze_wakes_on_time_and_early_when_the_finding_turns_high():
    medium = Finding(dedup_key="location:A:2026-11-13", severity="medium", title="A empty 3 times", site_id="BRI")
    async with async_session() as db:
        await store.reconcile(db, CC, "location", [medium], now=NOW)
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        row.status, row.snoozed_until = "snoozed", NOW + timedelta(hours=2)
        await db.flush()
        # still medium: the snooze holds, and the row is still counted as live for the agent
        await store.reconcile(db, CC, "location", [medium], now=NOW + timedelta(minutes=30))
        assert await store.wake_snoozed(db, CC, now=NOW + timedelta(minutes=30)) == 0
        assert row.status == "snoozed"
        # turned HIGH: the snooze ends early
        high = Finding(dedup_key="location:A:2026-11-13", severity="high", title="A empty 6 times", site_id="BRI")
        await store.reconcile(db, CC, "location", [high], now=NOW + timedelta(minutes=40))
        assert row.status == "open" and row.snoozed_until is None
        # a snooze that runs out wakes on the tick
        row.status, row.snoozed_until = "snoozed", NOW + timedelta(hours=1)
        await db.flush()
        assert await store.wake_snoozed(db, CC, now=NOW + timedelta(hours=1, seconds=1)) == 1
        assert row.status == "open"
        await db.commit()


async def test_the_runner_seeds_the_switches_runs_what_is_due_announces_high_once_and_mirrors():
    announced: list = []

    async def enqueue(event):
        announced.append(event)
        return [uuid.uuid4()]

    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        db.add(_at_risk("D-1", "at_risk"))
        db.add(TeamsTenantBinding(tenant_id="t-179", customer_code=CC, enabled=True))
        await db.commit()
    # Step 1: the first tick seeds the switches from the registry and runs the three live agents
    ran = await runner.sweep_once(mirror, only=[CC], enqueue=enqueue, now=NOW)
    assert ran == {CC: 3}
    async with async_session() as db:
        switches = {r.agent: r for r in (await db.execute(
            select(AdvisoryAgent).where(AdvisoryAgent.customer_code == CC))).scalars()}
        assert set(switches) == {a.name for a in registry.AGENTS}
        assert switches["delivery"].enabled and switches["delivery"].cadence_seconds == 300
        assert switches["delivery"].next_run_at == NOW + timedelta(seconds=300)
        assert switches["delivery"].last_result == {"opened": 1, "resolved": 0, "found": 1, "error": None}
        assert not switches["forecast"].enabled and switches["forecast"].last_run_at is None
        runs = list((await db.execute(select(AdvisoryAgentRun).where(AdvisoryAgentRun.customer_code == CC))).scalars())
        assert sorted(r.agent for r in runs) == ["delivery", "location", "stock"] and all(r.error is None for r in runs)
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        assert row.notified_at == NOW
    # Step 2: the HIGH advisory went to the channel once, as an "advisory" event with the card's words
    assert len(announced) == 1
    event = announced[0]
    assert event.event_type == "advisory" and event.customer_code == CC and event.severity == "warning"
    assert event.title == "Delivery: Route R14 (Hove Deli) likely to leave 24 lines behind"
    assert event.dedup_key == f"advisory:{row.id}" and event.payload["site_id"] == "BRI"
    # Step 3: the ADVISORIES row the edge reads
    env = mirror.items[(f"EYE#{CC}", "ADVISORIES")]
    assert env.row_type == "advisories"
    payload = json.loads(env.payload_json)
    assert payload["counts"] == {"open": 1, "high": 1, "medium": 0, "low": 0, "acknowledged": 0, "snoozed": 0}
    assert payload["by_site"] == {"BRI": {"open": 1, "high": 1}}
    assert payload["live"][0]["title"] == "Route R14 (Hove Deli) likely to leave 24 lines behind"
    assert payload["live"][0]["actions"] == ["ask_why", "acknowledge", "assign", "snooze"]
    roster = {a["name"]: a for a in payload["roster"]}
    assert roster["delivery"]["enabled"] and roster["delivery"]["last_run_at"] == NOW.isoformat()
    assert roster["shortage"]["enabled"] is False and roster["shortage"]["available"] is False
    assert roster["shortage"]["off_reason"] == "needs supplier purchase-order due dates; not in the data"
    # Step 4: a second tick a minute later runs nothing (not due) and announces nothing again
    assert await runner.sweep_once(mirror, only=[CC], enqueue=enqueue, now=NOW + timedelta(minutes=1)) == {CC: 0}
    assert len(announced) == 1


async def test_an_agent_that_raises_lands_on_its_ledger_row_and_the_others_still_run(monkeypatch):
    async def broken(db, cc, now, tz):
        raise RuntimeError("the facts table is on fire")

    monkeypatch.setattr(stock_agent, "run", broken)
    async with async_session() as db:
        runs = await runner.run_due(db, CC, now=NOW)
        await db.commit()
    assert sorted((r.agent, r.error is None) for r in runs) == [("delivery", True), ("location", True), ("stock", False)]
    assert "RuntimeError: the facts table is on fire" in next(r for r in runs if r.agent == "stock").error


# ==================================================== 3. people act, inside their site
async def test_acknowledge_assign_snooze_and_dismiss_are_checked_and_audited():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await store.reconcile(db, CC, "delivery", [Finding(
            dedup_key="delivery:D-1:2026-11-13", severity="high", title="Route R14 likely to leave 24 lines behind",
            site_id="BRI")], now=NOW)
        await db.commit()
        advisory_id = str((await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one().id)
    # a supervisor of another site cannot touch it; a picker of the site can acknowledge but not dismiss
    with pytest.raises(access_commands.Refused, match="outside this user's sites"):
        await actions.run(_command("u-lon", {"advisory_id": advisory_id, "action": "acknowledge"}), mirror=mirror, now=NOW)
    with pytest.raises(access_commands.Refused, match="not allowed for Picker"):
        await actions.run(_command("u-pick", {"advisory_id": advisory_id, "action": "dismiss"}), mirror=mirror, now=NOW)
    outcome = await actions.run(_command("u-pick", {"advisory_id": advisory_id, "action": "acknowledge"}, "Pat"),
                                mirror=mirror, now=NOW)
    assert outcome.text == "acknowledged · Route R14 likely to leave 24 lines behind"
    assert outcome.blocks[0]["type"] == "receipt" and outcome.blocks[0]["reference"] == advisory_id
    # the supervisor assigns it to Pat, snoozes it, then dismisses it
    outcome = await actions.run(_command("u-sup", {"advisory_id": advisory_id, "action": "assign",
                                                   "assignee": {"object_id": "u-pick", "name": "Pat Picker"}}, "Sam"),
                                mirror=mirror, now=NOW + timedelta(minutes=1))
    assert outcome.text == "assigned to Pat Picker · Route R14 likely to leave 24 lines behind"
    outcome = await actions.run(_command("u-sup", {"advisory_id": advisory_id, "action": "snooze", "minutes": 30}, "Sam"),
                                mirror=mirror, now=NOW + timedelta(minutes=2))
    assert outcome.text == "snoozed until 09:32 UTC · Route R14 likely to leave 24 lines behind"
    outcome = await actions.run(_command("u-sup", {"advisory_id": advisory_id, "action": "dismiss"}, "Sam"),
                                mirror=mirror, now=NOW + timedelta(minutes=3))
    assert outcome.text == "dismissed · Route R14 likely to leave 24 lines behind"
    async with async_session() as db:
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        assert row.status == "dismissed" and row.assignee_name == "Pat Picker" and row.acknowledged_by == "u-pick"
        assert row.resolved_reason == "u-sup" and row.snoozed_until is None
        events = list((await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC)
                                        .order_by(AuditEvent.at))).scalars())
        assert [e.action for e in events if e.action.startswith("advisory.")] == [
            "advisory.acknowledge", "advisory.assign", "advisory.snooze", "advisory.dismiss"]
        assert all(e.subject_id == advisory_id and e.site_id == "BRI" for e in events if e.action.startswith("advisory."))
    # the mirror carries the dismissed row under recent, and the counts are empty
    payload = json.loads(mirror.items[(f"EYE#{CC}", "ADVISORIES")].payload_json)
    assert payload["counts"]["open"] == 0 and payload["live"] == []
    assert payload["recent"][0]["status"] == "dismissed"
    # a missing advisory and a bad action are named
    with pytest.raises(access_commands.Refused, match="no longer exists"):
        await actions.run(_command("u-sup", {"advisory_id": str(uuid.uuid4()), "action": "acknowledge"}), mirror=mirror)
    with pytest.raises(ValueError, match="unknown advisory action"):
        await actions.run(_command("u-sup", {"advisory_id": advisory_id, "action": "shout"}), mirror=mirror)


async def test_the_queue_routes_advisory_act_to_the_advisories_service(monkeypatch):
    seen = []

    async def fake_run(job):
        seen.append(job.command.kind)
        return "ok"

    monkeypatch.setattr(actions, "run", fake_run)
    assert await run_command(_command("u-sup", {"advisory_id": "x", "action": "acknowledge"})) == "ok"
    assert seen == ["advisory_act"]


def test_the_snooze_end_is_bounded():
    assert actions._until(None, None, NOW) == NOW + timedelta(hours=1)
    assert actions._until("2026-11-13T12:00:00Z", None, NOW) == NOW + timedelta(hours=3)
    assert actions._until(None, 60 * 24 * 30, NOW) == NOW + timedelta(days=7)
    assert actions._until("2026-11-13T08:00:00Z", None, NOW) == NOW + timedelta(minutes=1)
    with pytest.raises(ValueError, match="ISO"):
        actions._until("tomorrow", None, NOW)


async def test_the_payload_shape_is_stable_for_the_edge():
    async with async_session() as db:
        payload = await advisories_mirror.advisories_payload(db, CC, now=NOW)
    assert set(payload) == {"as_of", "counts", "by_site", "live", "recent", "roster"}
    assert payload["as_of"] == NOW.isoformat() and payload["live"] == [] and len(payload["roster"]) == 7


async def test_the_executive_home_counts_the_open_advisories():
    from app.services.teams import exec_home

    async with async_session() as db:
        await store.reconcile(db, CC, "delivery", [
            Finding(dedup_key="d:1", severity="high", title="Route R14 likely to leave 24 lines behind", site_id="BRI"),
            Finding(dedup_key="d:2", severity="medium", title="Route R2 behind its usual rhythm", site_id="LON")], now=NOW)
        summary = await exec_home.advisories_summary(db, CC)
        await db.commit()
    assert summary["open"] == 2 and summary["high"] == 1 and summary["medium"] == 1
    assert summary["by_site"] == {"BRI": {"open": 1, "high": 1}, "LON": {"open": 1, "high": 0}}
    assert summary["top"][0]["title"] == "Route R14 likely to leave 24 lines behind"
