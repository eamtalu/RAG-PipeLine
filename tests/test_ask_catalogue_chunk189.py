"""Chunk 189: Ask as a guided assistant (stage D, step 18, backend half).

One catalogue of topics and starters, filtered per record; a registry of actions that generates
the agent's tools (one per real action, one demo tool), builds each form, asks back for what is
missing, and turns a confirmed form into its command; the demo command that records a demo and
changes nothing; slash commands; the forecast and usage tools; "What can I ask" answered from the
catalogue without the model; the ASK_CATALOGUE row.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent, TeamsUser
from app.persistence.models.advisory import Advisory
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import NotificationGroup
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.models.tenant_site import TenantSite
from app.services.access import approvals, commands as access_commands, records, users
from app.services.access import mirror as access_mirror
from app.services.advisories import kinds, store as advisory_store
from app.services.advisories.registry import Finding
from app.services.analytics_agent import action_tools, advisory_tools, forecast_tools, usage_tools
from app.services.ask import actions, catalogue, demo, forms, slash
from app.services.ask import mirror as ask_mirror
from app.services.groups import commands as group_commands
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app import teams_consumer

CC = "test_c189"
NOW = datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        for model_ in (TeamsConversationTurn, AuditEvent, Advisory, NotificationGroup, CustomerNotificationChannel):
            await s.execute(delete(model_).where(model_.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C189", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        for oid, name, role, sites in (("u-adm", "Ada Admin", "admin", ()), ("u-ops", "Olu Ops", "ops_manager", ()),
                                       ("u-sup", "Sam Super", "supervisor", ("BRI",)),
                                       ("u-pick", "Pat Picker", "picker", ("BRI",)),
                                       ("u-exec", "Eve Exec", "executive", ())):
            await users.create_user(s, CC, oid, actor=SUPER, now=NOW,
                                    change=users.UserChange(display_name=name, role_code=role,
                                                            site_ids=sites or None, all_sites=not sites))
        await s.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question=f"ask: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by=name, payload=payload))


async def _record(oid: str) -> records.AccessRecord:
    async with async_session() as db:
        return await records.access_record_for(db, CC, oid)


# ==================================================== 1. the catalogue
def test_the_catalogue_is_honest_and_its_tools_are_named_apart():
    assert catalogue.check() == []
    names = [t["name"] for t in actions.tool_specs()]
    assert len(names) == len(set(names)) and names[-1] == actions.DEMO_TOOL
    assert all(n.startswith("propose_") for n in names[:-1])
    assert set(a.form_id for a in actions.ACTIONS) <= forms.FORM_IDS
    assert all(a.command_kind == "demo_act" for a in actions.ACTIONS if a.mode == actions.DEMO)
    # the step 8 names still stand, and the watch tools ride along
    assert {"propose_create_user", "propose_reminder", "propose_schedule", "watch_now", "demo_action"} <= set(
        t["name"] for t in action_tools.ACTION_TOOLS)
    row = ask_mirror.catalogue_payload()
    assert row["version"] == catalogue.CATALOGUE_VERSION and len(row["topics"]) == 7 and len(row["actions"]) == 9
    assert row["shortcuts"] == ["/graph", "/chart", "/table"]


async def test_the_catalogue_is_filtered_by_what_a_record_may_do():
    picker, exec_, admin = await _record("u-pick"), await _record("u-exec"), await _record("u-adm")
    assert [t.id for t in catalogue.visible_topics(picker)] == ["stock", "deliveries", "pickers", "planning", "watch"]
    seen_by_exec = {t.id: t for t in catalogue.visible_topics(exec_)}
    assert "usage" in seen_by_exec and "admin" not in seen_by_exec  # approve yes, run_actions no
    seen_by_admin = {t.id: t for t in catalogue.visible_topics(admin)}
    assert len(seen_by_admin) == 7
    assert [q.kind for q in seen_by_admin["admin"].questions].count(catalogue.DEMO) == 3
    # a supervisor runs actions (so sees the demo starters) but may not manage users without approval: kept
    sup = await _record("u-sup")
    admin_topic = {t.id: t for t in catalogue.visible_topics(sup)}["admin"]
    assert any(q.text.startswith("Create a user") for q in admin_topic.questions)
    assert not any(q.text.startswith("Approve") for q in admin_topic.questions)
    assert catalogue.visible_topics(None)[0].id == "stock" and len(catalogue.visible_topics(None)) == 5


async def test_what_can_i_ask_is_answered_from_the_catalogue_without_the_model():
    job = QuestionJob(job_id="j", tenant_id="t", customer_code=CC, conversation_id="c", sender_object_id="u-pick",
                      sender_name="Pat", question="What can I ask you about?", source="tab")
    text, direct = await teams_consumer._before_the_model(job)
    assert direct is not None and direct["model"] == "catalogue"
    assert direct["answer"].startswith("I can help with stock and locations, deliveries and vans")
    assert direct["blocks"][0]["type"] == "topics" and [t["id"] for t in direct["blocks"][0]["topics"]][-1] == "watch"
    # a data question goes to the model as before
    job2 = QuestionJob(**{**job.model_dump(), "question": "What was short today?"})
    text, direct = await teams_consumer._before_the_model(job2)
    assert direct is None and text == "What was short today?"


async def test_slash_commands_are_framed_for_the_model_or_answered_with_the_list():
    sup = await _record("u-sup")
    assert slash.resolve("/remind me at 5 check route 14", sup).text.startswith("(Command /remind-me: call propose_reminder")
    assert slash.resolve("/add-device for Fiona", sup).text.startswith("(Command /add-device: call demo_action with action_id add_device")
    assert slash.resolve("/graph fill rate this week", sup).text == "Show fill rate this week as a chart"
    picker = await _record("u-pick")
    answer = slash.resolve("/approve USR-1", picker).answer
    assert answer.startswith("I do not know /approve") and "/remind-me" in answer and "/approve" not in answer.split(":")[1]
    assert slash.resolve("/graph", sup).answer.startswith("Tell me what to draw")
    job = QuestionJob(job_id="j", tenant_id="t", customer_code=CC, conversation_id="c", sender_object_id="u-sup",
                      sender_name="Sam", question="/schedule the short products list every weekday at 4pm", source="tab")
    text, direct = await teams_consumer._before_the_model(job)
    assert direct is None and text.startswith("(Command /schedule: call propose_schedule")


# ==================================================== 2. the actions
async def test_a_real_action_proposes_its_form_asks_for_what_is_missing_and_refuses_the_wrong_role():
    async with async_session() as db:
        # a change: Sam becomes a supervisor of London, filled by name
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-adm", "BRI", "propose_change_user",
                                                            {"who": "Sam", "role": "supervisor", "site_id": "LON"}))
        assert out["shown"] == "form" and out["demo"] is False
        block = out["block"]
        assert block["form_id"] == "change_user" and block["command_kind"] == "access_set_user" and block["mode"] == "real"
        fields = {f["name"]: f for f in block["fields"]}
        assert fields["object_id"]["value"] == "u-sup" and "Sam Super · Supervisor" in fields["object_id"]["labels"]
        assert fields["site_id"]["value"] == "LON" and fields["site_id"]["labels"] == ["Brighton", "London"]
        # the thing that is missing is asked back, in words
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-adm", "BRI", "propose_reminder", {}))
        assert out["ask"] == "What should I remind you of?" and out["problems"][0].startswith("title is required")
        # a picker may not change users
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-pick", "BRI", "propose_change_user", {"who": "Sam"}))
        assert out["problems"][0].startswith("you cannot do that")
        # a supervisor's user change needs approval, and the form says so
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-sup", "BRI", "propose_change_user", {"who": "Pat"}))
        assert out["requires_approval"] is True and out["block"]["submit_label"] == "Send for approval"


async def test_a_confirmed_change_user_form_becomes_access_set_user_and_decide_becomes_access_decide():
    mirror = InMemoryMirrorWriter()
    outcome = await access_commands.run(
        _job("u-adm", "access_set_user", {"form_id": "change_user", "via": "ask",
                                          "values": {"object_id": "u-sup", "role": "ops_manager", "site_id": "LON", "status": "active"}}),
        mirror=mirror, now=NOW)
    assert outcome.text == "updated Sam Super"
    async with async_session() as db:
        sam = await users.get_user(db, CC, "u-sup")
        assert sam.role_code == "ops_manager"
    # a picker is refused outright; a waiting request is listed by the decision form and a confirmed decline applies
    with pytest.raises(access_commands.Refused):
        await access_commands.run(_job("u-pick", "access_set_user", {"form_id": "change_user", "via": "ask",
                                                                      "values": {"object_id": "u-exec", "status": "deactivated"}}),
                                  mirror=mirror, now=NOW)
    async with async_session() as db:
        row = await approvals.request(db, CC, kind="change_role", requested_by="u-sup", requested_by_name="Sam Super",
                                      requested_via="ask", title="Change Pat Picker", detail="role picker → shift_lead",
                                      payload={"object_id": "u-pick", "role": "shift_lead", "site_ids": ["BRI"]}, now=NOW)
        await db.commit()
        reference = row.reference
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-ops", "BRI", "propose_decide_request",
                                                            {"reference": reference, "decision": "decline"}))
        fields = {f["name"]: f for f in out["block"]["fields"]}
        assert fields["reference"]["value"] == reference and "Change Pat Picker · by Sam Super" in fields["reference"]["labels"][0]
        assert out["block"]["submit_label"] == "Decline" and out["block"]["command_kind"] == "access_decide"
    outcome = await access_commands.run(
        _job("u-ops", "access_decide", {"form_id": "decide_request", "via": "ask",
                                        "values": {"reference": reference, "decision": "decline", "note": "not now"}}),
        mirror=mirror, now=NOW)
    assert outcome.text.startswith(f"declined {reference}")


async def test_a_confirmed_group_form_becomes_group_set_with_its_topics_and_channel():
    async with async_session() as db:
        channel = CustomerNotificationChannel(customer_code=CC, channel_type="teams", name="stock-control", config={})
        db.add(channel)
        await db.commit()
        channel_id = str(channel.id)
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-ops", "BRI", "propose_set_group",
                                                            {"name": "Stock", "topics": ["stock", "location"],
                                                             "min_severity": "high", "channel": "stock"}))
        fields = {f["name"]: f for f in out["block"]["fields"]}
        assert fields["topic_stock"]["value"] == "true" and fields["topic_delivery"]["value"] == "false"
        assert fields["channel_id"]["value"] == channel_id and "stock-control" in fields["channel_id"]["labels"]
    values = {f["name"]: f.get("value") or "" for f in out["block"]["fields"]}
    outcome = await group_commands.run(_job("u-ops", "group_set", {"form_id": "set_group", "values": values, "via": "ask"}),
                                       mirror=InMemoryMirrorWriter(), now=NOW)
    assert outcome.text.startswith("group Stock saved")
    async with async_session() as db:
        group = (await db.execute(select(NotificationGroup).where(NotificationGroup.customer_code == CC))).scalar_one()
        assert group.topics == ["advisory:stock", "advisory:location"] and group.min_severity == "high"
        assert group.max_per_hour == 5


# ==================================================== 3. the demo
async def test_a_demo_action_shows_its_form_and_a_confirmed_one_records_a_demo_and_changes_nothing():
    async with async_session() as db:
        out = json.loads(await action_tools.run_action_tool(
            db, CC, "u-sup", "BRI", "demo_action",
            {"action_id": "add_device", "fields": {"holder": "Fiona Nachon", "login": "FNACHONLEO"}}))
        assert out["shown"] == "form" and out["demo"] is True
        block = out["block"]
        assert block["mode"] == "demo" and block["command_kind"] == "demo_act" and block["note"] == forms.DEMO_NOTE
        assert {f["name"]: f.get("value") for f in block["fields"]}["holder"] == "Fiona Nachon"
        # a demo asks for what it needs too, and an executive (no run_actions) is refused
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-sup", "BRI", "demo_action",
                                                            {"action_id": "reassign_picker", "fields": {"picker": "Dan"}}))
        assert out["ask"] == "Which route should they move to?"
        out = json.loads(await action_tools.run_action_tool(db, CC, "u-exec", "BRI", "demo_action",
                                                            {"action_id": "add_device", "fields": {"holder": "x"}}))
        assert out["problems"][0].startswith("you cannot do that")
        before = len(list((await db.execute(select(TeamsUser).where(TeamsUser.customer_code == CC))).scalars()))
    outcome = await run_command(_job("u-sup", "demo_act", {"action_id": "add_device", "form_id": "add_device",
                                                           "values": {"device_name": "HH-0421", "holder": "Fiona Nachon",
                                                                      "site_id": "BRI"}}, "Sam"))
    assert outcome.text == "Demo · nothing was changed · would add handheld HH-0421 for Fiona Nachon at BRI"
    assert outcome.blocks[0]["status"] == "demo" and outcome.blocks[0]["undo"] is None
    async with async_session() as db:
        events = list((await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC,
                                                                 AuditEvent.action == "demo.add_device"))).scalars())
        assert len(events) == 1 and events[0].decision == "demo" and events[0].detail["values"]["holder"] == "Fiona Nachon"
        after = len(list((await db.execute(select(TeamsUser).where(TeamsUser.customer_code == CC))).scalars()))
        assert after == before
    with pytest.raises(access_commands.Refused):
        await demo.run(_job("u-exec", "demo_act", {"action_id": "add_device", "values": {}}), now=NOW)
    with pytest.raises(ValueError, match="unknown demo action"):
        await demo.run(_job("u-sup", "demo_act", {"action_id": "create_user", "values": {}}), now=NOW)


# ==================================================== 4. the new data tools
async def test_the_forecast_tool_reads_the_cards_words_and_says_when_there_is_no_run():
    async with async_session() as db:
        out = json.loads(await forecast_tools.run_forecast_tool(db, CC, "forecast_ahead", {"days": 2}))
    assert out["available"] is False and out["days"] == [] and out["note"]
    block = {"available": True, "note": "n",
             "days": [{"label": "Fri 14", "kind": "today", "p50": 400.0}, {"label": "Sat 15", "kind": "ahead", "p50": 312.0,
                                                                            "p50_text": "312", "range_text": "280 – 350"},
                      {"label": "Sun 16", "kind": "ahead", "p50": 90.0, "p50_text": "90", "range_text": "70 – 110"}],
             "shifts": [{"label": "Fri 14", "quiet": False, "peak_hour": "18:00", "peak_pickers": 9, "peak_lines_text": "210 lines",
                         "picker_hours": 40}],
             "tiles": [{"id": "accuracy", "value": "8.1%", "caption": "live WAPE · 9 scored days · 1 day ahead"},
                       {"id": "week", "value": "402", "caption": "p10 – p90 350 – 460 · 2 days"}]}
    rows = forecast_tools.rows_of(block, 7)
    assert [d["day"] for d in rows["days"]] == ["Sat 15", "Sun 16"] and rows["busiest_day"] == "Sat 15"
    assert rows["shifts"][0]["peak_pickers"] == 9 and rows["accuracy"]["value"] == "8.1%" and rows["note"] == "n"


async def test_the_usage_tool_counts_people_questions_and_acknowledgements_for_those_who_may():
    async with async_session() as db:
        await users.touch_last_active(db, CC, "u-sup", now=NOW - timedelta(days=1))
        await users.touch_last_active(db, CC, "u-pick", now=NOW - timedelta(days=20))
        for i in range(3):
            db.add(TeamsConversationTurn(id=uuid.uuid4(), conversation_id="c", customer_code=CC, role="user", position=i,
                                         content=f"q{i}", created_at=NOW - timedelta(days=1, hours=i), sender_object_id="u-sup",
                                         site_id="BRI"))
        db.add(TeamsConversationTurn(id=uuid.uuid4(), conversation_id="c", customer_code=CC, role="assistant", position=9,
                                     content="a", created_at=NOW - timedelta(days=1), sender_object_id="u-sup", site_id="BRI"))
        await advisory_store.reconcile(db, CC, "location", [Finding(dedup_key="location:M02B:2026-11-14", severity="high",
                                                                     title="Location M02B turned up empty 32 times today",
                                                                     site_id="BRI", kind="empty_location",
                                                                     about=kinds.about_location("M02B"))], now=NOW)
        await db.commit()
        row_id = str((await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one().id)
    from app.services.advisories import actions as advisory_actions

    await advisory_actions.run(_job("u-sup", "advisory_act", {"advisory_id": row_id, "action": "acknowledge"}, "Sam"),
                               mirror=InMemoryMirrorWriter(), now=NOW)
    async with async_session() as db:
        out = json.loads(await usage_tools.run_usage_tool(db, CC, "u-adm", "app_usage", {"days": 7}))
        assert out["people"]["with_access"] == 5 and out["people"]["active_in_window"] == 1
        assert out["people"]["by_role"] == [{"role": "supervisor", "active": 1}]
        assert sum(d["questions"] for d in out["questions_per_day"]) == 3
        assert out["top_askers"] == [{"name": "Sam Super", "questions": 3}]
        assert out["advisories_acknowledged"] == [{"name": "Sam", "count": 1}]
        assert any(a["action"] == "advisory.acknowledge" for a in out["actions"])
        refused = json.loads(await usage_tools.run_usage_tool(db, CC, "u-sup", "app_usage", {}))
        assert "manage users or approve" in refused["problems"][0]
        # find_advisory by the location's code, then its id explains it
        found = json.loads(await advisory_tools.run_advisory_tool(db, CC, "find_advisory", {"words": "m02b"}))
        assert [m["id"] for m in found["matches"]] == [row_id] and found["note"].startswith("one match")
        assert json.loads(await advisory_tools.run_advisory_tool(db, CC, "find_advisory", {"words": "zzz"}))["matches"] == []


# ==================================================== 5. the row
async def test_the_access_mirror_writes_the_catalogue_row():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await access_mirror.mirror_customer(db, mirror, CC, now=NOW)
    env = mirror.items[(f"EYE#{CC}", "ASK_CATALOGUE")]
    payload = json.loads(env.payload_json)
    assert env.row_type == "ask_catalogue" and payload["version"] == catalogue.CATALOGUE_VERSION
    assert [t["id"] for t in payload["topics"]][:2] == ["stock", "deliveries"]
    assert {a["slash"] for a in payload["actions"]} >= {"/schedule", "/remind-me", "/create-user", "/add-device"}
