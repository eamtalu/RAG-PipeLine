"""Chunk 180: the reminders and schedules of the eSmart Eye Teams app (stage D, step 10, backend).

The schedule is data: a spec and a next firing on the row, so a restart loses nothing; the
consumer's tick claims what is due, writes the ledger row first, skips what became pointless,
re-runs the attached question as the owner, delivers, and books the next firing; a task missed
by more than the catch-up window is noted, not fired. People make, pause, run, finish and delete
their own tasks through the queue with an audit line each; the assistant proposes the two forms;
the person's USER#<id>#TASKS row lists it all with the suggestions from their own history.
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
from app.persistence.models.customer import Customer
from app.persistence.models.task import TeamsTask, TeamsTaskRun
from app.persistence.models.teams_conversation import TeamsConversation
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.models.tenant_site import TenantSite
from app.persistence.repositories.teams_repository import ExchangeDetails, TeamsConversationRepository
from app.services.access import commands as access_commands, users
from app.services.advisories import store as advisories_store
from app.services.advisories.registry import Finding
from app.services.analytics_agent import action_tools
from app.services.tasks import commands, forms, runner, specs, store
from app.services.tasks import mirror as tasks_mirror
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c180"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)  # a Friday
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        for model_ in (TeamsConversationTurn, TeamsConversation):
            await s.execute(delete(model_).where(model_.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C180", timezone="Europe/London"))
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


def _command(sender: str, kind: str, payload: dict, name: str = "Sam") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question=f"task: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by=name, payload=payload))


async def _reminder(db, **over) -> TeamsTask:
    fields = dict(owner_object_id="u-sup", owner_name="Sam Super", kind="reminder", title="Check the 9 short lines",
                  when={"kind": "once", "at": (NOW + timedelta(hours=6, minutes=30)).isoformat()}, tz=TZ, now=NOW, site_id="BRI")
    fields.update(over)
    return await store.create(db, CC, **fields)


# ==================================================== 1. the schedule is data
async def test_a_task_is_booked_claimed_fired_and_rebooked_from_the_planned_time():
    async with async_session() as db:
        weekdays = await _reminder(db, kind="schedule", title="Short products list", query_text="short products today",
                                   when={"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"})
        once = await _reminder(db)
        later = await _reminder(db, title="Plan cover", when={"kind": "once", "at": (NOW + timedelta(days=2)).isoformat()})
        await db.commit()
        assert weekdays.next_fire_at == datetime(2026, 11, 13, 16, 0, tzinfo=timezone.utc)
        assert once.next_fire_at == NOW + timedelta(hours=6, minutes=30)
        # nothing is due yet; at 16:05 the schedule is, the reminder at 15:30 too, the Sunday one not
        assert await store.claim_due(db, now=NOW) == []
        due = await store.claim_due(db, now=datetime(2026, 11, 13, 16, 5, tzinfo=timezone.utc))
        assert [t.title for t in due] == ["Check the 9 short lines", "Short products list"]
        await db.rollback()

    answered = []

    async def run_query(task):
        answered.append(task.query_text)
        return {"answer": "GINGER was short on 3 releases.", "blocks": [{"type": "table"}]}

    mirror = InMemoryMirrorWriter()
    at = datetime(2026, 11, 13, 16, 5, tzinfo=timezone.utc)
    fired = await runner.tick(mirror, run_query=run_query, only=[CC], now=at)
    assert fired == {CC: 2} and answered == ["short products today"]
    async with async_session() as db:
        rows = {t.title: t for t in (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalars()}
        # the schedule rebooked from its planned time to Monday 16:00; the once reminder is done
        assert rows["Short products list"].next_fire_at == datetime(2026, 11, 16, 16, 0, tzinfo=timezone.utc)
        assert rows["Short products list"].fire_count == 1 and rows["Short products list"].last_result["text"].startswith("GINGER")
        assert rows["Check the 9 short lines"].status == "done" and rows["Check the 9 short lines"].next_fire_at is None
        assert rows["Plan cover"].status == "active"
        runs = list((await db.execute(select(TeamsTaskRun).where(TeamsTaskRun.customer_code == CC))).scalars())
        assert sorted(r.outcome for r in runs) == ["sent", "sent"]
        assert {r.planned_at for r in runs} == {datetime(2026, 11, 13, 16, 0, tzinfo=timezone.utc), NOW + timedelta(hours=6, minutes=30)}
    # the owner's row was rewritten with the firing
    payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#TASKS")].payload_json)
    assert payload["counts"] == {"todo": 1, "done_today": 1, "running": 1}
    assert payload["coming_up"][0]["title"] == "Plan cover"
    # the same tick again fires nothing: the ledger and the rebooking hold
    assert await runner.tick(mirror, run_query=run_query, only=[CC], now=at) == {}


async def test_a_task_missed_by_more_than_the_window_is_noted_not_fired_and_a_pointless_one_is_skipped():
    async with async_session() as db:
        await advisories_store.reconcile(db, CC, "delivery", [Finding(dedup_key="d:1", severity="high", title="Route R14", site_id="BRI")], now=NOW)
        advisory = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        await _reminder(db, title="Move a picker to Route 14", skip_if={"kind": "advisory", "advisory_id": str(advisory.id)},
                        when={"kind": "once", "at": (NOW + timedelta(minutes=30)).isoformat()})
        await _reminder(db, title="Friday's list", kind="schedule", query_text="x",
                        when={"kind": "recurring", "weekdays": [4], "time": "10:30"})
        advisory.status = "resolved"
        await db.commit()
    calls = []

    async def run_query(task):
        calls.append(task.title)
        return {"answer": "…", "blocks": []}

    # Monday 09:00: the reminder (Friday 09:30) and Friday's list (10:30) are both long past
    monday = datetime(2026, 11, 16, 9, 0, tzinfo=timezone.utc)
    assert await runner.tick(None, run_query=run_query, only=[CC], now=monday) == {CC: 2}
    assert calls == []
    async with async_session() as db:
        runs = {r.task_id: r for r in (await db.execute(select(TeamsTaskRun).where(TeamsTaskRun.customer_code == CC))).scalars()}
        rows = {t.title: t for t in (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalars()}
        assert runs[rows["Move a picker to Route 14"].id].outcome == "missed"
        assert runs[rows["Friday's list"].id].outcome == "missed"
        # Friday's list is rebooked to next Friday from its planned time, not fired for Monday
        assert rows["Friday's list"].next_fire_at == datetime(2026, 11, 20, 10, 30, tzinfo=timezone.utc)
        # a reminder due now whose advisory is resolved is skipped with the reason
        await _reminder(db, title="Chase the substitute", skip_if={"kind": "advisory", "advisory_id": str(advisory.id)},
                        when={"kind": "once", "at": (monday + timedelta(minutes=1)).isoformat()})
        await db.commit()
    assert await runner.tick(None, run_query=run_query, only=[CC], now=monday + timedelta(minutes=2)) == {CC: 1}
    async with async_session() as db:
        row = (await db.execute(select(TeamsTask).where(TeamsTask.title == "Chase the substitute"))).scalar_one()
        assert row.status == "done" and row.last_result["outcome"] == "skipped"
        assert row.last_result["text"] == "skipped: the advisory is resolved"


async def test_a_condition_schedule_fires_when_the_figure_crosses_and_then_waits_its_cooldown():
    async with async_session() as db:
        await _reminder(db, kind="schedule", title="Fill rate warning",
                        when={"kind": "condition", "metric": "fill_rate", "op": "<", "value": 0.97, "cooldown_minutes": 60})
        await db.commit()
    figures = {"BRI": 0.99}
    sent = []

    async def read_fill_rate(cc, site_id):
        return figures.get(site_id)

    async def run_query(task):
        return {"answer": "fill rate 95.0%", "blocks": []}

    async def enqueue(event):
        sent.append(event)
        return [uuid.uuid4()]

    assert await runner.tick(None, run_query=run_query, enqueue=enqueue, read_fill_rate=read_fill_rate, only=[CC], now=NOW) == {}
    figures["BRI"] = 0.95
    assert await runner.tick(None, run_query=run_query, enqueue=enqueue, read_fill_rate=read_fill_rate, only=[CC], now=NOW + timedelta(minutes=1)) == {CC: 1}
    # still below, inside the cooldown: nothing; after it: again
    assert await runner.tick(None, run_query=run_query, enqueue=enqueue, read_fill_rate=read_fill_rate, only=[CC], now=NOW + timedelta(minutes=30)) == {}
    assert await runner.tick(None, run_query=run_query, enqueue=enqueue, read_fill_rate=read_fill_rate, only=[CC], now=NOW + timedelta(minutes=62)) == {CC: 1}
    assert sent == []  # aimed at "me": nothing goes to the channel
    async with async_session() as db:
        row = (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalar_one()
        assert row.status == "active" and row.fire_count == 2 and row.next_fire_at is None


async def test_a_schedule_aimed_at_the_channel_goes_through_the_outbox():
    async with async_session() as db:
        await _reminder(db, kind="schedule", title="Weekly deliveries graph", query_text="deliveries per day this week",
                        target="channel", show_as="chart", when={"kind": "recurring", "weekdays": [0], "time": "08:00"})
        await db.commit()
    sent = []

    async def enqueue(event):
        sent.append(event)
        return [uuid.uuid4()]

    async def run_query(task):
        return {"answer": "265 departures this week.", "blocks": [{"type": "chart", "title": "Deliveries"}]}

    monday = datetime(2026, 11, 16, 8, 0, 30, tzinfo=timezone.utc)
    assert await runner.tick(None, run_query=run_query, enqueue=enqueue, only=[CC], now=monday) == {CC: 1}
    assert len(sent) == 1 and sent[0].event_type == "schedule" and sent[0].title == "Weekly deliveries graph"
    assert sent[0].summary == "265 departures this week." and sent[0].payload["show_as"] == "chart"


# ==================================================== 2. people make and change their own tasks
async def test_the_commands_make_pause_run_finish_and_delete_with_an_audit_line_each():
    mirror = InMemoryMirrorWriter()
    outcome = await commands.run(_command("u-sup", "task_create", {
        "kind": "reminder", "title": "Check the 9 short lines", "when": {"kind": "once", "at": "2026-11-13T17:45:00+00:00"},
        "query_text": "which short lines were re-picked", "via": "ask"}), mirror=mirror, now=NOW)
    assert outcome.text == "reminder set · Fri 13 Nov, 17:45 · Check the 9 short lines"
    receipt = outcome.blocks[0]
    task_id = receipt["reference"]
    assert receipt["undo"] == {"task_id": task_id, "action": "delete"} and receipt["undo_until"].startswith("2026-11-13T09:10")
    outcome = await commands.run(_command("u-sup", "task_create", {
        "kind": "schedule", "title": "Short products list", "query_text": "short products today",
        "when": {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"}, "target": "me"}), mirror=mirror, now=NOW)
    assert outcome.text == "scheduled · Every weekday at 16:00 · Short products list"
    schedule_id = outcome.blocks[0]["reference"]
    # a picker may not schedule questions (ask needs approval); another person may not touch Sam's
    with pytest.raises(access_commands.Refused, match="needs approval"):
        await commands.run(_command("u-pick", "task_create", {"kind": "schedule", "title": "x", "query_text": "x",
                                                                "when": {"kind": "recurring", "weekdays": [0], "time": "08:00"}}), mirror=mirror, now=NOW)
    with pytest.raises(access_commands.Refused, match="not yours"):
        await commands.run(_command("u-pick", "task_update", {"task_id": schedule_id, "action": "pause"}), mirror=mirror, now=NOW)
    # pause, run now, done, delete
    assert (await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "pause"}), mirror=mirror, now=NOW)).text == "paused · Short products list"
    with pytest.raises(access_commands.Refused, match="resume"):
        await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "run_now"}), mirror=mirror, now=NOW)
    assert (await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "resume"}), mirror=mirror, now=NOW)).text == "resumed · Short products list"
    assert (await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "run_now"}), mirror=mirror, now=NOW)).text == "running now · Short products list"
    assert (await commands.run(_command("u-sup", "task_update", {"task_id": task_id, "action": "done"}), mirror=mirror, now=NOW)).text == "done · Check the 9 short lines"
    with pytest.raises(access_commands.Refused, match="window has passed"):
        await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "delete", "via": "undo"}), mirror=mirror, now=NOW + timedelta(minutes=11))
    assert (await commands.run(_command("u-sup", "task_update", {"task_id": schedule_id, "action": "delete", "via": "undo"}), mirror=mirror, now=NOW + timedelta(minutes=5))).text == "deleted · Short products list"
    async with async_session() as db:
        rows = {t.title: t for t in (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalars()}
        assert rows["Short products list"].status == "stopped" and rows["Check the 9 short lines"].status == "done"
        actions = [e.action for e in (await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC).order_by(AuditEvent.at, AuditEvent.id))).scalars()
                   if e.action.startswith("task.")]
        assert sorted(actions) == sorted(["task.created.reminder", "task.created.schedule", "task.pause", "task.resume", "task.run_now", "task.done", "task.delete"])
    payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#TASKS")].payload_json)
    assert payload["counts"] == {"todo": 0, "done_today": 1, "running": 0}
    assert [r["status"] for r in payload["schedules"]] == ["stopped"]


async def test_a_confirmed_ask_form_makes_the_task_through_the_same_command():
    mirror = InMemoryMirrorWriter()
    outcome = await commands.run(_command("u-sup", "task_create", {
        "form_id": "new_schedule",
        "values": {"what": "short products list", "how_often": "Every weekday (Mon – Fri)", "time": "16:00",
                   "send_to": "The customer's channel", "show_as": "A graph", "until": "2026-12-31"}}), mirror=mirror, now=NOW)
    assert outcome.text == "scheduled · Every weekday at 16:00 · short products list"
    async with async_session() as db:
        row = (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalar_one()
        assert row.target == "channel" and row.show_as == "chart" and row.created_via == "ask"
        assert row.until == datetime(2026, 12, 31, 23, 59, tzinfo=TZ) and row.query_text == "short products list"
    # the edge routes task commands here; an edit from the tab's dialog arrives as form values
    assert (await run_command(_command("u-sup", "task_update", {"task_id": str(row.id), "action": "pause"}))).text == "paused · short products list"
    outcome = await commands.run(_command("u-sup", "task_update", {"task_id": str(row.id), "action": "edit", "fields": {
        "form_id": "new_schedule", "values": {"what": "short products list", "how_often": "Once a week", "weekday": "Monday",
                                              "time": "08:30", "send_to": "Me, in the app", "show_as": "A table"}}}), mirror=mirror, now=NOW)
    assert outcome.text == "changed · short products list"
    async with async_session() as db:
        row = (await db.execute(select(TeamsTask).where(TeamsTask.customer_code == CC))).scalar_one()
        assert row.when == {"kind": "recurring", "weekdays": [0], "time": "08:30"} and row.target == "me" and row.until is None
        assert row.status == "paused" and row.next_fire_at is None


async def test_a_bad_spec_is_refused_with_the_reason():
    with pytest.raises(access_commands.Refused, match="already passed"):
        await commands.run(_command("u-sup", "task_create", {"kind": "reminder", "title": "x",
                                                               "when": {"kind": "once", "at": "2026-11-13T08:00:00+00:00"}}), mirror=InMemoryMirrorWriter(), now=NOW)
    with pytest.raises(access_commands.Refused, match="needs the question"):
        await commands.run(_command("u-sup", "task_create", {"kind": "schedule", "title": "x",
                                                               "when": {"kind": "recurring", "weekdays": [0], "time": "08:00"}}), mirror=InMemoryMirrorWriter(), now=NOW)
    with pytest.raises(ValueError, match="when.kind"):
        specs.validate({"kind": "weekly"})


# ==================================================== 3. the forms, the tools, the suggestions
async def test_the_assistant_proposes_the_two_forms_from_what_was_said():
    async with async_session() as db:
        raw = await action_tools.run_action_tool(db, CC, "u-sup", "BRI", "propose_schedule",
                                                 {"what": "the short products list", "how_often": "weekdays", "time": "4pm"})
        body = json.loads(raw)
        assert body["shown"] == "form" and body["block"]["form_id"] == "new_schedule"
        fields = {f["name"]: f["value"] for f in body["block"]["fields"]}
        assert fields["how_often"] == "Every weekday (Mon – Fri)" and fields["time"] == "16:00" and fields["send_to"] == "Me, in the app"
        assert "Brighton" in body["block"]["note"]
        raw = await action_tools.run_action_tool(db, CC, "u-sup", "BRI", "propose_reminder",
                                                 {"title": "check the 9 short lines were re-picked", "time": "17:45"})
        body = json.loads(raw)
        fields = {f["name"]: f["value"] for f in body["block"]["fields"]}
        assert body["block"]["form_id"] == "new_reminder" and fields["time"] == "17:45" and fields["repeat"] == "Just this once"
        assert json.loads(await action_tools.run_action_tool(db, CC, "u-sup", "BRI", "propose_reminder", {}))["problems"]
    # the pure helpers
    assert forms.clock("4pm", "08:00") == "16:00" and forms.clock("quarter to six", "08:00") == "08:00"
    reminder = forms.reminder_from_form({"title": "x", "date": "2026-11-13", "time": "17:45", "repeat": "Every weekday at this time"}, tz=TZ)
    assert reminder["when"] == {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "17:45"}
    schedule = forms.schedule_from_form({"what": "x", "how_often": "When fill rate drops below…", "threshold": "97"}, tz=TZ)
    assert schedule["when"] == {"kind": "condition", "metric": "fill_rate", "op": "<", "value": 0.97}
    assert specs.when_text(schedule["when"], TZ) == "Whenever fill rate drops below 97%"


async def test_the_suggestions_come_from_the_persons_own_habit():
    async with async_session() as db:
        repo = TeamsConversationRepository(db)
        for day in (9, 10, 11, 12):  # Mon..Thu around 15:00
            at = datetime(2026, 11, day, 15, 5, tzinfo=timezone.utc)
            await repo.append_exchange("c-1", CC, question="Which deliveries are at risk?", answer="none", job_id=f"j{day}",
                                       details=ExchangeDetails(sender_object_id="u-sup", site_id="BRI", blocks=[], asked_at=at,
                                                               answered_at=at, data_as_of=None, provisional=False))
            await db.execute(TeamsConversationTurn.__table__.update().where(TeamsConversationTurn.job_id == f"j{day}").values(created_at=at))
        await db.commit()
        payload = await tasks_mirror.user_payload(db, CC, "u-sup", now=NOW)
    assert payload["suggested"] == [{"question": "Which deliveries are at risk?", "time": "15:05", "days": 4, "weekdays": True}]
    # once scheduled, it is no longer suggested
    async with async_session() as db:
        await _reminder(db, kind="schedule", title="At risk at 3", query_text="which deliveries are at risk",
                        when={"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "15:00"})
        await db.commit()
        payload = await tasks_mirror.user_payload(db, CC, "u-sup", now=NOW)
    assert payload["suggested"] == [] and payload["counts"]["running"] == 1
