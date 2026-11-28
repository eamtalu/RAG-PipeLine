"""Chunk 251: a routine set up for a colleague (program step 3, slice 3).

A routine had one owner and could not be made for anyone else. Now a person proposes one to
colleagues; each colleague accepts, which makes it a routine of their own, or declines. So an
answer is always read with the access of the person it is for, and nothing is shared that
could break when its sender leaves. The owner's decisions (28 Nov 2026): a proposal that
becomes the colleague's own routine; anyone may propose; the number of active routines a
person may have is set by an Admin; a routine pauses itself after three failed runs in a row.

Pinned here
-----------
    proposing      one proposal per colleague picked; nothing runs until it is accepted
    accepting      makes a routine owned by the colleague, marked with who proposed it
    declining      removes it for the colleague; the sender reads "declined"
    withdrawing    its sender takes back one that was not answered
    whose          nobody answers or withdraws a proposal that is not theirs
    the limit      active routines per person, set for the whole customer, also when accepting
    failures       three failed runs in a row pause a routine, with the reason on its card
    leavers        a deactivated person's routines are paused, not left firing refusals
    the row        the person's row carries what waits for them and what they proposed
"""
import json
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.customer import Customer
from app.persistence.models.preference import Preference
from app.persistence.models.task import TeamsTask, TeamsTaskProposal, TeamsTaskRun
from app.persistence.models.tenant_site import TenantSite
from app.services import access, preferences
from app.services.access.people import users
from app.services.tasks import commands, proposals, runner, store
from app.services.tasks import mirror as tasks_mirror
from app.services.teams.command_outcome import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.settings import settings

CC = "test_c251"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 30, 9, 0, tzinfo=timezone.utc)  # a Monday
SUPER = users.Actor.system("superadmin")
WEEKDAYS_14 = {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "14:00"}
ROUTINE = {"kind": "schedule", "title": "Open picks", "query_text": "how many picks are open",
           "when": WEEKDAYS_14, "show_as": "summary"}


async def _wipe() -> None:
    async with async_session() as s:
        for model in (AuditEvent, TeamsTaskProposal, TeamsTask, Preference):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    """An all-sites Admin, a Brighton supervisor, a Brighton picker and a London supervisor."""
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C251", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
        for oid, name, role, sites in (("u-adm", "Ada Admin", "admin", ()),
                                       ("u-sam", "Sam Super", "supervisor", ("BRI",)),
                                       ("u-pat", "Pat Picker", "shift_lead", ("BRI",)),
                                       ("u-lee", "Lee London", "supervisor", ("LON",))):
            await users.create_user(s, CC, oid, actor=SUPER, now=NOW, change=users.UserChange(
                display_name=name, role_code=role, site_ids=sites or None, all_sites=not sites))
        await s.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict, site: str = "BRI") -> QuestionJob:
    names = {"u-adm": "Ada Admin", "u-sam": "Sam Super", "u-pat": "Pat Picker",
             "u-lee": "Lee London"}
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender,
                       sender_name=names.get(sender, sender), question=f"tasks: {kind}",
                       source="tab", site_id=site,
                       command=Command(kind=kind, by=sender, payload=payload))


async def _run(sender: str, kind: str, payload: dict, mirror=None, site: str = "BRI"):
    return await commands.run(_job(sender, kind, payload, site),
                              mirror=mirror or InMemoryMirrorWriter(), now=NOW)


async def _proposals(**where) -> list[TeamsTaskProposal]:
    async with async_session() as db:
        rows = (await db.execute(select(TeamsTaskProposal).where(
            TeamsTaskProposal.customer_code == CC).order_by(
            TeamsTaskProposal.receiver_object_id))).scalars().all()
    return [r for r in rows if all(getattr(r, k) == v for k, v in where.items())]


async def _tasks(owner: str) -> list[TeamsTask]:
    async with async_session() as db:
        return await store.list_for_owner(db, CC, owner)


def _row(mirror: InMemoryMirrorWriter, owner: str) -> dict:
    return json.loads(mirror.items[(f"EYE#{CC}", f"USER#{owner}#TASKS")].payload_json)


# ==================================================== 1. proposing

async def test_a_proposal_is_made_for_each_colleague_and_nothing_runs_yet():
    mirror = InMemoryMirrorWriter()
    out = await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam", "u-pat"]}, mirror)
    assert out.text == "proposed to Sam Super and Pat Picker · Every weekday at 14:00 · Open picks"
    waiting = await _proposals()
    assert [(p.receiver_object_id, p.status, p.sender_name) for p in waiting] == [
        ("u-pat", "waiting", "Ada Admin"), ("u-sam", "waiting", "Ada Admin")]
    # no routine exists for anyone, so nothing can fire
    for person in ("u-adm", "u-sam", "u-pat"):
        assert await _tasks(person) == []
    # both colleagues and the sender are mirrored at once
    assert [p["title"] for p in _row(mirror, "u-sam")["proposals"]["waiting"]] == ["Open picks"]
    assert _row(mirror, "u-sam")["proposals"]["waiting"][0]["from_name"] == "Ada Admin"
    sent = _row(mirror, "u-adm")["proposals"]["sent"]
    assert sorted((p["to_name"], p["status"]) for p in sent) == [
        ("Pat Picker", "waiting"), ("Sam Super", "waiting")]


async def test_a_proposal_is_checked_like_a_routine_and_goes_to_real_colleagues():
    for payload, words in (
            ({**ROUTINE, "to": []}, "at least one colleague"),
            ({**ROUTINE, "to": ["u-adm"]}, "yourself"),
            ({**ROUTINE, "to": ["u-ghost"]}, "no such person"),
            ({**ROUTINE, "to": ["u-sam"], "query_text": ""}, "question"),
            ({**ROUTINE, "to": ["u-sam"], "when": {"kind": "once", "at": "2020-01-01T09:00:00Z"}},
             "passed"),
            # a routine for a person is shown to them in the app; a channel is a group's
            ({**ROUTINE, "to": ["u-sam"], "target": "channel"}, "in the app")):
        with pytest.raises(Refused, match=words):
            await _run("u-adm", "task_propose", payload)
    assert await _proposals() == []


async def test_the_routine_dialogs_form_values_are_read_as_for_ones_own_routine():
    values = {"what": "short products today", "how_often": "Every weekday (Mon – Fri)",
              "time": "16:00", "send_to": "Me, in the app", "show_as": "A table"}
    out = await _run("u-adm", "task_propose", {"form_id": "new_schedule", "values": values,
                                               "to": ["u-sam"]})
    assert out.text == ("proposed to Sam Super · Every weekday at 16:00 · "
                        "short products today")
    fields = (await _proposals())[0].fields
    assert (fields["kind"], fields["query_text"], fields["show_as"]) == (
        "schedule", "short products today", "table")


async def test_anyone_may_propose_and_the_same_proposal_is_not_sent_twice():
    await _run("u-pat", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    with pytest.raises(Refused, match="already waiting for Sam Super"):
        await _run("u-pat", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    assert len(await _proposals()) == 1


# ==================================================== 2. accepting, declining, withdrawing

async def test_accepting_makes_the_colleagues_own_routine_marked_with_who_proposed_it():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    proposal = (await _proposals())[0]
    mirror = InMemoryMirrorWriter()
    out = await _run("u-sam", "task_proposal_answer",
                     {"proposal_id": str(proposal.id), "accept": True}, mirror)
    assert out.text == "added to your routines · Every weekday at 14:00 · Open picks"
    task = (await _tasks("u-sam"))[0]
    assert (task.owner_object_id, task.proposed_by, task.proposed_by_name) == (
        "u-sam", "u-adm", "Ada Admin")
    # theirs like any other: booked, at the site they accepted it on, shown in their app
    assert (task.status, task.target, task.site_id) == ("active", "me", "BRI")
    assert task.next_fire_at == datetime(2026, 11, 30, 14, 0, tzinfo=timezone.utc)
    done = (await _proposals())[0]
    assert (done.status, done.task_id) == ("accepted", task.id)
    # the colleague's card says where it came from; the sender reads that it was accepted
    row = _row(mirror, "u-sam")
    assert row["schedules"][0]["proposed_by_name"] == "Ada Admin"
    assert row["proposals"]["waiting"] == []
    assert _row(mirror, "u-adm")["proposals"]["sent"][0]["status"] == "accepted"
    # and they change it without asking anyone
    await _run("u-sam", "task_update", {"task_id": str(task.id), "action": "pause"})


async def test_the_answer_of_an_accepted_routine_is_read_as_its_new_owner():
    # Ada, who may see every site, proposes to Sam of Brighton: the routine is Sam's, so the
    # question is asked as Sam and never as Ada
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]}, site="all")
    await _run("u-sam", "task_proposal_answer",
               {"proposal_id": str((await _proposals())[0].id), "accept": True})
    task = (await _tasks("u-sam"))[0]
    asked_as = []

    async def run_query(t):
        asked_as.append((t.owner_object_id, t.site_id))
        return {"answer": "42 open", "blocks": []}

    async with async_session() as db:
        due = await db.get(TeamsTask, task.id)
        await runner.fire(db, due, now=due.next_fire_at, tz=TZ, run_query=run_query,
                          enqueue=None, catch_up=timedelta(hours=6))
        await db.commit()
    assert asked_as == [("u-sam", "BRI")]


async def test_declining_removes_it_for_the_colleague_and_the_sender_reads_declined():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    mirror = InMemoryMirrorWriter()
    out = await _run("u-sam", "task_proposal_answer",
                     {"proposal_id": str((await _proposals())[0].id), "accept": False}, mirror)
    assert out.text == "declined · Open picks from Ada Admin"
    assert await _tasks("u-sam") == []
    assert _row(mirror, "u-sam")["proposals"]["waiting"] == []
    assert _row(mirror, "u-adm")["proposals"]["sent"][0]["status"] == "declined"


async def test_the_sender_withdraws_one_that_was_not_answered():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam", "u-pat"]})
    sams = (await _proposals(receiver_object_id="u-sam"))[0]
    pats = (await _proposals(receiver_object_id="u-pat"))[0]
    await _run("u-pat", "task_proposal_answer", {"proposal_id": str(pats.id), "accept": True})
    mirror = InMemoryMirrorWriter()
    out = await _run("u-adm", "task_proposal_withdraw", {"proposal_id": str(sams.id)}, mirror)
    assert out.text == "withdrawn · Open picks for Sam Super"
    assert _row(mirror, "u-sam")["proposals"]["waiting"] == []
    # an answered one is the colleague's routine now, and is not the sender's to take back
    with pytest.raises(Refused, match="already accepted"):
        await _run("u-adm", "task_proposal_withdraw", {"proposal_id": str(pats.id)})
    with pytest.raises(Refused, match="withdrawn"):
        await _run("u-sam", "task_proposal_answer", {"proposal_id": str(sams.id),
                                                     "accept": True})


async def test_nobody_answers_or_withdraws_a_proposal_that_is_not_theirs():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    proposal = str((await _proposals())[0].id)
    with pytest.raises(Refused, match="not for you"):
        await _run("u-pat", "task_proposal_answer", {"proposal_id": proposal, "accept": True})
    with pytest.raises(Refused, match="not yours"):
        await _run("u-sam", "task_proposal_withdraw", {"proposal_id": proposal})
    with pytest.raises(Refused, match="no longer exists"):
        await _run("u-sam", "task_proposal_answer", {"proposal_id": str(uuid.uuid4()),
                                                     "accept": True})


async def test_every_proposal_answer_and_withdrawal_is_one_audit_line():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam", "u-pat"]})
    sams = (await _proposals(receiver_object_id="u-sam"))[0]
    pats = (await _proposals(receiver_object_id="u-pat"))[0]
    await _run("u-sam", "task_proposal_answer", {"proposal_id": str(sams.id), "accept": True})
    await _run("u-adm", "task_proposal_withdraw", {"proposal_id": str(pats.id)})
    async with async_session() as db:
        actions = sorted(e.action for e in (await db.execute(select(AuditEvent).where(
            AuditEvent.customer_code == CC, AuditEvent.action.like("task.propos%")))).scalars())
    assert actions == ["task.proposal.accepted", "task.proposal.withdrawn", "task.proposed",
                       "task.proposed"]


# ==================================================== 3. the limit

async def _set_limit(n: int) -> None:
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "routines", "customer": True, "settings": {"max_active": n}}),
        mirror=InMemoryMirrorWriter())


async def test_the_limit_is_twenty_until_an_admin_sets_another(monkeypatch):
    monkeypatch.setattr(settings, "teams_tasks_max_active", 20)
    async with async_session() as db:
        assert await preferences.max_active_routines(db, CC) == 20
    out = await preferences.run(_job("u-adm", "preference_set", {
        "area": "routines", "customer": True, "settings": {"max_active": 2}}),
        mirror=InMemoryMirrorWriter())
    assert out.text == "Each person can now have up to 2 active routines"
    async with async_session() as db:
        assert await preferences.max_active_routines(db, CC) == 2
    for wrong, words in ((0, "max_active"), (101, "100")):
        with pytest.raises(Refused, match=words):
            await preferences.run(_job("u-adm", "preference_set", {
                "area": "routines", "customer": True, "settings": {"max_active": wrong}}),
                mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="manage users"):
        await preferences.run(_job("u-pat", "preference_set", {
            "area": "routines", "customer": True, "settings": {"max_active": 50}}),
            mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="whole customer"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "routines", "settings": {"max_active": 50}}), mirror=InMemoryMirrorWriter())


async def test_a_routine_past_the_limit_is_refused_with_the_number():
    await _set_limit(2)
    for n in (1, 2):
        await _run("u-sam", "task_create", {**ROUTINE, "title": f"Routine {n}"})
    with pytest.raises(Refused, match="2 active routines"):
        await _run("u-sam", "task_create", {**ROUTINE, "title": "One too many"})
    # a paused one does not count, so pausing makes room
    first = (await _tasks("u-sam"))[-1]
    await _run("u-sam", "task_update", {"task_id": str(first.id), "action": "pause"})
    await _run("u-sam", "task_create", {**ROUTINE, "title": "Fits now"})
    # and resuming the paused one is refused while the person is at the limit
    with pytest.raises(Refused, match="2 active routines"):
        await _run("u-sam", "task_update", {"task_id": str(first.id), "action": "resume"})


async def test_accepting_counts_against_the_colleagues_own_limit():
    await _set_limit(1)
    await _run("u-sam", "task_create", {**ROUTINE, "title": "Sam's own"})
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    proposal = (await _proposals())[0]
    with pytest.raises(Refused, match="1 active routine;"):
        await _run("u-sam", "task_proposal_answer", {"proposal_id": str(proposal.id),
                                                     "accept": True})
    # it keeps waiting, so they can make room and accept it then
    assert (await _proposals())[0].status == "waiting"


# ==================================================== 4. failures and leavers

async def _fire(task_id, run_query, times: int) -> TeamsTask:
    async with async_session() as db:
        for _ in range(times):
            task = await db.get(TeamsTask, task_id)
            if task.status != "active":
                break
            await runner.fire(db, task, now=task.next_fire_at, tz=TZ, run_query=run_query,
                              enqueue=None, catch_up=timedelta(hours=6))
            await db.commit()
        return await db.get(TeamsTask, task_id)


async def test_three_failed_runs_in_a_row_pause_a_routine_with_the_reason(monkeypatch):
    monkeypatch.setattr(settings, "teams_tasks_pause_after_failures", 3)
    await _run("u-sam", "task_create", ROUTINE)
    task_id = (await _tasks("u-sam"))[0].id
    answers = iter([RuntimeError("the store is down"), {"answer": "42", "blocks": []},
                    RuntimeError("down"), RuntimeError("down"),
                    RuntimeError("the store is down")])

    async def run_query(t):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    # one failure, then a good run: the count starts again
    task = await _fire(task_id, run_query, 2)
    assert (task.status, task.failed_in_a_row) == ("active", 0)
    # three in a row: paused, no next firing, the reason kept for the card
    task = await _fire(task_id, run_query, 5)
    assert (task.status, task.next_fire_at, task.failed_in_a_row) == ("paused", None, 3)
    assert task.pause_reason == "Paused after 3 failed runs in a row: the store is down"
    async with async_session() as db:
        assert store.task_payload(task, TZ)["pause_reason"] == task.pause_reason
        runs = (await db.execute(select(TeamsTaskRun).where(
            TeamsTaskRun.task_id == task_id))).scalars().all()
    assert sorted(r.outcome for r in runs) == ["failed", "failed", "failed", "failed", "sent"]
    # resuming by hand clears the reason and the count
    await _run("u-sam", "task_update", {"task_id": str(task_id), "action": "resume"})
    task = (await _tasks("u-sam"))[0]
    assert (task.status, task.pause_reason, task.failed_in_a_row) == ("active", None, 0)


async def test_a_deactivated_persons_routines_are_paused_and_do_not_fire():
    await _run("u-sam", "task_create", ROUTINE)
    task_id = (await _tasks("u-sam"))[0].id
    async with async_session() as db:
        await users.update_user(db, CC, "u-sam", actor=SUPER, now=NOW,
                                change=users.UserChange(status="deactivated"))
        await db.commit()
    asked = []

    async def run_query(t):
        asked.append(t.id)
        return {"answer": "should not be asked", "blocks": []}

    due = datetime(2026, 11, 30, 14, 1, tzinfo=timezone.utc)
    fired = await runner.tick(None, run_query=run_query, now=due, only=[CC])
    assert fired == {} and asked == []
    task = (await _tasks("u-sam"))[0]
    assert (task.status, task.next_fire_at) == ("paused", None)
    assert task.pause_reason == "Paused: Sam Super no longer has access"
    async with async_session() as db:
        assert (await db.execute(select(TeamsTaskRun).where(
            TeamsTaskRun.task_id == task_id))).scalars().all() == []


# ==================================================== 5. the row

async def test_the_persons_row_carries_the_limit_and_their_proposals():
    await _set_limit(5)
    async with async_session() as db:
        row = await tasks_mirror.user_payload(db, CC, "u-sam", now=NOW)
    assert row["limit"] == {"max_active": 5, "active": 0}
    assert row["proposals"] == {"waiting": [], "sent": []}


async def test_someone_with_only_a_proposal_gets_a_row_in_the_sweep():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-lee"]})
    async with async_session() as db:
        # Lee has no routine, only a proposal waiting; Ada has none either, only one sent
        assert await tasks_mirror.owners_with_tasks(db, CC) == ["u-adm", "u-lee"]
    mirror = InMemoryMirrorWriter()
    assert await tasks_mirror.sweep_once(mirror, only=[CC], now=NOW) == {CC: 2}
    assert _row(mirror, "u-lee")["proposals"]["waiting"][0]["from_name"] == "Ada Admin"


async def test_the_shape_of_a_proposal_in_the_row():
    await _run("u-adm", "task_propose", {**ROUTINE, "to": ["u-sam"]})
    async with async_session() as db:
        waiting = (await proposals.for_person(db, CC, "u-sam", tz=TZ))["waiting"][0]
    assert {k: waiting[k] for k in ("title", "from_name", "when_text", "query_text", "kind")} == {
        "title": "Open picks", "from_name": "Ada Admin", "when_text": "Every weekday at 14:00",
        "query_text": "how many picks are open", "kind": "schedule"}
