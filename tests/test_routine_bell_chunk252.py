"""Chunk 252: a bell in Teams when a person's own routine has run (program step 3, slice 4a).

Nothing could tell a person anything in Teams. The owner's decision (28 Nov 2026): for now only
the person who owns a routine is told, when it has run; telling another person comes later.
The backend decides what to say and hands it to the edge, which speaks to Microsoft; the
Microsoft secret never lives on this server.

Pinned here
-----------
    the switch     a routine rings the bell only when its owner ticked it
    queued once    one notification per run, written with the run, never twice
    what it says   the routine's title and the first line of its answer, for its owner only
    not for        a run by hand (the person is looking at it)
    as well        a routine that posts its result to a group's channel rings the bell too
    paused         a routine that paused itself tells its owner once
    handing over   queued ones are posted to the edge with the tenant and the person; the
                   edge's answer decides: sent, refused (not retried), or tried again later
    not set up     an edge with no Microsoft secret switches the rest off without retrying
"""
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.task import TeamsTask
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.teams_notification import TeamsNotification
from app.persistence.models.tenant_site import TenantSite
from app.services.access.people import users
from app.services.tasks import commands, forms, runner, store
from app.services.teams import bell
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.settings import settings

CC = "test_c252"
TENANT = "tenant-252"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 30, 9, 0, tzinfo=timezone.utc)  # a Monday
SUPER = users.Actor.system("superadmin")
ROUTINE = {"kind": "schedule", "title": "Open picks", "query_text": "how many picks are open",
           "when": {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "14:00"}}


async def _wipe() -> None:
    async with async_session() as s:
        for model in (TeamsNotification, TeamsTask):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C252", timezone="Europe/London"))
        s.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        s.add(TeamsTenantBinding(tenant_id=TENANT, customer_code=CC, enabled=True))
        await s.commit()
        await users.create_user(s, CC, "u-sam", actor=SUPER, now=NOW, change=users.UserChange(
            display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await s.commit()
    yield
    await _wipe()


async def _make(**over) -> uuid.UUID:
    job = QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id=TENANT, customer_code=CC,
                      conversation_id="c", sender_object_id="u-sam", sender_name="Sam Super",
                      question="tasks", source="tab", site_id="BRI",
                      command=Command(kind="task_create", by="u-sam",
                                      payload={**ROUTINE, "notify_bell": True, **over}))
    await commands.run(job, mirror=InMemoryMirrorWriter(), now=NOW)
    async with async_session() as db:
        return (await store.list_for_owner(db, CC, "u-sam"))[0].id


async def _fire(task_id, answer="42 picks are open\nBrighton, as of 14:00") -> None:
    async def run_query(t):
        if isinstance(answer, Exception):
            raise answer
        return {"answer": answer, "blocks": []}

    async with async_session() as db:
        task = await db.get(TeamsTask, task_id)
        await runner.fire(db, task, now=task.next_fire_at, tz=TZ, run_query=run_query,
                          enqueue=None, catch_up=timedelta(hours=6))
        await db.commit()


async def _queued() -> list[TeamsNotification]:
    async with async_session() as db:
        return list((await db.execute(select(TeamsNotification).where(
            TeamsNotification.customer_code == CC).order_by(
            TeamsNotification.created_at))).scalars())


class FakePoster:
    """Answers each post as the edge would; keeps what it was sent."""

    def __init__(self, *answers):
        self.answers, self.sent = list(answers), []

    async def post(self, payload: dict) -> bell.Handed:
        self.sent.append(payload)
        return self.answers.pop(0) if self.answers else bell.Handed("sent")


# ==================================================== 1. the switch

async def test_a_routine_rings_only_when_its_owner_ticked_it():
    quiet = await _make(notify_bell=False)
    await _fire(quiet)
    assert await _queued() == []
    async with async_session() as db:
        task = await db.get(TeamsTask, quiet)
        assert store.task_payload(task, TZ)["notify_bell"] is False
        # ticking it later is an ordinary edit
        store.apply(task, "edit", {"notify_bell": True}, now=NOW, tz=TZ)
        await db.commit()
    await _fire(quiet)
    assert len(await _queued()) == 1


def test_the_dialogs_tick_becomes_the_routines_switch():
    values = {"what": "short products today", "send_to": "Me, in the app", "ring_bell": "yes"}
    assert forms.schedule_from_form(values, tz=TZ)["notify_bell"] is True
    assert forms.schedule_from_form({**values, "ring_bell": ""}, tz=TZ)["notify_bell"] is False
    # a form that does not know the tick leaves the switch as it is
    assert "notify_bell" not in forms.schedule_from_form({"what": "x"}, tz=TZ)


# ==================================================== 2. what is queued

async def test_one_notification_per_run_with_the_title_and_the_first_line_of_the_answer():
    task_id = await _make()
    await _fire(task_id)
    row = (await _queued())[0]
    assert (row.object_id, row.kind, row.status, row.tab) == (
        "u-sam", "routine_ran", "queued", "routines")
    assert (row.title, row.text) == ("Open picks", "42 picks are open")
    await _fire(task_id)
    assert len(await _queued()) == 2  # the next day's run is its own notification


async def test_a_long_first_line_is_cut_to_what_teams_shows():
    task_id = await _make()
    await _fire(task_id, "x" * 400)
    assert len((await _queued())[0].text) == 150


async def test_a_run_by_hand_does_not_ring():
    task_id = await _make()

    async def run_query(t):
        return {"answer": "42", "blocks": []}

    async with async_session() as db:
        task = await db.get(TeamsTask, task_id)
        await runner.run_by_hand(db, task, actor_object_id="u-sam", actor_name="Sam", now=NOW,
                                 run_query=run_query)
        await db.commit()
    assert await _queued() == []


async def test_a_routine_posted_to_a_channel_rings_its_owners_bell_as_well():
    # the owner's decision (28 Nov 2026): the result can go to a Teams channel as well
    task_id = await _make()
    async with async_session() as db:
        task = await db.get(TeamsTask, task_id)
        task.target = "channel"
        await db.commit()
    await _fire(task_id)
    assert [(r.kind, r.title) for r in await _queued()] == [("routine_ran", "Open picks")]


async def test_a_routine_that_pauses_itself_tells_its_owner_once(monkeypatch):
    monkeypatch.setattr(settings, "teams_tasks_pause_after_failures", 2)
    task_id = await _make()
    await _fire(task_id, RuntimeError("the store is down"))
    assert await _queued() == []  # one failure is not worth a bell
    await _fire(task_id, RuntimeError("the store is down"))
    row = (await _queued())[0]
    assert (row.kind, row.title) == ("routine_paused", "Open picks")
    assert row.text == "Paused after 2 failed runs in a row: the store is down"


# ==================================================== 3. handing over to the edge

async def test_queued_ones_are_posted_with_the_tenant_and_the_person_and_marked_sent():
    task_id = await _make()
    await _fire(task_id)
    poster = FakePoster()
    assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == {"sent": 1}
    assert poster.sent == [{
        "notification_id": str((await _queued())[0].id), "tenant_id": TENANT,
        "user_object_id": "u-sam", "kind": "routine_ran", "title": "Open picks",
        "text": "42 picks are open", "tab": "routines"}]
    row = (await _queued())[0]
    assert (row.status, row.sent_at) == ("sent", NOW)
    # nothing is posted twice
    assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == {}
    assert len(poster.sent) == 1


async def test_a_refusal_is_recorded_and_not_tried_again():
    task_id = await _make()
    await _fire(task_id)
    poster = FakePoster(bell.Handed("refused", "the app is not installed for this person"))
    assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == {"refused": 1}
    row = (await _queued())[0]
    assert (row.status, row.error) == ("refused", "the app is not installed for this person")
    assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == {}


async def test_a_passing_failure_is_tried_again_and_given_up_after_three():
    task_id = await _make()
    await _fire(task_id)
    poster = FakePoster(*[bell.Handed("retry", "edge HTTP 502")] * 3)
    for expected in ({"retry": 1}, {"retry": 1}, {"failed": 1}):
        assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == expected
    row = (await _queued())[0]
    assert (row.status, row.attempts, row.error) == ("failed", 3, "edge HTTP 502")


async def test_an_edge_without_the_microsoft_secret_switches_the_rest_off():
    task_id = await _make()
    await _fire(task_id)
    await _fire(task_id)
    poster = FakePoster(bell.Handed("not_configured", "the bell is not set up on the edge"))
    # the first answer says the bell is not set up, so the second is not even tried
    assert await bell.deliver_pending(poster, now=NOW, only=[CC]) == {"not_configured": 2}
    assert len(poster.sent) == 1
    assert {r.status for r in await _queued()} == {"not_configured"}


async def test_an_old_notification_is_not_sent_late():
    task_id = await _make()
    await _fire(task_id)
    poster = FakePoster()
    late = NOW + timedelta(days=2)
    assert await bell.deliver_pending(poster, now=late, only=[CC]) == {"expired": 1}
    assert poster.sent == [] and (await _queued())[0].status == "expired"


def test_the_edges_answer_is_read_into_what_to_do_next():
    assert bell.handed_from(200, {"sent": True}) == bell.Handed("sent")
    assert bell.handed_from(200, {"sent": False, "reason": "Forbidden"}) == bell.Handed(
        "refused", "Forbidden")
    assert bell.handed_from(503, {"detail": "the bell is not set up"}).outcome == "not_configured"
    assert bell.handed_from(502, {}).outcome == "retry"
    assert bell.handed_from(401, {"detail": "invalid secret"}).outcome == "refused"
