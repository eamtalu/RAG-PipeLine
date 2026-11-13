"""Chunk 183: notification groups (stage D, step 12).

A group is a named route: topics, a severity line, channels and members. The store makes and
changes them; one lookup says where a finding goes; an Admin or an Ops manager manages groups
and channels through the queue (audited, mirrored), anyone joins or leaves; the GROUPS row and
the person's USER#<id>#GROUPS row are what the edge reads.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import NotificationGroup
from app.persistence.models.tenant_site import TenantSite
from app.services.access import commands as access_commands, users
from app.services.groups import commands, routing, store
from app.services.groups import mirror as groups_mirror
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c183"
NOW = datetime(2026, 11, 13, 14, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(CustomerNotificationChannel).where(CustomerNotificationChannel.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C183", timezone="Europe/London"))
        s.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        s.add_all([CustomerNotificationChannel(id=uuid.UUID(int=1), customer_code=CC, channel_type="teams", name="ops", config={}),
                   CustomerNotificationChannel(id=uuid.UUID(int=2), customer_code=CC, channel_type="teams", name="stock", config={})])
        await s.commit()
        await users.create_user(s, CC, "u-ops", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Olu Ops", role_code="ops_manager", all_sites=True))
        await users.create_user(s, CC, "u-sup", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await s.commit()
    yield
    await _wipe()


def _command(sender: str, kind: str, payload: dict, name: str = "Someone") -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id=sender, sender_name=name, question=f"groups: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by=name, payload=payload))


def test_the_routing_rules():
    assert routing.topic_of("location") == "advisory:location"
    assert routing.admits(["advisory:stock", "schedule"], "medium", "advisory:stock", "high")
    assert not routing.admits(["advisory:stock"], "high", "advisory:stock", "low")
    assert routing.clean_topics(["schedule", "advisory:delivery", "nonsense"]) == ["advisory:delivery", "schedule"]
    with pytest.raises(ValueError):
        routing.clean_topics([])
    with pytest.raises(ValueError):
        routing.clean_severity("urgent")


async def test_groups_are_made_changed_looked_up_and_deleted():
    async with async_session() as db:
        stock = await store.set_group(db, CC, group_id=None, name="Stock", topics=["advisory:stock", "advisory:location"],
                                      min_severity="medium", channel_ids=[str(uuid.UUID(int=2))], member_object_ids=["u-sup", "u-ops"],
                                      actor_object_id="u-ops", now=NOW)
        desk = await store.set_group(db, CC, group_id=None, name="Delivery desk", topics=["advisory:delivery", "schedule"],
                                     min_severity="high", channel_ids=[str(uuid.UUID(int=1))], member_object_ids=[],
                                     actor_object_id="u-ops", now=NOW)
        await db.commit()
        # the lookup: a MEDIUM stock finding goes to Stock; a MEDIUM delivery one to nobody; HIGH delivery to the desk
        route = await store.routes_for(db, CC, "advisory:stock", "medium")
        assert route.group_names == ("Stock",) and route.channel_ids == (str(uuid.UUID(int=2)),) and set(route.member_object_ids) == {"u-sup", "u-ops"}
        assert (await store.routes_for(db, CC, "advisory:delivery", "medium")).is_empty
        assert (await store.routes_for(db, CC, "advisory:delivery", "high")).group_ids == (str(desk.id),)
        assert (await store.group_route(db, CC, str(desk.id))).channel_ids == (str(uuid.UUID(int=1)),)
        assert (await store.group_route(db, CC, None)).is_empty
        # a change replaces channels and members as one shape; a bad channel and a taken name are refused
        await store.set_group(db, CC, group_id=str(stock.id), name="Stock control", topics=["advisory:stock"], min_severity="low",
                              channel_ids=[], member_object_ids=["u-sup"], actor_object_id="u-ops", now=NOW)
        with pytest.raises(ValueError, match="not one of this customer"):
            await store.set_group(db, CC, group_id=None, name="X", topics=["schedule"], min_severity="low",
                                  channel_ids=[str(uuid.uuid4())], member_object_ids=[], actor_object_id="u-ops", now=NOW)
        with pytest.raises(ValueError, match="already exists"):
            await store.set_group(db, CC, group_id=None, name="Delivery desk", topics=["schedule"], min_severity="low",
                                  channel_ids=[], member_object_ids=[], actor_object_id="u-ops", now=NOW)
        views = {v.group.name: v for v in await store.list_groups(db, CC)}
        assert views["Stock control"].channel_ids == [] and views["Stock control"].member_object_ids == ["u-sup"]
        assert [g.name for g in await store.groups_of(db, CC, "u-sup")] == ["Stock control"]
        await store.delete_group(db, CC, str(stock.id))
        await db.commit()
        assert [v.group.name for v in await store.list_groups(db, CC)] == ["Delivery desk"]


async def test_managing_needs_configure_agents_and_everyone_may_join_or_leave():
    mirror = InMemoryMirrorWriter()
    # a Supervisor may not manage; an Ops manager may
    with pytest.raises(access_commands.Refused, match="configure_agents"):
        await commands.run(_command("u-sup", "group_set", {"name": "Stock", "topics": ["advisory:stock"]}), mirror=mirror, now=NOW)
    outcome = await commands.run(_command("u-ops", "group_set", {
        "name": "Stock", "topics": ["advisory:stock"], "min_severity": "medium", "channel_ids": [str(uuid.UUID(int=2))],
        "members": ["u-ops"]}, "Olu"), mirror=mirror, now=NOW)
    assert outcome.text == "group Stock saved · 1 channel · 1 member"
    group_id = outcome.blocks[0]["reference"]
    # the supervisor joins, then leaves
    assert (await commands.run(_command("u-sup", "group_join", {"group_id": group_id}, "Sam"), mirror=mirror, now=NOW)).text == "joined Stock"
    payload = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#GROUPS")].payload_json)
    assert [g["name"] for g in payload["groups"]] == ["Stock"]
    groups_row = json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)
    assert groups_row["groups"][0]["members"] == ["u-ops", "u-sup"] and groups_row["groups"][0]["channels"] == [{"id": str(uuid.UUID(int=2)), "name": "stock"}]
    assert [c["name"] for c in groups_row["channels"]] == ["ops", "stock"]
    assert all(c["config"] == {"webhook_url_set": False} for c in groups_row["channels"])  # never the secret itself
    assert (await commands.run(_command("u-sup", "group_leave", {"group_id": group_id}, "Sam"), mirror=mirror, now=NOW)).text == "left Stock"
    # the queue routes group commands here; a bad field is a refusal with the reason
    assert (await run_command(_command("u-ops", "group_delete", {"group_id": group_id}, "Olu"))).text == "group Stock deleted"
    with pytest.raises(access_commands.Refused, match="no longer exists"):
        await commands.run(_command("u-ops", "group_delete", {"group_id": group_id}), mirror=mirror, now=NOW)
    async with async_session() as db:
        assert (await db.execute(select(NotificationGroup).where(NotificationGroup.customer_code == CC))).scalars().all() == []
        actions = sorted(e.action for e in (await db.execute(select(AuditEvent).where(AuditEvent.customer_code == CC))).scalars()
                         if e.action.startswith("group"))
        assert actions == ["group_delete", "group_join", "group_leave", "group_set"]


async def test_a_channel_is_added_by_its_webhook_which_never_shows_and_can_be_deleted():
    mirror = InMemoryMirrorWriter()
    with pytest.raises(access_commands.Refused, match="webhook URL"):
        await commands.run(_command("u-ops", "channel_set", {"name": "Floor"}), mirror=mirror, now=NOW)
    outcome = await commands.run(_command("u-ops", "channel_set", {"name": "Floor", "webhook_url": "https://example.test/hook/1"}, "Olu"),
                                 mirror=mirror, now=NOW)
    assert outcome.text == "channel Floor saved"
    channel_id = outcome.blocks[0]["reference"]
    groups_row = json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)
    floor = next(c for c in groups_row["channels"] if c["name"] == "Floor")
    assert floor["config"] == {"webhook_url_set": True} and "example.test" not in json.dumps(groups_row)
    async with async_session() as db:
        row = await db.get(CustomerNotificationChannel, uuid.UUID(channel_id))
        assert row is not None and "example.test" not in json.dumps(row.config)
    assert (await commands.run(_command("u-ops", "channel_delete", {"channel_id": channel_id}, "Olu"), mirror=mirror, now=NOW)).text == "channel Floor deleted"
    async with async_session() as db:
        assert await db.get(CustomerNotificationChannel, uuid.UUID(channel_id)) is None
    with pytest.raises(access_commands.Refused, match="no longer exists"):
        await commands.run(_command("u-ops", "channel_test", {"channel_id": channel_id}), mirror=mirror, now=NOW)


async def test_the_sweep_writes_the_rows_of_every_bound_customer_once():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await store.set_group(db, CC, group_id=None, name="Stock", topics=["advisory:stock"], min_severity="high",
                              channel_ids=[], member_object_ids=["u-sup"], actor_object_id="u-ops", now=NOW)
        await db.commit()
    assert await groups_mirror.sweep_once(mirror, only=[CC], now=NOW) == {CC: 2}
    assert set(k[1] for k in mirror.items) == {"GROUPS", "USER#u-sup#GROUPS"}
