"""Chunks 249 and 250: a group belongs to who made it (program step 3, slice 2, backend).

Until now only people who may configure agents (Admins and Ops managers) could make a group,
and each of them could change every group. The owner's decisions (28 Nov 2026): making groups
is a permission of its own, set per role; a group belongs to the person who made it, who
manages it, and to Admins (the role, nobody else); whoever makes a group connects a Teams
channel for it; a member may add a colleague.

Pinned here
-----------
    the capability   `manage_groups`, in the matrix, for Admin and Ops manager until an Admin
                     gives it to another role; a role the customer added starts without it
    making           whoever holds it makes a group, inside their own sites
    ownership        its maker changes and deletes it; an Admin does so for every group;
                     nobody else, not even an Ops manager
    channels         a maker connects a channel, and changes, tests and deletes their own; a
                     group posts only where its maker connected, an Admin's anywhere
    members          a member, the maker, or someone who manages every group adds a colleague
    the row          each group names who made it
"""
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import NotificationGroup
from app.persistence.models.tenant_site import TenantSite
from app.services import access
from app.services.access.authorization import matrix as matrix_file
from app.services.access.authorization import records, roles
from app.services.access.people import users
from app.services.groups import commands, store
from app.services.teams.command_outcome import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c249"
NOW = datetime(2026, 11, 28, 18, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")
CHANNEL = uuid.UUID(int=249)


async def _wipe() -> None:
    async with async_session() as s:
        for model in (AuditEvent, NotificationGroup, CustomerNotificationChannel):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    """An Admin, an Ops manager, two Brighton supervisors who may make groups, and a picker."""
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C249", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        s.add(CustomerNotificationChannel(id=CHANNEL, customer_code=CC, channel_type="teams",
                                          name="shift leads", config={}))
        await s.commit()
        for oid, name, role, sites in (
                ("u-adm", "Ada Admin", "admin", ()), ("u-ops", "Olu Ops", "ops_manager", ()),
                ("u-sam", "Sam Super", "supervisor", ("BRI",)),
                ("u-sue", "Sue Super", "supervisor", ("BRI",)),
                ("u-pat", "Pat Picker", "picker", ("BRI",))):
            await users.create_user(s, CC, oid, actor=SUPER, now=NOW, change=users.UserChange(
                display_name=name, role_code=role, site_ids=sites or None, all_sites=not sites))
        # the Admin's click in "What each role can do": Supervisors may make groups
        await records.set_matrix_cell(s, CC, "supervisor", "manage_groups", "allowed",
                                      actor=access.Actor("u-adm", "Ada"))
        await s.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"groups: {kind}", source="tab", site_id="BRI",
                       command=Command(kind=kind, by=sender, payload=payload))


async def _run(sender: str, kind: str, payload: dict, mirror=None):
    return await commands.run(_job(sender, kind, payload),
                              mirror=mirror or InMemoryMirrorWriter(), now=NOW)


async def _make(sender: str, name: str, channels: tuple[str, ...] = ()) -> str:
    await _run(sender, "group_set", {"name": name, "topics": ["advisory:stock"],
                                     "channel_ids": list(channels)})
    async with async_session() as db:
        return next(str(v.group.id) for v in await store.list_groups(db, CC)
                    if v.group.name == name)


async def _members(group_id: str) -> list[str]:
    async with async_session() as db:
        return next(sorted(v.member_object_ids) for v in await store.list_groups(db, CC)
                    if str(v.group.id) == group_id)


# ==================================================== 1. the capability

def test_making_groups_is_a_capability_of_its_own_with_todays_two_roles():
    m = matrix_file.load()
    assert "manage_groups" in m.capability_codes
    allowed = [role for role in m.role_codes if m.default(role, "manage_groups") == "allowed"]
    # nothing changes until an Admin gives it to another role
    assert allowed == ["ops_manager", "admin"]
    assert allowed == [r for r in m.role_codes if m.default(r, "configure_agents") == "allowed"]


async def test_a_role_the_customer_added_starts_without_it_and_can_be_given_it():
    async with async_session() as db:
        # a role added before the capability existed has no cell for it
        role, _ = await roles.set_role(db, CC, name="Night lead", copy_from="supervisor")
        from app.persistence.models.access import AccessRoleCapability
        await db.execute(delete(AccessRoleCapability).where(
            AccessRoleCapability.customer_code == CC,
            AccessRoleCapability.role_code == role.code,
            AccessRoleCapability.capability_code == "manage_groups"))
        await db.commit()
        cells = await records.customer_matrix(db, CC)
        assert cells[(role.code, "manage_groups")] == "no"
        # the cell exists, so the Admin's click in the table works
        await records.set_matrix_cell(db, CC, role.code, "manage_groups", "allowed",
                                      actor=access.Actor("u-adm", "Ada"))
        await db.commit()
        assert (await records.customer_matrix(db, CC))[(role.code, "manage_groups")] == "allowed"


# ==================================================== 2. making and owning

async def test_whoever_may_make_groups_makes_one_inside_their_own_sites():
    out = await _run("u-sam", "group_set", {"name": "Night shift", "topics": ["advisory:stock"]})
    assert out.text.startswith("group BRI · Night shift saved · 0 channels")
    with pytest.raises(Refused, match="LON is not one of your sites"):
        await _run("u-sam", "group_set", {"name": "London too", "topics": ["advisory:stock"],
                                          "site_ids": ["BRI", "LON"]})
    with pytest.raises(Refused, match="make groups"):
        await _run("u-pat", "group_set", {"name": "Mine", "topics": ["advisory:stock"]})


async def test_a_group_is_changed_and_deleted_by_its_maker_and_by_an_admin_only():
    sams = await _make("u-sam", "Night shift")
    edit = {"group_id": sams, "name": "Night shift B", "topics": ["advisory:stock"]}
    # Sue may make groups too, and Olu is an Ops manager, but this one is Sam's
    for other in ("u-sue", "u-ops"):
        for kind, payload in (("group_set", edit), ("group_delete", {"group_id": sams})):
            with pytest.raises(Refused, match="made by Sam Super; they or an Admin"):
                await _run(other, kind, payload)
    await _run("u-sam", "group_set", edit)
    # the Admin manages every group
    await _run("u-adm", "group_set", {**edit, "name": "Night shift C"})
    await _run("u-adm", "group_delete", {"group_id": sams})
    async with async_session() as db:
        assert await store.list_groups(db, CC) == []


async def test_someone_who_lost_the_capability_no_longer_changes_their_own_group():
    sams = await _make("u-sam", "Night shift")
    async with async_session() as db:
        await records.set_matrix_cell(db, CC, "supervisor", "manage_groups", "no",
                                      actor=access.Actor("u-adm", "Ada"))
        await db.commit()
    with pytest.raises(Refused, match="make groups"):
        await _run("u-sam", "group_delete", {"group_id": sams})


async def _channel_of(name: str) -> str:
    async with async_session() as db:
        return next(str(c.id) for c in await store.customer_channels(db, CC) if c.name == name)


async def test_a_maker_connects_a_channel_and_manages_only_their_own():
    out = await _run("u-sam", "channel_set", {"name": "night shift",
                                              "webhook_url": "https://x.test/hook"})
    assert out.text == "channel night shift saved"
    sams = await _channel_of("night shift")
    await _run("u-sam", "channel_set", {"channel_id": sams, "name": "night shift B"})
    # the channel that was there before belongs to nobody, so only an Admin touches it
    for kind, payload in (("channel_set", {"channel_id": str(CHANNEL), "name": "mine now"}),
                          ("channel_test", {"channel_id": str(CHANNEL)}),
                          ("channel_delete", {"channel_id": str(CHANNEL)})):
        with pytest.raises(Refused, match="connected by someone else"):
            await _run("u-sam", kind, payload)
    # Sue cannot delete Sam's; Sam and the Admin can
    with pytest.raises(Refused, match="connected by someone else"):
        await _run("u-sue", "channel_delete", {"channel_id": sams})
    await _run("u-adm", "channel_set", {"channel_id": sams, "name": "night shift C"})
    await _run("u-sam", "channel_delete", {"channel_id": sams})
    with pytest.raises(Refused, match="may not make groups"):
        await _run("u-pat", "channel_set", {"name": "x", "webhook_url": "https://x.test/hook"})


async def test_a_group_posts_only_where_its_maker_connected():
    await _run("u-sam", "channel_set", {"name": "night shift",
                                        "webhook_url": "https://x.test/hook"})
    sams = await _channel_of("night shift")
    # Sam's group to Sam's channel: fine. To the channel someone else connected: refused,
    # because only whoever connected a channel knows who reads it
    group = await _make("u-sam", "Night shift", (sams,))
    with pytest.raises(Refused, match="shift leads was connected by someone else"):
        await _make("u-sam", "Sneaky", (str(CHANNEL),))
    with pytest.raises(Refused, match="night shift was connected by someone else"):
        await _make("u-sue", "Sue's", (sams,))
    # an Admin posts a group anywhere, and a channel the group already has may stay
    await _run("u-adm", "group_set", {"group_id": group, "name": "Night shift",
                                      "topics": ["advisory:stock"],
                                      "channel_ids": [sams, str(CHANNEL)]})
    await _run("u-sam", "group_set", {"group_id": group, "name": "Night shift 2",
                                      "topics": ["advisory:stock"],
                                      "channel_ids": [sams, str(CHANNEL)]})
    async with async_session() as db:
        assert set((await store.group_route(db, CC, group)).channel_ids) == {sams, str(CHANNEL)}


# ==================================================== 3. members add colleagues

async def test_a_member_the_maker_and_an_admin_add_a_colleague_and_nobody_else_does():
    sams = await _make("u-sam", "Night shift")
    # Pat is not a member yet, and did not make it
    with pytest.raises(Refused, match="member"):
        await _run("u-pat", "group_member_add", {"group_id": sams, "object_id": "u-sue"})
    # the maker adds Pat; Pat, now a member, adds Sue; the Admin adds the Ops manager
    out = await _run("u-sam", "group_member_add", {"group_id": sams, "object_id": "u-pat"})
    assert out.text == "Pat Picker added to BRI · Night shift"
    await _run("u-pat", "group_member_add", {"group_id": sams, "object_id": "u-sue"})
    await _run("u-adm", "group_member_add", {"group_id": sams, "object_id": "u-ops"})
    assert await _members(sams) == ["u-ops", "u-pat", "u-sue"]
    # adding someone twice changes nothing, and a stranger is refused
    await _run("u-sam", "group_member_add", {"group_id": sams, "object_id": "u-pat"})
    assert await _members(sams) == ["u-ops", "u-pat", "u-sue"]
    with pytest.raises(Refused, match="no such person"):
        await _run("u-sam", "group_member_add", {"group_id": sams, "object_id": "u-ghost"})
    with pytest.raises(Refused, match="no longer exists"):
        await _run("u-sam", "group_member_add", {"group_id": str(uuid.uuid4()),
                                                 "object_id": "u-pat"})


async def test_adding_a_colleague_is_audited_and_their_row_follows():
    sams = await _make("u-sam", "Night shift")
    mirror = InMemoryMirrorWriter()
    await _run("u-sam", "group_member_add", {"group_id": sams, "object_id": "u-pat"}, mirror)
    pats = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-pat#GROUPS")].payload_json)["groups"]
    assert [g["display_name"] for g in pats] == ["BRI · Night shift"]


# ==================================================== 4. the row names the maker

async def test_each_group_names_who_made_it():
    await _make("u-sam", "Night shift")
    await _make("u-ops", "Everything")
    mirror = InMemoryMirrorWriter()
    await _run("u-ops", "group_join", {"group_id": await _make("u-adm", "Board")}, mirror)
    payload = json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)
    assert {g["name"]: (g["created_by"], g["created_by_name"]) for g in payload["groups"]} == {
        "Night shift": ("u-sam", "Sam Super"), "Everything": ("u-ops", "Olu Ops"),
        "Board": ("u-adm", "Ada Admin")}
    # the people a member can add as a colleague, and who connected each channel
    assert {p["object_id"]: p["name"] for p in payload["people"]}["u-pat"] == "Pat Picker"
    assert payload["channels"][0]["created_by"] is None
