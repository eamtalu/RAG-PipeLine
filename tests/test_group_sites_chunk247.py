"""Chunk 247: a notification group has sites (program step 3, slice 1).

Until now a group took the advisories of every site of the customer, and a routine aimed at a
group's channel answered with everything its owner may see. A Teams channel is not a person, so
nothing of step 1 limited what was posted into it: a London stock alert reached a channel that
only Brighton people read. A group now names the sites it is about.

Pinned here
-----------
    the label     the sites are written in front of the name: "BRI · Stock", "All sites · Ops"
    routing       an advisory reaches a group only when its place is one of the group's sites,
                  decided by the same rule that limits what a person reads
    who           nobody gives a group more sites than they may see themselves
    existing      a group made before this has every site, as it behaved, until it is narrowed
    routines      a routine sent to a group's channel reads inside the group's sites and the
                  owner's own, whichever is narrower
    the row       the edge reads each group's sites and its full name
"""
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete

from app import teams_consumer
from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.advisory import Advisory
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import NotificationGroup
from app.persistence.models.tenant_site import TenantSite, TenantSitePlace
from app.services import access, scoped_data
from app.services.access.people import users
from app.services.groups import commands, routing, store
from app.services.groups import mirror as groups_mirror
from app.services.groups.store import GroupSites
from app.services.scoped_data import DataScope
from app.services.teams.command_outcome import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c247"
NOW = datetime(2026, 11, 28, 10, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")
CHANNEL = uuid.UUID(int=247)


async def _wipe() -> None:
    async with async_session() as s:
        for model in (AuditEvent, Advisory, NotificationGroup, CustomerNotificationChannel):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    """Three sites (Brighton stands for 915/BRI), an all-sites Ops manager and a Brighton one."""
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C247", timezone="Europe/London"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="BIC", name="Bicester", sort_order=2),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=3),
                   TenantSite(customer_code=CC, site_id="OLD", name="Old depot", sort_order=4,
                              active=False)])
        s.add(CustomerNotificationChannel(id=CHANNEL, customer_code=CC, channel_type="teams",
                                          name="shift leads", config={}))
        await s.flush()
        s.add(TenantSitePlace(customer_code=CC, site_id="BRI", company="915", warehouse="BRI"))
        await s.commit()
        for oid, name, sites in (("u-ops", "Olu Ops", ()), ("u-bri", "Bea Brighton", ("BRI",))):
            await users.create_user(s, CC, oid, actor=SUPER, now=NOW, change=users.UserChange(
                display_name=name, role_code="ops_manager", site_ids=sites or None,
                all_sites=not sites))
        await s.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"groups: {kind}", source="tab", site_id="BRI",
                       command=Command(kind=kind, by=sender, payload=payload))


async def _make(name: str, sites: GroupSites | None, topics=("advisory:stock",)) -> str:
    async with async_session() as db:
        group = await store.set_group(db, CC, group_id=None, name=name, topics=list(topics),
                                      min_severity="low", channel_ids=[str(CHANNEL)],
                                      member_object_ids=[], actor_object_id="u-ops", now=NOW,
                                      sites=sites)
        await db.commit()
        return str(group.id)


async def _advisory(site: str | None, company: str | None = None) -> uuid.UUID:
    async with async_session() as db:
        row = Advisory(id=uuid.uuid4(), customer_code=CC, agent="stock", kind="count_gap",
                       dedup_key=uuid.uuid4().hex, severity="high", status="open",
                       title=f"gap at {site}", site_id=site, company=company, opened_at=NOW)
        db.add(row)
        await db.commit()
        return row.id


async def _takers(advisory_id: uuid.UUID) -> list[str]:
    async with async_session() as db:
        return [v.display_name for v in await store.takers(db, CC, "advisory:stock", "high",
                                                           advisory_id=advisory_id)]


# ==================================================== 1. the label

def test_the_sites_are_written_in_front_of_the_name():
    assert routing.sites_label(True, []) == "All sites"
    assert routing.sites_label(False, ["LON", "BRI"]) == "BRI, LON"
    assert routing.display_name("Stock alerts", False, ["BRI"]) == "BRI · Stock alerts"
    assert routing.display_name("Ops managers", True, ["BRI"]) == "All sites · Ops managers"
    # a group that lost its last site says so, and receives nothing
    assert routing.display_name("Stock alerts", False, []) == "No site · Stock alerts"


# ==================================================== 2. routing by place

async def test_an_advisory_reaches_only_the_groups_of_its_place():
    await _make("Stock", GroupSites(False, ("BRI",)))
    await _make("South", GroupSites(False, ("BRI", "BIC")))
    await _make("Everything", GroupSites(True, ()))
    # London at 10:00: only the all-sites group; Brighton at 10:05: all three
    assert await _takers(await _advisory("LON", "915")) == ["All sites · Everything"]
    assert await _takers(await _advisory("BRI", "915")) == [
        "All sites · Everything", "BIC, BRI · South", "BRI · Stock"]
    assert await _takers(await _advisory("BIC")) == ["All sites · Everything", "BIC, BRI · South"]


async def test_the_place_is_decided_as_a_persons_reading_is():
    await _make("Stock", GroupSites(False, ("BRI",)))
    # Brighton is 915/BRI: the same warehouse code under another company is another place
    assert await _takers(await _advisory("BRI", "920")) == []
    # an advisory written before it had a company still follows its site
    assert await _takers(await _advisory("BRI", None)) == ["BRI · Stock"]
    # one that names no site belongs to the whole customer, so only an all-sites group takes it
    assert await _takers(await _advisory(None)) == []
    await _make("Everything", GroupSites(True, ()))
    assert await _takers(await _advisory(None)) == ["All sites · Everything"]


async def test_a_lookup_without_an_advisory_keeps_working_as_before():
    await _make("Stock", GroupSites(False, ("BRI",)))
    async with async_session() as db:
        assert (await store.routes_for(db, CC, "advisory:stock", "high")).group_names == (
            "BRI · Stock",)


# ==================================================== 3. who may give a group which sites

async def test_nobody_gives_a_group_more_sites_than_they_may_see():
    mirror = InMemoryMirrorWriter()
    base = {"name": "Stock", "topics": ["advisory:stock"], "channel_ids": [str(CHANNEL)]}
    with pytest.raises(Refused, match="LON is not one of your sites"):
        await commands.run(_job("u-bri", "group_set", {**base, "site_ids": ["BRI", "LON"]}),
                           mirror=mirror, now=NOW)
    with pytest.raises(Refused, match="every site"):
        await commands.run(_job("u-bri", "group_set", {**base, "all_sites": True}),
                           mirror=mirror, now=NOW)
    with pytest.raises(Refused, match="NOPE is not a site"):
        await commands.run(_job("u-ops", "group_set", {**base, "site_ids": ["NOPE"]}),
                           mirror=mirror, now=NOW)
    with pytest.raises(Refused, match="OLD is retired"):
        await commands.run(_job("u-ops", "group_set", {**base, "site_ids": ["OLD"]}),
                           mirror=mirror, now=NOW)
    with pytest.raises(Refused, match="at least one site"):
        await commands.run(_job("u-ops", "group_set", {**base, "all_sites": False,
                                                       "site_ids": []}), mirror=mirror, now=NOW)
    outcome = await commands.run(_job("u-bri", "group_set", {**base, "site_ids": ["BRI"]}),
                                 mirror=mirror, now=NOW)
    assert outcome.text.startswith("group BRI · Stock saved · 1 channel")


async def test_a_group_made_without_naming_sites_gets_its_makers_own():
    mirror = InMemoryMirrorWriter()
    await commands.run(_job("u-bri", "group_set", {"name": "Mine", "topics": ["schedule"]}),
                       mirror=mirror, now=NOW)
    await commands.run(_job("u-ops", "group_set", {"name": "Wide", "topics": ["schedule"]}),
                       mirror=mirror, now=NOW)
    async with async_session() as db:
        names = [v.display_name for v in await store.list_groups(db, CC)]
    assert names == ["BRI · Mine", "All sites · Wide"]


async def test_a_change_keeps_the_sites_unless_it_names_them_and_cannot_widen_past_the_editor():
    mirror = InMemoryMirrorWriter()
    wide = await _make("Wide", GroupSites(True, ()))
    edit = {"group_id": wide, "name": "Wide desk", "topics": ["advisory:stock"]}
    # a Brighton person renames an all-sites group: its sites are not theirs to widen, but
    # leaving them as they are is not a widening
    await commands.run(_job("u-bri", "group_set", edit), mirror=mirror, now=NOW)
    await commands.run(_job("u-bri", "group_set", {**edit, "all_sites": True}), mirror=mirror,
                       now=NOW)
    # narrowing to their own site is theirs to do; after that they cannot bring London back
    out = await commands.run(_job("u-bri", "group_set", {**edit, "site_ids": ["BRI"]}),
                             mirror=mirror, now=NOW)
    assert out.text.startswith("group BRI · Wide desk saved")
    with pytest.raises(Refused, match="LON is not one of your sites"):
        await commands.run(_job("u-bri", "group_set", {**edit, "site_ids": ["BRI", "LON"]}),
                           mirror=mirror, now=NOW)


# ==================================================== 4. groups made before this

async def test_a_group_made_before_sites_existed_has_every_site():
    async with async_session() as db:
        # as the migration leaves an existing row: the column's default, no site rows
        db.add(NotificationGroup(id=uuid.uuid4(), customer_code=CC, name="Old one",
                                 topics=["advisory:stock"], min_severity="low", created_at=NOW,
                                 updated_at=NOW))
        await db.commit()
    assert await _takers(await _advisory("LON", "915")) == ["All sites · Old one"]


# ==================================================== 5. a routine sent to a group's channel

async def _narrowed(scope: DataScope, sites: GroupSites) -> DataScope:
    async with async_session() as db:
        return await scoped_data.narrow(db, scope, all_sites=sites.all_sites,
                                        site_ids=sites.site_ids)


async def test_a_scope_is_narrowed_to_what_both_the_owner_and_the_group_cover():
    everything = DataScope(CC, all_sites=True)
    brighton = DataScope(CC, site_ids=("BRI",), places=(scoped_data.Place("915", "BRI"),))
    # you, allowed all sites, send to "BRI · Stock": Brighton only, with its place
    mine = await _narrowed(everything, GroupSites(False, ("BRI",)))
    assert (mine.all_sites, mine.site_ids, mine.places) == (
        False, ("BRI",), (scoped_data.Place("915", "BRI"),))
    # Bea, Brighton only, sends to "All sites · Ops": still Brighton only
    assert await _narrowed(brighton, GroupSites(True, ())) == brighton
    # Bea sends to a London group: nothing is common, so nothing is read
    none = await _narrowed(brighton, GroupSites(False, ("LON",)))
    assert (none.all_sites, none.site_ids) == (False, ()) and not none.covers("BRI")
    assert await _narrowed(everything, GroupSites(True, ())) == everything


async def test_a_routine_for_a_groups_channel_runs_inside_the_groups_sites(monkeypatch):
    stock = await _make("Stock", GroupSites(False, ("BRI",)), topics=("schedule",))
    seen = {}

    async def answered(job, history, progress=None, within=None):
        seen["within"] = within
        return {"answer": "42 open", "blocks": []}, "trace-1"

    async def allowed(job):
        return None

    monkeypatch.setattr(teams_consumer, "_run_agent_traced", answered)
    monkeypatch.setattr(teams_consumer, "_check_access", allowed)

    class Task:
        id, customer_code, owner_object_id, owner_name = uuid.uuid4(), CC, "u-ops", "Olu"
        query_text, title, site_id = "how many picks are open", "Open picks", "all"
        target, target_group_id = "channel", uuid.UUID(stock)

    await teams_consumer._run_task_query(Task())
    assert seen["within"] == GroupSites(False, ("BRI",))
    # the same routine sent to the owner alone is not narrowed by any group
    Task.target, Task.target_group_id = "me", None
    await teams_consumer._run_task_query(Task())
    assert seen["within"] is None


async def test_the_asker_reads_inside_the_group_when_one_is_named():
    job = _job("u-ops", "group_join", {})  # any job of Olu: only who asks matters here
    _, wide = await teams_consumer._asker(job)
    _, narrow = await teams_consumer._asker(job, within=GroupSites(False, ("BRI",)))
    assert wide.all_sites and narrow.site_ids == ("BRI",) and not narrow.covers("LON")


# ==================================================== 6. the rows the edge reads

async def test_the_group_rows_carry_the_sites_and_the_full_name():
    group_id = await _make("Stock", GroupSites(False, ("BRI", "BIC")))
    mirror = InMemoryMirrorWriter()
    await commands.run(_job("u-ops", "group_join", {"group_id": group_id}), mirror=mirror,
                       now=NOW)
    row = json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)["groups"][0]
    assert (row["name"], row["display_name"], row["sites_label"]) == (
        "Stock", "BIC, BRI · Stock", "BIC, BRI")
    assert (row["all_sites"], row["site_ids"]) == (False, ["BIC", "BRI"])
    mine = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-ops#GROUPS")].payload_json)["groups"]
    assert mine[0]["display_name"] == "BIC, BRI · Stock"
    async with async_session() as db:
        payload = await groups_mirror.groups_payload(db, CC, now=NOW)
        route = await store.group_route(db, CC, group_id)
    assert payload["groups"][0]["site_ids"] == ["BIC", "BRI"]
    # the card that lands in the channel names the group in full
    assert route.group_names == ("BIC, BRI · Stock",)
