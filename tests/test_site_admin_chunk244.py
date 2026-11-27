"""Chunk 244: an Admin sets up sites and their places from Teams (program step 4, slice 1).

A site is a named set of WMS company and warehouse pairs. Until now only two scripts on the
server could create a site or say which pairs it stands for. They become two commands of the
Admin tab, and one row tells the tab what exists and which pairs the data really carries.

Pinned here
-----------
    overview   the sites with their places, and the pairs seen in the data with the site each
               belongs to, the ones in no site marked
    a site     an Admin adds, renames and retires a site; a supervisor is refused
    places     a site's places are replaced as sent; a pair already in another site is refused
               with that site's name
    audit      every change is one audit line
    mirror     the Admin row and the switcher row follow a change at once
"""
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.access import AuditEvent
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.models.tenant_site import TenantSite, TenantSitePlace
from app.services.access.people import users
from app.services.access.tenancy import site_admin
from app.services.teams.command_outcome import Refused
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.mirror_rows import KEY_LAYOUT

CC = "test_c244"
NOW = datetime.now(timezone.utc).replace(microsecond=0)


async def _wipe() -> None:
    async with async_session() as s:
        for model in (LogTransaction, Job):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def customer():
    """One site BRI = 915/BRI, an Admin and a supervisor, and transactions at three pairs."""
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, display_name="C244"))
        db.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        await db.flush()
        db.add(TenantSitePlace(customer_code=CC, site_id="BRI", company="915", warehouse="BRI"))
        for object_id, role, sites, all_sites in (("u-adm", "admin", (), True),
                                                  ("u-sup", "supervisor", ("BRI",), False)):
            await users.create_user(
                db, CC, object_id, actor=users.Actor.system("superadmin"), now=NOW,
                change=users.UserChange(display_name=object_id, role_code=role, site_ids=sites,
                                        all_sites=all_sites))
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        # 3 calls at 915/BRI, 2 at 915/LON, 1 at 920/BRI, 1 with no warehouse, 1 too old
        for company, warehouse, days in (("915", "BRI", 1), ("915", "BRI", 2), ("915", "BRI", 3),
                                         ("915", "LON", 1), ("915", "LON", 5), ("920", "BRI", 2),
                                         (None, None, 1), ("915", "OLD", 45)):
            at = NOW - timedelta(days=days)
            db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=at,
                                  ended_at=at, date=at.date(), method="Pick", company=company,
                                  warehouse=warehouse, status=LogTransactionStatus.success))
        await db.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"admin: {kind}", source="tab",
                       command=Command(kind=kind, by=sender, payload=payload))


async def _overview() -> dict:
    async with async_session() as db:
        return await site_admin.overview(db, CC, now=NOW)


async def _audit_actions() -> list[str]:
    async with async_session() as db:
        return list((await db.execute(select(AuditEvent.action).where(
            AuditEvent.customer_code == CC, AuditEvent.action.like("sites.%"))
            .order_by(AuditEvent.at))).scalars())


# ==================================================== 1. the overview

async def test_the_overview_lists_sites_places_and_the_pairs_the_data_carries():
    view = await _overview()
    assert [(s["site_id"], s["name"], s["active"], s["places"]) for s in view["sites"]] == [
        ("BRI", "Brighton", True, [{"company": "915", "warehouse": "BRI"}])]
    # the last 30 days, busiest first; the pair of 45 days ago and the call with no warehouse
    # are not offered
    assert [(p["company"], p["warehouse"], p["transactions"], p["site_id"])
            for p in view["pairs"]] == [("915", "BRI", 3, "BRI"), ("915", "LON", 2, None),
                                        ("920", "BRI", 1, None)]
    assert view["days"] == 30 and view["unplaced"] == 2


async def test_a_site_with_no_place_stands_for_its_own_warehouse_under_any_company():
    async with async_session() as db:
        db.add(TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2))
        await db.commit()
    view = await _overview()
    london = next(s for s in view["sites"] if s["site_id"] == "LON")
    assert london["places"] == [] and london["stands_for_own_warehouse"] is True
    assert {(p["company"], p["warehouse"]): p["site_id"] for p in view["pairs"]}[
        ("915", "LON")] == "LON"


# ==================================================== 2. a site

async def test_an_admin_adds_renames_and_retires_a_site():
    mirror = InMemoryMirrorWriter()
    out = await site_admin.run(_job("u-adm", "site_set", {"site_id": "LON", "name": "London"}),
                               mirror=mirror)
    assert "London" in out.text
    await site_admin.run(_job("u-adm", "site_set", {
        "site_id": "LON", "name": "London Park Royal"}), mirror=mirror)
    await site_admin.run(_job("u-adm", "site_set", {"site_id": "LON", "active": False}),
                         mirror=mirror)
    london = next(s for s in (await _overview())["sites"] if s["site_id"] == "LON")
    assert london["name"] == "London Park Royal" and london["active"] is False
    assert await _audit_actions() == ["sites.site_added", "sites.site_changed",
                                      "sites.site_retired"]


async def test_the_command_reaches_the_service_through_the_dispatcher():
    out = await run_command(_job("u-adm", "site_set", {"site_id": "BIC", "name": "Bicester"}))
    assert "Bicester" in out.text


@pytest.mark.parametrize("payload, words", [
    ({"site_id": "lon don", "name": "London"}, "site id"),
    ({"site_id": "LON"}, "name"),                              # a new site needs a name
    ({"site_id": "LON", "name": "x" * 65}, "name"),
])
async def test_a_wrong_site_is_refused(payload, words):
    with pytest.raises(Refused, match=words):
        await site_admin.run(_job("u-adm", "site_set", payload), mirror=InMemoryMirrorWriter())


async def test_a_supervisor_may_not_change_sites():
    for kind, payload in (("site_set", {"site_id": "LON", "name": "London"}),
                          ("site_places_set", {"site_id": "BRI", "places": []})):
        with pytest.raises(Refused, match="manage users"):
            await site_admin.run(_job("u-sup", kind, payload), mirror=InMemoryMirrorWriter())
    assert await _audit_actions() == []


# ==================================================== 3. places

async def test_a_sites_places_are_replaced_as_sent():
    await site_admin.run(_job("u-adm", "site_places_set", {"site_id": "BRI", "places": [
        {"company": "915", "warehouse": "BRI"}, {"company": "920", "warehouse": "BRI"}]}),
        mirror=InMemoryMirrorWriter())
    view = await _overview()
    assert view["sites"][0]["places"] == [{"company": "915", "warehouse": "BRI"},
                                          {"company": "920", "warehouse": "BRI"}]
    assert view["unplaced"] == 1 and await _audit_actions() == ["sites.places_set"]


async def test_a_pair_already_in_another_site_is_refused_with_that_sites_name():
    await site_admin.run(_job("u-adm", "site_set", {"site_id": "LON", "name": "London"}),
                         mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="Brighton"):
        await site_admin.run(_job("u-adm", "site_places_set", {"site_id": "LON", "places": [
            {"company": "915", "warehouse": "BRI"}]}), mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="not a site"):
        await site_admin.run(_job("u-adm", "site_places_set", {"site_id": "MARS", "places": []}),
                             mirror=InMemoryMirrorWriter())


# ==================================================== 4. the mirror

async def test_the_admin_row_and_the_switcher_row_follow_a_change():
    assert KEY_LAYOUT["site_admin"].key(CC) == (f"EYE#{CC}", "SITES#ADMIN")
    mirror = InMemoryMirrorWriter()
    await site_admin.run(_job("u-adm", "site_set", {"site_id": "LON", "name": "London"}),
                         mirror=mirror)
    admin_row = json.loads(mirror.items[(f"EYE#{CC}", "SITES#ADMIN")].payload_json)
    assert [s["site_id"] for s in admin_row["sites"]] == ["BRI", "LON"]
    assert (f"EYE#{CC}", "SITES") in mirror.items


async def test_the_sweep_writes_the_admin_row_before_anyone_has_changed_anything():
    from app.services.teams.mirror import ChangeTracker
    mirror, tracker = InMemoryMirrorWriter(), ChangeTracker(max_age_seconds=3600)
    assert await site_admin.sweep_once(mirror, only=[CC], tracker=tracker) == {CC: 2}
    row = json.loads(mirror.items[(f"EYE#{CC}", "SITES#ADMIN")].payload_json)
    assert [p["warehouse"] for p in row["pairs"]] == ["BRI", "LON", "BRI"]
    # unchanged: nothing is written again
    assert await site_admin.sweep_once(mirror, only=[CC], tracker=tracker) == {CC: 0}

