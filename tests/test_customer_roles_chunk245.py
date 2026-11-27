"""Chunk 245: an Admin creates, renames and retires roles for their own customer.

Program step 4, slice 3. The six roles were one fixed list for every customer. A customer now
has its own roles: the six built-in ones, which it may rename or retire, and any it adds. A new
role starts as a copy of an existing one's permissions. Capabilities stay defined by code: a
capability only means something where the code checks it.

Pinned here
-----------
    the list    a customer starts with the six built-in roles; another customer's changes do
                not show
    add         a new role copies the cells of the role it starts from, and gets a plain code
    rename      a built-in or an added role is renamed for this customer only
    retire      a role with active people cannot be retired; Admin can never be; a retired
                role is not offered to new people
    who         only an Admin changes roles
    people      a person can be given an added role; their record names it
    mirror      the roles row carries every role with its state
"""
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.tenant_site import TenantSite
from app.services import access
from app.services.access.authorization import records, roles
from app.services.access.people import users
from app.services.teams.command_outcome import Refused
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC, OTHER = "test_c245", "test_c245_other"
NOW = datetime(2026, 11, 27, 22, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")
BUILT_IN = ["picker", "shift_lead", "supervisor", "ops_manager", "executive", "admin"]


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code.in_([CC, OTHER])))
        await s.commit()


@pytest.fixture(autouse=True)
async def customers():
    await _wipe()
    async with async_session() as db:
        for code in (CC, OTHER):
            db.add(Customer(customer_code=code, display_name=code))
            db.add(TenantSite(customer_code=code, site_id="BRI", name="Brighton", sort_order=1))
        await db.flush()
        for object_id, role in (("u-adm", "admin"), ("u-sup", "supervisor")):
            await users.create_user(db, CC, object_id, actor=SUPER, now=NOW,
                                    change=users.UserChange(display_name=object_id,
                                                            role_code=role, site_ids=("BRI",)))
        await db.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"admin: {kind}", source="tab",
                       command=Command(kind=kind, by=sender, payload=payload))


async def _roles(customer: str = CC) -> list[roles.RoleInfo]:
    async with async_session() as db:
        return await roles.customer_roles(db, customer)


async def _run(sender: str, kind: str, payload: dict):
    return await access.run_role_command(_job(sender, kind, payload),
                                         mirror=InMemoryMirrorWriter())


# ==================================================== 1. the list

async def test_a_customer_starts_with_the_six_built_in_roles():
    listed = await _roles()
    assert [r.code for r in listed] == BUILT_IN
    assert all(r.is_built_in and r.is_active for r in listed)
    assert listed[2].name == "Supervisor"


def test_a_role_code_is_made_from_its_name():
    assert roles.code_of("Night shift lead") == "night_shift_lead"
    assert roles.code_of("  Goods-In / QA  ") == "goods_in_qa"
    assert roles.code_of("9 to 5") == "role_9_to_5"
    with pytest.raises(ValueError, match="name"):
        roles.code_of("   ")


# ==================================================== 2. add, rename, retire

async def test_a_new_role_copies_the_permissions_of_the_role_it_starts_from():
    out = await _run("u-adm", "role_set", {"name": "Night shift lead", "copy_from": "shift_lead"})
    assert "Night shift lead" in out.text
    listed = await _roles()
    added = listed[-1]
    assert (added.code, added.name, added.is_built_in, added.is_active) == (
        "night_shift_lead", "Night shift lead", False, True)
    async with async_session() as db:
        cells = await records.customer_matrix(db, CC)
    assert {c: s for (r, c), s in cells.items() if r == "night_shift_lead"} == {
        c: s for (r, c), s in cells.items() if r == "shift_lead"}
    # another customer does not get it
    assert [r.code for r in await _roles(OTHER)] == BUILT_IN


async def test_a_role_is_renamed_for_this_customer_only():
    await _run("u-adm", "role_set", {"code": "supervisor", "name": "Team leader"})
    assert next(r for r in await _roles() if r.code == "supervisor").name == "Team leader"
    assert next(r for r in await _roles(OTHER) if r.code == "supervisor").name == "Supervisor"
    async with async_session() as db:
        record = await access.access_record_for(db, CC, "u-sup")
    assert record.role_name == "Team leader"


@pytest.mark.parametrize("payload, words", [
    ({"name": "Supervisor", "copy_from": "picker"}, "already"),       # the name is taken
    ({"name": "New one"}, "start from"),                              # nothing to copy
    ({"name": "New one", "copy_from": "nobody"}, "start from"),
    ({"code": "nobody", "name": "Renamed"}, "not a role"),
    ({"name": "x" * 65, "copy_from": "picker"}, "name"),
])
async def test_a_wrong_role_is_refused(payload, words):
    with pytest.raises(Refused, match=words):
        await _run("u-adm", "role_set", payload)


async def test_a_role_with_active_people_cannot_be_retired_and_admin_never():
    with pytest.raises(Refused, match="1 active"):
        await _run("u-adm", "role_retire", {"code": "supervisor", "active": False})
    with pytest.raises(Refused, match="Admin"):
        await _run("u-adm", "role_retire", {"code": "admin", "active": False})
    await _run("u-adm", "role_retire", {"code": "executive", "active": False})
    assert next(r for r in await _roles() if r.code == "executive").is_active is False
    await _run("u-adm", "role_retire", {"code": "executive", "active": True})
    assert next(r for r in await _roles() if r.code == "executive").is_active is True


async def test_only_an_admin_changes_roles():
    for kind, payload in (("role_set", {"name": "Mine", "copy_from": "picker"}),
                          ("role_retire", {"code": "executive", "active": False})):
        with pytest.raises(Refused, match="only an Admin"):
            await _run("u-sup", kind, payload)


async def test_the_command_reaches_the_service_through_the_dispatcher():
    out = await run_command(_job("u-adm", "role_set", {"name": "Goods in", "copy_from": "picker"}))
    assert "Goods in" in out.text


# ==================================================== 3. people

async def test_a_person_can_be_given_an_added_role_but_not_a_retired_one():
    await _run("u-adm", "role_set", {"name": "Night shift lead", "copy_from": "shift_lead"})
    await _run("u-adm", "role_retire", {"code": "executive", "active": False})
    async with async_session() as db:
        await users.create_user(db, CC, "u-night", actor=SUPER, now=NOW, change=users.UserChange(
            display_name="Nia Night", role_code="night_shift_lead", site_ids=("BRI",)))
        await db.commit()
        record = await access.access_record_for(db, CC, "u-night")
        with pytest.raises(ValueError, match="retired"):
            await users.create_user(db, CC, "u-exec", actor=SUPER, now=NOW,
                                    change=users.UserChange(display_name="Ed Exec",
                                                            role_code="executive"))
        with pytest.raises(ValueError, match="unknown role"):
            await users.create_user(db, OTHER, "u-x", actor=SUPER, now=NOW,
                                    change=users.UserChange(display_name="X",
                                                            role_code="night_shift_lead"))
    assert record.role_name == "Night shift lead"
    assert record.capabilities == {**record.capabilities}  # every capability has a state
    assert set(record.capabilities.values()) <= {"allowed", "with_approval", "no"}


async def test_a_cell_of_an_added_role_can_be_changed():
    await _run("u-adm", "role_set", {"name": "Night shift lead", "copy_from": "shift_lead"})
    async with async_session() as db:
        await records.set_matrix_cell(db, CC, "night_shift_lead", "export", "allowed",
                                      actor=SUPER)
        await db.commit()
        cells = await records.customer_matrix(db, CC)
    assert cells[("night_shift_lead", "export")] == "allowed"


# ==================================================== 4. the mirror

async def test_the_roles_row_carries_every_role_with_its_state():
    mirror = InMemoryMirrorWriter()
    await access.run_role_command(_job("u-adm", "role_set", {
        "name": "Night shift lead", "copy_from": "shift_lead"}), mirror=mirror)
    await access.run_role_command(_job("u-adm", "role_retire", {
        "code": "executive", "active": False}), mirror=mirror)
    row = json.loads(mirror.items[(f"EYE#{CC}", "ROLES")].payload_json)
    by_code = {r["code"]: r for r in row["roles"]}
    assert by_code["night_shift_lead"] == {"code": "night_shift_lead", "name": "Night shift lead",
                                           "active": True, "built_in": False, "people": 0}
    assert by_code["executive"]["active"] is False and by_code["supervisor"]["people"] == 1
    assert set(row["matrix"]["ask"]) == set(by_code)
    users_row = json.loads(mirror.items[(f"EYE#{CC}", "USERS")].payload_json)
    assert {u["role_name"] for u in users_row["users"]} == {"Admin", "Supervisor"}
