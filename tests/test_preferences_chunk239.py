"""Chunk 239: a person's and a role's preferences, kept in the backend (program step 2, slice 1).

Layer 3 of the model: what do I want to see first. Until now the Teams app kept the site, the
Ask history panel and the Advisories view in the browser only, so they were lost on another
device. They become rows of one table, saved by a command and mirrored to the edge as one row
per person. Since chunk 242 a page's preference is a list of saved views (see
tests/test_saved_views_chunk242.py); these tests keep to the `app` area, which is still saved
field by field. A preference never widens access: a value the person may no longer use is replaced
when it is read.

Pinned here
-----------
    areas        an unknown area or a wrong value is refused, and the message names the field
    resolve      built-in default, then the role's, then the person's own, field by field
    narrow       a default site outside the person's sites resolves to one of their own
    commands     a person saves and resets their own; a role's needs the capability to manage
                 users; nobody writes for another person
    mirror       the person's row holds the resolved values and is rewritten after a change
    boundary     access does not import preferences; the table is customer-wide data
"""
import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.preference import Preference
from app.persistence.models.tenant_site import TenantSite
from app.services import access, preferences, scoped_data
from app.services.access.people import users
from app.services.preferences import areas
from app.services.teams.command_outcome import Refused
from app.services.teams.commands import run_command
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.mirror_rows import KEY_LAYOUT

CC = "test_c239"
NOW = datetime(2026, 11, 27, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(Preference).where(Preference.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def people():
    """A customer with two sites, a Brighton supervisor, a two-site manager and an Admin."""
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, display_name="C239"))
        db.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                    TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await db.flush()
        for object_id, name, role, sites, all_sites in (
                ("u-sup", "Sam Super", "supervisor", ("BRI",), False),
                ("u-two", "Tia Two", "supervisor", ("BRI", "LON"), False),
                ("u-adm", "Ada Admin", "admin", (), True)):
            await users.create_user(db, CC, object_id, actor=SUPER, now=NOW,
            change=users.UserChange(
                display_name=name, role_code=role, site_ids=sites, all_sites=all_sites))
        await db.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"preferences: {kind}", source="tab",
                       command=Command(kind=kind, by=sender, payload=payload))


async def _resolved(object_id: str) -> dict:
    async with async_session() as db:
        record = await access.access_record_for(db, CC, object_id)
        return await preferences.resolve(db, record)


# ==================================================== 1. the areas

def test_an_area_checks_its_values_and_names_the_field():
    assert areas.validate("app", {"is_history_open": False}) == {"is_history_open": False}
    with pytest.raises(ValueError, match="is_history_open"):
        areas.validate("app", {"is_history_open": "perhaps"})
    with pytest.raises(ValueError, match="shoe_size"):
        areas.validate("app", {"shoe_size": 9})
    with pytest.raises(ValueError, match="unknown area"):
        areas.validate("weather", {})


def test_every_area_has_complete_built_in_defaults():
    # the app shows past conversations until the person hides them
    assert areas.defaults("app") == {"default_site": None, "is_history_open": True}
    # a page starts on its default with no saved view (chunk 242)
    assert areas.defaults("advisories") == {"use": "default", "views": []}


# ==================================================== 2. resolving

async def test_values_resolve_from_default_then_role_then_person_field_by_field():
    assert (await _resolved("u-two"))["app"] == areas.defaults("app")
    async with async_session() as db:
        await preferences.save(db, CC, "role", "supervisor", "app",
                               {"default_site": "BRI", "is_history_open": False},
                               by="u-adm", now=NOW)
        await preferences.save(db, CC, "user", "u-two", "app", {"default_site": "LON"},
                               by="u-two", now=NOW)
        await db.commit()
    # the person's site, the role's panel setting
    assert (await _resolved("u-two"))["app"] == {"default_site": "LON", "is_history_open": False}
    # another supervisor gets the role's values, an Admin the built-in ones
    assert (await _resolved("u-sup"))["app"] == {"default_site": "BRI", "is_history_open": False}
    assert (await _resolved("u-adm"))["app"] == areas.defaults("app")


async def test_a_second_save_changes_only_the_fields_it_names():
    async with async_session() as db:
        await preferences.save(db, CC, "user", "u-sup", "app", {"default_site": "BRI"},
                               by="u-sup", now=NOW)
        await preferences.save(db, CC, "user", "u-sup", "app", {"is_history_open": False},
                               by="u-sup", now=NOW)
        await db.commit()
        rows = (await db.execute(select(Preference).where(
            Preference.customer_code == CC))).scalars().all()
    assert len(rows) == 1
    assert rows[0].settings == {"default_site": "BRI", "is_history_open": False}


async def test_a_default_site_the_person_may_not_see_resolves_to_one_of_their_own():
    async with async_session() as db:
        await preferences.save(db, CC, "role", "supervisor", "app",
                               {"default_site": "LON"}, by="u-adm", now=NOW)
        await db.commit()
    assert (await _resolved("u-sup"))["app"]["default_site"] == "BRI"   # LON is not theirs
    assert (await _resolved("u-two"))["app"]["default_site"] == "LON"   # LON is theirs
    async with async_session() as db:
        await preferences.save(db, CC, "user", "u-sup", "app", {"default_site": "all"},
                               by="u-sup", now=NOW)
        await db.commit()
    assert (await _resolved("u-sup"))["app"]["default_site"] == "BRI"   # "all" needs all sites
    assert (await _resolved("u-adm"))["app"]["default_site"] is None    # nothing chosen


# ==================================================== 3. the commands

async def test_a_person_saves_and_resets_their_own_preferences_by_command():
    mirror = InMemoryMirrorWriter()
    out = await preferences.run(_job("u-sup", "preference_set", {
        "area": "app", "settings": {"is_history_open": False}}), mirror=mirror)
    assert "saved" in out.text.lower()
    assert (await _resolved("u-sup"))["app"]["is_history_open"] is False
    await preferences.run(_job("u-sup", "preference_reset", {"area": "app"}), mirror=mirror)
    assert (await _resolved("u-sup"))["app"] == areas.defaults("app")


async def test_the_command_reaches_the_service_through_the_dispatcher():
    out = await run_command(_job("u-two", "preference_set", {
        "area": "app", "settings": {"default_site": "LON", "is_history_open": True}}))
    assert "saved" in out.text.lower()
    assert (await _resolved("u-two"))["app"] == {"default_site": "LON", "is_history_open": True}


@pytest.mark.parametrize("payload, words", [
    ({"area": "app", "settings": {"is_history_open": "perhaps"}}, "is_history_open"),
    ({"area": "weather", "settings": {}}, "unknown area"),
    ({"area": "app", "settings": {"default_site": "MARS"}}, "MARS"),
    ({"area": "app", "settings": {"default_site": "LON"}}, "LON"),      # not Sam's site
])
async def test_a_wrong_value_is_refused_and_nothing_is_stored(payload, words):
    with pytest.raises(Refused, match=words):
        await preferences.run(_job("u-sup", "preference_set", payload),
                              mirror=InMemoryMirrorWriter())
    async with async_session() as db:
        assert (await db.execute(select(Preference).where(
            Preference.customer_code == CC))).scalars().all() == []


async def test_a_roles_defaults_need_the_capability_to_manage_users():
    role_payload = {"area": "app", "settings": {"is_history_open": False},
                    "role": "supervisor"}
    with pytest.raises(Refused, match="manage users"):
        await preferences.run(_job("u-sup", "preference_set", role_payload),
                              mirror=InMemoryMirrorWriter())
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-adm", "preference_set", role_payload), mirror=mirror)
    assert (await _resolved("u-sup"))["app"]["is_history_open"] is False
    # everyone with that role is mirrored again, because their resolved values moved
    written = {env.sk for env in mirror.items.values()}
    assert written == {"USER#u-sup#PREFS", "USER#u-two#PREFS"}


async def test_only_a_roles_defaults_are_written_to_the_audit_log():
    from app.persistence.models.access import AuditEvent

    async def lines() -> list[str]:
        async with async_session() as db:
            return list((await db.execute(select(AuditEvent.action).where(
                AuditEvent.customer_code == CC,
                AuditEvent.action.like("preference%")))).scalars())

    # a person changing their own view is not an access event; the Admin's list stays readable
    await preferences.run(_job("u-sup", "preference_set", {
        "area": "app", "settings": {"is_history_open": False}}), mirror=InMemoryMirrorWriter())
    assert await lines() == []
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "app", "settings": {"is_history_open": False}, "role": "supervisor"}),
        mirror=InMemoryMirrorWriter())
    assert await lines() == ["preference_set"]


async def test_a_person_without_an_active_record_is_refused():
    with pytest.raises(Refused, match="no active access record"):
        await preferences.run(_job("u-ghost", "preference_set", {
            "area": "app", "settings": {}}), mirror=InMemoryMirrorWriter())


# ==================================================== 4. the mirror row

async def test_the_persons_row_holds_the_resolved_values_and_follows_a_change():
    assert KEY_LAYOUT["user_prefs"].key(CC, user_id="u-sup") == (f"EYE#{CC}", "USER#u-sup#PREFS")
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-sup", "preference_set", {
        "area": "app", "settings": {"is_history_open": False}}), mirror=mirror)
    row = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#PREFS")].payload_json)
    assert row["areas"]["app"] == {"default_site": None, "is_history_open": False}
    assert row["own"] == {"app": {"is_history_open": False}}
    assert row["areas"]["advisories"] == {"use": "default", "views": []}


# ==================================================== 5. the boundary

def test_access_does_not_import_preferences():
    import pathlib
    root = pathlib.Path(access.__file__).parent
    offenders = [str(p) for p in root.rglob("*.py") if "preferences" in p.read_text()]
    assert offenders == []


def test_the_table_is_customer_wide_data_in_the_registry():
    rule = scoped_data.registry.BY_TABLE["preferences"]
    assert isinstance(rule, scoped_data.registry.CustomerWide)
