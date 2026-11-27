"""Chunk 242: a person's saved views of a page, and which one the page opens on.

Program step 2, rework after the owner tried it live (27 Nov 2026): preferences belong on
each page, with a visible switch between "Default" and what the person saved. A page's
preference is therefore a short list of named views plus `use`, the one the page opens on.
Nothing is saved field by field any more; a view is saved whole.

Pinned here
-----------
    the shape    a page holds up to five named views and `use`; a view's settings are checked
    use          `use` names "default" or one of the saved views, never something else
    save         saving replaces the list as sent; removing the last view returns to default
    mirror       the person's row also carries each page's default view
    conversion   what was saved under the two old Advisories areas becomes one view
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
from app.services import access, preferences
from app.services.access.people import users
from app.services.preferences import areas, conversion
from app.services.teams.command_outcome import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c242"
NOW = datetime(2026, 11, 27, 20, 0, tzinfo=timezone.utc)
VIEW = {"id": "v1", "name": "Stock, high only",
        "settings": {"kinds": ["count_gap"], "severities": ["high"], "group_by": "area"}}


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(Preference).where(Preference.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def person():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, display_name="C242"))
        db.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        await db.flush()
        await users.create_user(db, CC, "u-sup", actor=users.Actor.system("superadmin"), now=NOW,
                                change=users.UserChange(display_name="Sam Super",
                                                        role_code="supervisor", site_ids=("BRI",)))
        await db.commit()
    yield
    await _wipe()


def _job(kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id="u-sup", sender_name="Sam",
                       question=f"preferences: {kind}", source="tab",
                       command=Command(kind=kind, by="Sam", payload=payload))


async def _resolved() -> dict:
    async with async_session() as db:
        record = await access.access_record_for(db, CC, "u-sup")
        return await preferences.resolve(db, record)


# ==================================================== 1. the shape

def test_a_page_starts_on_the_default_with_no_saved_view():
    assert areas.defaults("advisories") == {"use": "default", "views": []}
    assert areas.page_defaults("advisories") == {
        "status": "open", "severities": ["high", "medium", "low"], "agents": [],
        "holder": "anyone", "kinds": [], "group_by": "kind", "sort_by": "severity",
        "reversed": False}
    assert set(areas.PAGES) >= {"advisories"}


def test_a_saved_view_is_checked_and_completed():
    checked = areas.validate("advisories", {"use": "v1", "views": [VIEW]})
    assert checked["use"] == "v1"
    assert checked["views"][0]["settings"]["severities"] == ["high"]
    # a field the view did not name takes the page's default, so a view is always whole
    assert checked["views"][0]["settings"]["sort_by"] == "severity"


@pytest.mark.parametrize("settings, words", [
    ({"use": "v9", "views": [VIEW]}, "use"),                                   # not a saved view
    ({"use": "default", "views": [VIEW, VIEW]}, "id"),                         # the same id twice
    ({"use": "default", "views": [{**VIEW, "name": ""}]}, "name"),
    ({"use": "default", "views": [{**VIEW, "name": "x" * 41}]}, "name"),
    ({"use": "default", "views": [{**VIEW, "settings": {"severities": []}}]}, "severities"),
    ({"use": "default", "views": [{**VIEW, "settings": {"group_by": "colour"}}]}, "group_by"),
    ({"use": "default", "views": [{**VIEW, "id": f"v{i}"} for i in range(6)]}, "views"),
])
def test_a_wrong_view_is_refused_and_the_message_names_the_field(settings, words):
    with pytest.raises(ValueError, match=words):
        areas.validate("advisories", settings)


def test_the_two_old_advisories_areas_are_gone():
    assert "advisories_view" not in areas.AREAS and "advisories_focus" not in areas.AREAS


# ==================================================== 2. saving

async def test_a_view_is_saved_whole_and_use_alone_can_be_switched():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("preference_set", {
        "area": "advisories", "settings": {"use": "v1", "views": [VIEW]}}), mirror=mirror)
    assert (await _resolved())["advisories"]["use"] == "v1"
    # switching to the default keeps the saved view
    await preferences.run(_job("preference_set", {
        "area": "advisories", "settings": {"use": "default"}}), mirror=mirror)
    resolved = (await _resolved())["advisories"]
    assert resolved["use"] == "default" and [v["id"] for v in resolved["views"]] == ["v1"]


async def test_use_cannot_name_a_view_that_is_not_saved():
    with pytest.raises(Refused, match="use"):
        await preferences.run(_job("preference_set", {
            "area": "advisories", "settings": {"use": "v1"}}), mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="use"):
        # removing the view that is in use, without saying what to use instead
        await preferences.run(_job("preference_set", {
            "area": "advisories", "settings": {"use": "v1", "views": []}}),
            mirror=InMemoryMirrorWriter())


async def test_the_persons_row_carries_each_pages_default_view():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("preference_set", {
        "area": "advisories", "settings": {"use": "v1", "views": [VIEW]}}), mirror=mirror)
    row = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#PREFS")].payload_json)
    assert row["defaults"]["advisories"] == areas.page_defaults("advisories")
    assert row["areas"]["advisories"]["views"][0]["name"] == "Stock, high only"
    assert row["own"]["advisories"]["use"] == "v1"


# ==================================================== 3. the conversion

def test_what_was_saved_under_the_old_areas_becomes_one_view():
    converted = conversion.advisories_from_old(
        view={"group_by": "area", "sort_by": "newest", "reversed": False},
        focus={"kinds": ["count_gap"], "severities": ["high"]})
    assert converted["use"] == converted["views"][0]["id"]
    assert converted["views"][0]["name"] == "My view"
    assert converted["views"][0]["settings"] == {
        "group_by": "area", "sort_by": "newest", "reversed": False,
        "kinds": ["count_gap"], "severities": ["high"]}
    areas.validate("advisories", converted)   # what the conversion writes is a valid page


def test_nothing_saved_converts_to_nothing():
    assert conversion.advisories_from_old(view=None, focus=None) is None
    # only a layout was saved, as for the owner on 27 Nov
    only_layout = conversion.advisories_from_old(view={"group_by": "area"}, focus=None)
    assert only_layout["views"][0]["settings"] == {"group_by": "area"}


async def test_the_rows_of_a_person_are_converted_and_the_old_ones_removed():
    async with async_session() as db:
        for area, settings in (("advisories_view", {"group_by": "area", "sort_by": "newest"}),
                               ("app", {"default_site": "BRI"})):
            db.add(Preference(customer_code=CC, subject_kind="user", subject_id="u-sup",
                              area=area, settings=settings, updated_at=NOW))
        await db.commit()
        assert await conversion.convert_old_advisories(db, customer_code=CC) == 1
        await db.commit()
        rows = {r.area: r.settings for r in (await db.execute(select(Preference).where(
            Preference.customer_code == CC))).scalars()}
    assert set(rows) == {"advisories", "app"}
    assert rows["advisories"]["views"][0]["settings"] == {"group_by": "area", "sort_by": "newest"}
    assert (await _resolved())["advisories"]["views"][0]["settings"]["group_by"] == "area"
