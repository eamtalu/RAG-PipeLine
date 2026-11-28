"""Chunk 246: the defaults an Admin sets from Teams (program step 4, slice 4).

Three things needed a developer or the person themselves until now: what "Default" shows on
a page for everyone of a role, how many days a line search looks back when a question names
no time range (one server setting for every customer), and a person's saved views, which only
that person could remove.

Pinned here
-----------
    role default   a role holds one whole page as its default (`<page>_default`); a person's
                   row cannot hold it, and a role cannot hold a list of views
    the row        a person reads their role's page under `defaults`, their own saved view
                   still decides what the page opens on
    search range   one value for the whole customer, 1 to 14 days, read by the line search;
                   without it the server setting stays
    reset          someone who may manage users resets another person's views of a page
    who            all three need the capability to manage users, and each is one audit line
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
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.preference import Preference
from app.persistence.models.tenant_site import TenantSite
from app.services import access, preferences
from app.services.access.people import users
from app.services.log_agent import tools
from app.services.preferences import areas
from app.services.teams.command_outcome import Refused
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from app.settings import settings

CC = "test_c246"
NOW = datetime(2026, 11, 27, 23, 0, tzinfo=timezone.utc)
PICKER_PAGE = {**areas.page_defaults("advisories"), "severities": ["high"],
               "group_by": "location"}
VIEW = {"id": "v1", "name": "Stock", "settings": {"kinds": ["count_gap"]}}


async def _wipe() -> None:
    async with async_session() as s:
        for model in (LogEntry, Job, Preference, Customer):
            await s.execute(delete(model).where(model.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def people():
    """A customer with a supervisor, a picker and an Admin."""
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, display_name="C246"))
        db.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        await db.flush()
        for object_id, name, role, all_sites in (("u-sup", "Sam Super", "supervisor", False),
                                                 ("u-pic", "Pat Picker", "picker", False),
                                                 ("u-adm", "Ada Admin", "admin", True)):
            await users.create_user(
                db, CC, object_id, actor=users.Actor.system("superadmin"), now=NOW,
                change=users.UserChange(display_name=name, role_code=role,
                                        site_ids=() if all_sites else ("BRI",),
                                        all_sites=all_sites))
        await db.commit()
    yield
    await _wipe()


def _job(sender: str, kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                       conversation_id="c", sender_object_id=sender, sender_name=sender,
                       question=f"preferences: {kind}", source="tab",
                       command=Command(kind=kind, by=sender, payload=payload))


def _row(mirror: InMemoryMirrorWriter, object_id: str) -> dict:
    return json.loads(mirror.items[(f"EYE#{CC}", f"USER#{object_id}#PREFS")].payload_json)


async def _audit() -> list[tuple[str, str, str]]:
    async with async_session() as db:
        return [(e.action, e.subject_kind, e.subject_id) for e in (await db.execute(
            select(AuditEvent).where(AuditEvent.customer_code == CC,
                                           AuditEvent.action.like("preference_%"))
            .order_by(AuditEvent.at))).scalars()]


# ==================================================== 1. a role's default view of a page

def test_a_role_holds_one_whole_page_and_nothing_else_does():
    assert areas.default_area("advisories") == "advisories_default"
    checked = areas.validate("advisories_default", {"severities": ["high"]}, "role")
    assert checked == {"severities": ["high"]}
    with pytest.raises(ValueError, match="role"):
        areas.validate("advisories_default", PICKER_PAGE, "user")
    # a list of views is a person's; a role's and a person's lists cannot be merged
    with pytest.raises(ValueError, match="person"):
        areas.validate("advisories", {"use": "v1", "views": [VIEW]}, "role")
    with pytest.raises(ValueError, match="group_by"):
        areas.validate("advisories_default", {"group_by": "weather"}, "role")


async def test_people_of_the_role_read_its_page_as_their_default():
    mirror = InMemoryMirrorWriter()
    outcome = await preferences.run(_job("u-adm", "preference_set", {
        "area": "advisories_default", "role": "picker", "settings": PICKER_PAGE}), mirror=mirror)
    assert outcome.text == "Default view of advisories set for Picker"
    # the picker is mirrored at once and reads the role's page; nothing of it is "their own"
    row = _row(mirror, "u-pic")
    assert row["defaults"]["advisories"] == PICKER_PAGE
    assert row["own"] == {} and row["areas"]["advisories"] == {"use": "default", "views": []}
    # a supervisor still reads the built-in page
    async with async_session() as db:
        sup = await preferences.user_payload(db, await access.access_record_for(db, CC, "u-sup"))
    assert sup["defaults"]["advisories"] == areas.page_defaults("advisories")
    assert await _audit() == [("preference_set", "preference_role", "picker")]


async def test_a_persons_own_view_still_decides_what_the_page_opens_on():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-pic", "preference_set", {
        "area": "advisories", "settings": {"use": "v1", "views": [VIEW]}}), mirror=mirror)
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "advisories_default", "role": "picker", "settings": PICKER_PAGE}), mirror=mirror)
    row = _row(mirror, "u-pic")
    assert row["areas"]["advisories"]["use"] == "v1"
    assert row["defaults"]["advisories"]["group_by"] == "location"


async def test_resetting_the_roles_default_brings_the_built_in_page_back():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "advisories_default", "role": "picker", "settings": PICKER_PAGE}), mirror=mirror)
    outcome = await preferences.run(_job("u-adm", "preference_reset", {
        "area": "advisories_default", "role": "picker"}), mirror=mirror)
    assert outcome.text == "Default view of advisories for Picker is the built-in one again"
    assert _row(mirror, "u-pic")["defaults"]["advisories"] == areas.page_defaults("advisories")


async def test_a_roles_default_needs_manage_users_a_real_role_and_the_right_area():
    payload = {"area": "advisories_default", "role": "picker", "settings": PICKER_PAGE}
    with pytest.raises(Refused, match="manage users"):
        await preferences.run(_job("u-sup", "preference_set", payload),
                              mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="no role"):
        await preferences.run(_job("u-adm", "preference_set", {**payload, "role": "wizard"}),
                              mirror=InMemoryMirrorWriter())
    # nobody stores a page default as their own
    with pytest.raises(Refused, match="role"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "advisories_default", "settings": PICKER_PAGE}),
            mirror=InMemoryMirrorWriter())
    assert await _audit() == []


async def test_someone_who_manages_users_sees_which_roles_have_a_default():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "advisories_default", "role": "picker", "settings": PICKER_PAGE}), mirror=mirror)
    async with async_session() as db:
        adm = await preferences.user_payload(db, await access.access_record_for(db, CC, "u-adm"))
        pic = await preferences.user_payload(db, await access.access_record_for(db, CC, "u-pic"))
    assert adm["role_defaults"] == {"advisories": ["picker"]}
    assert "role_defaults" not in pic


# ==================================================== 2. the line search range

async def _resolved_days(object_id: str) -> int:
    async with async_session() as db:
        record = await access.access_record_for(db, CC, object_id)
        return (await preferences.resolve(db, record))["ask"]["line_search_days"]


async def test_the_customers_range_reaches_everyone_and_is_one_audit_line(monkeypatch):
    monkeypatch.setattr(settings, "log_search_default_days", 7)
    assert await _resolved_days("u-pic") == 7
    mirror = InMemoryMirrorWriter()
    outcome = await preferences.run(_job("u-adm", "preference_set", {
        "area": "ask", "customer": True, "settings": {"line_search_days": 10}}), mirror=mirror)
    assert outcome.text == "Line search now covers the last 10 days"
    # everyone active is mirrored, with the limit the screen needs
    for person in ("u-pic", "u-sup", "u-adm"):
        row = _row(mirror, person)
        assert row["areas"]["ask"] == {"line_search_days": 10}
        # the routine limit's own maximum joined with chunk 251
        assert row["limits"] == {"line_search_days_max": 14, "routines_max_active_max": 100}
    assert await _audit() == [("preference_set", "preference_customer", CC)]
    # back to the server's setting
    outcome = await preferences.run(_job("u-adm", "preference_reset", {
        "area": "ask", "customer": True}), mirror=mirror)
    assert outcome.text == "Line search covers the last 7 days again"
    assert await _resolved_days("u-pic") == 7


async def test_the_range_has_a_hard_limit_and_belongs_to_the_whole_customer():
    with pytest.raises(Refused, match="14"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "ask", "customer": True, "settings": {"line_search_days": 15}}),
            mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="line_search_days"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "ask", "customer": True, "settings": {"line_search_days": 0}}),
            mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="manage users"):
        await preferences.run(_job("u-sup", "preference_set", {
            "area": "ask", "customer": True, "settings": {"line_search_days": 10}}),
            mirror=InMemoryMirrorWriter())
    # not a person's own choice, and not a role's, in this slice
    for extra in ({}, {"role": "picker"}):
        with pytest.raises(Refused, match="whole customer"):
            await preferences.run(_job("u-adm", "preference_set", {
                "area": "ask", "settings": {"line_search_days": 10}, **extra}),
                mirror=InMemoryMirrorWriter())
    # the customer level holds the search range only
    with pytest.raises(Refused, match="customer"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "app", "customer": True, "settings": {"is_history_open": False}}),
            mirror=InMemoryMirrorWriter())


async def _plant(message: str, days_ago: int) -> None:
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogEntry(customer_code=CC, job_id=job.id, source_file="t.log", line_number=1,
                        timestamp=datetime.now(timezone.utc) - timedelta(days=days_ago, hours=1),
                        level="INFO", entry_type=LogEntryType.info, message=message,
                        entry_hash=uuid.uuid4().hex))
        await db.commit()


async def test_a_line_search_with_no_time_range_uses_the_customers_days(monkeypatch):
    monkeypatch.setattr(settings, "log_search_default_days", 7)
    await _plant("range-word nine days old", 9)
    async with async_session() as db:
        before = await tools._search_entries(db, {"q": "range-word"}, CC)
    assert before["entries"] == [] and before["window"]["days"] == 7
    await preferences.run(_job("u-adm", "preference_set", {
        "area": "ask", "customer": True, "settings": {"line_search_days": 12}}),
        mirror=InMemoryMirrorWriter())
    async with async_session() as db:
        after = await tools._search_entries(db, {"q": "range-word"}, CC)
        assert await preferences.line_search_days(db, "another-customer") == 7
    assert [e["message"] for e in after["entries"]] == ["range-word nine days old"]
    assert after["window"]["days"] == 12 and "12 days" in after["note"]


# ==================================================== 3. resetting a person's views

async def test_someone_who_manages_users_resets_another_persons_views():
    mirror = InMemoryMirrorWriter()
    await preferences.run(_job("u-pic", "preference_set", {
        "area": "advisories", "settings": {"use": "v1", "views": [VIEW]}}), mirror=mirror)
    with pytest.raises(Refused, match="manage users"):
        await preferences.run(_job("u-sup", "preference_reset", {
            "area": "advisories", "user": "u-pic"}), mirror=mirror)
    assert _row(mirror, "u-pic")["own"]["advisories"]["use"] == "v1"
    outcome = await preferences.run(_job("u-adm", "preference_reset", {
        "area": "advisories", "user": "u-pic"}), mirror=mirror)
    assert outcome.text == "Pat Picker's advisories views were reset"
    # the person's row follows at once
    assert _row(mirror, "u-pic")["own"] == {}
    assert await _audit() == [("preference_reset", "preference_user", "u-pic")]


async def test_nobody_writes_another_persons_views_or_resets_a_stranger():
    with pytest.raises(Refused, match="only be reset"):
        await preferences.run(_job("u-adm", "preference_set", {
            "area": "advisories", "user": "u-pic",
            "settings": {"use": "v1", "views": [VIEW]}}), mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="no such person"):
        await preferences.run(_job("u-adm", "preference_reset", {
            "area": "advisories", "user": "u-ghost"}), mirror=InMemoryMirrorWriter())
    with pytest.raises(Refused, match="one of"):
        await preferences.run(_job("u-adm", "preference_reset", {
            "area": "advisories", "user": "u-pic", "role": "picker"}),
            mirror=InMemoryMirrorWriter())


# ==================================================== 4. the sweep

async def test_the_sweep_reaches_everyone_when_the_customer_has_a_value():
    async with async_session() as db:
        db.add(Preference(customer_code=CC, subject_kind="customer", subject_id=CC, area="ask",
                          settings={"line_search_days": 9}, updated_at=NOW))
        await db.commit()
    mirror = InMemoryMirrorWriter()
    assert await preferences.sweep_once(mirror, only=[CC]) == {CC: 3}
    assert _row(mirror, "u-sup")["areas"]["ask"] == {"line_search_days": 9}
