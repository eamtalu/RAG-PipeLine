"""Chunk 176: access control of the eSmart Eye Teams app (stage D, step 7, backend half).

The matrix of screen 07 as data, seeded per customer from the file and editable one cell at a
time; a person's record (role, sites, effective capabilities) and the one decision function both
sides use; users created and changed with an audit line each; approvals that grant a capability
for good; the mirror rows the edge reads; the consumer refusing a question before any model runs.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete

from app import teams_consumer
from app.api.v1 import teams_access as access_api
from app.config.database import async_session
from app.main import app
from app.persistence.models.access import AuditEvent, TeamsUser
from app.persistence.models.customer import Customer
from app.persistence.models.tenant_site import TenantSite
from app.services.access import approvals, audit, mirror as access_mirror, records, users
from app.services.access.matrix import ALLOWED, NO, WITH_APPROVAL, load
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.test_teams_consumer_chunk123 import Harness, job

CC = "test_c176"
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C176"))
        s.add_all([TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1),
                   TenantSite(customer_code=CC, site_id="LON", name="London", sort_order=2)])
        await s.commit()
    yield
    await _wipe()


async def _supervisor(db, object_id: str = "u-sup") -> TeamsUser:
    return await users.create_user(
        db, CC, object_id, actor=SUPER, now=NOW,
        change=users.UserChange(display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))


# ==================================================== 1. the matrix


async def test_the_matrix_is_seeded_from_the_file_once_and_a_cell_can_be_changed():
    m = load()
    assert m.default("supervisor", "export") == WITH_APPROVAL and m.default("picker", "ask") == WITH_APPROVAL
    async with async_session() as db:
        assert await records.ensure_matrix(db, CC) == 48
        assert await records.ensure_matrix(db, CC) == 0
        await records.set_matrix_cell(db, CC, "supervisor", "export", ALLOWED, actor=SUPER)
        await db.commit()
        cells = await records.customer_matrix(db, CC)
        assert cells[("supervisor", "export")] == ALLOWED and cells[("picker", "export")] == NO
        with pytest.raises(ValueError, match="unknown role"):
            await records.set_matrix_cell(db, CC, "boss", "export", ALLOWED, actor=SUPER)
        events = await audit.recent_events(db, CC)
        assert events[0].action == "access.matrix_changed" and "with_approval → allowed" in events[0].text


# ==================================================== 2. records and decisions


async def test_a_supervisor_sees_their_site_only_and_cannot_approve():
    async with async_session() as db:
        await _supervisor(db)
        await db.commit()
        record = await records.access_record_for(db, CC, "u-sup")
    assert record is not None and (record.role_name, record.site_ids) == ("Supervisor", ("BRI",))
    assert record.capabilities["ask"] == ALLOWED and record.capabilities["approve"] == NO
    assert records.decide(record, "ask", "BRI").is_allowed
    refused = records.decide(record, "ask", "LON")
    assert refused.outcome == "refused" and "outside" in refused.reason
    assert records.decide(record, "approve").outcome == "refused"
    assert records.decide(record, "view_all_sites", "all").outcome == "needs_approval"
    assert records.decide(record, "ask", "all").outcome == "refused"
    assert records.decide(None, "ask").outcome == "refused"


async def test_an_override_and_an_admin_with_all_sites():
    async with async_session() as db:
        await _supervisor(db)
        # distinct moments, so the audit list's order is the order of the changes
        await users.update_user(db, CC, "u-sup", actor=SUPER, now=NOW + timedelta(seconds=1),
                                change=users.UserChange(capabilities={"export": ALLOWED}))
        await users.create_user(db, CC, "u-adm", actor=SUPER, now=NOW + timedelta(seconds=2),
                                change=users.UserChange(display_name="Ada Admin", role_code="admin",
                                                        all_sites=True, email="ada@example.com"))
        await db.commit()
        sup = await records.access_record_for(db, CC, "u-sup")
        adm = await records.access_record_for(db, CC, "u-adm")
        events = await audit.recent_events(db, CC)
    assert sup.capabilities["export"] == ALLOWED and sup.capabilities["view_all_sites"] == WITH_APPROVAL
    assert adm.all_sites and adm.site_ids == () and records.decide(adm, "ask", "LON").is_allowed
    assert records.decide(adm, "approve").is_allowed
    assert [e.action for e in events][:3] == ["access.user_created", "access.user_updated", "access.user_created"]
    assert "Ada Admin as Admin for all sites" in events[0].text


async def test_a_deactivated_user_is_refused_and_bad_input_is_named():
    async with async_session() as db:
        await _supervisor(db)
        await users.update_user(db, CC, "u-sup", actor=SUPER, change=users.UserChange(status="deactivated"))
        await db.commit()
        record = await records.access_record_for(db, CC, "u-sup")
        assert records.decide(record, "ask", "BRI").reason == "user is deactivated"
        with pytest.raises(ValueError, match="unknown site"):
            await users.update_user(db, CC, "u-sup", actor=SUPER, change=users.UserChange(site_ids=("XYZ",)))
        with pytest.raises(ValueError, match="already exists"):
            await _supervisor(db)


# ==================================================== 3. approvals


async def test_an_approval_grants_the_capability_for_good():
    async with async_session() as db:
        await _supervisor(db)
        await users.create_user(db, CC, "u-ops", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Olu Ops", role_code="ops_manager", all_sites=True))
        row = await approvals.request(
            db, CC, kind="grant_capability", requested_by="u-sup", requested_by_name="Sam Super",
            requested_via="ask", title="View all sites", site_id=None,
            payload={"capability": "view_all_sites"}, now=NOW)
        await db.commit()
        assert row.reference == "ACC-1" and row.status == "pending"
        assert row.expires_at == NOW + timedelta(hours=24)
        assert [a.reference for a in await approvals.pending(db, CC)] == ["ACC-1"]
        decided = await approvals.decide(db, CC, "ACC-1", approve=True, decided_by="u-ops",
                                         decided_by_name="Olu Ops", now=NOW + timedelta(hours=1))
        await db.commit()
        assert decided.status == "approved" and decided.decided_by == "u-ops"
        record = await records.access_record_for(db, CC, "u-sup")
        assert record.all_sites and record.capabilities["view_all_sites"] == ALLOWED
        assert records.decide(record, "ask", "LON").is_allowed
        with pytest.raises(ValueError, match="not pending"):
            await approvals.decide(db, CC, "ACC-1", approve=False, decided_by="u-ops", decided_by_name=None)
        # a second request of the same kind counts on; a declined one changes nothing
        second = await approvals.request(db, CC, kind="grant_capability", requested_by="u-sup",
                                         requested_by_name="Sam", requested_via="ask", title="Export",
                                         payload={"capability": "export"}, now=NOW)
        await approvals.decide(db, CC, second.reference, approve=False, decided_by="u-ops",
                               decided_by_name="Olu", note="not yet", now=NOW)
        await db.commit()
        assert second.reference == "ACC-2" and second.status == "declined"
        record = await records.access_record_for(db, CC, "u-sup")
        assert record.capabilities["export"] == WITH_APPROVAL
        # an old pending one expires on the sweep
        stale = await approvals.request(db, CC, kind="export", requested_by="u-sup", requested_by_name="Sam",
                                        requested_via="ask", title="Board pack", now=NOW - timedelta(days=2))
        await db.commit()
        assert await approvals.expire_once(db, now=NOW) == 1
        assert stale.status == "expired"
        texts = [e.text for e in await audit.recent_events(db, CC)]
        assert any("expired unanswered" in t for t in texts) and any("approved ACC-1" in t for t in texts)


# ==================================================== 4. the mirror rows and the audit purge


async def test_the_access_rows_are_written_per_person_and_per_customer():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _supervisor(db)
        await users.create_user(db, CC, "u-old", actor=SUPER,
                                change=users.UserChange(display_name="Old Leaver", role_code="picker",
                                                        site_ids=("BRI",), status="deactivated"))
        await db.commit()
        written = await access_mirror.mirror_customer(db, mirror, CC, now=NOW)
        await db.commit()
    assert written == 5  # one record, roles, users, approvals, audit
    access = json.loads(mirror.items[(f"EYE#{CC}", "USER#u-sup#ACCESS")].payload_json)
    assert access["role"] == "supervisor" and access["site_ids"] == ["BRI"]
    assert access["capabilities"]["approve"] == NO and access["status"] == "active"
    assert (f"EYE#{CC}", "USER#u-old#ACCESS") not in mirror.items
    roles = json.loads(mirror.items[(f"EYE#{CC}", "ROLES")].payload_json)
    assert [r["code"] for r in roles["roles"]][0] == "picker"
    assert roles["matrix"]["approve"]["supervisor"] == NO and roles["matrix"]["approve"]["admin"] == ALLOWED
    listing = json.loads(mirror.items[(f"EYE#{CC}", "USERS")].payload_json)
    assert {u["object_id"]: u["status"] for u in listing["users"]} == {"u-sup": "active", "u-old": "deactivated"}
    assert json.loads(mirror.items[(f"EYE#{CC}", "APPROVALS")].payload_json)["pending"] == []
    log = json.loads(mirror.items[(f"EYE#{CC}", "AUDIT")].payload_json)
    assert log["events"][0]["action"] == "access.user_created"
    assert mirror.items[(f"EYE#{CC}", "ROLES")].ttl is None


async def test_old_audit_events_are_purged():
    async with async_session() as db:
        db.add(AuditEvent(customer_code=CC, at=NOW - timedelta(days=100), actor_object_id="x",
                          action="old", decision="allowed", text="old"))
        db.add(AuditEvent(customer_code=CC, at=NOW, actor_object_id="x", action="new",
                          decision="allowed", text="new"))
        await db.commit()
        assert await audit.purge_once(db, now=NOW, retention_days=90) == 1
        assert [e.action for e in await audit.recent_events(db, CC)] == ["new"]


# ==================================================== 5. the superadmin's API


@pytest.fixture
def api_mirror() -> InMemoryMirrorWriter:
    m = InMemoryMirrorWriter()
    app.dependency_overrides[access_api.get_access_mirror] = lambda: m
    yield m
    app.dependency_overrides.pop(access_api.get_access_mirror, None)


async def test_the_first_admin_is_made_through_the_api_and_mirrored(api_mirror):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as c:
        r = await c.put(f"/api/v1/teams/access/{CC}/users/u-first",
                        json={"display_name": "First Admin", "role": "admin", "all_sites": True})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["role"] == "admin" and body["all_sites"] is True and body["mirrored"] is True
        assert body["capabilities"]["approve"] == ALLOWED
        r = await c.put(f"/api/v1/teams/access/{CC}/users/u-first", json={"role": "boss"})
        assert r.status_code == 400 and "unknown role" in r.json()["detail"]
        r = await c.get(f"/api/v1/teams/access/{CC}/users")
        assert [u["object_id"] for u in r.json()["users"]] == ["u-first"]
        r = await c.put(f"/api/v1/teams/access/{CC}/matrix/supervisor/export", json={"state": "allowed"})
        assert r.status_code == 200 and r.json()["mirrored"] is True
        r = await c.get(f"/api/v1/teams/access/{CC}/matrix")
        assert r.json()["matrix"]["export"]["supervisor"] == ALLOWED
        assert (await c.get("/api/v1/teams/access/no-such-customer/users")).status_code == 404
    assert (f"EYE#{CC}", "USER#u-first#ACCESS") in api_mirror.items


# ==================================================== 6. the consumer


async def test_the_consumer_refuses_before_the_agent_when_access_says_so():
    calls = []

    async def agent(code, question, history_):
        calls.append(question)
        return {"answer": "ran", "tool_calls": []}

    h = Harness([job(question="q1"), job(question="q2", job_id="job-2")], agent=agent)
    seen = []

    async def check(job_):
        seen.append(job_.question)
        return "You have no access yet." if job_.question == "q1" else None

    h.consumer._check_access = check
    await h.drain()
    answers = {p.job_id: (p.status, p.answer) for p in h.poster.posted}
    assert answers["job-1"] == ("error", "You have no access yet.")
    assert answers["job-2"] == ("ok", "ran") and calls == ["q2"] and seen == ["q1", "q2"]


async def test_the_real_check_reads_the_record_and_audits_a_refusal():
    def _job(object_id: str, site_id: str | None) -> QuestionJob:
        return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC,
                           conversation_id="c", sender_object_id=object_id, sender_name="Sam",
                           question="how many?", site_id=site_id, source="tab")

    async with async_session() as db:
        await _supervisor(db)
        await db.commit()
    assert await teams_consumer._check_access(_job("nobody", "BRI")) == teams_consumer.NO_ACCESS_YET
    refusal = await teams_consumer._check_access(_job("u-sup", "LON"))
    assert refusal and "outside" in refusal
    assert await teams_consumer._check_access(_job("u-sup", "BRI")) is None
    async with async_session() as db:
        user = await users.get_user(db, CC, "u-sup")
        assert user.last_active_at is not None
        refusals = [e for e in await audit.recent_events(db, CC) if e.action == "ask.refused"]
        assert len(refusals) == 2 and {e.decision for e in refusals} == {"refused"}
        assert refusals[0].site_id == "LON" and "outside" in refusals[0].text
