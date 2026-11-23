"""Chunk 218: invite a Teams user by email, claimed at their first sign-in (stage D, step 43).

An Admin (or the superadmin at install) invites an email within the customer's own domains; the
person is a pending user with no object id until they open the app, when the edge sends
`access_claim` with the email from their verified token and the backend links their object id.
Invites match once, expire after `teams_invite_days`, and the edge reads them by a hash of the
email, never the email itself.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.v1 import teams as teams_api
from app.api.v1 import teams_access as access_api
from app.config.database import async_session
from app.main import app
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.services.access import audit, mirror as access_mirror, records, users
from app.services.access import commands as access_commands
from app.services.teams.binding_mirror import InMemoryMirror
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.mirror_rows import invite_key
from app.settings import settings
from tests.test_access_chunk176 import CC, NOW, SUPER, _command, _customer  # noqa: F401

JO = "Jo.Bloggs@Cust.com"


async def _bind(db, *domains: str) -> None:
    db.add(TeamsTenantBinding(tenant_id=str(uuid.uuid4()), customer_code=CC,
                              email_domains=list(domains)))
    await db.flush()


async def _admin(db, object_id: str = "u-adm") -> None:
    await users.create_user(db, CC, object_id, actor=SUPER, now=NOW,
                            change=users.UserChange(display_name="Ada Admin", role_code="admin",
                                                    all_sites=True))


def _invite(**kw) -> users.UserChange:
    return users.UserChange(display_name="Jo Bloggs", role_code="supervisor", site_ids=("BRI",), **kw)


def _invite_row(mirror: InMemoryMirrorWriter, email: str = JO):
    return mirror.items.get((f"EYE#{CC}", f"INVITE#{invite_key(email)}"))


# ==================================================== 1. inviting


async def test_an_invite_is_a_pending_user_with_no_object_id_and_a_normalised_email():
    async with async_session() as db:
        await _bind(db, "cust.com")
        user = await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await db.commit()
        assert user.object_id is None and user.status == "pending"
        assert user.email == "jo.bloggs@cust.com"
        events = await audit.recent_events(db, CC)
        assert events[0].action == "access.user_invited"
        assert "invited Jo Bloggs (jo.bloggs@cust.com) as Supervisor for BRI" in events[0].text


async def test_an_email_is_invited_once_whatever_its_case():
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        with pytest.raises(ValueError, match="already"):
            await users.invite_user(db, CC, " jo.bloggs@CUST.COM ", change=_invite(), actor=SUPER, now=NOW)


async def test_an_invite_outside_the_customer_domains_is_refused():
    async with async_session() as db:
        with pytest.raises(ValueError, match="no email domains"):
            await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await _bind(db, "cust.com")
        with pytest.raises(ValueError, match="outside"):
            await users.invite_user(db, CC, "jo@gmail.com", change=_invite(), actor=SUPER, now=NOW)
        with pytest.raises(ValueError, match="email"):
            await users.invite_user(db, CC, "not-an-email", change=_invite(), actor=SUPER, now=NOW)


# ==================================================== 2. claiming at first sign-in


async def test_the_first_sign_in_claims_the_invite_and_becomes_an_active_record():
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        user = await users.claim_invite(db, CC, "jo.bloggs@cust.com", "u-jo", name="Jo Bloggs",
                                        now=NOW + timedelta(hours=1))
        await db.commit()
        assert user.object_id == "u-jo" and user.status == "active"
        record = await records.access_record_for(db, CC, "u-jo")
        assert record.role_code == "supervisor" and record.site_ids == ("BRI",)
        events = await audit.recent_events(db, CC)
        assert events[0].action == "access.user_claimed" and "Jo Bloggs joined" in events[0].text


async def test_an_invite_matches_once_and_never_an_existing_person():
    async with async_session() as db:
        await _bind(db, "cust.com")
        await _admin(db)
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        with pytest.raises(ValueError, match="already has access"):
            await users.claim_invite(db, CC, JO, "u-adm", name="Ada", now=NOW)
        await users.claim_invite(db, CC, JO, "u-jo", name="Jo", now=NOW)
        with pytest.raises(ValueError, match="no pending invite"):
            await users.claim_invite(db, CC, JO, "u-other", name="Other", now=NOW)
        with pytest.raises(ValueError, match="no pending invite"):
            await users.claim_invite(db, CC, "nobody@cust.com", "u-x", name="X", now=NOW)


async def test_an_old_invite_cannot_be_claimed_and_the_sweep_marks_it_expired():
    later = NOW + timedelta(days=settings.teams_invite_days, minutes=1)
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        with pytest.raises(ValueError, match="expired"):
            await users.claim_invite(db, CC, JO, "u-jo", name="Jo", now=later)
        assert await users.expire_invites_once(db, now=later) == 1
        await db.commit()
        user = await users.get_user_by_email(db, CC, JO)
        assert user.status == "expired"
        assert (await audit.recent_events(db, CC))[0].action == "access.invite_expired"


async def test_a_claim_from_outside_the_customer_domains_is_refused():
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await db.execute(TeamsTenantBinding.__table__.update().where(
            TeamsTenantBinding.customer_code == CC).values(email_domains=["other.com"]))
        with pytest.raises(ValueError, match="outside"):
            await users.claim_invite(db, CC, JO, "u-jo", name="Jo", now=NOW)


# ==================================================== 3. the rows the edge reads


async def test_a_pending_invite_is_mirrored_by_hash_and_removed_once_claimed():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await db.commit()
        await access_mirror.mirror_customer(db, mirror, CC, now=NOW)
        row = _invite_row(mirror)
        assert row is not None and "jo.bloggs" not in row.payload_json.lower()
        invite = json.loads(row.payload_json)
        assert invite["role_name"] == "Supervisor" and invite["display_name"] == "Jo Bloggs"
        assert not any(sk.startswith("USER#") for _, sk in mirror.items)
        listing = json.loads(mirror.items[(f"EYE#{CC}", "USERS")].payload_json)
        assert listing["users"][0]["status"] == "pending" and listing["users"][0]["object_id"] is None
        await users.claim_invite(db, CC, JO, "u-jo", name="Jo", now=NOW)
        await db.commit()
        await access_mirror.mirror_customer(db, mirror, CC, now=NOW)
    assert _invite_row(mirror) is None
    assert (f"EYE#{CC}", "USER#u-jo#ACCESS") in mirror.items


# ==================================================== 4. the Admin tab's commands


async def test_an_admin_invites_by_email_and_the_edge_claims_it_by_command():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _bind(db, "cust.com")
        await _admin(db)
        await db.commit()
    text = (await access_commands.run(
        _command("u-adm", "access_set_user", {"email": JO, "display_name": "Jo Bloggs",
                                              "role": "supervisor", "site_ids": ["BRI"]}, "Ada"),
        mirror=mirror, now=NOW)).text
    assert text == "invited Jo Bloggs (jo.bloggs@cust.com) as Supervisor for BRI"
    assert _invite_row(mirror) is not None
    # the edge's claim: the sender is the new person, who has no record yet
    text = (await access_commands.run(_command("u-jo", "access_claim", {"email": JO}, "Jo Bloggs"),
                                      mirror=mirror, now=NOW)).text
    assert text == "access ready for Jo Bloggs"
    assert (f"EYE#{CC}", "USER#u-jo#ACCESS") in mirror.items and _invite_row(mirror) is None
    with pytest.raises(access_commands.Refused, match="no pending invite"):
        await access_commands.run(_command("u-x", "access_claim", {"email": JO}), mirror=mirror, now=NOW)


async def test_a_pending_invite_is_withdrawn_by_email():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _bind(db, "cust.com")
        await _admin(db)
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await db.commit()
    text = (await access_commands.run(_command("u-adm", "access_deactivate_user", {"email": JO}, "Ada"),
                                      mirror=mirror, now=NOW)).text
    assert text == "deactivated Jo Bloggs" and _invite_row(mirror) is None


async def test_a_pending_invite_is_edited_by_email_not_invited_twice():
    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _bind(db, "cust.com")
        await _admin(db)
        await users.invite_user(db, CC, JO, change=_invite(), actor=SUPER, now=NOW)
        await db.commit()
    text = (await access_commands.run(
        _command("u-adm", "access_set_user", {"email": JO, "role": "ops_manager", "all_sites": True}, "Ada"),
        mirror=mirror, now=NOW)).text
    assert text == "updated Jo Bloggs"
    async with async_session() as db:
        user = await users.get_user_by_email(db, CC, JO)
    assert user.role_code == "ops_manager" and user.status == "pending" and user.object_id is None


async def test_a_supervisors_invite_waits_for_an_ops_manager():
    from app.services.access import approvals

    mirror = InMemoryMirrorWriter()
    async with async_session() as db:
        await _bind(db, "cust.com")
        await users.create_user(db, CC, "u-sup", actor=SUPER, now=NOW, change=users.UserChange(
            display_name="Sam Super", role_code="supervisor", site_ids=("BRI",)))
        await users.create_user(db, CC, "u-ops", actor=SUPER, now=NOW, change=users.UserChange(
            display_name="Olu Ops", role_code="ops_manager", all_sites=True))
        await db.commit()
    outcome = await access_commands.run(
        _command("u-sup", "access_set_user", {"email": JO, "display_name": "Jo Bloggs",
                                              "role": "picker", "site_ids": ["BRI"]}, "Sam"),
        mirror=mirror, now=NOW)
    assert outcome.text.startswith("sent for approval")
    async with async_session() as db:
        [pending] = await approvals.pending(db, CC)
        await approvals.decide(db, CC, pending.reference, approve=True, decided_by="u-ops",
                               decided_by_name="Olu", now=NOW)
        await db.commit()
        user = await users.get_user_by_email(db, CC, JO)
    assert user.status == "pending" and user.object_id is None and user.role_code == "picker"


# ==================================================== 5. the superadmin's doors


@pytest.fixture
def api_mirrors():
    access, binding = InMemoryMirrorWriter(), InMemoryMirror()
    app.dependency_overrides[access_api.get_access_mirror] = lambda: access
    app.dependency_overrides[teams_api.get_binding_mirror] = lambda: binding
    yield access, binding
    app.dependency_overrides.pop(access_api.get_access_mirror, None)
    app.dependency_overrides.pop(teams_api.get_binding_mirror, None)


async def test_the_install_sets_the_domains_and_invites_the_first_admin(api_mirrors):
    access, binding = api_mirrors
    tenant = str(uuid.uuid4())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as c:
        r = await c.put(f"/api/v1/teams/bindings/{tenant}",
                        json={"customer_code": CC, "email_domains": ["Cust.com", " cust.co.uk "]})
        assert r.status_code == 200, r.text
        assert r.json()["email_domains"] == ["cust.com", "cust.co.uk"]
        assert binding.items[tenant]["email_domains"] == ["cust.com", "cust.co.uk"]
        r = await c.post(f"/api/v1/teams/access/{CC}/invites",
                         json={"email": "it.admin@cust.com", "display_name": "IT Admin",
                               "role": "admin", "all_sites": True})
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "pending" and r.json()["email"] == "it.admin@cust.com"
        r = await c.post(f"/api/v1/teams/access/{CC}/invites",
                         json={"email": "x@gmail.com", "display_name": "X", "role": "admin"})
        assert r.status_code == 400 and "outside" in r.json()["detail"]
    assert _invite_row(access, "it.admin@cust.com") is not None
