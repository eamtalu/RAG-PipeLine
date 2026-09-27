"""Chunk 121: the Teams bot's tenant binding, source of truth here, mirrored to the edge.

The bot edge on AWS learns which customer a Microsoft 365 tenant is from a DynamoDB row. That row is
a COPY; Postgres decides. Pinned here:

    repository   upsert creates then replaces; a replaced row reads as needing a mirror again;
                 mark_mirrored stamps the row's own updated_at; delete is by tenant
    admin API    PUT validates the tenant GUID and the customer, saves, mirrors, reports mirrored;
                 a mirror outage saves the row and reports mirrored=false; DELETE removes locally
                 and from the edge, and says when the edge part failed
    memory       recent_turns returns the last N turns oldest first, scoped to the tenant
"""

import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete

from app.api.v1 import teams as teams_api
from app.config.database import async_session
from app.main import app
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.repositories.teams_repository import TeamsBindingRepository, TeamsConversationRepository
from app.services.teams.binding_mirror import InMemoryMirror

CC = "test_c121"
CC2 = "test_c121_b"
TENANT = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(TeamsConversationTurn).where(TeamsConversationTurn.customer_code.in_([CC, CC2])))
        await s.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.customer_code.in_([CC, CC2])))
        await s.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.tenant_id == TENANT))
        await s.execute(delete(Customer).where(Customer.customer_code.in_([CC, CC2])))
        await s.commit()


@pytest.fixture(autouse=True)
async def _tenants():
    await _wipe()
    async with async_session() as s:
        s.add_all([Customer(customer_code=CC, display_name="C121"), Customer(customer_code=CC2)])
        await s.commit()
    yield
    await _wipe()


@pytest.fixture
def mirror() -> InMemoryMirror:
    m = InMemoryMirror()
    app.dependency_overrides[teams_api.get_binding_mirror] = lambda: m
    yield m
    app.dependency_overrides.pop(teams_api.get_binding_mirror, None)


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


# ------------------------------------------------------------------------------- repository
async def test_upsert_creates_then_replaces_and_flags_for_mirror():
    async with async_session() as db:
        repo = TeamsBindingRepository(db)
        row = await repo.upsert(TENANT, CC, display_name="Acme", created_by="amin")
        assert row.customer_code == CC and row.enabled is True and row.needs_mirror
        assert [b.tenant_id for b in await repo.list_needing_mirror() if b.tenant_id == TENANT] == [TENANT]

        await repo.mark_mirrored(TENANT)
        row = await repo.get_by_tenant(TENANT)
        assert row.needs_mirror is False
        assert TENANT not in [b.tenant_id for b in await repo.list_needing_mirror()]

        row = await repo.upsert(TENANT, CC2, enabled=False)
        assert row.customer_code == CC2 and row.enabled is False
        assert row.needs_mirror, "a changed row must be pushed to the edge again"

        assert await repo.delete(TENANT) is True
        assert await repo.delete(TENANT) is False
        assert await repo.get_by_tenant(TENANT) is None


# ------------------------------------------------------------------------------- admin API
async def test_put_saves_mirrors_and_reports(mirror: InMemoryMirror):
    async with _client() as c:
        r = await c.put(f"/api/v1/teams/bindings/{TENANT.upper()}",
                        json={"customer_code": CC, "display_name": " Acme ", "created_by": "amin"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["tenant_id"] == TENANT  # normalised to lowercase
    assert body["customer_code"] == CC and body["enabled"] is True
    assert body["display_name"] == "Acme" and body["mirrored"] is True
    assert mirror.items[TENANT] == {"customer_code": CC, "enabled": True, "display_name": "Acme"}

    async with _client() as c:
        r = await c.get("/api/v1/teams/bindings")
    listed = [b for b in r.json()["bindings"] if b["tenant_id"] == TENANT]
    assert len(listed) == 1 and listed[0]["mirrored"] is True


async def test_put_rejects_bad_tenant_and_unknown_customer(mirror: InMemoryMirror):
    async with _client() as c:
        r1 = await c.put("/api/v1/teams/bindings/not-a-guid", json={"customer_code": CC})
        r2 = await c.put(f"/api/v1/teams/bindings/{TENANT}", json={"customer_code": "nope_c121"})
        r3 = await c.put(f"/api/v1/teams/bindings/{TENANT}", json={"customer_code": "BAD CODE!"})
    assert r1.status_code == 400
    assert r2.status_code == 404
    assert r3.status_code == 400
    assert mirror.items == {}


async def test_mirror_outage_saves_the_row_and_reports_unmirrored():
    broken = InMemoryMirror(fail=True)
    app.dependency_overrides[teams_api.get_binding_mirror] = lambda: broken
    try:
        async with _client() as c:
            r = await c.put(f"/api/v1/teams/bindings/{TENANT}", json={"customer_code": CC})
    finally:
        app.dependency_overrides.pop(teams_api.get_binding_mirror, None)
    assert r.status_code == 200
    assert r.json()["mirrored"] is False
    async with async_session() as db:
        row = await TeamsBindingRepository(db).get_by_tenant(TENANT)
    assert row is not None and row.needs_mirror


async def test_delete_removes_locally_and_from_edge(mirror: InMemoryMirror):
    async with _client() as c:
        await c.put(f"/api/v1/teams/bindings/{TENANT}", json={"customer_code": CC})
        r = await c.delete(f"/api/v1/teams/bindings/{TENANT}")
        r_again = await c.delete(f"/api/v1/teams/bindings/{TENANT}")
    assert r.status_code == 200 and r.json() == {"tenant_id": TENANT, "deleted": True, "removed_from_edge": True}
    assert TENANT not in mirror.items
    assert r_again.status_code == 404


# ------------------------------------------------------------------------------- memory
async def test_recent_turns_are_bounded_oldest_first_and_tenant_scoped():
    conv = f"conv-{uuid.uuid4()}"
    async with async_session() as db:
        repo = TeamsConversationRepository(db)
        for i in range(4):
            await repo.append_exchange(conv, CC, question=f"q{i}", answer=f"a{i}", job_id=f"j{i}")
        await repo.append_exchange(conv, CC2, question="other tenant", answer="never seen", job_id=None)

        turns = await repo.recent_turns(conv, CC, limit=4)
        assert [(t["role"], t["content"]) for t in turns] == [
            ("user", "q2"), ("assistant", "a2"), ("user", "q3"), ("assistant", "a3")]
        assert await repo.recent_turns(conv, CC, limit=0) == []
        assert all(t["content"] != "never seen" for t in await repo.recent_turns(conv, CC, limit=50))
        assert await repo.purge_customer(CC2) == 2
