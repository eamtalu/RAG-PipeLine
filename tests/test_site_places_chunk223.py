"""Chunk 223: a site is a named set of company and warehouse pairs (stage D, step 47).

A data row is identified by customer, company and warehouse. One site, Brighton for example, can
belong to several WMS companies of a customer, and two companies can each have a warehouse BRI
that are different places. `tenant_site_places` says which pairs a site stands for; a site with
no row stands for the warehouse of its own id under any company, which is what every site meant
before this chunk. The data scope carries the pairs and the tables that have a company column are
limited by the pair.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.tenant_site import TenantSite, TenantSitePlace
from app.persistence.repositories.site_repository import SiteRepository
from app.services import scoped_data
from app.services.access import AccessRecord
from app.services.scoped_data import DataScope, Place

CC = "c223"


def _record(*sites: str, all_sites: bool = False) -> AccessRecord:
    return AccessRecord(CC, "u-sup", "Sam", "supervisor", "Supervisor", "active", all_sites,
                        tuple(sites), {"ask": "allowed"})


def _sql(model, scope: DataScope) -> str:
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    return str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))


# --- the scope ----------------------------------------------------------------------------------

def test_a_site_with_no_places_stands_for_its_own_warehouse_under_any_company():
    scope = DataScope.from_record(_record("BRI", "LON"))
    assert scope.site_places == (Place(None, "BRI"), Place(None, "LON"))


def test_a_scope_given_places_reads_through_them():
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("911", "BRI"), Place("920", "BRI")))
    assert scope.site_places == (Place("911", "BRI"), Place("920", "BRI"))


# --- the rules ----------------------------------------------------------------------------------

def test_a_table_with_a_company_is_limited_by_the_pair():
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
    sql = _sql(LogTransaction, scope)
    assert "log_transactions.warehouse = 'BRI'" in sql and "log_transactions.company = '915'" in sql


def test_a_place_with_no_company_matches_any_company():
    sql = _sql(LogTransaction, DataScope.from_record(_record("BRI")))
    assert "log_transactions.warehouse = 'BRI'" in sql and "company" not in sql


def test_a_table_without_a_company_column_is_limited_by_the_warehouse():
    from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    sql = _sql(AnalyticsAtRiskDelivery, scope)
    assert "analytics_at_risk_deliveries.warehouse IN ('BRI')" in sql and "company" not in sql


def test_a_table_keyed_by_site_id_matches_the_site_ids_and_the_places_warehouses():
    from app.persistence.models.advisory import Advisory
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    assert "advisories.site_id IN ('BRI', 'BRI-911')" in _sql(Advisory, scope)


# --- on real rows -------------------------------------------------------------------------------

async def _plant(db) -> None:
    """One BRI transaction under each of two companies, one under none, and one LON."""
    job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
              storage_key="k223")
    db.add(job)
    await db.flush()
    for name, company, warehouse in (("t-911", "911", "BRI"), ("t-920", "920", "BRI"),
                                     ("t-none", None, "BRI"), ("t-lon", "920", "LON")):
        db.add(LogTransaction(customer_code=CC, job_id=job.id, transaction_name=name,
                              company=company, warehouse=warehouse))
    await db.flush()


async def _names(db) -> set[str]:
    stmt = select(LogTransaction.transaction_name).where(
        LogTransaction.transaction_name.like("t-%"))
    return set((await db.execute(stmt)).scalars())


@pytest.mark.asyncio
async def test_two_companies_with_the_same_warehouse_code_are_kept_apart(db):
    await _plant(db)
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    scoped_data.limit_session(db, scope)
    assert await _names(db) == {"t-911"}


@pytest.mark.asyncio
async def test_one_site_can_cover_a_warehouse_under_several_companies(db):
    await _plant(db)
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("911", "BRI"), Place("920", "BRI")))
    scoped_data.limit_session(db, scope)
    assert await _names(db) == {"t-911", "t-920"}


@pytest.mark.asyncio
async def test_a_site_without_places_reads_as_before(db):
    await _plant(db)
    scoped_data.limit_session(db, DataScope.from_record(_record("BRI")))
    assert await _names(db) == {"t-911", "t-920", "t-none"}


# --- the places table and the scope built from it -----------------------------------------------

@pytest.fixture
async def _sites():
    async def wipe():
        async with async_session() as s:
            await s.execute(delete(Customer).where(Customer.customer_code == CC))
            await s.commit()
    await wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C223"))
        await s.flush()
        repo = SiteRepository(s)
        await repo.upsert(CC, "BRI", "Brighton", sort_order=1)
        await repo.upsert(CC, "LON", "London", sort_order=2)
        await s.commit()
    yield
    await wipe()


@pytest.mark.asyncio
async def test_a_sites_places_are_set_replaced_and_read(_sites):
    async with async_session() as s:
        repo = SiteRepository(s)
        await repo.set_places(CC, "BRI", [("911", "BRI"), ("920", "BRI")], actor="t")
        await repo.set_places(CC, "BRI", [("915", "BRI"), ("915", "BRI")], actor="t")
        await s.commit()
        assert await repo.places_of(CC, ["BRI", "LON"]) == {"BRI": [("915", "BRI")]}


@pytest.mark.asyncio
async def test_a_place_of_an_unknown_site_is_refused(_sites):
    async with async_session() as s:
        with pytest.raises(ValueError):
            await SiteRepository(s).set_places(CC, "NOPE", [("915", "BRI")], actor="t")


@pytest.mark.asyncio
async def test_removing_a_site_removes_its_places(_sites):
    async with async_session() as s:
        repo = SiteRepository(s)
        await repo.set_places(CC, "BRI", [("915", "BRI")], actor="t")
        await repo.remove(CC, "BRI")
        await s.commit()
        left = (await s.execute(select(TenantSitePlace).where(
            TenantSitePlace.customer_code == CC))).scalars().all()
        assert left == []
        assert await s.get(TenantSite, (CC, "BRI")) is None


@pytest.mark.asyncio
async def test_the_scope_is_built_from_the_sites_places(_sites):
    async with async_session() as s:
        await SiteRepository(s).set_places(CC, "BRI", [("911", "BRI"), ("920", "BRI")], actor="t")
        await s.commit()
        scope = await scoped_data.scope_for(s, _record("BRI", "LON"), customer_code=CC)
    assert scope.site_ids == ("BRI", "LON")
    assert scope.site_places == (Place("911", "BRI"), Place("920", "BRI"), Place(None, "LON"))


@pytest.mark.asyncio
async def test_the_scope_of_an_all_sites_person_and_of_nobody_needs_no_places(_sites):
    async with async_session() as s:
        everything = await scoped_data.scope_for(s, _record(all_sites=True), customer_code=CC)
        nobody = await scoped_data.scope_for(s, None, customer_code=CC)
    assert everything.all_sites and everything.places == ()
    assert nobody.customer_code == CC and nobody.site_places == ()


@pytest.mark.asyncio
async def test_a_data_tool_may_read_the_places_table(db):
    scoped_data.limit_session(db, DataScope.from_record(_record("BRI")))
    await db.execute(select(TenantSitePlace.id).where(TenantSitePlace.id == uuid.uuid4()))


# --- the operator's script ----------------------------------------------------------------------

from app.persistence.models.access import AuditEvent  # noqa: E402
from scripts import site_places  # noqa: E402


@pytest.mark.asyncio
async def test_the_script_sets_lists_and_audits_a_sites_places(_sites):
    assert await site_places.set_places(CC, "BRI", [("920", "BRI"), ("911", "BRI")]) == [
        ("911", "BRI"), ("920", "BRI")]
    assert await site_places.list_places(CC) == [
        "BRI  Brighton  911:BRI 920:BRI", "LON  London  any:LON (default)"]
    async with async_session() as s:
        mine = select(AuditEvent).where(AuditEvent.customer_code == CC,
                                        AuditEvent.action == "sites.places_set")
        events = (await s.execute(mine)).scalars().all()
    assert [e.text for e in events] == ["site BRI now stands for 911:BRI, 920:BRI"]
    assert await site_places.set_places(CC, "BRI", []) == []
    assert (await site_places.list_places(CC))[0] == "BRI  Brighton  any:BRI (default)"


@pytest.mark.asyncio
async def test_the_script_refuses_a_site_the_customer_does_not_have(_sites):
    args = site_places.build_parser().parse_args(["set", CC, "NOPE", "915:BRI"])
    assert await site_places.run(args) == 2
    both = site_places.build_parser().parse_args(["set", CC, "BRI"])
    assert await site_places.run(both) == 2
