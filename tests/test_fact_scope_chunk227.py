"""Chunk 227: a site-limited person reads the facts of their own company and warehouse pairs.

Step 1d stage 2a, the last part. Facts carry the WMS company since chunk 226 and the older ones
are backfilled, so the scope rule for `analytics_facts` can match the pair, as the rule for
`log_transactions` has done since chunk 223. The ledger keeps the warehouse rule: part of its
history was left without a company on the dev server, and no data tool reads it.
"""
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.services import scoped_data
from app.services.access import AccessRecord
from app.services.scoped_data import DataScope, Place

CC = "c227"
AT = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)


def _record(*sites: str) -> AccessRecord:
    return AccessRecord(CC, "u-sup", "Sam", "supervisor", "Supervisor", "active", False,
                        tuple(sites), {"ask": "allowed"})


def _sql(model, scope: DataScope) -> str:
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    return str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))


def test_facts_are_limited_by_the_company_and_warehouse_pair():
    sql = _sql(AnalyticsFact, DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),)))
    assert "analytics_facts.warehouse = 'BRI'" in sql and "analytics_facts.company = '915'" in sql


def test_a_site_with_no_company_reads_its_warehouse_under_any_company():
    sql = _sql(AnalyticsFact, DataScope.from_record(_record("BRI")))
    assert "analytics_facts.warehouse = 'BRI'" in sql and "company" not in sql


def test_the_ledger_is_still_limited_by_the_warehouse_alone():
    sql = _sql(AnalyticsFactLedger, DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),)))
    assert "analytics_fact_ledger.warehouse IN ('BRI')" in sql and "company" not in sql


async def _plant(db) -> None:
    """One BRI fact under each of two companies, one under none, and one at LON."""
    for item, company, warehouse in (("f-911", "911", "BRI"), ("f-920", "920", "BRI"),
                                     ("f-none", None, "BRI"), ("f-lon", "920", "LON")):
        db.add(AnalyticsFact(customer_code=CC, source_transaction_id=uuid.uuid4(),
                             source_started_at=AT, event_time=AT, source_version_hash="h" * 64,
                             item_number=item, company=company, warehouse=warehouse))
    await db.flush()


async def _items(db) -> set[str]:
    stmt = select(AnalyticsFact.item_number).where(AnalyticsFact.item_number.like("f-%"))
    return set((await db.execute(stmt)).scalars())


@pytest.mark.asyncio
async def test_facts_of_two_companies_with_the_same_warehouse_code_are_kept_apart(db):
    await _plant(db)
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    scoped_data.limit_session(db, scope)
    assert await _items(db) == {"f-911"}


@pytest.mark.asyncio
async def test_one_site_can_cover_a_warehouse_under_several_companies(db):
    await _plant(db)
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("911", "BRI"), Place("920", "BRI")))
    scoped_data.limit_session(db, scope)
    assert await _items(db) == {"f-911", "f-920"}


@pytest.mark.asyncio
async def test_a_site_without_places_reads_its_facts_as_before(db):
    await _plant(db)
    scoped_data.limit_session(db, DataScope.from_record(_record("BRI")))
    assert await _items(db) == {"f-911", "f-920", "f-none"}
