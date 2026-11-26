"""Chunk 231: an advisory says which WMS company it is about (step 53; step 1d stage 2c).

An advisory agent writes the warehouse of the row it read into `site_id`. The rows it reads
(facts, settled rows, at-risk deliveries) carry their company since chunks 226 to 229, so the
advisory can carry it too, and a person limited to one company's warehouse no longer sees the
advisories of another company that uses the same warehouse code.

Pinned here
-----------
    agents     each finding carries the company of the row behind it
    store      the advisory row keeps it
    scope      an advisory is matched by the company and warehouse pair; one with no company
               still follows its site; a site id that is not a warehouse still matches by id
    backfill   stored advisories take the customer's only company for their warehouse
"""
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.customer import Customer
from app.persistence.models.teams_conversation import TeamsConversation
from app.services import scoped_data
from app.services.advisories import registry, store
from app.services.advisories.agents import location as location_agent
from app.services.advisories.agents import stock as stock_agent
from app.services.advisories.registry import Finding
from app.services.analytics import company_backfill as backfill
from app.services.analytics_at_risk import RULE_VERSION
from app.services.scoped_data import DataScope, Place

CC = "test_c231"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc)


async def _wipe() -> None:
    async with async_session() as db:
        for model in (AnalyticsAtRiskDelivery, AnalyticsFact, Advisory):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, display_name="C231", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


def _fact(method: str, when: datetime, *, item: str, attributes: dict, company: str | None,
          classification: str | None = None, warehouse: str = "BRI") -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(),
        source_started_at=when, source_version_hash=uuid.uuid4().hex[:8], revision=1,
        event_time=when, business_date=when.astimezone(TZ).date(), method=method, status="success",
        transaction_name="Quick Stock Count", quantity_classification=classification,
        company=company, warehouse=warehouse, item_number=item, user_name="FNACHONLEO",
        attributes=attributes, created_at=when)


# ==================================================== 1. the agents

async def test_the_delivery_agent_carries_the_company_of_the_delivery():
    row = AnalyticsAtRiskDelivery(
        customer_code=CC, delivery_number="D-1", departure_date=NOW.astimezone(TZ).date(),
        departure_at=NOW + timedelta(minutes=45), route="R14", customer_name="Hove Deli",
        company="915", warehouse="BRI", tier="at_risk", max_tier="at_risk",
        first_flagged_at=NOW, first_flagged_tier="at_risk", lines_expected=84, lines_confirmed=60,
        lines_picked=60, packages_created=3, packages_loaded=1, status="open",
        usual_ready_at=NOW + timedelta(minutes=25), usual_ready_source="learned",
        rule_version=RULE_VERSION, last_evaluated_at=NOW)
    async with async_session() as db:
        db.add(row)
        await db.commit()
        (finding,) = await registry.spec("delivery").run(db, CC, NOW, TZ)
    assert (finding.company, finding.site_id) == ("915", "BRI")


async def test_the_stock_agent_carries_the_company_of_the_count():
    async with async_session() as db:
        db.add(_fact("ReportCount", NOW - timedelta(minutes=30), item="200100", company="915",
                     attributes={"Location": "B-07-01", "CountedQuantity": "0",
                                 "BalanceQuantity": "13.33"}))
        await db.commit()
        (finding,) = await stock_agent.run(db, CC, NOW, TZ)
    assert (finding.company, finding.site_id) == ("915", "BRI")


async def test_the_stock_agent_carries_the_company_of_a_chronic_short(monkeypatch):
    async def releases(_db, spec, _customer):
        assert spec["group_by"][-1] == "company"
        return {"rows": [{"dimensions": ["104568", "Ginger", "BRI", "915"], "rows": 9,
                          "shortfall": "-12"}]}

    monkeypatch.setattr(stock_agent, "aggregate_releases", releases)
    async with async_session() as db:
        (finding,) = await stock_agent.run(db, CC, NOW, TZ)
    assert (finding.kind, finding.company, finding.site_id) == ("chronic_short", "915", "BRI")
    assert finding.dedup_key == "chronic:104568:BRI"  # unchanged, so open advisories stay open


async def test_the_location_agent_carries_the_company_of_the_picks():
    async with async_session() as db:
        for i in range(6):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=10 * i), item="100",
                         company="915", classification="attempt",
                         attributes={"FromLocation": "A-03-04", "QuantityPicked": "0"}))
        await db.commit()
        (finding,) = await location_agent.run(db, CC, NOW, TZ)
    assert (finding.company, finding.site_id) == ("915", "BRI")


# ==================================================== 2. the store

async def _reconcile(*findings: Finding) -> None:
    async with async_session() as db:
        await store.reconcile(db, CC, "stock", list(findings), now=NOW)
        await db.commit()


async def _stored() -> dict[str, tuple[str | None, str | None]]:
    async with async_session() as db:
        rows = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars()
        return {r.dedup_key: (r.company, r.site_id) for r in rows}


async def test_the_advisory_row_keeps_the_company_of_its_finding():
    await _reconcile(Finding("k1", "high", "t", site_id="BRI", company="915"),
                     Finding("k2", "high", "t", site_id="BRI"))
    assert await _stored() == {"k1": ("915", "BRI"), "k2": (None, "BRI")}


# ==================================================== 3. the scope

def _sql(model, scope: DataScope) -> str:
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    return str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))


async def _visible(scope: DataScope) -> set[str]:
    async with scoped_data.open_session(async_session, scope) as db:
        return set((await db.execute(select(Advisory.dedup_key))).scalars())


async def test_an_advisory_is_matched_by_the_company_and_warehouse_pair():
    await _reconcile(Finding("a-911", "high", "t", site_id="BRI", company="911"),
                     Finding("a-920", "high", "t", site_id="BRI", company="920"),
                     Finding("a-none", "high", "t", site_id="BRI"),
                     Finding("a-lon", "high", "t", site_id="LON", company="911"))
    one = DataScope(CC, site_ids=("BRI",), places=(Place("911", "BRI"),))
    assert await _visible(one) == {"a-911", "a-none"}
    any_company = DataScope(CC, site_ids=("BRI",), places=(Place(None, "BRI"),))
    assert await _visible(any_company) == {"a-911", "a-920", "a-none"}
    assert await _visible(DataScope(CC, all_sites=True)) == {"a-911", "a-920", "a-none", "a-lon"}


def test_a_site_id_that_is_not_a_warehouse_still_matches_by_id():
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    sql = _sql(Advisory, scope)
    assert "advisories.site_id = 'BRI'" in sql and "advisories.company = '911'" in sql
    assert "advisories.site_id IN ('BRI-911')" in sql


def test_a_table_with_a_site_id_and_no_company_matches_as_before():
    scope = DataScope(CC, site_ids=("BRI-911",), places=(Place("911", "BRI"),))
    assert "teams_conversations.site_id IN ('BRI', 'BRI-911')" in _sql(TeamsConversation, scope)


# ==================================================== 4. the backfill

async def test_stored_advisories_take_the_customers_only_company_for_their_warehouse():
    async with async_session() as db:
        db.add_all([
            _fact("ReportCount", NOW, item="1", company="915", attributes={}),
            _fact("ReportCount", NOW, item="2", company="911", warehouse="LON", attributes={}),
            _fact("ReportCount", NOW, item="3", company="920", warehouse="LON", attributes={})])
        await db.commit()
    await _reconcile(Finding("k-bri", "high", "t", site_id="BRI"),
                     Finding("k-lon", "high", "t", site_id="LON"),
                     Finding("k-all", "high", "t"))
    dry = await backfill.fill_advisories(async_session, CC, dry_run=True)
    assert dry == 1 and (await _stored())["k-bri"] == (None, "BRI")
    assert await backfill.fill_advisories(async_session, CC) == 1
    assert await _stored() == {"k-bri": ("915", "BRI"), "k-lon": (None, "LON"),
                               "k-all": (None, None)}
