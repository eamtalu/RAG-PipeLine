"""Chunk 220: an answer can only use data the asker may see (stage D, step 45).

Before this chunk Ask checked the page's site only, and every data tool received just the customer
code, so a supervisor of BRI could ask about LON and be answered. A `DataScope` is built from the
person's access record, and a scoped session adds the customer and the person's sites to every
select of a site-limited table, wherever the query is built: in a tool or in a service a tool
calls. A table nobody has classified cannot be read through a scoped session at all.
"""

from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import delete, func, select

from app.agent.gateway import build_context, snapshot_settings
from app.config.database import async_session
from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.persistence.models.customer import Customer
from app.persistence.models.log_ssh_source import LogSshSource
from app.services import scoped_data
from app.services.access import AccessRecord
from app.services.analytics_agent.namespaces import analytics_namespace
from tests.agent_helpers import request_for

CC = "c220"
OTHER = "c220-other"


def _record(*, all_sites: bool = False, sites: tuple[str, ...] = ("BRI",)) -> AccessRecord:
    return AccessRecord(CC, "u-sup", "Sam Super", "supervisor", "Supervisor", "active", all_sites,
                        sites, {"ask": "allowed"})


async def _plant(db) -> None:
    """Three releases of the customer (BRI, LON, no warehouse) and one BRI release of another."""
    for cc, key, warehouse in ((CC, "r-bri", "BRI"), (CC, "r-lon", "LON"), (CC, "r-none", None),
                               (OTHER, "r-other", "BRI")):
        db.add(AnalyticsSettledRow(customer_code=cc, settlement="pick_release", key=key,
                                   key_parts=[key], warehouse=warehouse, attributes={}))
    await db.flush()


async def _keys(db) -> set[str]:
    stmt = select(AnalyticsSettledRow.key).where(AnalyticsSettledRow.settlement == "pick_release",
                                                AnalyticsSettledRow.key.like("r-%"))
    return set((await db.execute(stmt)).scalars())


def test_the_scope_comes_from_the_access_record():
    scope = scoped_data.DataScope.from_record(_record())
    assert (scope.customer_code, scope.all_sites, scope.site_ids) == (CC, False, ("BRI",))
    assert scope.covers("BRI") and not scope.covers("LON") and not scope.covers(None)
    everything = scoped_data.DataScope.from_record(_record(all_sites=True, sites=()))
    assert everything.covers("LON") and everything.covers(None)


def test_no_record_gives_a_scope_that_covers_nothing():
    scope = scoped_data.DataScope.from_record(None, customer_code=CC)
    assert not scope.all_sites and scope.site_ids == () and not scope.covers("BRI")


@pytest.mark.asyncio
async def test_a_site_limited_person_reads_only_their_sites(db):
    await _plant(db)
    scoped_data.limit_session(db, scoped_data.DataScope.from_record(_record()))
    assert await _keys(db) == {"r-bri"}


@pytest.mark.asyncio
async def test_the_limit_holds_when_the_query_forgets_the_customer_and_the_site(db):
    await _plant(db)
    both = scoped_data.DataScope.from_record(_record(sites=("BRI", "LON")))
    scoped_data.limit_session(db, both)
    count = (await db.execute(select(func.count()).select_from(AnalyticsSettledRow).where(
        AnalyticsSettledRow.key.like("r-%")))).scalar_one()
    # BRI and LON of this customer; not the other customer's, not the unplaced row
    assert count == 2


@pytest.mark.asyncio
async def test_an_all_sites_person_reads_every_row_of_their_customer_only(db):
    await _plant(db)
    everything = scoped_data.DataScope.from_record(_record(all_sites=True, sites=()))
    scoped_data.limit_session(db, everything)
    assert await _keys(db) == {"r-bri", "r-lon", "r-none"}


@pytest.mark.asyncio
async def test_the_limit_reaches_a_subquery(db):
    await _plant(db)
    scoped_data.limit_session(db, scoped_data.DataScope.from_record(_record()))
    inner = (select(AnalyticsSettledRow.key.label("k"))
             .where(AnalyticsSettledRow.key.like("r-%")).subquery())
    assert set((await db.execute(select(inner.c.k))).scalars()) == {"r-bri"}


@pytest.mark.asyncio
async def test_a_table_nobody_classified_cannot_be_read(db):
    scoped_data.limit_session(db, scoped_data.DataScope.from_record(_record()))
    with pytest.raises(scoped_data.UnclassifiedTable) as refused:
        await db.execute(select(LogSshSource.id).where(LogSshSource.id == uuid.uuid4()))
    assert "log_ssh_sources" in str(refused.value)


@pytest.mark.asyncio
async def test_a_plain_session_is_not_limited(db):
    await _plant(db)
    assert {"r-bri", "r-lon", "r-none", "r-other"} <= await _keys(db)


# --- through the tool runner, as the agent calls it ---------------------------------------------

@pytest.fixture
async def _advisories():
    """One open advisory at BRI and one at LON, committed: the runner opens its own session."""
    async with async_session() as s:
        await s.execute(delete(Advisory).where(Advisory.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        s.add(Customer(customer_code=CC, display_name="C220"))
        await s.flush()
        for site in ("BRI", "LON"):
            s.add(Advisory(customer_code=CC, site_id=site, agent="stock", severity="high",
                           title=f"Ginger is short at {site}", dedup_key=f"ginger-{site}"))
        await s.commit()
    yield
    async with async_session() as s:
        await s.execute(delete(Advisory).where(Advisory.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


async def _found(data_scope) -> list[str]:
    """The titles the advisory tool finds for "Ginger" when the request carries this scope."""
    request = request_for(CC, data_scope=data_scope)
    context = build_context(request, snapshot_settings())
    out = await analytics_namespace().runner(context, request, "find_advisory", {"words": "Ginger"})
    return sorted(m["title"] for m in json.loads(out)["matches"])


@pytest.mark.asyncio
async def test_a_tool_serves_a_bri_supervisor_only_bri(_advisories):
    assert await _found(scoped_data.DataScope.from_record(_record())) == ["Ginger is short at BRI"]


@pytest.mark.asyncio
async def test_a_tool_serves_an_all_sites_person_every_site(_advisories):
    everything = scoped_data.DataScope.from_record(_record(all_sites=True, sites=()))
    assert await _found(everything) == ["Ginger is short at BRI", "Ginger is short at LON"]


@pytest.mark.asyncio
async def test_a_person_without_a_scope_is_served_nothing(_advisories):
    assert await _found(None) == []


def test_a_system_caller_reads_every_site_and_a_person_without_a_record_none():
    system = scoped_data.scope_of(None, customer_code=CC, is_service=True)
    nobody = scoped_data.scope_of(None, customer_code=CC, is_service=False)
    assert system.all_sites and not nobody.all_sites and nobody.site_ids == ()


# --- the forecast, and the tables with no site yet ------------------------------------------------

def _sql(model, scope) -> str:
    rule = scoped_data.registry.BY_TABLE[model.__tablename__]
    return str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))


def test_a_site_limited_person_reads_their_sites_forecast_and_never_the_total():
    from app.persistence.models.analytics_forecast import AnalyticsForecastSeries
    sql = _sql(AnalyticsForecastSeries, scoped_data.DataScope.from_record(_record()))
    assert "subject_kind = 'warehouse'" in sql and "subject IN ('BRI')" in sql


def test_a_table_with_no_site_is_read_only_by_an_all_sites_person():
    # The rule itself: no table uses it since chunk 236, where log lines began to follow
    # their transaction; it stays for the next table that arrives without a site.
    from app.persistence.models.log_entry import LogEntry
    rule = scoped_data.registry.AllSitesOnly(LogEntry)

    def sql(scope):
        return str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))

    assert sql(scoped_data.DataScope.from_record(_record())) == "false"
    everything = scoped_data.DataScope.from_record(_record(all_sites=True, sites=()))
    assert sql(everything) == f"log_entries.customer_code = '{CC}'"


# --- the Teams consumer hands the asker's scope to the run ------------------------------------

from app import teams_consumer  # noqa: E402
from app.services.teams.contracts import QuestionJob  # noqa: E402
from tests.test_access_chunk176 import CC as CC176, _customer, _supervisor  # noqa: E402,F401


def _job(sender: str) -> QuestionJob:
    return QuestionJob(job_id="j-220", tenant_id="", customer_code=CC176, conversation_id="c-220",
                       sender_object_id=sender, sender_name="Sam Super", question="how is LON?",
                       source="tab", site_id="BRI")


@pytest.mark.asyncio
async def test_the_consumer_builds_the_scope_from_the_askers_record(_customer):  # noqa: F811
    async with async_session() as db:
        await _supervisor(db)
        await db.commit()
    scope = await teams_consumer._data_scope(_job("u-sup"))
    assert (scope.customer_code, scope.all_sites, scope.site_ids) == (CC176, False, ("BRI",))


@pytest.mark.asyncio
async def test_the_consumer_gives_a_stranger_a_scope_that_covers_nothing(_customer):  # noqa: F811
    scope = await teams_consumer._data_scope(_job("u-nobody"))
    assert scope.customer_code == CC176 and not scope.all_sites and scope.site_ids == ()


def test_the_model_is_told_which_sites_the_answer_may_use():
    limited = scoped_data.DataScope.from_record(_record(sites=("BRI", "LON")))
    assert "BRI, LON" in limited.note and "outside" in limited.note
    assert scoped_data.DataScope.for_system(CC).note == ""
