"""Chunk 220: an answer can only use data the asker may see (stage D, step 45).

Before this chunk Ask checked the page's site only, and every data tool received just the customer
code, so a supervisor of BRI could ask about LON and be answered. A `DataScope` is built from the
person's access record, and a scoped session adds the customer and the person's sites to every
select of a site-limited table, wherever the query is built: in a tool or in a service a tool
calls. A table nobody has classified cannot be read through a scoped session at all.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select

from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.persistence.models.log_ssh_source import LogSshSource
from app.services import scoped_data
from app.services.access import AccessRecord

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
