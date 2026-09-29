"""Chunk 127: the Home snapshot behind the Teams tab.

The tab on the edge cannot reach this server, so the consumer computes "today so far" per bound
customer every minute and writes it to the edge's DynamoDB table; the edge serves it to the page.
Everything here is derived from the same settlement reads the agent uses, on the tenant's clock.
"""

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.services.teams import home_snapshot
from tests.test_analytics_agent_chunk124 import BODY

CC = "test_chunk127"
TZ = ZoneInfo("Europe/London")


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsSettledRow, AnalyticsSettlement, AnalyticsLookupValue, AnalyticsLookup,
                      AnalyticsFieldRegistry, AnalyticsFact):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(TeamsTenantBinding).where(TeamsTenantBinding.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="Snapshot probe", display_name="Brighton", timezone="Europe/London"))
        for f in ("ReportingNumber", "ExpectedQuantity", "QuantityPicked", "OrderLine"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="request", field=f,
                                          captured=True, seen_count=9))
        await db.commit()
    yield
    await _wipe()


def _fact(when: datetime, expected, picked, *, rep, item="104568", delivery="27907", user="FNACHONLEO",
          status="success"):
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when,
        business_date=when.astimezone(TZ).date(),
        transaction_name="JIT and Shorts Pick (Brighton)", method="ConfirmPickLine", status=status,
        quantity_classification="pick" if picked > 0 else "attempt",
        warehouse="BRI", delivery_number=delivery, item_number=item, lot_number="2609161191", user_name=user,
        attributes={"ReportingNumber": rep, "ExpectedQuantity": str(expected), "QuantityPicked": str(picked),
                    "OrderLine": "21"}, created_at=when)


async def _plant(now: datetime):
    """Today: four releases in the last two hours (one zero-pick, one partial, two exact) across two
    deliveries. Yesterday at the same hours: two releases, so today is up on yesterday-at-this-time."""
    t = now - timedelta(minutes=90)
    facts = [
        _fact(t, 10, 10, rep="T1", delivery="D1", item="100606"),
        _fact(t + timedelta(minutes=20), 4, 4, rep="T2", delivery="D1", item="100606", user="DBOBOC"),
        _fact(t + timedelta(minutes=40), 7, 4, rep="T3", delivery="D2", item="100230", user="DBOBOC"),
        _fact(t + timedelta(minutes=60), 7, 0, rep="T4", delivery="D2", item="100230", user="DBOBOC"),
        _fact(t - timedelta(days=1), 5, 5, rep="Y1", delivery="D9", item="100606"),
        _fact(t - timedelta(days=1) + timedelta(minutes=30), 5, 5, rep="Y2", delivery="D9", item="100606"),
    ]
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)


def _kpi(snapshot: dict, kpi_id: str) -> dict:
    return next(k for k in snapshot["kpis"] if k["id"] == kpi_id)


# ==================================================== 1. the numbers

async def test_the_snapshot_counts_today_on_the_tenant_clock():
    now = datetime.now(timezone.utc)
    if now.astimezone(TZ).hour < 2:
        pytest.skip("the planted rows would fall on yesterday this close to midnight")
    await _plant(now)
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    assert snap["customer_code"] == CC and snap["site"] == "Brighton"
    releases = _kpi(snap, "releases")
    assert releases["value"] == "4" and releases["delta_direction"] == "up"
    assert len(releases["spark"]) == 9 and sum(releases["spark"]) == 4
    deliveries = _kpi(snap, "deliveries")
    assert deliveries["value"] == "2" and deliveries["caption"] == "4 lines"
    fill = _kpi(snap, "fill_rate")
    # expected 28, short 3 + 7 = 10 units -> 18/28
    assert fill["value"] == "64.3%" and fill["caption"] == "2 short lines"
    attention = _kpi(snap, "attention")
    assert attention["value"] == "1" and attention["caption"] == "1 zero-pick · 1 partial"
    assert snap["rail"] == {"releases": "4", "deliveries": "2", "fill_rate": "64.3%", "alerts": "1", "customers": "0"}


async def test_attention_items_customers_and_activity_come_from_the_rows():
    now = datetime.now(timezone.utc)
    if now.astimezone(TZ).hour < 2:
        pytest.skip("the planted rows would fall on yesterday this close to midnight")
    await _plant(now)
    async with async_session() as db:
        snap = await home_snapshot.compute(db, CC)
    zero = [a for a in snap["attention"] if a["kind"] == "stock"]
    assert len(zero) == 1 and zero[0]["title"].startswith("100230 zero-picked 1 time")
    assert "7 units short" in zero[0]["detail"] and "100230" in zero[0]["ask_text"]
    # no customer lookup is declared for this probe, so the customers panel is empty, not broken
    assert snap["customers"] == []
    assert [a["alert"] for a in snap["activity"]] == [True, False]
    assert "DBOBOC" in snap["activity"][0]["text"] and "0 of 7" in snap["activity"][0]["text"]
    assert snap["as_of"].endswith("+00:00") or snap["as_of"].endswith("Z")


async def test_an_empty_day_is_a_snapshot_of_zeros_not_an_error():
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        snap = await home_snapshot.compute(db, CC)
    assert _kpi(snap, "releases")["value"] == "0" and _kpi(snap, "releases")["delta_direction"] is None
    assert _kpi(snap, "fill_rate")["value"] == "–" and snap["attention"] == [] and snap["activity"] == []


async def test_a_customer_without_the_settlement_gives_no_snapshot():
    async with async_session() as db:
        assert await home_snapshot.compute(db, CC) is None


# ==================================================== 2. writing it to the edge

def test_the_writer_stores_the_snapshot_as_json_with_a_ttl():
    class Table:
        def __init__(self):
            self.items = []

        def put_item(self, Item):
            self.items.append(Item)

    table = Table()
    writer = home_snapshot.DynamoHomeSnapshotWriter.__new__(home_snapshot.DynamoHomeSnapshotWriter)
    writer._table = table
    writer._ttl_seconds = 3600
    import asyncio
    asyncio.run(writer.put(CC, {"site": "Brighton", "kpis": []}, now=1_000_000))
    item = table.items[0]
    assert item["pk"] == f"HOME#{CC}" and item["sk"] == "SNAPSHOT" and item["ttl"] == 1_003_600
    assert json.loads(item["snapshot_json"])["site"] == "Brighton"


async def test_one_sweep_writes_a_snapshot_per_bound_customer_and_survives_one_failure():
    now = datetime.now(timezone.utc)
    if now.astimezone(TZ).hour < 2:
        pytest.skip("the planted rows would fall on yesterday this close to midnight")
    await _plant(now)
    async with async_session() as db:
        db.add(TeamsTenantBinding(tenant_id="11111111-1111-1111-1111-111111111111", customer_code=CC, enabled=True))
        db.add(TeamsTenantBinding(tenant_id="22222222-2222-2222-2222-222222222222", customer_code=CC, enabled=True))
        await db.commit()
    written: dict[str, dict] = {}

    class Writer:
        async def put(self, customer_code, snapshot, *, now=None):
            written[customer_code] = snapshot

    codes = await home_snapshot.sweep_once(Writer(), only=[CC, "no_such_space"])
    assert codes == [CC] and _kpi(written[CC], "releases")["value"] == "4"
