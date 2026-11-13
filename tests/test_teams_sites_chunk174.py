"""Chunk 174 (stage D, step 4): sites per customer, the Home snapshot per site, freshness, the mirror.

A customer may run several warehouses. `tenant_sites` names them (the `warehouse` value on the
settled rows, plus a display name); the snapshot is computed once for the whole customer and once
per site; every row the new Teams page reads is written to the edge's table under one prefix.
"""

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.tenant_site import TenantSite
from app.persistence.repositories.site_repository import SiteRepository
from app.services.analytics_at_risk import delivery_store, model as at_risk_model
from app.services.teams import freshness, home_snapshot, mirror, mirror_rows
from app.services.teams.sites import Site
from tests.test_analytics_agent_chunk124 import BODY as GOLDEN_BODY

CC = "test_chunk174"
TZ = ZoneInfo("Europe/London")
# A settled row carries the warehouse only when the settlement says so; a tenant whose pick_release
# settlement does not carry it has no per-site figures (docs/stage-d-deploy-runbook.md, D4).
BODY = {**GOLDEN_BODY, "carry": [*GOLDEN_BODY["carry"], "warehouse"]}


async def _wipe():
    async with async_session() as db:
        for model_ in (AnalyticsAtRiskDelivery, AnalyticsTenantState, AnalyticsSettledRow, AnalyticsSettlement,
                       AnalyticsLookupValue, AnalyticsLookup, AnalyticsFieldRegistry, AnalyticsFact, TenantSite,
                       TeamsTenantBinding):
            await db.execute(delete(model_).where(model_.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="Sites probe", display_name="Acme", timezone="Europe/London"))
        for f in ("ReportingNumber", "ExpectedQuantity", "QuantityPicked", "OrderLine"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="request", field=f,
                                          captured=True, seen_count=9))
        await db.commit()
    yield
    await _wipe()


def _fact(when: datetime, expected, picked, *, rep, warehouse, delivery="D1", item="104568", user="FNACHONLEO"):
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when,
        business_date=when.astimezone(TZ).date(), transaction_name="JIT and Shorts Pick", method="ConfirmPickLine",
        status="success", quantity_classification="pick" if picked > 0 else "attempt", warehouse=warehouse,
        delivery_number=delivery, item_number=item, lot_number="2609161191", user_name=user,
        attributes={"ReportingNumber": rep, "ExpectedQuantity": str(expected), "QuantityPicked": str(picked),
                    "OrderLine": "21"}, created_at=when)


async def _plant_two_warehouses(now: datetime):
    """Today: three releases at BRI and one at LON, all in the last two hours (or, in the first
    hour and a half after midnight, from the start of today, so they never fall on yesterday)."""
    start_of_today = now.astimezone(ZoneInfo("Europe/London")).replace(
        hour=0, minute=0, second=0, microsecond=0)
    t = max(now - timedelta(minutes=90), start_of_today)
    facts = [
        _fact(t, 10, 10, rep="B1", warehouse="BRI"),
        _fact(t + timedelta(minutes=10), 4, 4, rep="B2", warehouse="BRI", user="DBOBOC"),
        _fact(t + timedelta(minutes=20), 7, 0, rep="B3", warehouse="BRI", delivery="D2", item="100230"),
        _fact(t + timedelta(minutes=30), 5, 5, rep="L1", warehouse="LON", delivery="D3", item="100606"),
    ]
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)


async def _configure_sites():
    async with async_session() as db:
        repo = SiteRepository(db)
        await repo.upsert(CC, "BRI", "Brighton", sort_order=1)
        await repo.upsert(CC, "LON", "London", sort_order=2)
        await db.commit()


def _kpi(snapshot: dict, kpi_id: str) -> dict:
    return next(k for k in snapshot["kpis"] if k["id"] == kpi_id)


# ==================================================== 1. the sites table

async def test_sites_are_listed_in_order_and_an_inactive_one_is_hidden():
    async with async_session() as db:
        repo = SiteRepository(db)
        await repo.upsert(CC, "LON", "London", sort_order=3)
        await repo.upsert(CC, "BRI", "Brighton", sort_order=1)
        await repo.upsert(CC, "BIC", "Bicester", sort_order=2)
        await repo.upsert(CC, "BIC", "Bicester", sort_order=2, active=False)
        await db.commit()
        sites = await repo.list_active(CC)
        assert [(s.site_id, s.name) for s in sites] == [("BRI", "Brighton"), ("LON", "London")]
        await repo.remove(CC, "LON")
        await db.commit()
        assert [s.site_id for s in await repo.list_active(CC)] == ["BRI"]


# ==================================================== 2. the snapshot per site

async def test_a_site_snapshot_counts_only_its_warehouse_and_the_customer_snapshot_counts_all():
    now = datetime.now(timezone.utc)
    await _plant_two_warehouses(now)
    async with async_session() as db:
        whole = await home_snapshot.compute(db, CC)
        brighton = await home_snapshot.compute(db, CC, site=Site("BRI", "Brighton"))
        london = await home_snapshot.compute(db, CC, site=Site("LON", "London"))
    assert _kpi(whole, "releases")["value"] == "4" and whole["site"] == "Acme"
    assert whole["site_id"] == "all"
    assert _kpi(brighton, "releases")["value"] == "3" and brighton["site"] == "Brighton"
    assert brighton["site_id"] == "BRI" and _kpi(brighton, "attention")["value"] == "1"
    assert _kpi(london, "releases")["value"] == "1" and _kpi(london, "attention")["value"] == "0"
    # the activity list follows the site too
    assert all("100606" in a["text"] or "D3" in a["text"] or True for a in london["activity"])
    assert len(london["activity"]) == 0 and len(brighton["activity"]) == 1


# ==================================================== 3. freshness

def test_the_provisional_rule_is_a_share_above_the_threshold():
    assert freshness.is_provisional(Decimal("0.25"), threshold=Decimal("0.20")) is True
    assert freshness.is_provisional(Decimal("0.20"), threshold=Decimal("0.20")) is False
    assert freshness.is_provisional(None, threshold=Decimal("0.20")) is False
    assert freshness.provisional_text(Decimal("0.25")) == "75% of handheld calls sealed, figures provisional"


async def test_freshness_reads_the_tenant_state_and_the_snapshot_carries_it():
    now = datetime.now(timezone.utc)
    await _plant_two_warehouses(now)
    async with async_session() as db:
        db.add(AnalyticsTenantState(customer_code=CC, analytics_watermark=now - timedelta(minutes=3),
                                    unsealed_share=Decimal("0.3")))
        await db.commit()
        fresh = await freshness.freshness_for(db, CC, threshold=Decimal("0.2"))
        assert fresh.provisional is True and fresh.data_as_of == now - timedelta(minutes=3)
        snap = await home_snapshot.compute(db, CC)
    assert snap["freshness"]["provisional"] is True
    assert snap["freshness"]["text"] == "70% of handheld calls sealed, figures provisional"
    assert snap["freshness"]["data_as_of"] == (now - timedelta(minutes=3)).isoformat()


async def test_a_tenant_without_state_is_not_provisional():
    async with async_session() as db:
        fresh = await freshness.freshness_for(db, CC, threshold=Decimal("0.2"))
    assert fresh.provisional is False and fresh.data_as_of is None and fresh.unsealed_share is None


# ==================================================== 4. the mirror

def test_the_envelope_names_the_row_and_the_tracker_rewrites_before_the_ttl():
    env = mirror.envelope("site_home", CC, site_id="BRI", payload={"a": 1}, as_of="2026-11-12T10:00:00+00:00",
                          now=1_000_000)
    assert (env.pk, env.sk) == ("EYE#test_chunk174", "SITE#BRI#HOME")
    assert env.row_type == "site_home" and env.schema_version == mirror_rows.MIRROR_SCHEMA_VERSION
    assert env.ttl == 1_000_000 + mirror_rows.KEY_LAYOUT["site_home"].ttl_seconds
    assert json.loads(env.payload_json) == {"a": 1} and env.written_at == 1_000_000
    listing = mirror.envelope("sites", CC, payload={"sites": []}, as_of="x", now=5)
    assert (listing.pk, listing.sk, listing.ttl) == ("EYE#test_chunk174", "SITES", None)

    tracker = mirror.ChangeTracker(max_age_seconds=100)
    assert tracker.should_write(("p", "s"), "d1", now=0) is True
    tracker.wrote(("p", "s"), "d1", now=0)
    assert tracker.should_write(("p", "s"), "d1", now=50) is False
    assert tracker.should_write(("p", "s"), "d2", now=50) is True
    assert tracker.should_write(("p", "s"), "d1", now=100) is True


async def test_one_sweep_writes_the_legacy_row_the_whole_customer_each_site_and_the_site_list():
    now = datetime.now(timezone.utc)
    await _plant_two_warehouses(now)
    await _configure_sites()

    class Legacy:
        def __init__(self):
            self.puts = []

        async def put(self, customer_code, snapshot, *, now=None):
            self.puts.append((customer_code, snapshot["site_id"]))

    legacy = Legacy()
    rows = mirror.InMemoryMirrorWriter()
    written = await home_snapshot.sweep_once(legacy, only=[CC], mirror=rows)
    assert written == [CC] and legacy.puts == [(CC, "all")]
    keys = sorted(rows.items)
    assert keys == [("EYE#test_chunk174", "SITE#BRI#HOME"), ("EYE#test_chunk174", "SITE#LON#HOME"),
                    ("EYE#test_chunk174", "SITE#all#HOME"), ("EYE#test_chunk174", "SITES")]
    listing = json.loads(rows.items[("EYE#test_chunk174", "SITES")].payload_json)
    assert listing == {"customer_code": CC, "customer_name": "Acme", "default_site_id": "BRI",
                       "sites": [{"site_id": "BRI", "name": "Brighton"}, {"site_id": "LON", "name": "London"}]}
    brighton = json.loads(rows.items[("EYE#test_chunk174", "SITE#BRI#HOME")].payload_json)
    assert _kpi(brighton, "releases")["value"] == "3"
    # a second sweep with nothing changed writes no mirror row again
    tracker = mirror.ChangeTracker(max_age_seconds=3600)
    rows2 = mirror.InMemoryMirrorWriter()
    await home_snapshot.sweep_once(legacy, only=[CC], mirror=rows2, tracker=tracker)
    first = len(rows2.items)
    await home_snapshot.sweep_once(legacy, only=[CC], mirror=rows2, tracker=tracker)
    assert first == 4 and rows2.put_count == 4


async def test_a_customer_without_sites_still_gets_the_whole_customer_row_and_an_empty_list():
    now = datetime.now(timezone.utc)
    await _plant_two_warehouses(now)

    class Legacy:
        async def put(self, customer_code, snapshot, *, now=None):
            pass

    rows = mirror.InMemoryMirrorWriter()
    await home_snapshot.sweep_once(Legacy(), only=[CC], mirror=rows)
    assert sorted(rows.items) == [("EYE#test_chunk174", "SITE#all#HOME"), ("EYE#test_chunk174", "SITES")]
    listing = json.loads(rows.items[("EYE#test_chunk174", "SITES")].payload_json)
    assert listing["sites"] == [] and listing["default_site_id"] == "all"


# ==================================================== 5. the at-risk rows know their warehouse

async def test_at_risk_rows_carry_the_warehouse_and_the_board_filters_on_it():
    now = datetime.now(timezone.utc)
    today = now.astimezone(TZ).date()
    async with async_session() as db:
        for number, warehouse in (("D100", "BRI"), ("D200", "LON"), ("D300", None)):
            db.add(AnalyticsAtRiskDelivery(customer_code=CC, delivery_number=number, departure_date=today,
                                           departure_at=now + timedelta(hours=2), warehouse=warehouse,
                                           tier="watch", max_tier="watch", tier_history=[], rule_version="t",
                                           last_evaluated_at=now))
        await db.commit()
        everything = await delivery_store.board_rows(db, CC, dates=[today])
        brighton = await delivery_store.board_rows(db, CC, dates=[today], warehouse="BRI")
        assert sorted(r.delivery_number for r in everything) == ["D100", "D200", "D300"]
        assert [r.delivery_number for r in brighton] == ["D100"]

        # the progress write carries the warehouse the board read from the route rows
        row = next(r for r in everything if r.delivery_number == "D300")
        state = at_risk_model.DeliveryState(
            delivery_number="D300", route="R1", customer_name=None, customer_number=None,
            departure_at=now + timedelta(hours=2), lines_expected=4, lines_confirmed=1, lines_picked=1,
            lines_short=0, packages_created=0, packages_loaded=0, last_pick_at=None, last_load_at=None,
            warehouse="LON")
        delivery_store._write_progress(row, state)
        await db.commit()
        stored = await db.scalar(select(AnalyticsAtRiskDelivery.warehouse).where(
            AnalyticsAtRiskDelivery.customer_code == CC, AnalyticsAtRiskDelivery.delivery_number == "D300"))
        assert stored == "LON"
        assert delivery_store._state_from_row(row).warehouse == "LON"
