"""Chunk 100, the database half: harvesting during the fold, and resolving at read time.

The scenario is the live one. Picks carry a delivery number and no customer; a packing record carries
both, and it is written AFTER the picks it belongs to. That ordering is the whole reason the value is
resolved when somebody reads rather than stamped onto the pick: a stamp would be blank forever, and
fixing it would need an index from every key back to every fact that uses it.

Measured on tmp-live before this chunk: of 3,059 facts carrying a delivery number, 3,057 resolve to a
customer; 114 delivery keys map to 69 customers with zero contradictions; and 0 of 322 item keys ever
had a second description, so the validity periods are insurance rather than daily machinery.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import lookup as lk
from app.services.analytics import lookup_store
from app.services.analytics import read as n6
from app.services.analytics import registry
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "test_chunk100"
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, AnalyticsFieldRegistry, AnalyticsLookup, AnalyticsLookupValue,
          LogTransaction)

DELIVERY = lk.Lookup(
    name="delivery", key_field="delivery_number",
    attributes=(lk.Attribute("customer_name", stable=True, sources=(
        lk.Source("NewDeliveryPackage", "delivery_number", "CustomerName"),
        lk.Source("GetNextDeliveryByRoute", "resp.DeliveryNumber", "resp.CustomerName"),
    )),))


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="lookup probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()




async def _declare(lookup=DELIVERY, *, enabled=True):
    async with async_session() as db:
        db.add(AnalyticsLookup(customer_code=CC, name=lookup.name, key_field=lookup.key_field,
                               attributes=lookup_store.to_row(lookup), enabled=enabled))
        await db.commit()


def _pick(job_id, *, delivery, qty, at):
    return LogTransaction(
        customer_code=CC, job_id=job_id, sealed=True, started_at=at, ended_at=at, date=at.date(),
        duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
        transaction_type="002001", status=LogTransactionStatus.success, item_number="A",
        user_name="EDA", warehouse="BRI", warehouse_id="1", delivery_number=delivery,
        attributes={QF["ConfirmPickLine"]: str(qty), "DeliveryNumber": delivery})


def _packing(job_id, *, delivery, customer, at):
    return LogTransaction(
        customer_code=CC, job_id=job_id, sealed=True, started_at=at, ended_at=at, date=at.date(),
        duration_ms=100, method="NewDeliveryPackage", transaction_name="Pick",
        transaction_type="002001", status=LogTransactionStatus.success, user_name="EDA",
        warehouse="BRI", warehouse_id="1", delivery_number=delivery,
        attributes={"DeliveryNumber": delivery, "CustomerName": customer})


async def _fold(*transactions):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        for make in transactions:
            db.add(make(job.id))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()
    await n3.consume_tenant(CC)


async def _values():
    async with async_session() as db:
        return (await db.execute(select(AnalyticsLookupValue).where(
            AnalyticsLookupValue.customer_code == CC))).scalars().all()


# ==================================================== 1. harvesting happens inside the fold

async def test_the_fold_harvests_the_map_from_the_facts_it_already_holds():
    await _declare()
    await _fold(lambda j: _pick(j, delivery="25810", qty="10", at=T0),
                lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON",
                                   at=T0 + timedelta(minutes=5)))
    rows = await _values()
    assert [(r.lookup, r.key, r.attribute, r.value) for r in rows] == \
        [("delivery", "25810", "customer_name", "BAGELMAN BRIGHTON")]
    assert rows[0].source_method == "NewDeliveryPackage"


async def test_a_first_value_is_true_from_the_beginning_not_from_when_it_was_said():
    """The rule the whole design rests on. The packing record is five minutes AFTER the pick."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="25810", qty="10", at=T0),
                lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON",
                                   at=T0 + timedelta(minutes=5)))
    row = (await _values())[0]
    assert row.valid_from == lk.BEGINNING
    assert row.valid_to is None


async def test_no_lookup_declared_means_nothing_is_harvested_and_nothing_costs_anything():
    await _fold(lambda j: _packing(j, delivery="25810", customer="X", at=T0))
    assert await _values() == []


async def test_a_disabled_lookup_is_not_harvested():
    await _declare(enabled=False)
    await _fold(lambda j: _packing(j, delivery="25810", customer="X", at=T0))
    assert await _values() == []


# ==================================================== 2. the read, which is the point

async def _series(group_by):
    async with async_session() as db:
        did, definition = next((i, dfn) for i, dfn in await registry.active_definitions(db, CC)
                               if dfn.name == d.CONSUMPTION.name)
        state = await db.scalar(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))
        lookups = await lookup_store.load(db, CC)
        decision = n6.resolve(definition, group_by=group_by, lookups=lookups)
        out = await n6.series(db, CC, did, definition, measure="quantity",
                              window=UtcWindow(start=T0 - WIDE, end=T0 + WIDE),
                              group_by=group_by, ad_hoc=decision.ad_hoc,
                              watermark=state.analytics_watermark if state else None,
                              lookups=lookups)
    return decision, out


async def test_a_pick_logged_before_its_delivery_was_named_still_resolves_to_the_customer():
    """The failure that read-time resolution exists to prevent. Stamped onto the pick at fold time the
    customer would be blank here, and would stay blank until something went back and rebuilt it."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="25810", qty="10", at=T0),
                lambda j: _pick(j, delivery="25810", qty="4", at=T0 + timedelta(minutes=1)),
                lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON",
                                   at=T0 + timedelta(minutes=5)))
    _decision, out = await _series(("lookup:delivery.customer_name",))
    assert [t["dimensions"] for t in out["totals"]] == [["BAGELMAN BRIGHTON"]]
    assert out["total"]["sum_value"] == "14"


async def test_several_deliveries_of_one_customer_are_merged_and_the_total_is_unchanged():
    """114 delivery keys to 69 customers on the live tenant, so the merge is the normal path."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="1", qty="10", at=T0),
                lambda j: _pick(j, delivery="2", qty="5", at=T0),
                lambda j: _pick(j, delivery="3", qty="7", at=T0),
                lambda j: _packing(j, delivery="1", customer="SAME CUSTOMER", at=T0),
                lambda j: _packing(j, delivery="2", customer="SAME CUSTOMER", at=T0),
                lambda j: _packing(j, delivery="3", customer="OTHER", at=T0))
    _decision, out = await _series(("lookup:delivery.customer_name",))
    got = {tuple(t["dimensions"])[0]: t["roles"]["sum_value"] for t in out["totals"]}
    assert got == {"SAME CUSTOMER": "15", "OTHER": "7"}
    assert out["total"]["sum_value"] == "22", "re-keying may never create or lose units"


async def test_a_delivery_nobody_named_keeps_its_units_under_not_known():
    """The 2 picks of 1,343 that resolve to nothing still happened. Never dropped, never zero."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="1", qty="10", at=T0),
                lambda j: _pick(j, delivery="unnamed", qty="3", at=T0),
                lambda j: _packing(j, delivery="1", customer="KNOWN", at=T0))
    _decision, out = await _series(("lookup:delivery.customer_name",))
    got = {(tuple(t["dimensions"])[0]): t["roles"]["sum_value"] for t in out["totals"]}
    assert got == {"KNOWN": "10", None: "3"}


async def test_the_same_rollup_answers_by_delivery_and_by_customer():
    """Two views for the price of one slot, which is what makes the key-in-the-slot price cheap."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="1", qty="10", at=T0),
                lambda j: _pick(j, delivery="2", qty="5", at=T0),
                lambda j: _packing(j, delivery="1", customer="SAME", at=T0),
                lambda j: _packing(j, delivery="2", customer="SAME", at=T0))
    by_key = await _series(("delivery_number",))
    by_value = await _series(("lookup:delivery.customer_name",))
    assert sorted(tuple(t["dimensions"])[0] for t in by_key[1]["totals"]) == ["1", "2"]
    assert [tuple(t["dimensions"])[0] for t in by_value[1]["totals"]] == ["SAME"]
    assert by_key[1]["total"]["sum_value"] == by_value[1]["total"]["sum_value"] == "15"


async def test_a_lookup_grouping_says_which_tier_answered_it():
    """`delivery_number` is not a dimension of the seeded consumption metric, so this one is honest
    about falling back rather than quietly running slow."""
    await _declare()
    await _fold(lambda j: _pick(j, delivery="1", qty="10", at=T0),
                lambda j: _packing(j, delivery="1", customer="KNOWN", at=T0))
    decision, out = await _series(("lookup:delivery.customer_name",))
    assert decision.ad_hoc is True
    assert "delivery_number" in decision.reason, \
        "the reason must name the KEY, which is what no rollup is keyed by"
    assert out["group_by"] == ["lookup:delivery.customer_name"], \
        "the answer reports what was ASKED for, not the substitution used to get it"


async def test_a_grouping_with_no_lookup_is_untouched():
    await _declare()
    await _fold(lambda j: _pick(j, delivery="1", qty="10", at=T0))
    decision, out = await _series(("method",))
    assert decision.ad_hoc is False
    assert [tuple(t["dimensions"])[0] for t in out["totals"]] == ["ConfirmPickLine"]


# ==================================================== 3. conflicts and change

async def test_a_stable_attribute_keeps_its_first_value_and_counts_the_contradiction():
    """Zero contradictions across 114 live delivery keys, so a non-zero count means the declaration
    names the wrong source. Counted and logged rather than silently overwritten."""
    await _declare()
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, [
            lk.Observation("delivery", "1", "customer_name", "FIRST", T0, "NewDeliveryPackage"),
            lk.Observation("delivery", "1", "customer_name", "SECOND", T0 + timedelta(hours=1),
                           "PrintPackageLabel"),
        ], {"delivery": DELIVERY})
        await db.commit()
    assert stats["conflicts"] == 1
    assert [(r.value, r.valid_to) for r in await _values()] == [("FIRST", None)]


async def test_an_attribute_that_is_not_stable_opens_a_new_period():
    """Point in time, as chosen. An item renamed today must not rewrite last month's report."""
    changing = lk.Lookup("item", "item_number", (lk.Attribute("description", stable=False, sources=(
        lk.Source("GetOldestItemBalanceAPI", "item_number", "resp.ItemDescription"),)),))
    await _declare(changing)
    async with async_session() as db:
        await lookup_store.record(db, CC, [
            lk.Observation("item", "A", "description", "OLD NAME", T0, "GetOldestItemBalanceAPI")],
            {"item": changing})
        await db.commit()
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, [
            lk.Observation("item", "A", "description", "NEW NAME", T0 + timedelta(days=1),
                           "GetOldestItemBalanceAPI")], {"item": changing})
        await db.commit()
    assert stats["changed"] == 1
    periods = sorted(await _values(), key=lambda r: r.valid_from)
    assert [(p.value, p.valid_from, p.valid_to) for p in periods] == [
        ("OLD NAME", lk.BEGINNING, T0 + timedelta(days=1)),
        ("NEW NAME", T0 + timedelta(days=1), None)]

    async with async_session() as db:
        resolver = await lookup_store.resolver(db, CC, {"item": {"A"}}, (("item", "description"),))
    assert resolver.value("item", "A", "description", T0) == "OLD NAME"
    assert resolver.value("item", "A", "description", T0 + timedelta(days=2)) == "NEW NAME"


async def test_seeing_the_same_value_again_only_moves_the_counters():
    await _declare()
    async with async_session() as db:
        await lookup_store.record(db, CC, [
            lk.Observation("delivery", "1", "customer_name", "X", T0, "NewDeliveryPackage")],
            {"delivery": DELIVERY})
        await db.commit()
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, [
            lk.Observation("delivery", "1", "customer_name", "X", T0 + timedelta(hours=2),
                           "NewDeliveryPackage")], {"delivery": DELIVERY})
        await db.commit()
    assert (stats["extended"], stats["inserted"], stats["changed"]) == (1, 0, 0)
    row = (await _values())[0]
    assert row.observations == 2 and row.valid_from == lk.BEGINNING


# ==================================================== 4. backfill and suggestion

async def test_backfill_fills_a_new_lookup_from_history_without_touching_a_fact():
    """The reason a lookup can be declared at any time. Stamping the value in instead would mean
    rewriting 6,586 facts after two days, for an item description alone."""
    await _fold(lambda j: _pick(j, delivery="25810", qty="10", at=T0),
                lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON", at=T0))
    assert await _values() == [], "nothing is harvested before the lookup exists"
    async with async_session() as db:
        before = [(f.id, f.source_version_hash, f.revision) for f in
                  (await db.execute(select(AnalyticsFact)
                   .where(AnalyticsFact.customer_code == CC))).scalars().all()]
    await _declare()

    async with async_session() as db:
        declared = (await lookup_store.load(db, CC))["delivery"]
        report = await lookup_store.backfill(db, CC, declared, days=3650)
        await db.commit()
    assert report["inserted"] == 1
    assert report["methods"] == ["GetNextDeliveryByRoute", "NewDeliveryPackage"], \
        "only the methods the declaration names are read, never the whole table"
    assert [(r.key, r.value) for r in await _values()] == [("25810", "BAGELMAN BRIGHTON")]

    async with async_session() as db:
        after = [(f.id, f.source_version_hash, f.revision) for f in
                 (await db.execute(select(AnalyticsFact)
                  .where(AnalyticsFact.customer_code == CC))).scalars().all()]
    assert sorted(before) == sorted(after), "a backfill rewrites no fact and bumps no revision"


async def test_a_backfilled_value_is_also_true_from_the_beginning():
    """So a report over last month answers the moment the lookup is declared, with no rebuild."""
    await _fold(lambda j: _packing(j, delivery="25810", customer="X", at=T0))
    await _declare()
    async with async_session() as db:
        declared = (await lookup_store.load(db, CC))["delivery"]
        await lookup_store.backfill(db, CC, declared, days=3650)
        await db.commit()
    assert (await _values())[0].valid_from == lk.BEGINNING


async def test_the_suggester_finds_both_spellings_of_the_key_and_what_sits_beside_them():
    """Nobody should have to discover by hand that a delivery number is `DeliveryNumber` on a packing
    record and `resp.DeliveryNumber` on a routing one. Both spellings occur live: the typed column is
    populated on 653 NewDeliveryPackage facts and on 0 GetNextDeliveryByRoute facts."""
    await _fold(lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON", at=T0),
                lambda j: _packing(j, delivery="25811", customer="JUNIPER", at=T0))
    async with async_session() as db:
        body = await lookup_store.suggest_sources(db, CC, key_field="delivery_number", days=3650)
    assert body["keys_seen"] == 2
    packing = next(m for m in body["methods"] if m["method"] == "NewDeliveryPackage")
    spellings = {s["field"] for s in packing["key_spellings"]}
    assert "delivery_number" in spellings, "the typed column is the simplest spelling of all"
    assert "DeliveryNumber" in spellings, "and the handheld's own spelling is recognised by its values"
    assert "CustomerName" in {c["field"] for c in packing["candidates"]}


async def test_a_field_that_merely_overlaps_the_keys_is_not_called_a_key():
    """The share threshold earns its keep here: a field holding one key value and four other things
    is a different field, not a spelling of the key."""
    await _fold(lambda j: _packing(j, delivery="25810", customer="25810", at=T0),
                lambda j: _packing(j, delivery="25811", customer="A REAL NAME", at=T0))
    async with async_session() as db:
        body = await lookup_store.suggest_sources(db, CC, key_field="delivery_number", days=3650)
    packing = next(m for m in body["methods"] if m["method"] == "NewDeliveryPackage")
    assert "CustomerName" not in {s["field"] for s in packing["key_spellings"]}


async def test_the_values_endpoint_shows_where_a_value_came_from():
    """The screen shows this so a wrong value can be traced back to the method that said it."""
    await _declare()
    await _fold(lambda j: _packing(j, delivery="25810", customer="BAGELMAN BRIGHTON", at=T0))
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsLookupValue).where(
            AnalyticsLookupValue.customer_code == CC))).scalars().all()
    assert [(r.value, r.source_method, r.valid_to) for r in rows] == \
        [("BAGELMAN BRIGHTON", "NewDeliveryPackage", None)]
