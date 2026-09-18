"""Chunk 117: declaring a settlement from the API, previewing one key, and reading rows grouped.

The store is pinned in `test_analytics_settle_store_chunk117.py`. This pins what a screen sees:
that a declaration is validated against the tenant's own approved fields, that creating one settles
every existing key straight away, that editing the rules rebuilds the rows, that the preview shows
nine calls becoming one row, and that a grouped read resolves a `lookup:` path the way a metric does.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer

CC = "test_chunk117api"
T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)
MODELS = (AnalyticsSettledRow, AnalyticsSettlement, AnalyticsLookupValue, AnalyticsLookup,
          AnalyticsFieldRegistry, AnalyticsFact)

BODY = {
    "name": "pick_release", "description": "One row per pick-list release",
    "reads": ["ConfirmPickLine"], "key": ["attr:ReportingNumber"],
    "carry": ["delivery_number", "item_number", "attr:OrderLine", "warehouse", "lot_number"],
    "values": [
        {"name": "expected", "rule": "first", "field": "attr:ExpectedQuantity"},
        {"name": "picked", "rule": "sum", "field": "attr:QuantityPicked", "statuses": ["success"]},
        {"name": "calls", "rule": "count"},
        {"name": "refused", "rule": "count", "statuses": ["error"]},
        {"name": "shortfall", "rule": "difference", "left": "picked", "right": "expected"},
        {"name": "is_short", "rule": "flag", "left": "shortfall", "op": "<", "right_value": "0"},
    ],
}


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="settle api probe", timezone="Europe/London"))
        for f in ("ReportingNumber", "ExpectedQuantity", "QuantityPicked", "OrderLine"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="request",
                                          field=f, captured=True, seen_count=9))
        await db.commit()
    yield
    await _wipe()


def _fact(minutes, expected, picked, *, rep="540551", status="success", delivery="27907", item="104568"):
    when = T0 + timedelta(minutes=minutes)
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.date(),
        transaction_name="JIT and Shorts Pick (Brighton)", method="ConfirmPickLine", status=status,
        quantity_classification="pick" if Decimal(str(picked)) > 0 else "attempt",
        warehouse="BRI", delivery_number=delivery, item_number=item, lot_number="2609161191",
        user_name="FNACHONLEO",
        attributes={"ReportingNumber": rep, "ExpectedQuantity": str(expected),
                    "QuantityPicked": str(picked), "OrderLine": "21"}, created_at=when)


async def _plant(facts):
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()


def _release_540551():
    return [_fact(0, 10, 9)] + [_fact(7 + i, 1, 1, status="error") for i in range(8)]


# ==================================================== 1. declaring

async def test_creating_a_settlement_settles_every_existing_key_at_once():
    """The rows exist the moment the declaration does, rather than trickling in with the next fold."""
    await _plant(_release_540551() + [_fact(30, 4, 4, rep="A1", item="100606")])
    async with async_session() as db:
        out = await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
    assert out["rows"] == 2 and out["name"] == "pick_release"
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == CC))).scalars().all()
    assert {r.key for r in rows} == {"540551", "A1"}


async def test_the_listing_says_how_many_rows_each_settlement_holds():
    await _plant(_release_540551())
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        listed = await api.list_settlements(customer=CC, db=db)
    assert listed["settlements"][0]["rows"] == 1
    assert "first" in listed["rules"] and "flag" in listed["rules"]


async def test_a_field_nobody_approved_is_refused_by_name():
    """Fails closed like every other name here: a typo would otherwise settle nothing, silently."""
    body = {**BODY, "values": BODY["values"] + [{"name": "x", "rule": "sum", "field": "attr:Typo"}]}
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.create_settlement(body=body, backfill=False, customer=CC, db=db)
    assert caught.value.status_code == 400
    assert any("attr:Typo" in p and "approved" in p for p in caught.value.detail)


async def test_the_pure_rules_are_applied_too():
    body = {**BODY, "values": [{"name": "d", "rule": "difference", "left": "nope", "right": "expected"}]}
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.create_settlement(body=body, backfill=False, customer=CC, db=db)
    assert any("'nope'" in p for p in caught.value.detail)


async def test_a_name_that_could_not_be_addressed_is_refused():
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.create_settlement(body={**BODY, "name": "pick:release"}, backfill=False,
                                        customer=CC, db=db)
    assert caught.value.status_code == 400


async def test_the_same_name_twice_is_a_conflict():
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=False, customer=CC, db=db)
        with pytest.raises(HTTPException) as caught:
            await api.create_settlement(body=BODY, backfill=False, customer=CC, db=db)
    assert caught.value.status_code == 409


# ==================================================== 2. editing

async def test_changing_the_rules_rebuilds_every_row_under_the_new_rules():
    """A changed rule set makes every existing row wrong, so they are all remade."""
    await _plant(_release_540551())
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        # picked now counts EVERY call, refused ones included: 17 rather than 9
        loose = [dict(v) for v in BODY["values"]]
        loose[1] = {"name": "picked", "rule": "sum", "field": "attr:QuantityPicked"}
        out = await api.update_settlement(name="pick_release", body={"values": loose}, customer=CC, db=db)
    assert "rebuilt 1 row" in out["detail"]
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsSettledRow).where(AnalyticsSettledRow.customer_code == CC))
    assert row.attributes["picked"] == "17"


async def test_changing_only_the_description_rebuilds_nothing():
    await _plant(_release_540551())
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        before = (await db.scalar(select(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == CC))).settled_at
        out = await api.update_settlement(name="pick_release", body={"description": "renamed"},
                                          customer=CC, db=db)
    assert "detail" not in out
    async with async_session() as db:
        after = (await db.scalar(select(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == CC))).settled_at
    assert after == before


async def test_an_unknown_settlement_is_a_404():
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.update_settlement(name="nope", body={}, customer=CC, db=db)
    assert caught.value.status_code == 404


# ==================================================== 3. the preview

async def test_the_preview_shows_nine_calls_becoming_one_row():
    """The thing that makes a rule checkable before it is switched on."""
    await _plant(_release_540551())
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=False, customer=CC, db=db)
        out = await api.preview_settlement_key(name="pick_release", key=["540551"], customer=CC, db=db)
    assert len(out["calls"]) == 9
    assert out["calls"][0]["QuantityPicked"] == "9" and out["calls"][0]["status"] == "success"
    assert out["calls"][1]["status"] == "error"
    assert out["settled"]["values"] == {
        "expected": "10", "picked": "9", "calls": "9", "refused": "8", "shortfall": "-1", "is_short": "1"}
    assert out["settled"]["carried"]["delivery_number"] == "27907"


async def test_the_preview_refuses_a_key_of_the_wrong_shape():
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=False, customer=CC, db=db)
        with pytest.raises(HTTPException) as caught:
            await api.preview_settlement_key(name="pick_release", key=["a", "b"], customer=CC, db=db)
    assert caught.value.status_code == 400


# ==================================================== 4. reading grouped

async def _plant_two_deliveries():
    await _plant(_release_540551() + [
        _fact(30, 4, 4, rep="A1", delivery="27907", item="100606"),
        _fact(40, 7, 4, rep="B1", delivery="25810", item="100230"),
        _fact(50, 7, 0, rep="B2", delivery="25810", item="100230"),
    ])


async def test_rows_grouped_by_a_carried_field_sum_one_expectation_per_release():
    """The sum the calls could never give."""
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.read_settlement_rows(name="pick_release", group_by=["delivery_number"],
                                             start=None, end=None, limit=500, customer=CC, db=db)
    by = {r["dimensions"][0]: r for r in out["rows"]}
    assert by["27907"]["expected"] == "14" and by["27907"]["picked"] == "13" and by["27907"]["rows"] == 2
    assert by["25810"]["expected"] == "14" and by["25810"]["picked"] == "4" and by["25810"]["is_short"] == "2"
    assert out["values"][0] == "expected"


async def test_rows_grouped_by_a_looked_up_customer_resolve_through_the_delivery_key():
    """Exactly as a metric does: grouped by the lookup's KEY, then re-labelled, with groups that now
    coincide merged. Two deliveries of one customer become one row."""
    await _plant_two_deliveries()
    async with async_session() as db:
        db.add(AnalyticsLookup(customer_code=CC, name="delivery", key_field="delivery_number",
                               attributes=[{"name": "customer_name", "stable": True,
                                            "on_conflict": "first_wins", "sources": []}], enabled=True))
        for key in ("27907", "25810"):
            db.add(AnalyticsLookupValue(customer_code=CC, lookup="delivery", key=key,
                                        attribute="customer_name", value="GOODWOOD",
                                        valid_from=datetime(1970, 1, 1, tzinfo=timezone.utc),
                                        origin="observed", observations=1,
                                        first_seen_at=T0, last_seen_at=T0))
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.read_settlement_rows(name="pick_release",
                                             group_by=["lookup:delivery.customer_name"],
                                             start=None, end=None, limit=500, customer=CC, db=db)
    assert len(out["rows"]) == 1
    row = out["rows"][0]
    assert row["dimensions"] == ["GOODWOOD"]
    assert row["expected"] == "28" and row["picked"] == "17" and row["rows"] == 4


async def test_rows_can_be_windowed_on_when_the_release_began():
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.read_settlement_rows(name="pick_release", group_by=[],
                                             start=T0 + timedelta(minutes=35), end=None, limit=500,
                                             customer=CC, db=db)
    assert out["rows"][0]["rows"] == 2


# ==================================================== 5. seeing the rows themselves

async def test_the_rows_can_be_listed_newest_first_with_a_total():
    """A grouped read answers "how much"; this answers "show me"."""
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.list_settlement_rows(name="pick_release", start=None, end=None, search=None,
                                             limit=2, offset=0, customer=CC, db=db)
    assert out["total"] == 4 and len(out["rows"]) == 2
    assert out["rows"][0]["key"] == ["B2"]          # +50 minutes, the newest
    assert out["rows"][0]["attributes"]["expected"] == "7"
    assert out["values"] == ["expected", "picked", "calls", "refused", "shortfall", "is_short"]


async def test_the_list_can_be_searched_by_the_things_somebody_types():
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        by_delivery = await api.list_settlement_rows(name="pick_release", start=None, end=None,
                                                     search="25810", limit=100, offset=0, customer=CC, db=db)
        by_key = await api.list_settlement_rows(name="pick_release", start=None, end=None,
                                                search="540551", limit=100, offset=0, customer=CC, db=db)
    assert by_delivery["total"] == 2 and by_key["total"] == 1


async def test_grouping_by_the_key_itself_reaches_one_release():
    """The bottom of the drill-down: one row per release, and the sums are its own values."""
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.read_settlement_rows(name="pick_release", group_by=["delivery_number", "key"],
                                             start=None, end=None, limit=500, customer=CC, db=db)
    rows = {tuple(r["dimensions"]): r for r in out["rows"]}
    assert rows[("27907", "540551")]["picked"] == "9" and rows[("27907", "540551")]["rows"] == 1


async def test_a_grouped_read_says_when_it_hit_its_limit():
    """Grouped by its own key a settlement has as many groups as rows, 6,252 on the live tenant, and
    a silent cap dropped whole releases from the bottom of the drill-down. Hitting the cap is now
    reported, and the cap is high enough that the screen never has to."""
    await _plant_two_deliveries()
    async with async_session() as db:
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        capped = await api.read_settlement_rows(name="pick_release", group_by=["key"], start=None, end=None,
                                                limit=2, customer=CC, db=db)
        whole = await api.read_settlement_rows(name="pick_release", group_by=["key"], start=None, end=None,
                                               limit=500, customer=CC, db=db)
    assert capped["truncated"] is True and len(capped["rows"]) == 2
    assert whole["truncated"] is False and len(whole["rows"]) == 4


async def test_the_list_resolves_what_a_lookup_can_reach_from_a_carried_key():
    """A settled row carries the delivery number and nothing else about the delivery, as a fact
    does. Listing rows without the customer name would show the keys and hide the names."""
    await _plant_two_deliveries()
    async with async_session() as db:
        db.add(AnalyticsLookup(customer_code=CC, name="delivery", key_field="delivery_number",
                               attributes=[{"name": "customer_name", "stable": True,
                                            "on_conflict": "first_wins", "sources": []}], enabled=True))
        db.add(AnalyticsLookupValue(customer_code=CC, lookup="delivery", key="27907",
                                    attribute="customer_name", value="GOODWOOD",
                                    valid_from=datetime(1970, 1, 1, tzinfo=timezone.utc),
                                    origin="observed", observations=1, first_seen_at=T0, last_seen_at=T0))
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)
        out = await api.list_settlement_rows(name="pick_release", start=None, end=None, search=None,
                                             limit=100, offset=0, customer=CC, db=db)
    assert out["looked_up"] == ["delivery.customer_name"]
    by_key = {r["key"][0]: r for r in out["rows"]}
    assert by_key["540551"]["looked_up"]["delivery.customer_name"] == "GOODWOOD"
    # Delivery 25810 has no value in the lookup: honest absence, never a blank that reads as a name.
    assert by_key["B1"]["looked_up"]["delivery.customer_name"] is None
