"""Chunk 117: settled rows, kept current by the fold and read back grouped.

The pure rules are pinned in `test_analytics_settle_chunk116.py`. This pins the seam around them:
that a declaration round-trips through its stored document, that the fold's hook recomputes exactly
the keys it touched from ALL their calls, that folding the same window twice leaves the rows
byte-identical, and that a grouped read over settled rows sums what the calls could not.

Release 540551 is the worked example throughout: one successful pick of 9, then eight attempts to
confirm the last unit, every one refused by M3 and every one still carrying `QuantityPicked = 1`.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.services.analytics import settle as st
from app.services.analytics import settle_store

CC = "test_chunk117"
T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsSettledRow, AnalyticsSettlement, AnalyticsFact):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="settle probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


def _fact(minutes, expected, picked, *, rep="540551", status="success", delivery="27907",
          item="104568", lot="2609161191", line="21"):
    when = T0 + timedelta(minutes=minutes)
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.date(),
        transaction_name="JIT and Shorts Pick (Brighton)", method="ConfirmPickLine", status=status,
        quantity_classification="pick" if Decimal(str(picked)) > 0 else "attempt",
        warehouse="BRI", delivery_number=delivery, item_number=item, lot_number=lot or None,
        user_name="FNACHONLEO",
        attributes={"ReportingNumber": rep, "ExpectedQuantity": str(expected),
                    "QuantityPicked": str(picked), "OrderLine": line, "PickListSuffix": "3"},
        created_at=when)


def _fact_dict(f: AnalyticsFact) -> dict:
    return {c.name: getattr(f, c.name) for c in AnalyticsFact.__table__.columns}


PICK_RELEASE = st.Settlement(
    name="pick_release", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number", "item_number", "attr:OrderLine", "warehouse", "transaction_name",
           "user_name", "lot_number"),
    values=(
        st.Settled("expected", st.Rule.first, field="attr:ExpectedQuantity"),
        st.Settled("picked", st.Rule.sum, field="attr:QuantityPicked", statuses=frozenset({"success"})),
        st.Settled("calls", st.Rule.count),
        st.Settled("refused", st.Rule.count, statuses=frozenset({"error"})),
        st.Settled("shortfall", st.Rule.difference, left="picked", right="expected"),
        st.Settled("is_short", st.Rule.flag, left="shortfall", op="<", right_value=Decimal(0)),
    ))

def _release_540551():
    """Fresh ORM instances every call. Built once at import, the first test to add them would make
    them persistent, and every later `db.add` of the same objects would insert nothing."""
    return [_fact(0, 10, 9)] + [_fact(7 + i, 1, 1, status="error") for i in range(8)]


async def _declare(settlement=PICK_RELEASE):
    async with async_session() as db:
        db.add(AnalyticsSettlement(customer_code=CC, name=settlement.name,
                                   definition=settle_store.to_json(settlement), enabled=True))
        await db.commit()


async def _rows():
    async with async_session() as db:
        return (await db.execute(select(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == CC).order_by(AnalyticsSettledRow.key))).scalars().all()


# ==================================================== 1. the declaration survives storage

def test_a_settlement_round_trips_through_its_document():
    """What is stored is what was declared, rule for rule, filter for filter."""
    doc = settle_store.to_json(PICK_RELEASE)
    assert settle_store.from_json("pick_release", doc) == PICK_RELEASE


def test_an_unknown_rule_is_refused_on_the_way_in():
    with pytest.raises(ValueError, match="unknown rule"):
        settle_store.from_json("x", {"reads": ["A"], "key": ["attr:K"], "carry": [],
                                     "values": [{"name": "v", "rule": "median"}]})


# ==================================================== 2. the fold's hook

async def test_the_hook_settles_the_keys_the_facts_touched():
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
    async with async_session() as db:
        written = await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
    assert written == {"pick_release": 1}
    rows = await _rows()
    assert len(rows) == 1
    row = rows[0]
    assert row.key == "540551" and row.key_parts == ["540551"]
    assert row.attributes["expected"] == "10"
    assert row.attributes["picked"] == "9"
    assert row.attributes["refused"] == "8"
    assert row.attributes["shortfall"] == "-1"
    assert row.attributes["is_short"] == "1"
    assert row.calls == 9


async def test_a_settled_row_has_the_shape_of_a_fact_row():
    """Typed columns filled where carried, so the existing group-by and the delivery lookup read it."""
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
        await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
    row = (await _rows())[0]
    assert row.delivery_number == "27907" and row.item_number == "104568"
    assert row.warehouse == "BRI" and row.lot_number == "2609161191"
    assert row.method == "ConfirmPickLine"
    assert row.event_time == T0 and row.business_date == T0.date()
    assert row.attributes["OrderLine"] == "21"


async def test_a_key_is_recomputed_from_all_its_calls_not_just_the_window():
    """The window carries only the last refused attempt. The row must still say 9 picked over 9
    calls, because the release is a whole and the fold's batching is an accident of timing."""
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
        # only the final call is "in the window"
        await settle_store.settle_touched(db, CC, [_fact_dict(release[-1])])
        await db.commit()
    row = (await _rows())[0]
    assert row.calls == 9 and row.attributes["picked"] == "9"


async def test_settling_the_same_window_twice_changes_nothing():
    """Recompute-and-replace. Fold it twice and the row is the same row."""
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
        dicts = [_fact_dict(f) for f in release]
        await settle_store.settle_touched(db, CC, dicts)
        await db.commit()
        first = (await _rows())[0]
        await settle_store.settle_touched(db, CC, dicts)
        await db.commit()
    rows = await _rows()
    assert len(rows) == 1
    assert rows[0].attributes == first.attributes and rows[0].calls == first.calls


async def test_a_new_call_for_a_settled_key_rewrites_its_row():
    """A late success of the missing unit turns a short release into a filled one."""
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
        await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
        late = _fact(20, 1, 1)
        db.add(late)
        await db.commit()
        await settle_store.settle_touched(db, CC, [_fact_dict(late)])
        await db.commit()
    row = (await _rows())[0]
    assert row.attributes["picked"] == "10" and row.attributes["is_short"] == "0" and row.calls == 10


async def test_facts_that_touch_no_settlement_write_nothing():
    release = _release_540551()
    await _declare()
    other = _fact(0, 10, 9)
    other.method = "GetOldestItemBalanceAPI"
    async with async_session() as db:
        db.add(other)
        await db.commit()
        written = await settle_store.settle_touched(db, CC, [_fact_dict(other)])
        await db.commit()
    assert written == {} and await _rows() == []


async def test_a_disabled_settlement_is_not_maintained():
    release = _release_540551()
    async with async_session() as db:
        db.add(AnalyticsSettlement(customer_code=CC, name="pick_release",
                                   definition=settle_store.to_json(PICK_RELEASE), enabled=False))
        for f in release:
            db.add(f)
        await db.commit()
        written = await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
    assert written == {} and await _rows() == []


# ==================================================== 3. resettling from scratch

async def test_resettle_all_builds_every_key_a_settlement_has():
    """For a settlement declared after the facts arrived, or edited after its rows were made."""
    release = _release_540551()
    await _declare()
    seven = []
    for i, rep in enumerate(["509464", "514512", "514522", "514540", "514966", "514970", "515066"]):
        seven.append(_fact(i * 10, 9, 0, rep=rep, delivery="25810", item="104526", line="4"))
    async with async_session() as db:
        for f in seven + release:
            db.add(f)
        await db.commit()
        written = await settle_store.resettle_all(db, CC, PICK_RELEASE)
        await db.commit()
    assert written == 8
    assert len(await _rows()) == 8


# ==================================================== 4. reading back, grouped

async def _seed_two_deliveries():
    release = _release_540551()
    await _declare()
    rows = release + [
        _fact(30, 4, 4, rep="A1", delivery="27907", item="100606"),
        _fact(40, 7, 4, rep="B1", delivery="25810", item="100230"),
        _fact(50, 7, 0, rep="B2", delivery="25810", item="100230"),
    ]
    async with async_session() as db:
        for f in rows:
            db.add(f)
        await db.commit()
        await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in rows])
        await db.commit()


async def test_a_grouped_read_sums_settled_values_across_releases():
    """This is the sum the calls could never give: one expectation per release, added across them."""
    release = _release_540551()
    await _seed_two_deliveries()
    async with async_session() as db:
        out = await settle_store.read_grouped(db, CC, PICK_RELEASE, group_by=["delivery_number"],
                                              since=None, until=None)
    by = {o["dimensions"][0]: o for o in out}
    assert by["27907"]["rows"] == 2 and by["27907"]["expected"] == "14" and by["27907"]["picked"] == "13"
    assert by["25810"]["rows"] == 2 and by["25810"]["expected"] == "14" and by["25810"]["picked"] == "4"
    assert by["25810"]["shortfall"] == "-10" and by["25810"]["is_short"] == "2"


async def test_a_grouped_read_can_group_by_a_carried_attribute():
    release = _release_540551()
    await _seed_two_deliveries()
    async with async_session() as db:
        out = await settle_store.read_grouped(db, CC, PICK_RELEASE, group_by=["delivery_number", "attr:OrderLine"],
                                              since=None, until=None)
    assert {tuple(o["dimensions"]) for o in out} == {("27907", "21"), ("25810", "21")}


async def test_a_grouped_read_with_no_grouping_is_one_total():
    release = _release_540551()
    await _seed_two_deliveries()
    async with async_session() as db:
        out = await settle_store.read_grouped(db, CC, PICK_RELEASE, group_by=[], since=None, until=None)
    assert len(out) == 1 and out[0]["rows"] == 4 and out[0]["expected"] == "28"


async def test_a_grouped_read_windows_on_when_the_release_began():
    release = _release_540551()
    await _seed_two_deliveries()
    async with async_session() as db:
        out = await settle_store.read_grouped(db, CC, PICK_RELEASE, group_by=[],
                                              since=T0 + timedelta(minutes=35), until=None)
    assert out[0]["rows"] == 2  # B1 at +40 and B2 at +50; 540551 at +0 and A1 at +30 are out


async def test_the_preview_shows_the_calls_and_the_row_they_settle_to():
    """Computed live from the calls, so it is right before the fold has ever run."""
    release = _release_540551()
    await _declare()
    async with async_session() as db:
        for f in release:
            db.add(f)
        await db.commit()
        calls, settled = await settle_store.read_key(db, CC, PICK_RELEASE, ("540551",))
    assert len(calls) == 9 and calls[0]["event_time"] == T0
    assert settled is not None and settled.values["picked"] == Decimal("9")


async def test_the_preview_of_an_unknown_key_is_honest():
    await _declare()
    async with async_session() as db:
        calls, settled = await settle_store.read_key(db, CC, PICK_RELEASE, ("nope",))
    assert calls == [] and settled is None


async def test_a_release_with_no_lot_is_written_beside_one_that_has_a_lot():
    """The first live backfill was a 500. 1,025 of 6,245 releases carry no lot, so their rows lacked
    a column the others had, and a multi-row insert needs every row to name the same columns. Every
    typed column is now on every row, None where there was nothing to carry."""
    await _declare()
    with_lot = _release_540551()
    without = [_fact(60, 4, 4, rep="NOLOT", item="100606", lot="")]
    async with async_session() as db:
        for f in with_lot + without:
            db.add(f)
        await db.commit()
        written = await settle_store.resettle_all(db, CC, PICK_RELEASE)
        await db.commit()
    assert written == 2
    rows = {r.key: r for r in await _rows()}
    assert rows["540551"].lot_number == "2609161191"
    assert rows["NOLOT"].lot_number is None
    assert "lot_number" not in rows["NOLOT"].attributes


# ==================================================== how long a release took (chunk 118)

def _timed_release():
    """Release 540551 with the handheld's start time on every call, as a London-clock string."""
    rows = _release_540551()
    starts = ["2026-09-18 06:43:42.000"] + [f"2026-09-18 06:5{i}:00.000" for i in range(8)]
    for f, s in zip(rows, starts):
        f.attributes = {**f.attributes, "StartDateTime": s}
    return rows


TIMED_RELEASE = st.Settlement(
    name="pick_release", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number",),
    values=PICK_RELEASE.values + (
        st.Settled("started_at", st.Rule.first, field="attr:StartDateTime"),
        st.Settled("finished_at", st.Rule.max, field="event_time"),
        st.Settled("duration_s", st.Rule.difference, left="finished_at", right="started_at")))


async def test_the_store_reads_a_start_time_in_the_tenant_zone_and_stores_it_as_iso():
    """The tenant is Europe/London; 06:43:42 on its clock is 05:43:42 UTC. The row keeps the zone."""
    release = _timed_release()
    await _declare(TIMED_RELEASE)
    async with async_session() as db:
        db.add_all(release)
        await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
        row = (await db.execute(select(AnalyticsSettledRow).where(AnalyticsSettledRow.customer_code == CC))).scalar_one()
    assert row.attributes["started_at"] == "2026-09-18T06:43:42+01:00"
    assert row.attributes["finished_at"] == "2026-09-18T05:58:42+00:00"
    assert row.attributes["duration_s"] == "900"


async def test_a_grouped_read_sums_the_seconds_and_leaves_a_time_blank():
    """A grouped read casts every settled value to a number. A time is not one, and until this the
    cast raised inside PostgreSQL and the whole read failed."""
    release = _timed_release()
    await _declare(TIMED_RELEASE)
    async with async_session() as db:
        db.add_all(release)
        await settle_store.settle_touched(db, CC, [_fact_dict(f) for f in release])
        await db.commit()
        out = await settle_store.read_grouped(db, CC, TIMED_RELEASE, group_by=["delivery_number"],
                                              since=None, until=None)
    assert out[0]["duration_s"] == "900" and out[0]["started_at"] is None and out[0]["picked"] == "9"
