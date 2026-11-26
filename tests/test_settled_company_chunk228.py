"""Chunk 228: a settled row says which WMS company it belongs to (stage D, step 50; 1d stage 2b).

A settled row is recomputed from the facts of one key, and facts carry their company since
chunk 226. The company rides with every settled row, whatever the settlement's own `carry` list
says: it is part of the row's place, not a field somebody chooses to keep, and a settlement
written tomorrow must not be able to forget it.

Pinned here
-----------
    settle     the company of the first call that has one, with or without it in `carry`
    store      the typed column is filled; the attribute bag is unchanged unless the settlement
               carries the company itself; a grouped read can group and filter by it
    validate   a settled value may not be named like a field every row carries
    backfill   older rows take the customer's only company for their warehouse, from the facts;
               a warehouse filed under two companies is left for a re-settle
    scope      a site-limited person reads settled rows by the company and warehouse pair
"""
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.services import scoped_data
from app.services.analytics import company_backfill as backfill
from app.services.analytics import settle as st
from app.services.analytics import settle_query as sq
from app.services.analytics import settle_store
from app.services.scoped_data import DataScope, Place

CC = "test_chunk228"
T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)

#: Carries the warehouse and NOT the company, as both live settlements do.
PICK_RELEASE = st.Settlement(
    name="pick_release", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number", "warehouse"),
    values=(st.Settled("picked", st.Rule.sum, field="attr:QuantityPicked"),
            st.Settled("calls", st.Rule.count)))


def _call(minutes: int, *, rep: str = "540551", company: str | None = "915",
          warehouse: str | None = "BRI") -> dict:
    when = T0 + timedelta(minutes=minutes)
    return {"event_time": when, "method": "ConfirmPickLine", "status": "success",
            "company": company, "warehouse": warehouse, "delivery_number": "27907",
            "attributes": {"ReportingNumber": rep, "QuantityPicked": "1"}}


# ==================================================== 1. settling, pure

def test_a_settled_row_takes_the_company_of_its_first_call_that_has_one():
    calls = [_call(0, company=None), _call(1, company="915"), _call(2, company="920")]
    (row,) = st.settle(calls, PICK_RELEASE).values()
    assert row.carried["company"] == "915" and row.carried["warehouse"] == "BRI"


def test_calls_without_a_company_give_a_row_without_one():
    (row,) = st.settle([_call(0, company=None)], PICK_RELEASE).values()
    assert "company" not in row.carried


def test_a_settlement_that_carries_the_company_itself_is_not_carried_twice():
    carrying = st.Settlement(name="x", reads=PICK_RELEASE.reads, key=PICK_RELEASE.key,
                             carry=("company", "warehouse"), values=PICK_RELEASE.values)
    assert st.carried_fields(carrying) == ("company", "warehouse")
    assert st.carried_fields(PICK_RELEASE) == ("delivery_number", "warehouse", "company")


def test_a_settled_value_may_not_be_named_like_a_field_every_row_carries():
    clash = st.Settlement(name="x", reads=PICK_RELEASE.reads, key=PICK_RELEASE.key, carry=(),
                          values=(st.Settled("company", st.Rule.count),))
    assert any("company" in problem for problem in st.validate(clash))
    assert st.validate(PICK_RELEASE) == []


# ==================================================== 2. the store

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
        db.add(Customer(customer_code=CC, name="settled company probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


def _fact(minutes: int, *, rep: str, company: str | None, warehouse: str | None = "BRI"):
    when = T0 + timedelta(minutes=minutes)
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(),
        source_started_at=when, source_version_hash=uuid.uuid4().hex[:8], revision=1,
        event_time=when, business_date=when.date(), method="ConfirmPickLine", status="success",
        quantity_classification="pick", company=company, warehouse=warehouse,
        delivery_number="27907", attributes={"ReportingNumber": rep, "QuantityPicked": "1"},
        created_at=when)


async def _settle(*facts, settlement: st.Settlement = PICK_RELEASE) -> None:
    async with async_session() as db:
        db.add(AnalyticsSettlement(customer_code=CC, name=settlement.name,
                                   definition=settle_store.to_json(settlement), enabled=True))
        db.add_all(facts)
        await db.commit()
        rows = [{col.name: getattr(f, col.name) for col in AnalyticsFact.__table__.columns}
                for f in facts]
        await settle_store.settle_touched(db, CC, rows)
        await db.commit()


async def _rows() -> dict[str, AnalyticsSettledRow]:
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsSettledRow).where(
            AnalyticsSettledRow.customer_code == CC))).scalars().all()
    return {r.key: r for r in rows}


async def test_the_stored_row_has_the_company_as_a_typed_column():
    await _settle(_fact(0, rep="A", company="915"), _fact(1, rep="B", company="920"),
                  _fact(2, rep="C", company=None, warehouse=None))
    rows = await _rows()
    assert {k: (r.company, r.warehouse) for k, r in rows.items()} == {
        "A": ("915", "BRI"), "B": ("920", "BRI"), "C": (None, None)}
    # the bag keeps exactly what the settlement asked for: the company was not asked for
    assert "company" not in rows["A"].attributes and rows["A"].attributes["warehouse"] == "BRI"


async def test_a_settlement_that_carries_the_company_has_it_in_the_bag_too():
    carrying = st.Settlement(name="pick_release", reads=PICK_RELEASE.reads, key=PICK_RELEASE.key,
                             carry=("company", "warehouse"), values=PICK_RELEASE.values)
    await _settle(_fact(0, rep="A", company="915"), settlement=carrying)
    row = (await _rows())["A"]
    assert row.company == "915" and row.attributes["company"] == "915"


async def test_a_grouped_read_can_group_and_filter_by_the_company():
    await _settle(_fact(0, rep="A", company="915"), _fact(1, rep="B", company="915"),
                  _fact(2, rep="C", company="920"))
    async with async_session() as db:
        grouped = await settle_store.read_grouped(db, CC, PICK_RELEASE, group_by=["company"],
                                                  since=None, until=None)
        only = await settle_store.read_grouped(
            db, CC, PICK_RELEASE, group_by=[], since=None, until=None,
            filters=[sq.Filter("company", "==", "920")])
    assert {o["dimensions"][0]: o["rows"] for o in grouped} == {"915": 2, "920": 1}
    assert only[0]["rows"] == 1


# ==================================================== 3. the backfill

async def _forget() -> None:
    async with async_session() as db:
        await db.execute(update(AnalyticsSettledRow).where(AnalyticsSettledRow.customer_code == CC)
                         .values(company=None))
        await db.commit()


async def _companies() -> dict[str, str | None]:
    return {k: r.company for k, r in (await _rows()).items()}


async def test_older_rows_take_the_customers_only_company_for_their_warehouse():
    await _settle(_fact(0, rep="A", company="915"), _fact(1, rep="B", company="915"),
                  _fact(2, rep="C", company=None, warehouse=None))
    await _forget()
    totals = await backfill.fill_settled_rows(async_session, CC)
    assert await _companies() == {"A": "915", "B": "915", "C": None}
    assert totals.filled == 2 and totals.is_finished is True


async def test_a_warehouse_filed_under_two_companies_is_left_for_a_re_settle():
    await _settle(_fact(0, rep="A", company="915"), _fact(1, rep="B", company="920"),
                  _fact(2, rep="L", company="920", warehouse="LON"))
    await _forget()
    totals = await backfill.fill_settled_rows(async_session, CC)
    assert await _companies() == {"A": None, "B": None, "L": "920"}
    assert totals.read == 3 and totals.filled == 1


async def test_a_dry_run_of_the_settled_rows_writes_nothing():
    await _settle(_fact(0, rep="A", company="915"))
    await _forget()
    totals = await backfill.fill_settled_rows(async_session, CC, dry_run=True)
    assert totals.filled == 1 and await _companies() == {"A": None}


async def test_the_script_fills_settled_rows(capsys):
    from scripts import backfill_fact_company as script
    await _settle(_fact(0, rep="A", company="915"))
    await _forget()
    args = script.build_parser().parse_args(
        [CC, "--table", "analytics_settled_rows", "--pause", "0"])
    assert await script.run(args) == 0
    assert "filled 1" in capsys.readouterr().out and await _companies() == {"A": "915"}


# ==================================================== 4. the scope

def test_settled_rows_are_limited_by_the_company_and_warehouse_pair():
    rule = scoped_data.registry.BY_TABLE["analytics_settled_rows"]
    scope = DataScope(CC, site_ids=("BRI",), places=(Place("915", "BRI"),))
    sql = str(rule.condition(scope).compile(compile_kwargs={"literal_binds": True}))
    assert "analytics_settled_rows.warehouse = 'BRI'" in sql
    assert "analytics_settled_rows.company = '915'" in sql
