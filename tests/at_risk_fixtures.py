"""Shared planting helpers for the deliveries-at-risk tests (chunks 144-150).

They write FACTS the way the fold would have and then settle them, so the board reads exactly the
rows the live system would hold: a routing call with the response fields the registry captures, pick
confirmations, package creations, standard loads and the milk load's list call. Lookup values are
written directly, because harvesting a list response is chunk 129's business, not this feature's.
Every helper takes the tenant code so each chunk keeps its own.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import (AnalyticsAtRiskCheck, AnalyticsAtRiskDelivery,
                                                      AnalyticsAtRiskRouteProfile, AnalyticsAtRiskSettings,
                                                      AnalyticsAtRiskTenantState)
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction
from app.persistence import partitioning as pt
from app.services.analytics import lookup as lk
from app.services.analytics import lookup_store
from app.services.analytics import settle as st
from app.services.analytics import settle_store
from app.services.analytics_at_risk import RULE_VERSION

LONDON = ZoneInfo("Europe/London")
ROUTE_SETTLEMENT_NAME = "delivery_route"
PICK_SETTLEMENT_NAME = "pick_release"
PICK_LINE_LOOKUP = "pick line"

#: The tenant-config settlement the deploy declares (plan, backend appendix section 2).
ROUTE_SETTLEMENT = st.Settlement(
    name=ROUTE_SETTLEMENT_NAME, reads=("GetNextDeliveryByRoute",), key=("attr:resp.DeliveryNumber",),
    carry=("attr:resp.Route", "attr:resp.CustomerName", "attr:resp.CustomerNumber", "warehouse"),
    values=(
        st.Settled(name="departure_date", rule=st.Rule.last, field="attr:resp.DeparatureDate"),
        st.Settled(name="departure_time", rule=st.Rule.last, field="attr:resp.DeparatureTime"),
        st.Settled(name="first_seen", rule=st.Rule.first, field="event_time"),
        st.Settled(name="last_seen", rule=st.Rule.last, field="event_time"),
        st.Settled(name="zones", rule=st.Rule.distinct_count, field="attr:resp.PickListSuffix"),
        st.Settled(name="calls", rule=st.Rule.count),
    ))

PICK_RELEASE = st.Settlement(
    name=PICK_SETTLEMENT_NAME, reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number", "item_number", "warehouse", "lot_number", "user_name", "transaction_name"),
    values=(
        st.Settled(name="expected", rule=st.Rule.first, field="attr:ExpectedQuantity"),
        st.Settled(name="picked", rule=st.Rule.sum, field="attr:QuantityPicked", statuses=frozenset({"success"})),
        st.Settled(name="calls", rule=st.Rule.count),
        st.Settled(name="shortfall", rule=st.Rule.difference, left="picked", right="expected"),
        st.Settled(name="is_short", rule=st.Rule.flag, left="shortfall", op="<", right_value=Decimal("0")),
        st.Settled(name="finished_at", rule=st.Rule.max, field="event_time"),
    ))

PICK_LINE_LOOKUP_DECL = lk.Lookup(
    name=PICK_LINE_LOOKUP, key_field="attr:ReportingNumber",
    attributes=(
        lk.Attribute(name="Location", sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "Location", list=True),)),
        lk.Attribute(name="DeliveryNumber", sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "DeliveryNumber", list=True),)),
        lk.Attribute(name="LineStatus", stable=False, on_conflict="latest_wins",
                     sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "LineStatus", list=True),)),
        lk.Attribute(name="ExpectedQty", sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "ExpectedQty", list=True),)),
    ))

MODELS = (AnalyticsAtRiskCheck, AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile, AnalyticsAtRiskSettings,
          AnalyticsAtRiskTenantState, AnalyticsSettledRow, AnalyticsSettlement, AnalyticsLookupValue, AnalyticsLookup,
          AnalyticsFact, AnalyticsFieldRegistry)


async def wipe(cc: str) -> None:
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == cc))
        await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == cc))
        await db.execute(delete(Job).where(Job.customer_code == cc))
        await db.execute(delete(Customer).where(Customer.customer_code == cc))
        await db.commit()


async def seed_tenant(cc: str, *, timezone_name: str = "Europe/London", settings_row: dict | None = None) -> None:
    """A tenant with both settlements, the extended pick-line lookup and, optionally, a settings row."""
    async with async_session() as db:
        db.add(Customer(customer_code=cc, name=f"{cc} at-risk probe", timezone=timezone_name))
        db.add(AnalyticsSettlement(customer_code=cc, name=ROUTE_SETTLEMENT_NAME, description="probe",
                                   definition=settle_store.to_json(ROUTE_SETTLEMENT), enabled=True))
        db.add(AnalyticsSettlement(customer_code=cc, name=PICK_SETTLEMENT_NAME, description="probe",
                                   definition=settle_store.to_json(PICK_RELEASE), enabled=True))
        db.add(AnalyticsLookup(customer_code=cc, name=PICK_LINE_LOOKUP, description="probe",
                               key_field=PICK_LINE_LOOKUP_DECL.key_field,
                               attributes=lookup_store.to_row(PICK_LINE_LOOKUP_DECL), enabled=True))
        if settings_row is not None:
            db.add(AnalyticsAtRiskSettings(customer_code=cc, **settings_row))
        await db.commit()


# ============================================================== facts

def _fact(cc: str, method: str, when: datetime, *, status: str = "success", delivery: str | None = None,
          transaction_name: str = "Brighton Stock Pick", attributes: dict, **columns) -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=cc, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when,
        business_date=when.astimezone(LONDON).date(), transaction_name=transaction_name, method=method,
        status=status, quantity_classification=columns.pop("quantity_classification", "non_quantity"),
        warehouse="BRI", delivery_number=delivery, attributes=attributes, created_at=when, **columns)


def route_fact(cc: str, delivery: str, *, route: str, dep_date: str, dep_time: str, when: datetime,
               suffix: str = "1", lines: int = 10, customer_name: str = "BOK SHOP HORSHAM",
               customer_number: str = "10567", status: str = "success") -> AnalyticsFact:
    """One `GetNextDeliveryByRoute` call that returned this delivery, with the response fields the
    registry captures. `delivery_number` is NOT set: on the live facts it lives only in the response."""
    return _fact(cc, "GetNextDeliveryByRoute", when, status=status, attributes={
        "Route": route, "DepartureDate": dep_date, "ToStockZone": "A1", "Picker": "BCHAM",
        "resp.DeliveryNumber": delivery, "resp.Route": route, "resp.DeparatureDate": dep_date,
        "resp.DeparatureTime": dep_time, "resp.PickListSuffix": suffix, "resp.StockZone": "A1",
        "resp.NumberOfLines": str(lines), "resp.LinesToPick": str(lines), "resp.LinesToPack": str(lines),
        "resp.PickingStatus": "40", "resp.PackingStatus": "10", "resp.HasPackages": "False",
        "resp.CustomerName": customer_name, "resp.CustomerNumber": customer_number}, user_name="BCHAM")


def pick_fact(cc: str, delivery: str, reporting_number: str, when: datetime, *, expected: str, picked: str,
              status: str = "success", item: str = "104568", user: str = "BCHAM", package: str | None = None) -> AnalyticsFact:
    """One pick confirmation. `package` is the package number the line was packed into; on the live
    data 93% of confirmations carry one, and it is where most packages are born."""
    return _fact(cc, "ConfirmPickLine", when, status=status, delivery=delivery, attributes={
        "ReportingNumber": reporting_number, "ExpectedQuantity": expected, "QuantityPicked": picked,
        "PickListSuffix": "1", "OrderLine": "1", "PackageNumber": package if package is not None else f"{delivery}/1-1"},
        quantity_classification="pick" if Decimal(picked) > 0 else "attempt", item_number=item, user_name=user,
        quantity=Decimal(picked))


def package_fact(cc: str, delivery: str, package: str, when: datetime, *, route: str = "BRI03") -> AnalyticsFact:
    """`NewDeliveryPackage`: the response value is the new package's number."""
    return _fact(cc, "NewDeliveryPackage", when, delivery=delivery, attributes={
        "DeliveryNumber": delivery, "Route": route, "DepartureDate": "20261002", "resp.value": package})


def load_fact(cc: str, delivery: str, package: str, when: datetime, *, dock: str) -> AnalyticsFact:
    return _fact(cc, "LoadDeliveryPackage", when, delivery=delivery, transaction_name="Standard Load", attributes={
        "DeliveryNumber": delivery, "PackageNumber": package, "LoadingDock": dock, "resp.value": "OK"})


def load_list_fact(cc: str, pairs: list[tuple[str, str]], when: datetime, *, dock: str = "BRI05") -> AnalyticsFact:
    """`LoadDeliveryPackageList`: the milk load. Its deliveries live inside a JSON string."""
    return _fact(cc, "LoadDeliveryPackageList", when, transaction_name="Milk Load (Brighton)", attributes={
        "LoadingDock": dock, "resp.value": "All Packages Loaded",
        "PackagesToLoad": json.dumps([{"DeliveryNumber": d, "PackageNumber": p} for d, p in pairs])})


# ============================================================== raw routing calls (for the backfill)

def routing_call(delivery: str, *, route: str, dep_date: str, dep_time: str, when: datetime,
                 customer_name: str = "BOK SHOP HORSHAM", customer_number: str = "10567", status: str = "success") -> dict:
    """One `GetNextDeliveryByRoute` call as `log_transactions` holds it: the response text, capped at
    500 characters by the ingest, with the fields in the order the live WMS writes them."""
    summary = json.dumps({"DeliveryNumber": delivery, "PickListSuffix": "1", "CustomerNumber": customer_number,
                          "CustomerName": customer_name, "CustomerPostCode": "BN3 4AD", "StockZone": "A1", "PickingSequence": "0",
                          "Route": route, "PickingStatus": "40", "Picker": "", "DeparatureDate": dep_date, "DeparatureTime": dep_time,
                          "PackingStatus": "10", "NumberOfLines": "1", "LinesToPick": "1", "LinesToPack": "1", "HasPackages": False,
                          "TotalDeliveries": 12, "TotalLines": 102, "PackageNumbers": [], "PackageDetails": [],
                          "M3UserCredentials": "HIDDEN", "AccessToken": "HIDDEN"}, separators=(",", ":"))[:500]
    return dict(method="GetNextDeliveryByRoute", started_at=when, ended_at=when, date=when.astimezone(LONDON).date(),
                status=status, response_summary=summary, transaction_name="Brighton Stock Pick", route=route,
                attributes={"Route": route, "DepartureDate": dep_date})


async def plant_calls(cc: str, calls: list[dict]) -> None:
    """Write raw transactions under one job, provisioning the day partitions the rows need."""
    async with async_session() as db:
        await pt.ensure_coverage(db, days=sorted({c["started_at"].astimezone(timezone.utc).date() for c in calls}),
                                 tables=("log_transactions",))
        job = Job(customer_code=cc, filename=f"{cc}.log", storage_key=f"{cc}/{uuid.uuid4().hex}/calls.log",
                  document_type="transaction_log", status="completed")
        db.add(job)
        await db.flush()
        for c in calls:
            db.add(LogTransaction(id=uuid.uuid4(), job_id=job.id, customer_code=cc, sealed=True, warehouse="BRI", **c))
        await db.commit()


# ============================================================== lookup values

def pick_line_values(cc: str, delivery: str, reporting_numbers: list[str], *, status: str = "40",
                     seen_at: datetime | None = None) -> list[AnalyticsLookupValue]:
    """What the `pick line` lookup holds once `ListPickLinesByUser` has listed these lines."""
    seen = seen_at or datetime.now(timezone.utc)
    out = []
    for rep in reporting_numbers:
        for attribute, value in (("DeliveryNumber", delivery), ("LineStatus", status), ("Location", "A03A")):
            out.append(AnalyticsLookupValue(customer_code=cc, lookup=PICK_LINE_LOOKUP, key=rep, attribute=attribute,
                                            value=value, valid_from=lk.BEGINNING, valid_to=None, origin="observed",
                                            source_method="ListPickLinesByUser", observations=1,
                                            first_seen_at=seen, last_seen_at=seen))
    return out


# ============================================================== planting and settling

async def plant(rows) -> None:
    async with async_session() as db:
        for r in rows:
            db.add(r)
        await db.commit()


async def settle(cc: str) -> None:
    """Both settlements from scratch over the planted facts, so settled rows exist without the fold."""
    async with async_session() as db:
        await settle_store.resettle_all(db, cc, ROUTE_SETTLEMENT)
        await settle_store.resettle_all(db, cc, PICK_RELEASE)
        await db.commit()


# ============================================================== clocks

WARN = timedelta(minutes=30)
GONE = timedelta(minutes=20)


def clock(usual_ready_at: datetime, *, source: str = "learned", warn: timedelta = WARN, gone: timedelta = GONE):
    """One van clock, for tests that judge a single delivery."""
    from app.services.analytics_at_risk import model
    return model.RouteClock(usual_ready_at=usual_ready_at, source=source, warn_before=warn, gone_after=gone)


def clock_before_departure(minutes: int, *, source: str = "learned"):
    """A `clock_for` whose van is usually ready `minutes` before each delivery's WMS departure."""
    def for_state(state):
        return clock(state.departure_at - timedelta(minutes=minutes), source=source)
    return for_state


# ============================================================== stored board rows

def closed_delivery(cc: str, delivery: str, *, route: str, departure_at: datetime, last_load_at: datetime | None,
                    outcome: str, last_pick_at: datetime | None = None, max_tier: str = "none",
                    first_flagged_at: datetime | None = None, transaction_names: tuple[str, ...] = ("Brighton Stock Pick",),
                    lines_expected: int | None = 5, lines_picked: int = 5, customer_name: str | None = "BOK SHOP HORSHAM",
                    route_loaded_at: datetime | None = None, lines_confirmed: int | None = None,
                    usual_ready_at: datetime | None = None, usual_ready_source: str | None = None,
                    route_loading_from: datetime | None = None) -> AnalyticsAtRiskDelivery:
    """A closed row, for profile, accuracy and history tests."""
    lead = None if last_load_at is None else Decimal(str(round((departure_at - last_load_at).total_seconds() / 60, 2)))
    return AnalyticsAtRiskDelivery(
        customer_code=cc, delivery_number=delivery, departure_date=departure_at.astimezone(LONDON).date(),
        departure_at=departure_at, route=route, customer_name=customer_name, tier="none", max_tier=max_tier,
        first_flagged_at=first_flagged_at, first_flagged_tier=max_tier if first_flagged_at else None,
        lines_expected=lines_expected, lines_confirmed=lines_picked if lines_confirmed is None else lines_confirmed,
        lines_picked=lines_picked, packages_created=2,
        packages_loaded=2 if outcome != "never_loaded" else 1, last_pick_at=last_pick_at, last_load_at=last_load_at,
        loading_expected=not outcome.startswith("picked_"), transaction_names=list(transaction_names), route_loaded_at=route_loaded_at,
        route_loading_from=route_loading_from, usual_ready_at=usual_ready_at,
        usual_ready_source=usual_ready_source or ("learned" if usual_ready_at is not None else None),
        status="closed", closed_at=departure_at, outcome=outcome, outcome_lead_min=lead,
        rule_version=RULE_VERSION, last_evaluated_at=departure_at)


def local_day(d: date, hour: int, minute: int = 0) -> datetime:
    """A London wall-clock instant as an aware UTC datetime."""
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=LONDON).astimezone(timezone.utc)
