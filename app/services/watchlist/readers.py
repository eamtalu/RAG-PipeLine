"""The data reads of the watchlist (stage D, step 14): one query per kind for all the watched
things of a customer at once, from the tables the pipeline already keeps (the at-risk delivery
rows, the facts, the harvested lookups) and the fill rate the Home snapshot loop already computed.
Used by runner.py (every tick), search.py (resolving names) and mirror.py (the WATCHABLES row).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_lookup import AnalyticsLookupValue
from app.services.advisories.agents import stock as stock_agent
from app.services.teams import home_snapshot

PICK_METHOD = "ConfirmPickLine"
ZERO_PICK = "attempt"
ITEM_LOOKUP, ITEM_NAME = "item description", "ItemDescription"
LOCATION_LOOKUP, ZONE_ATTRIBUTE = "location", "StockZone"


# ============================================================== deliveries and routes

async def delivery_rows(db: AsyncSession, customer_code: str, row_ids: Iterable[uuid.UUID]) -> dict[uuid.UUID, AnalyticsAtRiskDelivery]:
    """The at-risk rows of watched deliveries, by row id (a moved departure keeps its row)."""
    ids = list(row_ids)
    if not ids:
        return {}
    rows = (await db.execute(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == customer_code, AnalyticsAtRiskDelivery.id.in_(ids)))).scalars()
    return {r.id: r for r in rows}


async def open_delivery(db: AsyncSession, customer_code: str, delivery_number: str) -> AnalyticsAtRiskDelivery | None:
    """The open row of a delivery number (the latest departure when there are several), or None."""
    return await db.scalar(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == customer_code, AnalyticsAtRiskDelivery.status == "open",
        AnalyticsAtRiskDelivery.delivery_number == delivery_number).order_by(AnalyticsAtRiskDelivery.departure_at.desc()).limit(1))


async def open_rows(db: AsyncSession, customer_code: str) -> list[AnalyticsAtRiskDelivery]:
    """Every open at-risk row of the customer, by departure."""
    return list((await db.execute(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == customer_code, AnalyticsAtRiskDelivery.status == "open")
        .order_by(AnalyticsAtRiskDelivery.departure_at))).scalars())


async def route_aggregates(db: AsyncSession, customer_code: str, routes: Iterable[str], today: date) -> dict[str, dict]:
    """Per route: its open deliveries by tier with their lines, and what already closed today.

    (R14 with 3 open, 1 at risk, 2 closed today) -> {"R14": {"open": 3, "by_tier": {...}, "closed_today": 2, ...}}
    """
    wanted = sorted({r for r in routes if r})
    if not wanted:
        return {}
    out = {r: {"open": 0, "by_tier": {}, "lines_picked": 0, "lines_expected": 0, "next_departure": None,
               "closed_today": 0, "late_today": 0, "warehouses": set()} for r in wanted}
    d = AnalyticsAtRiskDelivery
    open_rows_ = (await db.execute(select(d).where(
        d.customer_code == customer_code, d.status == "open", d.route.in_(wanted)).order_by(d.departure_at))).scalars()
    for row in open_rows_:
        agg = out[row.route]
        agg["open"] += 1
        agg["by_tier"][row.tier or "none"] = agg["by_tier"].get(row.tier or "none", 0) + 1
        agg["lines_picked"] += int(row.lines_picked or 0)
        agg["lines_expected"] += int(row.lines_expected or 0)
        if agg["next_departure"] is None and row.departure_at is not None:
            agg["next_departure"] = row.departure_at.isoformat()
        if row.warehouse:
            agg["warehouses"].add(row.warehouse)
    closed = (await db.execute(
        select(d.route, func.count(), func.count().filter(d.outcome.in_(("loaded_late", "picked_late", "never_loaded"))))
        .where(d.customer_code == customer_code, d.status == "closed", d.route.in_(wanted), d.departure_date == today)
        .group_by(d.route))).all()
    for route, n, late in closed:
        out[route]["closed_today"] = int(n)
        out[route]["late_today"] = int(late)
    for agg in out.values():
        agg["warehouses"] = sorted(agg["warehouses"])
    return out


# ============================================================== products, locations, zones

async def pick_activity(db: AsyncSession, customer_code: str, *, since: datetime, items: Iterable[str] = (),
                        locations: Iterable[str] = ()) -> tuple[dict[str, dict], dict[str, dict]]:
    """Today's picks and zero-picks per watched item and per watched location, from the facts.

    (items [I1], locations [A-01]) -> ({"I1": {"picks": 5, "zero_picks": 1, ...}}, {"A-01": {...}})
    """
    f = AnalyticsFact
    by_item: dict[str, dict] = {}
    by_location: dict[str, dict] = {}
    wanted_items = sorted({i for i in items if i})
    if wanted_items:
        rows = (await db.execute(
            select(f.item_number, func.count(), func.count().filter(f.quantity_classification == ZERO_PICK),
                   func.max(f.event_time), func.min(f.warehouse))
            .where(f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
                   f.item_number.in_(wanted_items)).group_by(f.item_number))).all()
        for item, n, zero, last_at, warehouse in rows:
            by_item[item] = {"picks": int(n) - int(zero), "zero_picks": int(zero),
                             "last_pick_at": last_at.isoformat() if last_at else None, "warehouse": warehouse}
    wanted_locations = sorted({loc for loc in locations if loc})
    if wanted_locations:
        loc = f.attributes["FromLocation"].astext
        rows = (await db.execute(
            select(loc, func.count(), func.count().filter(f.quantity_classification == ZERO_PICK),
                   func.count(func.distinct(f.item_number)).filter(f.quantity_classification == ZERO_PICK),
                   func.max(f.event_time), func.min(f.warehouse))
            .where(f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
                   loc.in_(wanted_locations)).group_by(loc))).all()
        for location, n, zero, items_empty, last_at, warehouse in rows:
            by_location[location] = {"picks": int(n) - int(zero), "zero_picks": int(zero), "items_empty": int(items_empty),
                                     "last_pick_at": last_at.isoformat() if last_at else None, "warehouse": warehouse}
    return by_item, by_location


async def latest_counts(db: AsyncSession, customer_code: str, since: datetime) -> list[dict]:
    """The latest Quick Stock Count per item and location since the moment (the stock agent's read)."""
    return await stock_agent.latest_counts(db, customer_code, since)


def count_signals(counts: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    """Per item and per location: whether the latest count found it empty, or disagrees with the
    system, with the count's words.

    ([{"item": "I1", "location": "A-01", "counted": "0", "balance": "13", ...}]) -> ({"I1": {"empty": True, ...}}, {"A-01": {"variance": True, ...}})
    """
    by_item: dict[str, dict] = {}
    by_location: dict[str, dict] = {}
    for c in counts:
        variance = stock_agent.variance_of(c["counted"], c["balance"])
        counted, balance = stock_agent._dec(c["counted"]), stock_agent._dec(c["balance"])
        is_empty = counted == 0 and (balance or 0) > 0
        at = c["at"].isoformat() if c.get("at") else None
        words = (f"counted {stock_agent.plain(counted)} against {stock_agent.plain(balance)} at {c['location']}"
                 if counted is not None and balance is not None else "")
        item = by_item.setdefault(c["item"], {"empty": False, "variance": False, "at": at, "text": words, "location": c["location"]})
        if is_empty:
            item.update({"empty": True, "at": at, "text": words, "location": c["location"]})
        elif variance is not None and not item["empty"]:
            item.update({"variance": True, "at": at, "text": words, "location": c["location"]})
        location = by_location.setdefault(c["location"], {"variance": False, "at": at, "text": words, "item": c["item"]})
        if variance is not None:
            location.update({"variance": True, "at": at, "text": words, "item": c["item"]})
    return by_item, by_location


async def zone_locations(db: AsyncSession, customer_code: str, zones: Iterable[str] = ()) -> dict[str, list[str]]:
    """The locations of each zone from the harvested location lookup (every zone when none named)."""
    v = AnalyticsLookupValue
    q = select(v.key, v.value).where(v.customer_code == customer_code, v.lookup == LOCATION_LOOKUP,
                                     v.attribute == ZONE_ATTRIBUTE, v.valid_to.is_(None))
    wanted = sorted({z for z in zones if z})
    if wanted:
        q = q.where(v.value.in_(wanted))
    out: dict[str, list[str]] = {}
    for key, value in (await db.execute(q.order_by(v.value, v.key))).all():
        out.setdefault(value, []).append(key)
    return out


async def item_names(db: AsyncSession, customer_code: str, items: Iterable[str] = ()) -> dict[str, str]:
    """Item descriptions from the harvested lookup, by item number (every item when none named)."""
    v = AnalyticsLookupValue
    q = select(v.key, v.value).where(v.customer_code == customer_code, v.lookup == ITEM_LOOKUP,
                                     v.attribute == ITEM_NAME, v.valid_to.is_(None))
    wanted = sorted({i for i in items if i})
    if wanted:
        q = q.where(v.key.in_(wanted))
    return {key: value for key, value in (await db.execute(q)).all()}


async def items_matching(db: AsyncSession, customer_code: str, text: str, *, limit: int = 20) -> dict[str, str]:
    """Items whose number starts with, or whose description contains, the text (a few hundred rows
    per customer today; add a value index if that grows past ten thousand)."""
    v = AnalyticsLookupValue
    needle = text.strip()
    if not needle:
        return {}
    q = (select(v.key, v.value).where(v.customer_code == customer_code, v.lookup == ITEM_LOOKUP, v.attribute == ITEM_NAME,
                                      v.valid_to.is_(None), (v.key.ilike(f"{needle}%")) | (v.value.ilike(f"%{needle}%")))
         .order_by(v.key).limit(limit))
    return {key: value for key, value in (await db.execute(q)).all()}


async def locations_matching(db: AsyncSession, customer_code: str, text: str, *, limit: int = 20) -> dict[str, str | None]:
    """Locations whose code starts with the text, with their zone, from the harvested lookup."""
    v = AnalyticsLookupValue
    needle = text.strip()
    if not needle:
        return {}
    q = (select(v.key, v.value).where(v.customer_code == customer_code, v.lookup == LOCATION_LOOKUP,
                                      v.attribute == ZONE_ATTRIBUTE, v.valid_to.is_(None), v.key.ilike(f"{needle}%"))
         .order_by(v.key).limit(limit))
    return {key: value for key, value in (await db.execute(q)).all()}


# ============================================================== measures

def fill_rate(customer_code: str, site_id: str | None) -> dict | None:
    """The site's latest fill rate as the Home snapshot loop computed it, or None before the
    first snapshot: {"fill_rate": 0.976|None, "as_of": iso, "provisional": bool}."""
    return home_snapshot.latest_fill_rate(customer_code, site_id)
