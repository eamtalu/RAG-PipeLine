"""Why an advisory says what it says (fix of 13 November 2026): the finding's own words and the
facts behind it, read straight from the tables the agents read, so the assistant's "Ask why" has
something true to answer from instead of searching the logs until its iteration cap. One
function per kind of finding; the tool in app/services/analytics_agent/advisory_tools.py calls
`explain`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.advisories import store
from app.services.advisories.agents.stock import COUNT_METHOD, plain
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases
from app.services.teams import home_at_risk

PICK_METHOD = "ConfirmPickLine"
FACT_LIMIT = 20


def _dec(value) -> str | None:
    try:
        return plain(Decimal(str(value))) if value not in (None, "") else None
    except InvalidOperation:
        return None


def _hhmm(at: datetime | None, tz: ZoneInfo) -> str | None:
    return at.astimezone(tz).strftime("%H:%M") if at else None


async def counts_today(db: AsyncSession, customer_code: str, item: str, location: str, since: datetime,
                       tz: ZoneInfo) -> list[dict]:
    """Every Quick Stock Count of one item at one location since the moment, oldest first."""
    f = AnalyticsFact
    rows = (await db.execute(
        select(f.event_time, f.user_name, f.attributes["CountedQuantity"].astext, f.attributes["BalanceQuantity"].astext)
        .where(f.customer_code == customer_code, f.method == COUNT_METHOD, f.item_number == item,
               f.attributes["Location"].astext == location, f.event_time >= since)
        .order_by(f.event_time).limit(FACT_LIMIT))).all()
    return [{"at": _hhmm(r[0], tz), "by": r[1], "counted": _dec(r[2]), "system": _dec(r[3])} for r in rows]


async def picks_at(db: AsyncSession, customer_code: str, location: str, since: datetime, tz: ZoneInfo,
                   item: str | None = None) -> list[dict]:
    """The picks from one location since the moment (one item, or every item), oldest first:
    what was expected, what was picked, who, when."""
    f = AnalyticsFact
    q = (select(f.event_time, f.user_name, f.item_number, f.attributes["ExpectedQuantity"].astext,
                f.attributes["QuantityPicked"].astext, f.quantity_classification, f.delivery_number)
         .where(f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
                f.attributes["FromLocation"].astext == location))
    if item:
        q = q.where(f.item_number == item)
    rows = (await db.execute(q.order_by(f.event_time).limit(FACT_LIMIT))).all()
    return [{"at": _hhmm(r[0], tz), "by": r[1], "item": r[2], "expected": _dec(r[3]), "picked": _dec(r[4]),
             "kind": r[5], "delivery": r[6]} for r in rows]


async def zero_picks_by_item(db: AsyncSession, customer_code: str, location: str, since: datetime) -> list[dict]:
    """How many times each item was picked empty from one location since the moment, most first."""
    f = AnalyticsFact
    rows = (await db.execute(
        select(f.item_number, func.count()).where(
            f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
            f.quantity_classification == "attempt", f.attributes["FromLocation"].astext == location)
        .group_by(f.item_number).order_by(func.count().desc()).limit(FACT_LIMIT))).all()
    return [{"item": r[0], "zero_picks": int(r[1])} for r in rows]


async def short_releases(db: AsyncSession, customer_code: str, item: str, now: datetime) -> list[dict]:
    """The item's short releases over the last 7 days, by day."""
    try:
        result = await aggregate_releases(db, {
            "start": (now - timedelta(days=7)).isoformat(), "end": now.isoformat(),
            "where": ["shortfall<0", f"item_number=={item}"], "group_by": ["day"], "sort": "day", "limit": 10},
            customer_code)
    except (UnknownSettlement, ReadProblem):
        return []
    out = []
    for row in result.get("rows") or []:
        dims = row.get("dimensions") or []
        out.append({"day": str(dims[0]) if dims else None, "releases": int(row.get("rows") or 0),
                    "units_short": _dec(-(Decimal(str(row.get("shortfall") or 0))))})
    return out


async def delivery_facts(db: AsyncSession, customer_code: str, source: dict, tz: ZoneInfo) -> dict | None:
    """The at-risk board's row for the delivery, in its own words."""
    try:
        departure = datetime.fromisoformat(str(source.get("departure_date"))).date()
    except (TypeError, ValueError):
        return None
    row = await db.scalar(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == customer_code,
        AnalyticsAtRiskDelivery.delivery_number == str(source.get("delivery_number")),
        AnalyticsAtRiskDelivery.departure_date == departure))
    if row is None:
        return None
    return {
        "delivery": row.delivery_number, "route": row.route, "customer": row.customer_name, "tier": row.tier,
        "status": row.status, "progress": home_at_risk.progress_text(row), "clock": home_at_risk.clock_text(row, tz),
        "lines_expected": row.lines_expected, "lines_picked": row.lines_picked, "lines_short": row.lines_short,
        "packages_created": row.packages_created, "packages_loaded": row.packages_loaded,
        "last_pick": _hhmm(row.last_pick_at, tz), "last_load": _hhmm(row.last_load_at, tz),
        "usual_ready": _hhmm(row.usual_ready_at, tz), "departure": _hhmm(row.departure_at, tz),
    }


async def explain(db: AsyncSession, customer_code: str, advisory_id: str, *, now: datetime | None = None) -> dict:
    """The advisory and the facts behind it, or {"error": ...} when there is no such advisory.

    (count advisory item 104568 at A-01-02) -> {"advisory": {...}, "facts": {"counts_today": [...],
    "picks_from_location_today": [...]}} ; (unknown id) -> {"error": "no such advisory"}
    """
    row = await store.get(db, customer_code, advisory_id)
    if row is None:
        return {"error": "no such advisory", "hint": "the id comes with the question; answer from the question's own words"}
    now = now or datetime.now(timezone.utc)
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    since = now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    source = dict(row.source or {})
    facts: dict = {}
    # Step 1: The facts follow the kind of finding, because each agent read something different
    # and the answer should show exactly that
    if source.get("kind") == "count":
        item, location = str(source.get("item")), str(source.get("location"))
        facts["counts_today"] = await counts_today(db, customer_code, item, location, since - timedelta(days=1), tz)
        facts["picks_from_location_today"] = await picks_at(db, customer_code, location, since, tz, item=item)
    elif source.get("kind") == "location":
        location = str(source.get("location"))
        facts["zero_picks_by_item_today"] = await zero_picks_by_item(db, customer_code, location, since)
        facts["picks_from_location_today"] = await picks_at(db, customer_code, location, since, tz)
    elif source.get("kind") == "item":
        facts["short_releases_last_7_days"] = await short_releases(db, customer_code, str(source.get("item")), now)
    elif source.get("kind") == "delivery":
        facts["delivery"] = await delivery_facts(db, customer_code, source, tz)
    return {
        "advisory": {"id": str(row.id), "agent": row.agent, "severity": row.severity, "status": row.status,
                     "site_id": row.site_id, "title": row.title, "evidence": row.evidence, "impact": row.impact,
                     "recommended": row.recommended, "opened_at": _hhmm(row.opened_at, tz),
                     "last_seen_at": _hhmm(row.last_seen_at, tz), "source": source},
        "facts": facts,
        "note": "The advisory's words are the agent's reading of these facts; say what the facts show and "
                "what the recommended action is. There is nothing more in the logs.",
    }
