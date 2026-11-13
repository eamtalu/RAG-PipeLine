"""The Stock controller agent (step 9): the stock counts the handhelds report (Quick Stock Count,
`ReportCount`) carry the counted quantity and the system balance; a count that disagrees with
the system by at least two units and a tenth is a finding (HIGH when the location counted empty
or the gap is a quarter or more; a one-unit slip is noise, the first live run showed fifty of
them). An item short on several releases this week is a second kind of finding, per site. Both
read the facts the pipeline already keeps; nothing is estimated.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.services.advisories.registry import Finding
from app.services.analytics.settle_reads import ReadProblem, UnknownSettlement
from app.services.analytics_agent.tools import aggregate_releases

COUNT_METHOD = "ReportCount"
LOOKBACK_HOURS = 24
MIN_GAP_UNITS = Decimal("2")
MIN_GAP_SHARE = Decimal("0.10")
HIGH_GAP_SHARE = Decimal("0.25")
CHRONIC_RELEASES = 3
ITEM_GROUP = ["item_number", "lookup:item description.ItemDescription", "warehouse"]


def _dec(value) -> Decimal | None:
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except InvalidOperation:
        return None


def plain(value: Decimal) -> str:
    """A quantity as a person writes it: no trailing zeros, no exponent.

    >>> plain(Decimal("7.000000")), plain(Decimal("59.788000")), plain(Decimal("1E+1")), plain(Decimal("13.33"))
    ('7', '59.788', '10', '13.33')
    """
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def variance_of(counted, balance) -> tuple[Decimal, Decimal | None] | None:
    """The gap between a count and the system, and its share of the balance; None when too small
    (under two units, or under a tenth), unless the location counted empty.

    >>> variance_of("2", "4"), variance_of("97.5", "97.5"), variance_of("0", "13.33")
    ((Decimal('-2'), Decimal('0.5')), None, (Decimal('-13.33'), Decimal('1')))
    >>> variance_of("8", "7"), variance_of("0", "1")
    (None, (Decimal('-1'), Decimal('1')))
    """
    c, b = _dec(counted), _dec(balance)
    if c is None or b is None:
        return None
    gap = c - b
    if gap == 0 or (abs(gap) < MIN_GAP_UNITS and not (c == 0 and b > 0)):
        return None
    share = (abs(gap) / b) if b > 0 else None
    if share is not None and share < MIN_GAP_SHARE:
        return None
    return gap, share


async def latest_counts(db: AsyncSession, customer_code: str, since: datetime) -> list[dict]:
    """The latest count per item and location since the moment: item, location, counted, balance, at."""
    f = AnalyticsFact
    loc = f.attributes["Location"].astext
    latest = (select(f.item_number, loc.label("location"), func.max(f.event_time).label("at"))
              .where(f.customer_code == customer_code, f.method == COUNT_METHOD, f.event_time >= since,
                     f.attributes.has_key("CountedQuantity"), f.attributes.has_key("BalanceQuantity"))
              .group_by(f.item_number, loc).subquery())
    rows = (await db.execute(
        select(f.item_number, loc, f.attributes["CountedQuantity"].astext, f.attributes["BalanceQuantity"].astext,
               f.event_time, f.warehouse)
        .join(latest, (f.item_number == latest.c.item_number) & (loc == latest.c.location) & (f.event_time == latest.c.at))
        .where(f.customer_code == customer_code, f.method == COUNT_METHOD)
    )).all()
    return [{"item": r[0], "location": r[1], "counted": r[2], "balance": r[3], "at": r[4], "warehouse": r[5]} for r in rows]


async def chronic_shorts(db: AsyncSession, customer_code: str, now: datetime) -> list[dict]:
    """Items short on at least CHRONIC_RELEASES releases in the last 7 days, worst first."""
    try:
        result = await aggregate_releases(db, {
            "start": (now - timedelta(days=7)).isoformat(), "end": now.isoformat(), "where": ["shortfall<0"],
            "group_by": ITEM_GROUP, "sort": "rows", "limit": 10}, customer_code)
    except (UnknownSettlement, ReadProblem):
        return []
    out = []
    for row in result.get("rows") or []:
        dims = row.get("dimensions") or []
        releases = int(row.get("rows") or 0)
        if releases >= CHRONIC_RELEASES and dims:
            units = -(_dec(row.get("shortfall")) or Decimal(0))
            out.append({"item": str(dims[0]), "description": str(dims[1]) if len(dims) > 1 and dims[1] else "",
                        "warehouse": str(dims[2]) if len(dims) > 2 and dims[2] else None,
                        "releases": releases, "units_short": units})
    return out


async def run(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> list[Finding]:
    findings: list[Finding] = []
    # Step 1: Count variances, from the latest count per item and location in the last day
    for c in await latest_counts(db, customer_code, now - timedelta(hours=LOOKBACK_HOURS)):
        v = variance_of(c["counted"], c["balance"])
        if v is None:
            continue
        gap, share = v
        counted, balance = _dec(c["counted"]), _dec(c["balance"])
        empty = counted == 0 and (balance or 0) > 0
        severity = "high" if empty or (share is not None and share >= HIGH_GAP_SHARE) else "medium"
        when = c["at"].astimezone(tz).strftime("%H:%M")
        findings.append(Finding(
            dedup_key=f"count:{c['item']}:{c['location']}",
            severity=severity,
            title=(f"Item {c['item']} at {c['location']} counted empty, system has {plain(balance)}" if empty
                   else f"Item {c['item']} at {c['location']}: counted {plain(counted)}, system {plain(balance)}"),
            evidence=f"Quick Stock Count at {when} · gap {'+' if gap > 0 else '-'}{plain(abs(gap))} units"
                     + (f" · {share * 100:.0f}% of the balance" if share is not None else ""),
            impact="Picks from this location will be short until the balance is corrected"
                   if gap < 0 else "More stock here than the system knows; a pick may be sent elsewhere",
            recommended=f"Recount {c['location']} and adjust the balance for item {c['item']}",
            site_id=c["warehouse"],
            source={"kind": "count", "item": c["item"], "location": c["location"]},
        ))
    # Step 2: Items short on several releases this week
    for s in await chronic_shorts(db, customer_code, now):
        label = f"{s['description']} ({s['item']})" if s["description"] else f"Item {s['item']}"
        findings.append(Finding(
            dedup_key=f"chronic:{s['item']}:{s['warehouse'] or 'all'}",
            severity="medium",
            title=f"{label} short on {s['releases']} releases this week",
            evidence=f"{plain(s['units_short'])} units short over 7 days",
            site_id=s["warehouse"],
            impact="Customers on tomorrow's releases of this item are likely to be short again",
            recommended="Check the stock and the replenishment of this item before tomorrow's picks",
            source={"kind": "item", "item": s["item"]},
        ))
    return findings

