"""The Location agent (step 9): a pick location that keeps turning up empty is a location problem,
not an item problem: the stock is missing, mis-slotted, or the balance is wrong. The facts carry
every pick's location and whether it was a zero-pick (`quantity_classification` "attempt"); a
location with several zero-picks today is a finding (HIGH from six, MEDIUM from three), with the
items affected. The same facts show whether a location is also the scene of count variances.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_fact import AnalyticsFact
from app.services.advisories import kinds
from app.services.advisories.registry import Finding

PICK_METHOD = "ConfirmPickLine"
MEDIUM_FROM = 3
HIGH_FROM = 6
#: The JIT "location" is a staging word, not a slot; it is counted but reported as such.
STAGING = {"JIT"}


async def zero_picks_by_location(db: AsyncSession, customer_code: str, since: datetime) -> list[dict]:
    """Locations with zero-picks since the moment: location, zero_picks, items, warehouse."""
    f = AnalyticsFact
    loc = f.attributes["FromLocation"].astext
    rows = (await db.execute(
        select(loc, func.count().label("n"), func.count(func.distinct(f.item_number)).label("items"),
               func.min(f.warehouse).label("warehouse"))
        .where(f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
               f.quantity_classification == "attempt", loc.is_not(None))
        .group_by(loc).order_by(func.count().desc()).limit(20)
    )).all()
    return [{"location": r[0], "zero_picks": int(r[1]), "items": int(r[2]), "warehouse": r[3]} for r in rows]


async def items_at(db: AsyncSession, customer_code: str, location: str, since: datetime, limit: int = 3) -> list[str]:
    f = AnalyticsFact
    rows = (await db.execute(
        select(f.item_number, func.count()).where(
            f.customer_code == customer_code, f.method == PICK_METHOD, f.event_time >= since,
            f.quantity_classification == "attempt", f.attributes["FromLocation"].astext == location)
        .group_by(f.item_number).order_by(func.count().desc()).limit(limit)
    )).all()
    return [str(r[0]) for r in rows]


def severity_for(zero_picks: int) -> str | None:
    """HIGH from six zero-picks, MEDIUM from three, nothing below.

    >>> severity_for(6), severity_for(3), severity_for(2)
    ('high', 'medium', None)
    """
    if zero_picks >= HIGH_FROM:
        return "high"
    if zero_picks >= MEDIUM_FROM:
        return "medium"
    return None


async def run(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> list[Finding]:
    since = now.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    findings: list[Finding] = []
    for row in await zero_picks_by_location(db, customer_code, since):
        severity = severity_for(row["zero_picks"])
        if severity is None:
            continue
        items = await items_at(db, customer_code, row["location"], since)
        staging = row["location"] in STAGING
        title = (f"{row['zero_picks']} zero-picks at the {row['location']} staging today across {row['items']} items" if staging
                 else f"Location {row['location']} turned up empty {row['zero_picks']} times today")
        findings.append(Finding(
            dedup_key=f"location:{row['location']}:{since.date().isoformat()}",
            severity="medium" if staging else severity,
            title=title,
            evidence=f"{row['zero_picks']} zero-picks since midnight · items {', '.join(items)}" if items
                     else f"{row['zero_picks']} zero-picks since midnight",
            impact=f"Every release picking from {row['location']} loses the line" if not staging
                   else "Lines staged for just-in-time picking were not there when picked",
            recommended=("Check what is staged and when it arrives" if staging
                         else f"Check {row['location']}: stock missing, mis-slotted or the balance wrong; recount it"),
            site_id=row["warehouse"],
            source={"kind": "location", "location": row["location"]},
            kind="empty_location", about=kinds.about_location(str(row["location"]), is_staging=staging),
        ))
    return findings
