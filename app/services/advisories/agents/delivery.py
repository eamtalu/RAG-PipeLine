"""The Delivery agent (step 9): the at-risk board, already evaluated every minute, as advisories.
A delivery the board flags "at risk" is HIGH, "watch" is MEDIUM; the finding carries the board's
own words (progress, the learned ready time) and the precision of the flags over the last
fortnight as its confidence. It clears when the board stops flagging the delivery.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.advisories.registry import Finding
from app.services.analytics_at_risk import delivery_store
from app.services.teams import home_at_risk

SEVERITY = {"at_risk": "high", "watch": "medium"}


async def precision_last_fortnight(db: AsyncSession, customer_code: str, now: datetime) -> float | None:
    """How often a flagged delivery really left late over the last 14 closed days, 0..1 or None.

    (174 flagged, 46 late) -> 0.264 ; (nothing closed yet) -> None
    """
    today = now.date()
    agg = await delivery_store.accuracy(db, customer_code, start=today - timedelta(days=14), end=today - timedelta(days=1))
    flagged = sum(int(t.get("flagged", 0)) for t in agg.get("by_tier", {}).values())
    late = sum(int(t.get("late", 0)) for t in agg.get("by_tier", {}).values())
    return round(late / flagged, 3) if flagged else None


def title_for(row, tz: ZoneInfo) -> str:
    """The advisory's one line: the route, the customer, and what is likely to be left behind.

    (R14 Hove Deli, 60 of 84 lines, at risk) -> "Route R14 (Hove Deli) likely to leave 24 lines behind"
    """
    behind = (row.lines_expected or 0) - row.lines_picked if row.lines_expected is not None else None
    who = f" ({row.customer_name})" if row.customer_name else ""
    if behind and behind > 0:
        return f"Route {row.route}{who} likely to leave {behind} line{'s' if behind != 1 else ''} behind"
    return f"Route {row.route}{who} behind its usual rhythm"


async def run(db: AsyncSession, customer_code: str, now: datetime, tz: ZoneInfo) -> list[Finding]:
    """One finding per open delivery the board flags, worst first."""
    rows = [r for r in await delivery_store.open_rows(db, customer_code) if r.tier in SEVERITY]
    if not rows:
        return []
    confidence = await precision_last_fortnight(db, customer_code, now)
    findings: list[Finding] = []
    for row in rows:
        minutes = None
        if row.usual_ready_at is not None:
            minutes = (row.usual_ready_at - now).total_seconds() / 60
        evidence = f"Picking progress · {home_at_risk.progress_text(row)} · {home_at_risk.clock_text(row, tz)}"
        if minutes is not None and minutes > 0:
            evidence += f" · {int(minutes)} min to go"
        behind = (row.lines_expected or 0) - row.lines_picked if row.lines_expected is not None else 0
        impact = (f"{behind} line{'s' if behind != 1 else ''} not yet picked for {row.customer_name or 'the customer'}"
                  if behind > 0 else "the van may wait or leave short")
        findings.append(Finding(
            dedup_key=f"delivery:{row.delivery_number}:{row.departure_date.isoformat()}",
            severity=SEVERITY[row.tier],
            title=title_for(row, tz),
            evidence=evidence,
            impact=impact,
            recommended="Check the picker on the route; hold the van if the lines are close.",
            confidence=confidence,
            site_id=row.warehouse,
            due_at=row.usual_ready_at or row.departure_at,
            source={"kind": "delivery", "delivery_number": row.delivery_number,
                    "departure_date": row.departure_date.isoformat(), "route": row.route,
                    "lines_picked": row.lines_picked, "lines_expected": row.lines_expected},
            actions=("ask_why", "acknowledge", "assign", "snooze"),
        ))
    return findings
