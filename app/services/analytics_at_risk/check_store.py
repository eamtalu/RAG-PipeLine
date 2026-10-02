"""Acknowledgements: a person marks a delivery as checked, and the ledger remembers every action.

The check lives on the delivery row (five columns the API owns) so the board shows it in one read,
and in `analytics_at_risk_checks` as an append-only row so "who looked at this and when" survives the
delivery row being rewritten every minute. The worker calls `record` too, when an escalation re-opens
a checked delivery, so the ledger is the one place with the whole story.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskCheck, AnalyticsAtRiskDelivery

DEFAULT_ACTOR = "api"
WORKER_ACTOR = "worker"
MAX_NOTE = 1000


class NoOpenDelivery(LookupError):
    """No row for that delivery (and departure), so there is nothing to check."""


class AmbiguousDelivery(LookupError):
    """Two open rows share the delivery number; the caller must name the departure date."""


async def record(db: AsyncSession, row: AnalyticsAtRiskDelivery, *, action: str, actor: str, note: str | None,
                 at: datetime) -> AnalyticsAtRiskCheck:
    entry = AnalyticsAtRiskCheck(customer_code=row.customer_code, delivery_id=row.id, delivery_number=row.delivery_number,
                                 departure_date=row.departure_date, action=action, tier=row.tier,
                                 actor=(actor or DEFAULT_ACTOR)[:128], note=(note or None) and note[:MAX_NOTE], at=at)
    db.add(entry)
    return entry


async def find(db: AsyncSession, cc: str, delivery_number: str, *,
               departure_date: date | None = None) -> AnalyticsAtRiskDelivery:
    """The row a check names: the one for `departure_date` when given, else the single OPEN row."""
    stmt = select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == cc,
                                                 AnalyticsAtRiskDelivery.delivery_number == delivery_number)
    if departure_date is not None:
        row = await db.scalar(stmt.where(AnalyticsAtRiskDelivery.departure_date == departure_date))
        if row is None:
            raise NoOpenDelivery(f"no delivery {delivery_number} departing {departure_date.isoformat()}")
        return row
    rows = list((await db.execute(stmt.where(AnalyticsAtRiskDelivery.status == "open").limit(2))).scalars().all())
    if not rows:
        raise NoOpenDelivery(f"no open delivery {delivery_number}")
    if len(rows) > 1:
        raise AmbiguousDelivery(f"delivery {delivery_number} has {len(rows)} open rows; name the departure date")
    return rows[0]


async def check(db: AsyncSession, cc: str, delivery_number: str, *, actor: str | None, note: str | None,
                now: datetime | None = None, departure_date: date | None = None) -> AnalyticsAtRiskDelivery:
    """Mark the delivery as checked by `actor`. Checking again replaces the earlier check and is logged
    again, so a second person's word is not lost. Does NOT commit."""
    at = now or datetime.now(timezone.utc)
    row = await find(db, cc, delivery_number, departure_date=departure_date)
    row.checked_at, row.checked_by = at, (actor or DEFAULT_ACTOR)[:128]
    row.check_note = (note or None) and note[:MAX_NOTE]
    row.checked_tier = row.tier
    await db.flush()
    await record(db, row, action="checked", actor=row.checked_by, note=row.check_note, at=at)
    await db.flush()
    return row


async def uncheck(db: AsyncSession, cc: str, delivery_number: str, *, actor: str | None,
                  now: datetime | None = None, departure_date: date | None = None) -> AnalyticsAtRiskDelivery:
    at = now or datetime.now(timezone.utc)
    row = await find(db, cc, delivery_number, departure_date=departure_date)
    row.checked_at = row.checked_by = row.check_note = row.checked_tier = None
    await db.flush()
    await record(db, row, action="unchecked", actor=actor or DEFAULT_ACTOR, note=None, at=at)
    await db.flush()
    return row


async def list_checks(db: AsyncSession, cc: str, *, start: datetime | None, end: datetime | None,
                      limit: int = 200, after: tuple[datetime, str] | None = None) -> tuple[list[AnalyticsAtRiskCheck], bool]:
    """Ledger rows newest first, keyset-paged on `(at, id)`. Returns `(rows, truncated)`."""
    stmt = select(AnalyticsAtRiskCheck).where(AnalyticsAtRiskCheck.customer_code == cc)
    if start is not None:
        stmt = stmt.where(AnalyticsAtRiskCheck.at >= start)
    if end is not None:
        stmt = stmt.where(AnalyticsAtRiskCheck.at < end)
    if after is not None:
        at, ident = after
        stmt = stmt.where((AnalyticsAtRiskCheck.at < at) | ((AnalyticsAtRiskCheck.at == at) & (AnalyticsAtRiskCheck.id < ident)))
    rows = list((await db.execute(stmt.order_by(AnalyticsAtRiskCheck.at.desc(), AnalyticsAtRiskCheck.id.desc())
                                  .limit(limit + 1))).scalars().all())
    return rows[:limit], len(rows) > limit
