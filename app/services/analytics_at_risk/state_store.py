"""The per-tenant state row: one read for `/status`, upserted by the worker after every pass."""

from __future__ import annotations

from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskTenantState

_UNSET = object()


async def get(db: AsyncSession, cc: str) -> AnalyticsAtRiskTenantState | None:
    return await db.scalar(select(AnalyticsAtRiskTenantState).where(AnalyticsAtRiskTenantState.customer_code == cc))


async def touch(db: AsyncSession, cc: str, *, last_evaluated_at: datetime | None | object = _UNSET,
                last_profiled_date: date | None | object = _UNSET, open_rows: int | object = _UNSET,
                last_error: str | None | object = _UNSET) -> AnalyticsAtRiskTenantState:
    """Write only the fields given; the others keep their value. Does NOT commit."""
    row = await get(db, cc)
    if row is None:
        row = AnalyticsAtRiskTenantState(customer_code=cc)
        db.add(row)
    if last_evaluated_at is not _UNSET:
        row.last_evaluated_at = last_evaluated_at
    if last_profiled_date is not _UNSET:
        row.last_profiled_date = last_profiled_date
    if open_rows is not _UNSET:
        row.open_rows = int(open_rows)
    if last_error is not _UNSET:
        row.last_error = last_error
    row.updated_at = datetime.now(timezone.utc)
    await db.flush()
    return row
