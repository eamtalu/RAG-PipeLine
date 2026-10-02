"""The minute loop: every tenant with a delivery board is evaluated each pass, and once per tenant-local
day, after the profile hour, its route profiles are learned from the day before.

Modelled on the forecast worker: a loop isolated per tenant that must never compete with the folder.
It is driven by the CLOCK because the board is about where the warehouse stands right now, not about
rows that have arrived. The daily profile uses the forecast worker's `local_as_of` unchanged: the
as-of day is yesterday on the tenant's clock, once that clock has passed the hour.

Which tenants: every tenant with an enabled settlement of the configured name, minus tenants whose
at-risk settings row has `enabled = false`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskSettings
from app.persistence.models.analytics_settlement import AnalyticsSettlement
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_at_risk import runner, state_store
from app.services.workers.analytics_forecast_worker import local_as_of
from app.settings import settings

logger = logging.getLogger(__name__)


async def candidates() -> list[str]:
    async with async_session() as db:
        rows = await db.execute(select(AnalyticsSettlement.customer_code).where(
            AnalyticsSettlement.name == settings.analytics_at_risk_settlement, AnalyticsSettlement.enabled.is_(True)
        ).distinct().order_by(AnalyticsSettlement.customer_code))
        with_settlement = list(rows.scalars().all())
        if not with_settlement:
            return []
        off = await db.execute(select(AnalyticsAtRiskSettings.customer_code).where(
            AnalyticsAtRiskSettings.customer_code.in_(with_settlement), AnalyticsAtRiskSettings.enabled.is_(False)))
        switched_off = set(off.scalars().all())
    return [cc for cc in with_settlement if cc not in switched_off]


async def _profile_due(cc: str, now: datetime):
    """The as-of day to profile, or None when it is not yet the hour or the day is already done."""
    async with async_session() as db:
        tz = ZoneInfo(await get_customer_timezone(db, cc))
        as_of = local_as_of(now, tz, run_hour=settings.analytics_at_risk_profile_hour_local)
        if as_of is None:
            return None
        state = await state_store.get(db, cc)
        if state is not None and state.last_profiled_date is not None and state.last_profiled_date >= as_of:
            return None
    return as_of


async def evaluate_once(now: datetime | None = None) -> dict:
    """One pass: evaluate every candidate, then profile the ones whose day is due. A failing tenant is
    logged and does not stop the others."""
    now = now or datetime.now(timezone.utc)
    stats = {"tenants": 0, "completed": 0, "skipped": 0, "failed": 0, "profiled": 0}
    for cc in await candidates():
        stats["tenants"] += 1
        try:
            result = await runner.evaluate_tenant(cc, now=now)
        except Exception:
            stats["failed"] += 1
            logger.exception("At-risk evaluation for %s raised; other tenants are unaffected", cc)
            continue
        status = result.get("status")
        stats["completed" if status == "completed" else "skipped" if status == "skipped" else "failed"] += 1
        if status == "failed":
            logger.error("At-risk evaluation for %s: %s", cc, result)
            continue
        try:
            as_of = await _profile_due(cc, now)
            if as_of is not None:
                profiled = await runner.profile_tenant(cc, as_of=as_of, now=now)
                if profiled.get("status") == "completed":
                    stats["profiled"] += 1
                    logger.info("At-risk profiles for %s as of %s: %s", cc, as_of, profiled)
                else:
                    logger.error("At-risk profile for %s as of %s: %s", cc, as_of, profiled)
        except Exception:
            logger.exception("At-risk profile for %s raised; other tenants are unaffected", cc)
    return stats


async def _tick() -> None:
    try:
        stats = await evaluate_once()
    except Exception:
        logger.exception("At-risk pass failed entirely; retrying next interval")
        return
    if stats["failed"] or stats["profiled"]:
        logger.info("At-risk pass: %s", stats)


async def run_analytics_at_risk_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Analytics at-risk worker started (every %.0f s; profiles each tenant once a day after %02d:00 local, "
                "settlement %r)", settings.analytics_at_risk_poll_seconds, settings.analytics_at_risk_profile_hour_local,
                settings.analytics_at_risk_settlement)
    while True:
        await _tick()
        await asyncio.sleep(settings.analytics_at_risk_poll_seconds)
