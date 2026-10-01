"""The nightly forecast loop: once per tenant per tenant-local day, after the run hour.

Modelled on the reconciliation worker: a slow loop, isolated per tenant, that must never compete
with the folder. It is driven by the CLOCK rather than by tickets, because a forecast is about a
day that has ended, not about rows that have arrived: the as-of day is yesterday on the tenant's
clock, and the run happens once that clock has passed `analytics_forecast_run_hour_local`, so the
small hours of a 14:00 -> 08:00 shift have settled into their own date.

Which tenants: every tenant with an enabled settlement of the configured name. Not "every tenant
with analytics state", because a tenant that never declared `pick_release` has nothing to forecast
and would only produce a `skipped` run a night.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone, tzinfo
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.config.database import async_session
from app.persistence.models.analytics_settlement import AnalyticsSettlement
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_forecast import run_store, runner
from app.settings import settings

logger = logging.getLogger(__name__)


def local_as_of(now: datetime, tz: tzinfo, *, run_hour: int) -> date | None:
    """Yesterday on the tenant's clock, once that clock has passed `run_hour`; else None (not yet)."""
    local = now.astimezone(tz)
    if local.hour < run_hour:
        return None
    return local.date() - date.resolution


async def _candidates() -> list[str]:
    async with async_session() as db:
        rows = await db.execute(select(AnalyticsSettlement.customer_code).where(
            AnalyticsSettlement.name == settings.analytics_forecast_settlement, AnalyticsSettlement.enabled.is_(True)
        ).distinct().order_by(AnalyticsSettlement.customer_code))
        return list(rows.scalars().all())


async def due_tenants(now: datetime, *, run_hour: int) -> list[tuple[str, date]]:
    """Tenants whose run for their current as-of day has neither happened nor started."""
    out = []
    for cc in await _candidates():
        async with async_session() as db:
            tz = ZoneInfo(await get_customer_timezone(db, cc))
            as_of = local_as_of(now, tz, run_hour=run_hour)
            if as_of is None or await run_store.done_for(db, cc, as_of) or await run_store.running(db, cc):
                continue
        out.append((cc, as_of))
    return out


async def forecast_once(now: datetime | None = None) -> dict:
    """One pass: run every due tenant. A failing tenant is logged and does not stop the others."""
    now = now or datetime.now(timezone.utc)
    due = await due_tenants(now, run_hour=settings.analytics_forecast_run_hour_local)
    stats = {"due": len(due), "completed": 0, "skipped": 0, "failed": 0}
    for cc, as_of in due:
        try:
            result = await runner.run_tenant(cc, as_of_date=as_of, trigger="nightly")
        except Exception:
            stats["failed"] += 1
            logger.exception("Forecast for %s as of %s failed; other tenants are unaffected", cc, as_of)
            continue
        status = result.get("status")
        stats["completed" if status == "completed" else "skipped" if status == "skipped" else "failed"] += 1
        if status == "skipped":
            logger.info("Forecast for %s as of %s skipped: %s", cc, as_of, result.get("reason"))
        elif status != "completed":
            logger.error("Forecast for %s as of %s: %s", cc, as_of, result)
    return stats


async def _tick() -> None:
    try:
        stats = await forecast_once()
    except Exception:
        logger.exception("Forecast pass failed entirely; retrying next interval")
        return
    if stats["due"]:
        logger.info("Forecast pass: %s", stats)


async def run_analytics_forecast_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Analytics forecast worker started (every %.0f s; runs each tenant once a day after %02d:00 local, "
                "settlement %r)", settings.analytics_forecast_poll_seconds, settings.analytics_forecast_run_hour_local,
                settings.analytics_forecast_settlement)
    while True:
        await _tick()
        await asyncio.sleep(settings.analytics_forecast_poll_seconds)
