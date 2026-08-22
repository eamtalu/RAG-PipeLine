"""The reconciliation loop: run the three checks on a rolling window, per tenant.

Separate from the analytics worker on purpose. That one is a hot path polling every couple of seconds to
fold tickets; this one is a slow audit that reads whole windows and must never compete with it. Bundling
them would also mean a reconciliation bug could stop folding, which inverts the point of having an
auditor.

WINDOWED (A4). Each pass covers a rolling recent window rather than all retained history, because a full
recount grows with history and becomes a job nobody runs. Explicit full runs are the operator's tool
(`reconcile_tenant` with an open window), not the cadence.

**Report-only.** This worker never repairs, whatever `reconcile_tenant` is capable of. Phase 7 sequences
it that way -- "then report-only reconciliation" -- because a checker that silently fixes things cannot be
trusted to have found what it says it found, and the first weeks of findings are how you learn whether the
check itself is right. Repair is an explicit operator action.

Deliberately reconciles a window that has already SETTLED: reconciling the live tail would report drift
on every window whose contributors are still unsealed, which is not drift but work in progress. F4's
settledness numbers say the same thing from the other direction.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import distinct, select

from app.config.database import async_session
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.services.analytics.reconcile import reconcile_tenant
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
from app.settings import settings

logger = logging.getLogger(__name__)


def rolling_window(now: datetime | None = None) -> UtcWindow:
    """The window one pass covers: a span ending far enough back that it has settled.

    The lag matters as much as the span. Records are not final for 1.7 hours on average, so a window
    reaching to `now` would report every still-unsealed contributor as a discrepancy -- and a check that
    is always red is a check nobody reads.
    """
    now = now or datetime.now(timezone.utc)
    end = now - timedelta(hours=settings.analytics_reconcile_lag_hours)
    return UtcWindow(start=end - timedelta(hours=settings.analytics_reconcile_window_hours), end=end)


async def _tenants() -> list[str]:
    """Tenants with analytics state. Nothing to reconcile for a tenant that has never folded."""
    async with async_session() as db:
        return list((await db.execute(
            select(distinct(AnalyticsTenantState.customer_code)))).scalars().all())


async def reconcile_once() -> dict:
    """One pass over every tenant. Report-only. Per-tenant failures are isolated (A1's reasoning)."""
    window = rolling_window()
    stats = {"tenants": 0, "healthy": 0, "with_findings": 0, "findings": 0, "failed": 0}
    for cc in await _tenants():
        stats["tenants"] += 1
        try:
            async with async_session() as db:
                report = await reconcile_tenant(db, cc, window=window, repair=False)
                # Nothing to commit: report-only writes nothing. The session is closed without a
                # commit deliberately, so a future change that starts writing here fails loudly in
                # review rather than silently persisting from an auditor.
        except Exception:
            stats["failed"] += 1
            logger.exception("Reconciliation failed for %s; other tenants are unaffected", cc)
            continue
        if report["healthy"]:
            stats["healthy"] += 1
        else:
            stats["with_findings"] += 1
            stats["findings"] += len(report["findings"])
    return stats


async def _tick() -> None:
    try:
        stats = await reconcile_once()
    except Exception:
        logger.exception("Reconciliation pass failed entirely; retrying next interval")
        return
    if stats["with_findings"] or stats["failed"]:
        logger.error("Reconciliation: %s", stats)
    elif stats["tenants"]:
        logger.info("Reconciliation: %s", stats)


async def run_analytics_reconcile_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Analytics reconciliation worker started (every %.1f h, window %d h ending %d h back)",
                settings.analytics_reconcile_interval_seconds / 3600,
                settings.analytics_reconcile_window_hours, settings.analytics_reconcile_lag_hours)
    while True:
        await _tick()
        await asyncio.sleep(settings.analytics_reconcile_interval_seconds)
