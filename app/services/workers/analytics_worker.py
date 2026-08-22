"""Analytics worker — the consumer that drains `analytics_pending_windows` into facts.

The loop only. Everything it does per tenant lives in `services/analytics/consume.py`, mirroring how
`log_stitch_worker` sits over `finalize_pending`: a loop that owns polling and error containment, and a
cycle function that owns the work and its transaction boundaries.

Runs in the singleton `python -m app.worker` process, never in the four gunicorn web workers. Not a
preference: the cycle takes a per-tenant advisory lock and reads a range of transactions, and four web
workers each doing that would contend for the same lock while starving request handling. The `-w N`
processes run with RUN_BACKGROUND_WORKERS=false, so this is enforced by deployment rather than by
convention.

OFF by default (`analytics_worker_enabled`). Phase 2 already publishes tickets, so the queue is filling
whether or not this runs; keeping the consumer dark by default is what allows ticket coverage to be
verified against real traffic before a single fact is written. Turning it on is a deliberate act.

Why there are no lease columns, unlike the Stage 1 ingest queue: concurrency is handled inside the cycle
by a per-tenant `pg_advisory_xact_lock`, so two workers cannot fold overlapping ranges for one tenant. A
crashed worker needs no recovery either -- its tickets simply stay open (`consumed_at IS NULL`) and the
next tick picks them up.
"""

import asyncio
import logging

from sqlalchemy import func, select

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.services.analytics.consume import drain_once
from app.settings import settings

logger = logging.getLogger(__name__)


async def pending_backlog() -> int:
    """Open tickets across all tenants, ignoring the backoff gate.

    Deliberately NOT filtered by `available_at`: this answers "is there anything left to do at all",
    which is the question for a shutdown drain or a health readout. A ticket waiting out its backoff is
    still work outstanding.
    """
    async with async_session() as db:
        return await db.scalar(
            select(func.count()).select_from(AnalyticsPendingWindow).where(
                AnalyticsPendingWindow.consumed_at.is_(None),
                AnalyticsPendingWindow.abandoned_at.is_(None))) or 0


async def _tick() -> None:
    """One iteration: drain, and report only when there was something to do.

    Swallows errors so a single bad tick never kills the loop. Nothing is lost by carrying on: every
    per-ticket failure is already recorded durably on `analytics_pending_windows` by the cycle itself,
    with its attempt count and backoff.

    No `except asyncio.CancelledError: raise` here on purpose. Since 3.8 CancelledError inherits from
    BaseException, so shutdown passes through this handler untouched; adding a branch would read as if
    it were load-bearing when it is not.
    """
    try:
        stats = await drain_once()
    except Exception:
        logger.exception("Analytics worker error — retrying next tick")
        return
    if stats.get("customers"):
        logger.info("Analytics drain: %s", stats)


async def run_analytics_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Analytics worker started (poll=%.1fs, max_customers_per_tick=%d)",
                settings.analytics_poll_seconds, settings.analytics_max_customers_per_tick)
    while True:
        await _tick()
        await asyncio.sleep(settings.analytics_poll_seconds)
