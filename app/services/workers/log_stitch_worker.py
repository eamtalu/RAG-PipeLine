"""Stitch worker — the consumer that owns draining the Stage 2 queue.

`log_regroup_pending` has always been a proper work queue: Stage 1 writes a ticket in the SAME
transaction as the entries (parse_insert.py:178), which is exactly why Stage 2 can fail completely
and still be retried. But that queue had no consumer, so every producer had to remember to drain it
itself — the SFTP transport, the directory watcher, the parse worker. Three modules with no business
knowing stitching exists, and a trap where any new ingestion path that forgot the call would silently
leave data unstitched.

This worker owns it. Producers now only write tickets.

Why the unit of work is a CUSTOMER, not a row
---------------------------------------------
`finalize_pending` deliberately reads ALL of a tenant's open rows and coalesces them into clusters
(`_coalesce_pending`, gap = 2x the pad). Claiming one row at a time would destroy that and turn one
efficient rebuild into many overlapping ones. So this worker asks "which tenants have work due?" and
hands each whole tenant to `finalize_pending`.

That is also why there are no lease columns here, unlike the Stage 1 ingest queue. Concurrency is
already handled inside `finalize_pending` by a per-customer `pg_advisory_xact_lock`, so two stitch
workers cannot rebuild overlapping windows for the same tenant. A crashed worker needs no recovery
either: its rows simply stay open (`consumed_at IS NULL`) and the next tick picks them up.

This replaces the old log_grouping_worker, which polled `SELECT count(*)` on the ~40 GB log_entries
heap every 5 seconds just to detect "did anything change". That is why it shipped disabled. The
pending table answers the question directly and is tiny and indexed.
"""

import asyncio
import logging

from sqlalchemy import distinct, func, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.mnp_log_ingestion.pipeline.derive_transactions import finalize_pending

logger = logging.getLogger(__name__)


def _open_and_due():
    """The three exclusions that define claimable work, each load-bearing.

    - consumed_at IS NULL  : not already stitched
    - abandoned_at IS NULL : dead-lettered windows are not retried, or it is not a dead letter
    - available_at <= now(): the backoff gate; without it the delay would be pointless

    clock_timestamp(), NOT now(): now() is transaction_timestamp(), so any session whose transaction
    began before a row was written would treat that row as permanently not-yet-due.
    """
    return (
        LogRegroupPending.consumed_at.is_(None),
        LogRegroupPending.abandoned_at.is_(None),
        LogRegroupPending.available_at <= func.clock_timestamp(),
    )


async def customers_with_due_work(limit: int | None = None) -> list[str]:
    """Tenants with at least one open, due window. Backed by ix_log_regroup_pending_due."""
    cap = limit if limit is not None else settings.log_stitch_max_customers_per_tick
    async with async_session() as db:
        return list((await db.execute(
            select(distinct(LogRegroupPending.customer_code))
            .where(*_open_and_due())
            .limit(cap)
        )).scalars().all())


async def pending_backlog() -> int:
    """Open, due windows across all tenants — used to decide whether the worker must run at all."""
    async with async_session() as db:
        return await db.scalar(
            select(func.count()).select_from(LogRegroupPending).where(
                LogRegroupPending.consumed_at.is_(None),
                LogRegroupPending.abandoned_at.is_(None))
        ) or 0


async def drain_once() -> dict:
    """Stitch every tenant with due work. Per-tenant failures are isolated.

    `finalize_pending` already records its own attempt/backoff/dead-letter bookkeeping per window, so
    a failure here needs no extra handling beyond not letting it stop the other tenants — which is
    precisely the isolation the old source-level behaviour lacked.
    """
    stats = {"customers": 0, "windows": 0, "consumed": 0, "abandoned": 0, "failed": 0}
    for cc in await customers_with_due_work():
        stats["customers"] += 1
        try:
            async with async_session() as db:
                res = await finalize_pending(db, cc)
            stats["windows"] += res.get("windows", 0) or 0
            stats["consumed"] += res.get("pending_consumed", 0) or 0
            stats["abandoned"] += res.get("abandoned", 0) or 0
        except Exception:
            stats["failed"] += 1
            logger.exception(
                "Stitch worker: finalize failed for %s — its windows stay open for retry; "
                "other tenants are unaffected", cc)
    return stats


async def run_log_stitch_worker() -> None:
    """Forever loop. Survives errors; only CancelledError (shutdown) stops it."""
    logger.info("Log stitch worker started (poll=%.1fs, max_customers_per_tick=%d)",
                settings.log_stitch_poll_seconds, settings.log_stitch_max_customers_per_tick)
    while True:
        try:
            stats = await drain_once()
            if stats["customers"]:
                logger.info("Stitch drain: %s", stats)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Stitch worker error — retrying next tick")
        await asyncio.sleep(settings.log_stitch_poll_seconds)
