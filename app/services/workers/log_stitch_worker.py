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
from app.services.mnp_log_ingestion.pipeline import maintenance
from app.services.mnp_log_ingestion.pipeline import sealer
from app.services.mnp_log_ingestion.pipeline import stream_state

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
    """Tenants with at least one open, due window. Backed by ix_log_regroup_pending_due.

    Chunk 69: a tenant with a fresh RUNNING full rebuild is EXCLUDED - stitching it mid-rebuild is
    the collision that cost the 2026-08-27 repair four attempts. Its windows wait; a stale flag
    stops excluding and alarms (see `maintenance`)."""
    cap = limit if limit is not None else settings.log_stitch_max_customers_per_tick
    async with async_session() as db:
        await maintenance.alarm_on_stale(db)
        return list((await db.execute(
            select(distinct(LogRegroupPending.customer_code))
            .where(*_open_and_due(),
                   maintenance.not_under_maintenance(LogRegroupPending.customer_code))
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


# {stat key: the field finalize_pending reports it under}
_STAT_FIELDS = {"windows": "windows", "consumed": "pending_consumed", "abandoned": "abandoned"}


def _merge_result(stats: dict, result: dict) -> None:
    """Fold one tenant's finalize result into the running totals."""
    for key, field in _STAT_FIELDS.items():
        stats[key] += result.get(field) or 0


async def _stitch_customer(cc: str) -> dict:
    """Stitch one tenant. Its own short-lived session, so one tenant never holds a connection open
    across another's work."""
    async with async_session() as db:
        return await finalize_pending(db, cc)


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
            _merge_result(stats, await _stitch_customer(cc))
        except Exception:
            stats["failed"] += 1
            logger.exception(
                "Stitch worker: finalize failed for %s — its windows stay open for retry; "
                "other tenants are unaffected", cc)
    return stats


async def _tick() -> None:
    """One iteration: seal, drain, and report only when there was something to do.

    SEALING RUNS FIRST, and separately from the drain. It has its own tenant list on purpose: this
    worker drains tenants with an open `log_regroup_pending` row, and the rows that need sealing are
    stuck precisely because nothing tickets them any more (see `sealer`). Enumerating by ticket would
    have left the sealer unable to reach the rows it exists to fix.

    Its failures are swallowed the same way the drain's are - the rows simply stay unsealed and the
    next tick retries - so a sealer problem can never stop stitching, which is the load-bearing half.

    Swallows errors so a single bad tick never kills the loop — every per-window failure is already
    recorded durably on log_regroup_pending, so there is nothing to lose by carrying on.

    There is deliberately no `except asyncio.CancelledError: raise` here. Since Python 3.8
    CancelledError inherits from BaseException rather than Exception, so shutdown propagates through
    the handler below untouched; adding one would be redundant branching that reads as if it were
    load-bearing.
    """
    try:
        seal_stats = await sealer.seal_due()
    except Exception:
        logger.exception("Sealer error - stitching continues; rows stay unsealed for the next tick")
        seal_stats = {}
    if seal_stats.get("sealed") or seal_stats.get("failed"):
        logger.info("Sealer: %s", seal_stats)

    # S4's TTL sweep. Required rather than optional (section 18d): `evict_stale` closes a stream when
    # an ENTRY ARRIVES, so a tenant that stops ingesting leaves its rows behind forever. Derived state
    # could not leak because it was rebuilt from nothing every batch; persisted state can.
    #
    # Its failures are swallowed like the sealer's - a leak is a slow problem, and letting it stop
    # stitching would turn a slow problem into an outage.
    try:
        reaped = await stream_state.reap()
    except Exception:
        logger.exception("Stage 2 stream-state reaper failed; stitching continues")
        reaped = {}
    if reaped.get("streams_reaped") or reaped.get("pending_reaped"):
        logger.info("Stream state: %s", reaped)

    try:
        stats = await drain_once()
    except Exception:
        logger.exception("Stitch worker error - retrying next tick")
        return
    if stats["customers"]:
        logger.info("Stitch drain: %s", stats)


async def run_log_stitch_worker() -> None:
    """Forever loop. Survives errors; only cancellation (shutdown) stops it."""
    logger.info("Log stitch worker started (poll=%.1fs, max_customers_per_tick=%d)",
                settings.log_stitch_poll_seconds, settings.log_stitch_max_customers_per_tick)
    while True:
        await _tick()
        await asyncio.sleep(settings.log_stitch_poll_seconds)
