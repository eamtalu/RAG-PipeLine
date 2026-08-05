"""SSH log fetcher — a supervisor that runs ONE independent poll loop per customer_code.

Full tenant isolation: a slow or unreachable server for one customer never blocks another, because
each customer polls in its own task. A global semaphore (ssh_poll_max_concurrent) caps how many
fetches run at once so N tenants can't exhaust the DB pool. The supervisor reconciles the desired
set of customers (those with >= 1 enabled source) every ssh_poll_reconcile_seconds — spawning new
loops, cancelling departed ones, restarting any that unexpectedly finished — and cancels all child
loops on shutdown.

OFF by default — enable with settings.ssh_log_fetcher_enabled. The on-demand POST /logs/fetch-remote
trigger works regardless of this flag.

Each per-customer loop runs an incremental fetch_now(..., skip_if_busy=True, drive_breaker=True):
skip_if_busy hands a host already being fetched (e.g. by an on-demand run) back to the next tick;
drive_breaker lets a persistently-failing source auto-disable (see remote_fetcher._record_failure).
Cadence per customer = the min non-null poll_interval_seconds across its enabled sources, else the
global ssh_log_fetcher_poll_seconds.
"""

import asyncio
import logging

from sqlalchemy import func, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.services.mnp_log_ingestion.remote.remote_fetcher import fetch_now
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode

logger = logging.getLogger(__name__)

_semaphore: asyncio.Semaphore | None = None


def _sem() -> asyncio.Semaphore:
    """Lazily-created global concurrency cap (so the setting is read after config load)."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(settings.ssh_poll_max_concurrent)
    return _semaphore


async def _customers_with_enabled_sources() -> list[str]:
    async with async_session() as db:
        return list((await db.execute(
            select(LogSshSource.customer_code).where(LogSshSource.enabled.is_(True)).distinct()
        )).scalars().all())


async def _customer_interval(customer_code: str) -> float:
    """Min non-null poll_interval_seconds across the customer's enabled sources, else the global."""
    async with async_session() as db:
        v = await db.scalar(select(func.min(LogSshSource.poll_interval_seconds)).where(
            LogSshSource.customer_code == customer_code, LogSshSource.enabled.is_(True)))
    return float(v) if v else settings.ssh_log_fetcher_poll_seconds


# This will call remote_fetcher.py fetch_now
async def _poll_customer_once(customer_code: str) -> dict:
    async with _sem():  # global cap on concurrent per-customer fetches
        async with async_session() as db:
            return await fetch_now(db, customer_code, mode=LogSshFetchMode.incremental,
                                   enabled_only=True, skip_if_busy=True, drive_breaker=True)


async def _customer_loop(customer_code: str) -> None:
    """One tenant's forever loop: fetch, log anything notable, sleep its cadence. Survives errors;
    only CancelledError (reap/shutdown) stops it."""
    logger.info("SSH poll loop started for %s", customer_code)
    while True:
        try:
            stats = await _poll_customer_once(customer_code)
            if (stats.get("entries_ingested") or stats.get("errors") or stats.get("auto_disabled")
                    or stats.get("finalize_error")):
                logger.info("SSH poll %s: %s", customer_code,
                            {k: stats.get(k) for k in
                             ("files_fetched", "entries_ingested", "content_skipped", "errors",
                              "skipped", "auto_disabled", "finalize_error")})
            if stats.get("finalize_error"):
                logger.error("SSH poll %s: Stage 2 finalize failing — %d entries ingested but not "
                             "stitched; pending is accumulating. Error: %s", customer_code,
                             stats.get("entries_ingested") or 0, stats.get("finalize_error"))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("SSH poll loop error for %s — retrying next tick", customer_code)
        await asyncio.sleep(await _customer_interval(customer_code))


def _reconcile(loops: dict[str, asyncio.Task], desired: set[str], make_task) -> None:
    """Diff running per-customer loops against the desired set: spawn new (or restart finished) loops
    via make_task(customer_code); cancel + drop loops for customers no longer desired. Pure control
    logic (make_task injected) so it is unit-testable without a real event loop."""
    for cc in desired:
        t = loops.get(cc)
        if t is None or t.done():
            loops[cc] = make_task(cc)
    for cc in list(loops):
        if cc not in desired:
            loops[cc].cancel()
            del loops[cc]


# run_ssh_log_fetcher()          <- the "supervisor" / manager. ONE of these.
#       |
#       |-- every 30s: who are my customers?  -> _customers_with_enabled_sources()
#       |-- _reconcile(): hire/fire workers to match that list
#       |
#       +-- _customer_loop("ACME")   <- one forever-worker per customer
#       +-- _customer_loop("BECSI")
#       +-- _customer_loop("FOO")
#               |
#               +-- _poll_customer_once()  -> takes a wristband from _sem()
#                       |
#                       +-- _customer_interval()  -> how long to sleep
async def run_ssh_log_fetcher() -> None:
    logger.info("SSH log fetcher supervisor started (reconcile=%.1fs, max_concurrent=%d)",
                settings.ssh_poll_reconcile_seconds, settings.ssh_poll_max_concurrent)
    loops: dict[str, asyncio.Task] = {}
    try:
        while True:
            try:
                desired = set(await _customers_with_enabled_sources())
                _reconcile(loops, desired,
                           lambda cc: asyncio.create_task(_customer_loop(cc)))
            except Exception:
                logger.exception("SSH supervisor reconcile error — retrying next tick")
            await asyncio.sleep(settings.ssh_poll_reconcile_seconds)
    except asyncio.CancelledError:
        for t in loops.values():
            t.cancel()
        for t in loops.values():
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        raise
