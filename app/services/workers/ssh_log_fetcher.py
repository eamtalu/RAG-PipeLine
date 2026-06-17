"""SSH log fetcher — background poller that pulls each enabled Windows Server's new log tail.

Mirrors log_watcher: a simple polling loop. Each tick it finds every customer that has at least one
ENABLED LogSshSource and, per customer, runs an incremental pull of all that customer's enabled
sources followed by a single finalize (remote_fetcher.fetch_now). Per-source failures are isolated
inside fetch_now and recorded on the source row; the loop itself never dies.

OFF by default — enable with settings.ssh_log_fetcher_enabled. The on-demand POST /logs/fetch-remote
trigger works regardless of this flag.

NOTE: v1 polls every enabled source on the global ssh_log_fetcher_poll_seconds cadence. The
per-source poll_interval_seconds column is stored for a future finer-grained scheduler.
"""

import asyncio
import logging

from sqlalchemy import select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_ssh_source import LogSshSource
from app.services.mnp_log_ingestion.remote.remote_fetcher import fetch_now
from app.persistence.models.log_ssh_fetch_run import LogSshFetchMode

logger = logging.getLogger(__name__)


async def _customers_with_enabled_sources() -> list[str]:
    async with async_session() as db:
        return list((await db.execute(
            select(LogSshSource.customer_code).where(LogSshSource.enabled.is_(True)).distinct()
        )).scalars().all())


async def _poll_once() -> None:
    for customer_code in await _customers_with_enabled_sources():
        try:
            async with async_session() as db:
                stats = await fetch_now(db, customer_code, mode=LogSshFetchMode.incremental,
                                        enabled_only=True)
            if stats.get("entries_ingested") or stats.get("errors"):
                logger.info("SSH poll for %s: %s", customer_code,
                            {k: stats.get(k) for k in ("files_fetched", "entries_ingested", "errors")})
        except Exception:
            logger.exception("SSH poll failed for %s — will retry next tick", customer_code)


async def run_ssh_log_fetcher() -> None:
    logger.info("SSH log fetcher started (poll=%.1fs)", settings.ssh_log_fetcher_poll_seconds)
    while True:
        try:
            await _poll_once()
        except Exception:
            logger.exception("SSH log fetcher loop error — retrying after sleep")
        await asyncio.sleep(settings.ssh_log_fetcher_poll_seconds)
