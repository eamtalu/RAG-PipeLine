"""Log grouping worker — keeps log_transactions (Stage 2) in sync with log_entries.

Polls the log_entries row count; when it changes (a new file was ingested), it re-runs the
Stage 2 full rebuild so derived transactions stay current without manual /logs/regroup calls.

Full rebuild is fine at current volume; switch to incremental grouping if entry counts grow large.
"""

import asyncio
import logging

from sqlalchemy import func, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_entry import LogEntry
from app.services.mnp_log_ingestion.pipeline.derive_transactions import regroup_all

logger = logging.getLogger(__name__)


async def _entry_count() -> int:
    async with async_session() as db:
        return await db.scalar(select(func.count()).select_from(LogEntry)) or 0


async def run_log_grouping_worker() -> None:
    """Regroup whenever the entry count changes (i.e. after a file is ingested)."""
    logger.info("Log grouping worker started (poll=%.1fs)", settings.log_grouping_poll_seconds)
    last_count = -1
    while True:
        try:
            current = await _entry_count()
            if current != last_count:
                async with async_session() as db:
                    stats = await regroup_all(db)
                logger.info("Regrouped after entry-count change %s -> %s: %s", last_count, current, stats)
                last_count = current
            await asyncio.sleep(settings.log_grouping_poll_seconds)
        except Exception:
            logger.exception("Log grouping worker error — retrying after sleep")
            await asyncio.sleep(settings.log_grouping_poll_seconds * 2)
