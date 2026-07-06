"""Log-space cleanup worker — auto-expires disposables and sweeps stale presence.

Each tick:
  (1) hard-purge every disposable log space whose expires_at is due — the SAME purge as
      DELETE /api/v1/customers/{code} (code + aliases + presence + all associated data);
  (2) sweep presence rows not refreshed within the presence TTL.

Only rows with kind=disposable AND a non-NULL, due expires_at are purged, so permanent spaces and
never-expiring legacy disposables (expires_at IS NULL) are never touched. OFF by default — enable with
settings.logspace_cleanup_worker_enabled.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.customer import Customer, LogSpaceKind
from app.persistence.repositories.logspace_presence_repository import LogspacePresenceRepository
from app.services.logspace_cleanup import purge_logspace

logger = logging.getLogger(__name__)


async def run_logspace_cleanup_once() -> tuple[int, int]:
    """One cleanup pass. Returns (disposables_purged, presence_rows_swept). Safe to call directly
    (e.g. from tests) without the worker loop."""
    now = datetime.now(timezone.utc)
    purged = 0
    async with async_session() as session:
        due = list(
            (
                await session.execute(
                    select(Customer.customer_code).where(
                        Customer.kind == LogSpaceKind.disposable,
                        Customer.expires_at.is_not(None),
                        Customer.expires_at <= now,
                    )
                )
            ).scalars().all()
        )
        for code in due:
            if await purge_logspace(session, code):
                purged += 1

        presence_cutoff = now - timedelta(seconds=settings.logspace_presence_ttl_seconds)
        swept = await LogspacePresenceRepository(session).sweep(presence_cutoff)
    return purged, swept


async def run_logspace_cleanup_worker() -> None:
    logger.info("Log-space cleanup worker started (poll=%.1fs, presence_ttl=%ds)",
                settings.logspace_cleanup_poll_seconds, settings.logspace_presence_ttl_seconds)
    while True:
        try:
            purged, swept = await run_logspace_cleanup_once()
            if purged or swept:
                logger.info("Log-space cleanup: purged %d expired disposable(s), swept %d stale "
                            "presence row(s)", purged, swept)
        except Exception:
            logger.exception("Log-space cleanup worker error — retrying after sleep")
        await asyncio.sleep(settings.logspace_cleanup_poll_seconds)
