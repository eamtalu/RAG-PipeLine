"""Re-push tenant bindings the edge has not seen yet (or has an old version of)."""

import logging

from app.config.database import async_session
from app.persistence.repositories.teams_repository import TeamsBindingRepository
from app.services.teams.binding_mirror import BindingMirror

logger = logging.getLogger(__name__)


async def sweep_once(mirror: BindingMirror) -> int:
    """Mirror every stale binding. Returns how many were pushed. One failure does not stop the rest."""
    pushed = 0
    async with async_session() as db:
        repo = TeamsBindingRepository(db)
        for row in await repo.list_needing_mirror():
            try:
                await mirror.put(row)
            except Exception:
                logger.warning("binding %s still not mirrored", row.tenant_id, exc_info=True)
                continue
            await repo.mark_mirrored(row.tenant_id)
            pushed += 1
    if pushed:
        logger.info("Mirrored %d Teams binding(s) to the edge", pushed)
    return pushed
