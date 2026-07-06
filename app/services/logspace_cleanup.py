"""Full hard-purge of a log space (customer_code) and everything it owns.

Shared by DELETE /api/v1/customers/{code} and the auto-expiry worker. A disposable log space owns a
brand-new customer_code (1:1) into which logs are ingested; deleting it is a real purge, not a soft
deactivate. Permanent spaces are purged the same way on admin delete.

Cascade map (so this stays correct as the schema grows):
  - Deleting `jobs` for the tenant CASCADEs to chunks, chunks_entity, embedding_queue, log_entries,
    log_transactions (all FK job_id ON DELETE CASCADE).
  - The raw pgvector `embeddings` table is keyed by chunk/entity id with NO foreign key, so it is
    purged explicitly here (by the ids of the tenant's chunks/entities) before the jobs go.
  - Deleting `notification_events` CASCADEs its deliveries; deleting `log_ssh_sources` CASCADEs its
    file checkpoints.
  - Deleting the `customers` row CASCADEs customer_display_names + logspace_presence.
The remaining customer_code-keyed tables have no job/tenant FK, so they are deleted explicitly.
"""

import logging

from sqlalchemy import select, delete, text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.chunk import Chunk
from app.persistence.models.ChunkEntity import ChunkEntity
from app.persistence.models.saved_view import SavedView
from app.persistence.models.log_regroup_run import LogRegroupRun
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint
from app.persistence.models.log_ssh_fetch_run import LogSshFetchRun
from app.persistence.models.notification import (
    CustomerNotificationChannel,
    NotificationRule,
    NotificationEvent,
)

logger = logging.getLogger(__name__)


async def purge_logspace(db: AsyncSession, customer_code: str) -> bool:
    """Hard-delete a log space and all data keyed by its customer_code. Commits on success.

    Returns False (and makes no changes) if the code is not a registered customer.
    """
    exists = await db.scalar(select(Customer.id).where(Customer.customer_code == customer_code))
    if exists is None:
        return False

    # 1) Raw pgvector embeddings — no FK, keyed by the string id of each chunk/entity. Collect those
    #    ids from the tenant's jobs and delete the matching rows before the jobs (and their chunks) go.
    job_ids = list(
        (await db.execute(select(Job.id).where(Job.customer_code == customer_code))).scalars().all()
    )
    if job_ids:
        chunk_ids = list(
            (await db.execute(select(Chunk.id).where(Chunk.job_id.in_(job_ids)))).scalars().all()
        )
        entity_ids = list(
            (await db.execute(select(ChunkEntity.id).where(ChunkEntity.job_id.in_(job_ids)))).scalars().all()
        )
        emb_ids = [str(i) for i in (*chunk_ids, *entity_ids)]
        if emb_ids:
            stmt = text("DELETE FROM embeddings WHERE id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            )
            await db.execute(stmt, {"ids": emb_ids})

    # 2) Jobs → cascades chunks, chunks_entity, embedding_queue, log_entries, log_transactions.
    await db.execute(delete(Job).where(Job.customer_code == customer_code))

    # 3) Remaining customer_code-keyed tables that have no job/tenant FK cascade.
    await db.execute(delete(LogRegroupRun).where(LogRegroupRun.customer_code == customer_code))
    await db.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == customer_code))
    await db.execute(delete(LogSshFetchRun).where(LogSshFetchRun.customer_code == customer_code))
    await db.execute(delete(LogSshFileCheckpoint).where(LogSshFileCheckpoint.customer_code == customer_code))
    await db.execute(delete(LogSshSource).where(LogSshSource.customer_code == customer_code))
    await db.execute(delete(SavedView).where(SavedView.customer_code == customer_code))
    # events first — cascades notification_deliveries; then rules + channels.
    await db.execute(delete(NotificationEvent).where(NotificationEvent.customer_code == customer_code))
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == customer_code))
    await db.execute(delete(CustomerNotificationChannel).where(
        CustomerNotificationChannel.customer_code == customer_code))

    # 4) The tenant row → cascades customer_display_names + logspace_presence.
    await db.execute(delete(Customer).where(Customer.customer_code == customer_code))

    await db.commit()
    logger.info("Purged log space %r (%d job(s) and all associated data)", customer_code, len(job_ids))
    return True
