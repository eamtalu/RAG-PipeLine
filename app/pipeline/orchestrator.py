"""Pipeline orchestrator — runs Stages 2→5 for a single job.

Called after upload. Detects, parses, normalises, chunks, and enqueues
chunks for async embedding (Stage 6).
"""

import logging
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.chunk import Chunk
from app.models.embedding_queue import EmbeddingQueueItem
from app.models.job import Job, JobStatus
from app.pipeline.normalizer import normalize
from app.pipeline.chunker import chunk_document
from app.storage.base import ObjectStorage

logger = logging.getLogger(__name__)


async def run_pipeline(job_id: UUID, db: AsyncSession, storage: ObjectStorage) -> None:
    """Execute the ingestion pipeline for a single job."""
    job = await db.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")

    try:
        # --- Stage 2+3+4: Detect → Parse → Normalise ---
        await _set_status(db, job_id, JobStatus.detecting)
        data = await storage.load(job.storage_key)
        doc = normalize(data, job.filename)

        await db.execute(
            update(Job).where(Job.id == job_id).values(mime_type=doc.mime_type)
        )
        await _set_status(db, job_id, JobStatus.parsing)

        # --- Stage 5: Chunk ---
        await _set_status(db, job_id, JobStatus.chunking)
        raw_chunks = chunk_document(doc)

        # Persist chunks + enqueue for embedding
        chunk_models: list[Chunk] = []
        queue_items: list[EmbeddingQueueItem] = []

        for i, rc in enumerate(raw_chunks):
            chunk = Chunk(
                job_id=job_id,
                index=i,
                text=rc["text"],
                token_count=rc["token_count"],
                heading_breadcrumb=rc["heading_breadcrumb"],
                metadata_=rc["metadata"],
            )
            chunk_models.append(chunk)

        db.add_all(chunk_models)
        await db.flush()  # populate chunk IDs

        for chunk in chunk_models:
            queue_items.append(
                EmbeddingQueueItem(chunk_id=chunk.id, job_id=job_id)
            )

        db.add_all(queue_items)
        await db.execute(
            update(Job).where(Job.id == job_id).values(
                status=JobStatus.embedding,
                chunk_count=len(chunk_models),
            )
        )
        await db.commit()

        logger.info("Job %s: %d chunks enqueued for embedding", job_id, len(chunk_models))

    except Exception as exc:
        logger.exception("Pipeline failed for job %s", job_id)
        await db.rollback()
        await _set_status(db, job_id, JobStatus.failed, error=str(exc))
        raise


async def _set_status(
    db: AsyncSession, job_id: UUID, status: JobStatus, error: str | None = None
) -> None:
    values: dict = {"status": status}
    if error:
        values["error"] = error
    await db.execute(update(Job).where(Job.id == job_id).values(**values))
    await db.commit()
