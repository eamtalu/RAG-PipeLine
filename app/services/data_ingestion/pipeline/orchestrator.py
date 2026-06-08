# orchestrator.py — Pipeline Orchestrator (Stages 2→5 Glued Together)
#
#   run_pipeline(job_id, db, storage)
#
#   Step by step:
#   1. Fetches the Job, loads raw bytes, parses into ParsedDocument
#   2. Routes chunking:
#      - PDFs with raw_lines → multipass chunker → ChunkEntity
#      - Everything else     → hierarchical chunker → Chunk
#   3. Enqueues all chunks for async embedding via EmbeddingQueueItem
#   4. Updates job status to embedding

"""Pipeline orchestrator — runs Stages 2→5 for a single job."""

import logging
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.chunk import Chunk
from app.persistence.models.ChunkEntity import ChunkEntity
from app.persistence.models.embedding_queue import EmbeddingQueueItem
from app.persistence.models.job import Job, JobStatus
from app.services.data_ingestion.pipeline.helper.pipeline_helper import parse_document
from app.services.data_ingestion.pipeline.chunkers.hierarchical.chunker import chunk_document
from app.services.data_ingestion.pipeline.chunkers.multi_pass.ChunkerFactory import get_chunker
from app.persistence.storage.base import ObjectStorage

logger = logging.getLogger(__name__)


async def run_pipeline(job_id: UUID, db: AsyncSession, storage: ObjectStorage) -> None:
    """Execute the ingestion pipeline for a single job."""
    job = await db.get(Job, job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")

    try:
        # --- Stage 1: load the document + parse it ---
        await _set_status(db, job_id, JobStatus.detecting)
        data = await storage.load(job.storage_key)
        doc = parse_document(data, job.filename)

        await db.execute(
            update(Job).where(Job.id == job_id).values(mime_type=doc.mime_type)
        )
        await _set_status(db, job_id, JobStatus.parsing)

        # --- Stage 2: Chunk + Enqueue ---
        await _set_status(db, job_id, JobStatus.chunking)

        if doc.raw_lines:
            # PDF path: multipass chunker → ChunkEntity
            total = await _chunk_multipass(db, job_id, job.filename, doc)
        else:
            # Non-PDF path: hierarchical chunker → Chunk (existing flow)
            total = await _chunk_hierarchical(db, job_id, doc)

        await db.execute(
            update(Job).where(Job.id == job_id).values(
                status=JobStatus.embedding,
                chunk_count=total,
            )
        )
        await db.commit()

        logger.info("Job %s: %d chunks enqueued for embedding", job_id, total)

    except Exception as exc:
        logger.exception("Pipeline failed for job %s", job_id)
        await db.rollback()
        await _set_status(db, job_id, JobStatus.failed, error=str(exc))
        raise


async def _chunk_multipass(db: AsyncSession, job_id: UUID, filename: str, doc) -> int:
    """Run multipass chunker on PDF raw_lines, persist as ChunkEntity rows."""
    chunker = get_chunker(profile="auto")
    chunks = chunker.chunk(lines=doc.raw_lines, doc_meta=doc.metadata)

    entity_models: list[ChunkEntity] = []
    for i, cr in enumerate(chunks):
        ctx_path = cr.context_path
        entity = ChunkEntity(
            job_id=job_id,
            source_file=filename,
            chunk_index=i,
            text=cr.text,
            context_header=cr.context_header,
            full_text=cr.full_text,
            context_path=ctx_path,
            context_depth=len(ctx_path),
            page_numbers=cr.page_numbers,
            chunk_type=cr.chunk_type,
            profile=cr.metadata.get("profile", "generic"),
            token_estimate=cr.token_estimate,
            section_root=ctx_path[0] if len(ctx_path) >= 1 else None,
            section_parent=ctx_path[-2] if len(ctx_path) >= 2 else None,
            section_heading=ctx_path[-1] if len(ctx_path) >= 1 else None,
        )
        entity_models.append(entity)

    db.add_all(entity_models)
    await db.flush()

    # Enqueue for embedding
    queue_items = [
        EmbeddingQueueItem(chunk_entity_id=e.id, job_id=job_id)
        for e in entity_models
    ]
    db.add_all(queue_items)

    return len(entity_models)


async def _chunk_hierarchical(db: AsyncSession, job_id: UUID, doc) -> int:
    """Existing hierarchical chunker flow — Chunk model."""
    parent_dicts = chunk_document(doc)

    all_chunks: list[Chunk] = []
    global_index = 0

    # Pass 1: parent chunks
    parent_models: list[Chunk] = []
    for pd in parent_dicts:
        parent = Chunk(
            job_id=job_id,
            index=global_index,
            text=pd["text"],
            token_count=pd["token_count"],
            heading_breadcrumb=pd["heading_breadcrumb"],
            metadata_=pd["metadata"],
            chunk_type=pd["chunk_type"],
            parent_id=None,
        )
        parent_models.append(parent)
        global_index += 1

    db.add_all(parent_models)
    await db.flush()
    all_chunks.extend(parent_models)

    # Pass 2: leaf chunks
    leaf_models: list[Chunk] = []
    for parent_model, pd in zip(parent_models, parent_dicts):
        for child_dict in pd["_children"]:
            leaf = Chunk(
                job_id=job_id,
                index=global_index,
                text=child_dict["text"],
                token_count=child_dict["token_count"],
                heading_breadcrumb=child_dict["heading_breadcrumb"],
                metadata_=child_dict["metadata"],
                chunk_type=child_dict["chunk_type"],
                parent_id=parent_model.id,
            )
            leaf_models.append(leaf)
            global_index += 1

    db.add_all(leaf_models)
    await db.flush()
    all_chunks.extend(leaf_models)

    # Enqueue for embedding
    queue_items = [
        EmbeddingQueueItem(chunk_id=chunk.id, job_id=job_id)
        for chunk in all_chunks
    ]
    db.add_all(queue_items)

    logger.info("Job %s: %d parents, %d leaves", job_id, len(parent_models), len(leaf_models))
    return len(all_chunks)


async def _set_status(
    db: AsyncSession, job_id: UUID, status: JobStatus, error: str | None = None
) -> None:
    values: dict = {"status": status}
    if error:
        values["error"] = error
    await db.execute(update(Job).where(Job.id == job_id).values(**values))
    await db.commit()
