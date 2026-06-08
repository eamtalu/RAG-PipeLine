"""Stage 6 — Async embedding worker.

Polls the embedding_queue table for pending items, generates embeddings
in batches, and writes vectors to the configured vector store.
"""

import asyncio
import logging

import openai
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.chunk import Chunk
from app.persistence.models.ChunkEntity import ChunkEntity
from app.persistence.models.embedding_queue import EmbeddingQueueItem, QueueStatus
from app.persistence.models.job import Job, JobStatus
from app.persistence.vectorstore import get_vector_store

logger = logging.getLogger(__name__)


async def _fetch_pending_batch(session: AsyncSession, batch_size: int) -> list[EmbeddingQueueItem]:
    """Atomically claim a batch of pending items."""
    result = await session.execute(
        select(EmbeddingQueueItem)
        .where(EmbeddingQueueItem.status == QueueStatus.pending)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    items = list(result.scalars().all())
    if items:
        ids = [item.id for item in items]
        await session.execute(
            update(EmbeddingQueueItem)
            .where(EmbeddingQueueItem.id.in_(ids))
            .values(status=QueueStatus.processing)
        )
        await session.commit()
    return items


async def _generate_embeddings(texts: list[str]) -> list[list[float]]:
    """Call OpenAI embeddings API."""
    client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
    response = await client.embeddings.create(
        model=settings.embedding_model,
        input=texts,
        dimensions=settings.embedding_dimensions,
    )
    return [item.embedding for item in response.data]


async def _process_batch(items: list[EmbeddingQueueItem]) -> None:
    """Embed a batch and store the vectors.

    Handles two chunk sources:
      - chunk_id → old Chunk model (hierarchical chunker, non-PDF)
      - chunk_entity_id → ChunkEntity model (multipass chunker, PDF)
    """
    async with async_session() as session:
        # Split items by source
        legacy_items = [it for it in items if it.chunk_id is not None]
        entity_items = [it for it in items if it.chunk_entity_id is not None]

        # Load legacy Chunk rows
        chunks: dict = {}
        if legacy_items:
            chunk_ids = [it.chunk_id for it in legacy_items]
            result = await session.execute(select(Chunk).where(Chunk.id.in_(chunk_ids)))
            chunks = {c.id: c for c in result.scalars().all()}

        # Load ChunkEntity rows
        entities: dict = {}
        if entity_items:
            entity_ids = [it.chunk_entity_id for it in entity_items]
            result = await session.execute(select(ChunkEntity).where(ChunkEntity.id.in_(entity_ids)))
            entities = {e.id: e for e in result.scalars().all()}

        # Load job rows to get document_type
        job_ids = {item.job_id for item in items}
        job_result = await session.execute(select(Job).where(Job.id.in_(job_ids)))
        jobs = {j.id: j for j in job_result.scalars().all()}

        # Build parallel lists: ids, texts (for retrieval), contextualized_texts (for embedding), metadatas
        all_ids: list[str] = []
        all_texts: list[str] = []
        contextualized_texts: list[str] = []
        enriched_metadatas: list[dict] = []

        for item in items:
            doc_type = jobs[item.job_id].document_type if item.job_id in jobs else "general"

            if item.chunk_entity_id and item.chunk_entity_id in entities:
                # ChunkEntity path — full_text already contains context_header + text
                ce = entities[item.chunk_entity_id]
                all_ids.append(str(ce.id))
                all_texts.append(ce.full_text)
                contextualized_texts.append(ce.full_text)
                enriched_metadatas.append({
                    "heading_breadcrumb": ce.context_header,
                    "job_id": str(item.job_id),
                    "token_count": ce.token_estimate,
                    "chunk_type": ce.chunk_type,
                    "document_type": doc_type,
                    "profile": ce.profile,
                    "context_path": ce.context_path,
                    "context_depth": ce.context_depth,
                    "page_numbers": ce.page_numbers,
                    "section_root": ce.section_root,
                    "section_parent": ce.section_parent,
                    "section_heading": ce.section_heading,
                    "source_file": ce.source_file,
                })
            elif item.chunk_id and item.chunk_id in chunks:
                # Legacy Chunk path
                chunk = chunks[item.chunk_id]
                all_ids.append(str(chunk.id))
                all_texts.append(chunk.text)
                breadcrumb = chunk.heading_breadcrumb or ""
                if breadcrumb:
                    contextualized_texts.append(breadcrumb + "\n\n" + chunk.text)
                else:
                    contextualized_texts.append(chunk.text)
                meta = dict(chunk.metadata_) if chunk.metadata_ else {}
                meta["heading_breadcrumb"] = breadcrumb
                meta["job_id"] = str(item.job_id)
                meta["token_count"] = chunk.token_count
                meta["chunk_type"] = chunk.chunk_type
                meta["parent_id"] = str(chunk.parent_id) if chunk.parent_id else None
                meta["document_type"] = doc_type
                enriched_metadatas.append(meta)

        # Generate embeddings
        vectors = await _generate_embeddings(contextualized_texts)

        # Write to vector store
        store = get_vector_store()
        await store.upsert(ids=all_ids, vectors=vectors, texts=all_texts, metadatas=enriched_metadatas)

        # Mark items as done
        item_ids = [item.id for item in items]
        await session.execute(
            update(EmbeddingQueueItem)
            .where(EmbeddingQueueItem.id.in_(item_ids))
            .values(status=QueueStatus.done)
        )

        # Check if all items for each job are done → mark job completed
        for job_id in job_ids:
            remaining = await session.execute(
                select(EmbeddingQueueItem)
                .where(
                    EmbeddingQueueItem.job_id == job_id,
                    EmbeddingQueueItem.status != QueueStatus.done,
                )
            )
            if not remaining.scalars().first():
                await session.execute(
                    update(Job).where(Job.id == job_id).values(status=JobStatus.completed)
                )

        await session.commit()


async def run_worker() -> None:
    """Main worker loop — polls for pending embeddings."""
    logger.info("Embedding worker started (poll interval=%.1fs)", settings.worker_poll_seconds)

    # Wait for DB/vector store to become available before entering the loop
    while True:
        try:
            store = get_vector_store()
            await store.ensure_collection()
            break
        except Exception:
            logger.warning("Vector store not ready — retrying in %.0fs", settings.worker_poll_seconds * 2)
            await asyncio.sleep(settings.worker_poll_seconds * 2)

    while True:
        try:
            async with async_session() as session:
                items = await _fetch_pending_batch(session, settings.embedding_batch_size)

            if items:
                logger.info("Processing batch of %d items", len(items))
                await _process_batch(items)
            else:
                await asyncio.sleep(settings.worker_poll_seconds)

        except Exception:
            logger.exception("Embedding worker error — retrying after sleep")
            await asyncio.sleep(settings.worker_poll_seconds * 2)
