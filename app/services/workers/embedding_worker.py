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
    """Embed a batch and store the vectors."""
    async with async_session() as session:
        # Load chunk texts
        chunk_ids = [item.chunk_id for item in items]
        result = await session.execute(select(Chunk).where(Chunk.id.in_(chunk_ids)))
        chunks = {c.id: c for c in result.scalars().all()}

        texts = [chunks[item.chunk_id].text for item in items]
        ids = [str(item.chunk_id) for item in items]

        # FIX 1: Prepend heading_breadcrumb for richer embeddings
        contextualized_texts = []
        for item in items:
            chunk = chunks[item.chunk_id]
            breadcrumb = chunk.heading_breadcrumb or ""
            if breadcrumb:
                contextualized_texts.append(breadcrumb + "\n\n" + chunk.text)
            else:
                contextualized_texts.append(chunk.text)

        # FIX 2: Enrich metadata with heading_breadcrumb, job_id, token_count
        enriched_metadatas = []
        for item in items:
            chunk = chunks[item.chunk_id]
            meta = dict(chunk.metadata_) if chunk.metadata_ else {}
            meta["heading_breadcrumb"] = chunk.heading_breadcrumb or ""
            meta["job_id"] = str(item.job_id)
            meta["token_count"] = chunk.token_count
            enriched_metadatas.append(meta)

        # Generate embeddings from contextualized texts
        vectors = await _generate_embeddings(contextualized_texts)

        # Write to vector store (store raw texts for retrieval, enriched metadata)
        store = get_vector_store()
        await store.upsert(ids=ids, vectors=vectors, texts=texts, metadatas=enriched_metadatas)

        # Mark items as done
        item_ids = [item.id for item in items]
        await session.execute(
            update(EmbeddingQueueItem)
            .where(EmbeddingQueueItem.id.in_(item_ids))
            .values(status=QueueStatus.done)
        )

        # Check if all items for each job are done → mark job completed
        job_ids = {item.job_id for item in items}
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
