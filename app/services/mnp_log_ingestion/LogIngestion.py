"""Service that orchestrates transaction-log ingestion (Stage 1: parse → insert)."""

import asyncio
import logging
from uuid import UUID, uuid4

from fastapi import Depends

from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.repositories.job_repository import JobRepository, get_job_repository
from app.persistence.storage import get_storage
from app.persistence.storage.base import ObjectStorage
from app.services.mnp_log_ingestion.pipeline.parse_insert import run_log_parse_insert

DOCUMENT_TYPE = "transaction_log"

logger = logging.getLogger(__name__)


class LogIngestion:
    """Mirrors DataIngestion, but for M3 WMS logs: saves the file, tracks a Job, runs Stage 1."""

    def __init__(self, storage: ObjectStorage, job_repo: JobRepository):
        self.storage = storage
        self.job_repo = job_repo

    async def ingest(self, data: bytes, filename: str, customer_code: str, *, background: bool = True) -> Job:
        # 1. Persist file to object storage, namespaced by customer for per-tenant isolation
        storage_key = f"{customer_code}/{uuid4().hex}/{filename}"
        await self.storage.save(storage_key, data)

        # 2. Create Job record (document_type discriminates this from the document pipeline)
        job = await self.job_repo.create(
            filename=filename, storage_key=storage_key,
            document_type=DOCUMENT_TYPE, customer_code=customer_code,
        )

        # 3. Run Stage 1 (parse → insert). Background by default (fire-and-forget like DataIngestion);
        #    callers that need to await the result (e.g. tests) pass background=False.
        if background:
            asyncio.create_task(self._run_stage1_safe(job.id))
        else:
            await self._run_stage1_background(job.id)

        return job

    async def _run_stage1_background(self, job_id: UUID) -> None:
        async with async_session() as db:
            await run_log_parse_insert(job_id, db, self.storage)

    async def _run_stage1_safe(self, job_id: UUID) -> None:
        """Fire-and-forget wrapper for the upload path: Stage 1 already logs and marks the job failed
        (including on a disk I/O / bad-sector error), so swallow here to avoid an unretrieved-task
        warning. The poller path calls _run_stage1_background directly (background=False) so its
        per-file handler can see and isolate the failure."""
        try:
            await self._run_stage1_background(job_id)
        except Exception:
            logger.debug("background Stage-1 task ended with error for job %s (already recorded)",
                         job_id, exc_info=True)


def get_log_ingestion(
    storage: ObjectStorage = Depends(get_storage),
    job_repo: JobRepository = Depends(get_job_repository),
) -> LogIngestion:
    """FastAPI dependency — provides LogIngestion with deps injected."""
    return LogIngestion(storage, job_repo)
