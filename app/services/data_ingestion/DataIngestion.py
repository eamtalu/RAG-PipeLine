"""Service that orchestrates the full document ingestion flow."""

import asyncio
from uuid import UUID, uuid4

from fastapi import Depends

from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.repositories.job_repository import JobRepository, get_job_repository
from app.persistence.storage import get_storage
from app.persistence.storage.base import ObjectStorage
from app.services.data_ingestion.pipeline.orchestrator import run_pipeline


class DataIngestion:

    def __init__(self, storage: ObjectStorage, job_repo: JobRepository):
        self.storage = storage
        self.job_repo = job_repo

    async def ingest(self, data: bytes, filename: str, document_type: str = "general") -> Job:
        # 1. Persist file to object storage
        storage_key = f"{uuid4().hex}/{filename}"
        await self.storage.save(storage_key, data)

        # 2. Create Job record via repository
        job = await self.job_repo.create(filename=filename, storage_key=storage_key, document_type=document_type)

        # 3. Run pipeline in background (own DB session)
        asyncio.create_task(self._run_pipeline_background(job.id))

        return job

    #here job_id is the UUID object, job_id know where the file is
    #it can take any kind of file, based on file mime type, parser will do the needful.
    async def _run_pipeline_background(self, job_id: UUID) -> None:
        async with async_session() as db:
            #will do chunking and chunk will be enqueed in the database for embedding, but without indexing
            await run_pipeline(job_id, db, self.storage)


def get_data_ingestion(
    storage: ObjectStorage = Depends(get_storage),
    job_repo: JobRepository = Depends(get_job_repository),
) -> DataIngestion:
    """FastAPI dependency — provides DataIngestion with all deps injected."""
    return DataIngestion(storage, job_repo)
