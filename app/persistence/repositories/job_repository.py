"""Repository for Job entity — Spring Boot-style data access layer."""

from uuid import UUID

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.job import Job, JobStatus


class JobRepository:
    """Handles all database operations for the Job entity."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, filename: str, storage_key: str, document_type: str = "general") -> Job:
        job = Job(
            filename=filename,
            storage_key=storage_key,
            document_type=document_type,
            status=JobStatus.pending,
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job

    async def get_by_id(self, job_id: UUID) -> Job | None:
        return await self.db.get(Job, job_id)


def get_job_repository(db: AsyncSession = Depends(get_session)) -> JobRepository:
    """FastAPI dependency — provides JobRepository with session injected."""
    return JobRepository(db)
