"""Stage 1 — Upload endpoint: validate, persist, create job."""

import uuid

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import get_session
from app.models.job import Job, JobStatus
from app.storage.local import LocalStorage

router = APIRouter(prefix="/documents", tags=["documents"])

ALLOWED_MIMES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/markdown",
    "text/html",
    "text/plain",
}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


def get_storage() -> LocalStorage:
    return LocalStorage(settings.upload_dir)


@router.post("/upload", status_code=201)
async def upload_document(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_session),
    storage: LocalStorage = Depends(get_storage),
):
    # --- read + size guard ---
    data = await file.read()
    if len(data) > MAX_FILE_SIZE:
        raise HTTPException(413, detail="File exceeds 50 MB limit")
    if not data:
        raise HTTPException(400, detail="Empty file")

    # --- persist to object storage ---
    storage_key = f"{uuid.uuid4().hex}/{file.filename}"
    await storage.save(storage_key, data)

    # --- create job record ---
    job = Job(
        filename=file.filename or "unknown",
        storage_key=storage_key,
        status=JobStatus.pending,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    return {
        "job_id": str(job.id),
        "filename": job.filename,
        "status": job.status.value,
    }


@router.get("/jobs/{job_id}")
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_session)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, detail="Job not found")
    return {
        "job_id": str(job.id),
        "filename": job.filename,
        "mime_type": job.mime_type,
        "status": job.status.value,
        "chunk_count": job.chunk_count,
        "error": job.error,
        "created_at": job.created_at.isoformat(),
    }
