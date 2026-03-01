"""Stage 1 — Upload endpoint: validate, persist, create job."""

import uuid

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.job import Job
from app.services.data_ingestion.DataIngestion import DataIngestion, get_data_ingestion

router = APIRouter(prefix="/documents", tags=["documents"])

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


@router.post("/upload", status_code=201)
async def upload_document(
    file: UploadFile = File(...),
    data_ingestion: DataIngestion = Depends(get_data_ingestion),
):
    # --- read + size guard ---
    data = await file.read()
    if len(data) > MAX_FILE_SIZE:
        raise HTTPException(413, detail="File exceeds 50 MB limit")
    if not data:
        raise HTTPException(400, detail="Empty file")

    # --- delegate to service -> DataIngestion---
    job = await data_ingestion.ingest(data, file.filename or "unknown")

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
