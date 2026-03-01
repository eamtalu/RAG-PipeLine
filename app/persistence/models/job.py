# job.py — Orchestrating the Ingestion Pipeline
#
#   The jobs table is the top-level tracking unit for a single file upload. When a user uploads a document, one Job row is created to track it through the entire RAG ingestion
#   pipeline.
#
#   Key design intentions:
#   - status enum (pending → detecting → parsing → chunking → embedding → completed / failed) — models the sequential stages of the pipeline so the system (and the user) always
#   knows where a file stands
#   - storage_key — reference to where the raw file is stored (e.g. S3 or local filesystem), decoupling storage from processing
#   - error — captures failure reason so jobs can be diagnosed or retried
#   - chunk_count — denormalized count for quick status reporting without joining to the chunks table
#   - updated_at with onupdate — automatically tracks the last time the job progressed

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Enum, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class JobStatus(str, enum.Enum):
    pending = "pending"
    detecting = "detecting"
    parsing = "parsing"
    chunking = "chunking"
    embedding = "embedding"
    completed = "completed"
    failed = "failed"

#Entity
class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    filename: Mapped[str] = mapped_column(String(512))
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    storage_key: Mapped[str] = mapped_column(String(1024))
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.pending)
    chunk_count: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
