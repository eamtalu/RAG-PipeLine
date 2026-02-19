import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Enum, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class JobStatus(str, enum.Enum):
    pending = "pending"
    detecting = "detecting"
    parsing = "parsing"
    chunking = "chunking"
    embedding = "embedding"
    completed = "completed"
    failed = "failed"


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
