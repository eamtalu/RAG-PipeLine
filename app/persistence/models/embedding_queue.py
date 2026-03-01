# embedding_queue.py — Async Embedding Work Queue
#
#   The embedding_queue table acts as a task queue for generating vector embeddings. Instead of embedding chunks synchronously during upload, each chunk gets enqueued for async
#   processing.
#
#   Key design intentions:
#   - chunk_id FK — links to the specific chunk that needs an embedding
#   - job_id FK — allows batch-level tracking (e.g. "are all chunks for this job embedded yet?")
#   - status enum (pending → processing → done / failed) — enables a worker/consumer to claim items, process them, and mark results, functioning as a lightweight job queue without
#   needing Redis/Celery
#   - This pattern allows rate-limiting embedding API calls, retrying failed embeddings, and parallelizing work across multiple workers

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Enum, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class QueueStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    done = "done"
    failed = "failed"


#Entity
#Base has inherited declarativebase which means "Entity"
class EmbeddingQueueItem(Base):
    __tablename__ = "embedding_queue"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chunk_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("chunks.id", ondelete="CASCADE"))
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))
    status: Mapped[QueueStatus] = mapped_column(Enum(QueueStatus), default=QueueStatus.pending)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
