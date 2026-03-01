# chunk.py — Storing Text Segments
#
#   The chunks table holds the split pieces of a parsed document. After a file is parsed into plain text, it gets divided into smaller, semantically meaningful chunks for embedding
#   and retrieval.
#
#   Key design intentions:
#   - job_id FK (CASCADE) — ties every chunk back to its parent job; deleting a job deletes all its chunks
#   - index — preserves the original order of chunks within the document (important for reassembly/context)
#   - text — the actual chunk content that will be embedded and searched
#   - token_count — pre-computed token count, useful for staying within LLM context limits during retrieval
#   - heading_breadcrumb — stores the heading hierarchy (e.g. "Chapter 2 > Section 2.1 > Subsection"), giving retrieved chunks structural context so the LLM knows where in the
#   document a chunk came from
#   - metadata_ (JSONB) — flexible key-value store for extra info (page number, source URL, etc.)


import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class Chunk(Base):
    __tablename__ = "chunks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))
    index: Mapped[int] = mapped_column(Integer)
    text: Mapped[str] = mapped_column(Text)
    token_count: Mapped[int] = mapped_column(Integer)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, default=dict)
    heading_breadcrumb: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
