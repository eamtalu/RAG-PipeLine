import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class ChunkEntity(Base):
    __tablename__ = "chunks_entity"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))

    source_file: Mapped[str] = mapped_column(String(512))
    chunk_index: Mapped[int] = mapped_column(Integer)

    # ── Text fields (stored, not all embedded) ──
    text: Mapped[str] = mapped_column(Text)
    context_header: Mapped[str] = mapped_column(Text, default="")  # "> Amin Talukder\n  > BSc in..."
    full_text: Mapped[str] = mapped_column(Text)

    # ── Structured metadata (filterable, not embedded) ──
    context_path: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)  # ['Amin Talukder', 'Work Experience', 'Senior Dev @BECSI']
    context_depth: Mapped[int] = mapped_column(Integer, default=0) # len(context_path) — useful for filtering top-level vs detail
    page_numbers: Mapped[list[int]] = mapped_column(ARRAY(Integer), default=list)
    chunk_type: Mapped[str] = mapped_column(String(32), default="text")
    profile: Mapped[str] = mapped_column(String(64), default="generic")  # "cv", "report", "generic"
    token_estimate: Mapped[int] = mapped_column(Integer, default=0)

    # ── Derived fields for filtering/search ──
    section_root: Mapped[str | None] = mapped_column(String(512), nullable=True)  # context_path[0] if exists — top-level grouping
    section_parent: Mapped[str | None] = mapped_column(String(512), nullable=True) # context_path[-2] if depth >= 2 — immediate parent
    section_heading: Mapped[str | None] = mapped_column(String(512), nullable=True) # context_path[-1] — leaf heading

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
