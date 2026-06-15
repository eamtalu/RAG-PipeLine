# saved_view.py — "Saved Analyses" (Saved Views): a persisted, shareable analysis snapshot
#
#   A saved view is a captured analysis session: opaque filter/feed view-state (`state`) plus a
#   lightweight collaboration layer — a status workflow, an append-only comment thread, and a
#   closure ("done") record. It is tenant-scoped by `customer_code` (set from X-Customer-Code on
#   create). The frontend owns the shapes; the backend stores them verbatim.
#
#   `state`, `comments`, and `closure` are stored as JSONB and round-tripped untouched:
#     - `state`     — opaque blob; never inspected/validated/rewritten.
#     - `comments`  — append-only list of {id, author, body, created_at}; mutated only via add-comment.
#     - `closure`   — null until completed, then {summary, closed_by, closed_at}.
#   `name` is client-generated and stored verbatim (never generated, mutated, or unique-constrained).

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

# AnalysisStatus — valid `status` values. The current UI sets `in_progress` on create and
# `completed` on Complete; `open` / `due` are reserved for a future workflow but are accepted/stored.
ANALYSIS_STATUSES: tuple[str, ...] = ("open", "in_progress", "due", "completed")
DEFAULT_STATUS = "open"


class SavedView(Base):
    __tablename__ = "saved_views"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)  # tenant scope (from header)

    name: Mapped[str] = mapped_column(Text)  # client-generated, stored verbatim, not unique
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    saved_by: Mapped[str | None] = mapped_column(String(256), nullable=True)
    assignee: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default=DEFAULT_STATUS, index=True)
    due_date: Mapped[str | None] = mapped_column(String(64), nullable=True)  # ISO date string, opaque

    # Embedded collaboration layer (JSONB, round-tripped verbatim).
    comments: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")  # append-only
    closure: Mapped[dict | None] = mapped_column(JSONB, nullable=True)                # null until completed

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )  # immutable after create
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )  # bumped to now on every successful write (PATCH, add-comment)

    state: Mapped[dict] = mapped_column(JSONB)  # opaque captured analysis snapshot
