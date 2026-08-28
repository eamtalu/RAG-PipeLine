# log_regroup_run.py — Status record for an async finalize (scoped regroup) run
#
#   POST /logs/regroup/finalize is non-blocking: it creates one of these rows (status=running),
#   schedules the scoped regroup in the background, and returns the run id immediately. The frontend
#   polls GET /logs/regroup/runs/{id} until status is `completed` or `failed` — so a long regroup
#   never blocks the HTTP request into a timeout. Mirrors how the ingest Job is polled.

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, Text, Enum
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogRegroupRunStatus(str, enum.Enum):
    running = "running"      # scheduled / in progress
    completed = "completed"  # finalize_pending finished and committed
    failed = "failed"        # finalize_pending raised; pending rows stay open for a retry


# Entity
class LogRegroupRun(Base):
    __tablename__ = "log_regroup_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[LogRegroupRunStatus] = mapped_column(
        Enum(LogRegroupRunStatus), default=LogRegroupRunStatus.running, index=True
    )
    # Chunk 69: which flavour of run this row tracks. "finalize" = the scoped regroup the frontend
    # already polls; "full" = the tracked full rebuild that replaced the removed inline endpoint.
    # A RUNNING "full" row doubles as the tenant's MAINTENANCE FLAG: both worker sweeps skip the
    # tenant while one is fresh, which is what lets a rebuild run without racing the live stitcher.
    kind: Mapped[str] = mapped_column(String(16), default="finalize", server_default="finalize")
    # Chunk 70: a kind='full' run may be RANGED - rebuild only [range_start, range_end] instead of
    # the whole history. NULL/NULL (the default) means everything. Stored on the row because the
    # rebuild executes in a separate process, and the row is the only thing that crosses it.
    range_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    range_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # outcome (NULL until finished)
    windows: Mapped[int | None] = mapped_column(Integer, nullable=True)            # time-windows rebuilt
    pending_consumed: Mapped[int | None] = mapped_column(Integer, nullable=True)   # pending rows cleared
    error: Mapped[str | None] = mapped_column(Text, nullable=True)                 # set when status=failed
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)              # full finalize_pending stats

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
