# log_ssh_fetch_run.py — Status record for an async remote-fetch run (mirrors LogRegroupRun)
#
#   POST /logs/fetch-remote is non-blocking: it creates one of these (status=running), schedules the
#   SSH pull + ingest + finalize in the background, and returns the run id immediately. The frontend
#   polls GET /logs/fetch-remote/runs/{id} until status is `completed` or `failed` — exactly how the
#   ingest Job and the regroup run are polled. A run may target one source (source_id set) or all of
#   the customer's enabled sources (source_id NULL).

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, BigInteger, Text, Enum
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogSshFetchRunStatus(str, enum.Enum):
    running = "running"      # scheduled / in progress
    completed = "completed"  # pull + ingest + finalize finished and committed
    failed = "failed"        # raised; see `error`


class LogSshFetchMode(str, enum.Enum):
    incremental = "incremental"  # pull the new tail of each remote file (the poller's mode)
    timestamp = "timestamp"      # ensure coverage from `requested_from`; pull older files if missing
    full = "full"                # re-pull every matching remote file whole (repair / first sync)


# Entity
class LogSshFetchRun(Base):
    __tablename__ = "log_ssh_fetch_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    # NULL = all of the customer's enabled sources; else the single targeted source.
    source_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    mode: Mapped[LogSshFetchMode] = mapped_column(Enum(LogSshFetchMode), default=LogSshFetchMode.incremental)
    requested_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[LogSshFetchRunStatus] = mapped_column(
        Enum(LogSshFetchRunStatus), default=LogSshFetchRunStatus.running, index=True
    )
    # outcome (NULL until finished)
    files_considered: Mapped[int | None] = mapped_column(Integer, nullable=True)
    files_fetched: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bytes_fetched: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    entries_ingested: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # full per-source/file stats + finalize

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
