# log_ssh_file_checkpoint.py — Per-source, per-remote-file incremental cursor for SSH pull-ingestion
#
#   One row tracks how much of a single remote log file we've already pulled from a given
#   LogSshSource, so the fetcher only reads the NEW tail instead of the whole file each poll. Keyed
#   by (source_id, remote_path) — two different Windows Servers can legitimately hold the same path,
#   so the cursor must be per source, not per customer.
#
#   How it's used (services/.../remote/remote_fetcher.fetch_incremental):
#     - unchanged (same size+mtime as last_*) -> skipped, no transfer;
#     - grown -> read [last_offset, size), trim to the last newline, ingest, advance last_offset to
#       that boundary (never ingest a partial trailing line);
#     - shrank (rotation/truncation) -> reset last_offset=0 and re-read whole; the entry_hash content
#       dedup in Stage 1 drops anything already seen.
#   The byte cursor is an OPTIMISATION; correctness is guaranteed by entry_hash dedup regardless.

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, BigInteger, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class LogSshFileCheckpoint(Base):
    __tablename__ = "log_ssh_file_checkpoints"

    __table_args__ = (
        UniqueConstraint("source_id", "remote_path", name="uq_log_ssh_file_ckpt_source_path"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # the server this file lives on; cascades away with the source.
    source_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_ssh_sources.id", ondelete="CASCADE"), index=True
    )
    # kept for tenant-scoped queries / housekeeping (mirrors the rest of the log pipeline).
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    remote_path: Mapped[str] = mapped_column(String(1024))

    # remote file state at last fetch, to decide whether to re-pull.
    last_size: Mapped[int] = mapped_column(BigInteger, default=0)        # bytes
    last_mtime: Mapped[float] = mapped_column(default=0.0)               # remote st_mtime
    last_offset: Mapped[int] = mapped_column(BigInteger, default=0)      # bytes ingested (newline-aligned)
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # sha256 of the first settings.ssh_fingerprint_bytes of the file; if it changes for a given
    # remote_path the file was rotated/replaced -> re-read from 0. NULL until first computed (lazy
    # backfill). This is what makes rotation and cold-resume lossless (see design doc §5.2).
    head_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
