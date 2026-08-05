# log_source_object.py — the durable handoff between SSH fetching and Stage 1 parsing
#
#   One row = one contiguous byte range downloaded from a remote log file, saved to object storage,
#   and still awaiting (or having completed) Stage 1 parse+insert.
#
#   It does three jobs at once:
#     1. WORK QUEUE. The fetcher inserts the row and advances the file checkpoint in ONE transaction,
#        then walks away. A separate worker (log_parse_worker) leases the row and parses it. Without
#        this row the checkpoint would be the only record that the bytes were consumed, so a crash
#        between "checkpoint advanced" and "entries inserted" would skip those bytes forever.
#     2. PROVENANCE. log_entries.source_file is only a filename, and log_ssh_file_checkpoints is
#        OVERWRITTEN as a file advances, so neither can answer "which exact file version and byte
#        range produced this entry". This row can: path + offsets + size + mtime + fingerprint.
#     3. RETRY BUDGET. attempts/max_attempts/available_at/last_error give the fetch path the
#        dead-letter semantics Stage 2 already has on log_regroup_pending. Today a file that always
#        fails is retried EVERY poll forever, writing another ./uploads copy and another jobs row
#        each time; or, for a non-disk error, eventually auto-disables the whole SSH source.
#
#   Deliberate design choices:
#   - source_id has NO foreign key. Deleting an SSH source must never delete ingestion evidence
#     (same precedent as log_ssh_fetch_runs.source_id). source_name is denormalized so the history
#     survives a rename or delete.
#   - customer_code stays a soft tenant key, consistent with every other log table.
#   - job_id is a nullable soft reference and is TRANSITIONAL: the parse worker still creates a Job
#     and records which one, so nothing downstream changes. It disappears when `jobs` is retired
#     from the log path during the partitioning work.
#   - No UNIQUE on (source_id, remote_path, start_offset): a legitimate rotation re-read pulls
#     [0, size) again and a unique constraint would reject it. Idempotency comes from the single
#     fetch transaction, and entry_hash remains the correctness backstop.

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, Float, Index, Integer, String, Text, text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class SourceObjectStatus:
    """Queue states. Plain string constants (not a Python Enum) so the value stored in the
    varchar column is unambiguously the bare string on every SQLAlchemy/driver version.

        pending   -> waiting to be claimed (also where a transient failure returns it, with
                     available_at pushed into the future by the backoff)
        leased    -> a worker holds it; lease_expires_at recovers it if that worker dies
        ingested  -> parsed successfully; the stored file is now provably redundant
        abandoned -> dead-lettered, excluded from claiming, re-armable via the API

    There is deliberately no separate 'failed' state: a retryable failure IS 'pending' with a
    future available_at, which keeps the claim query a single predicate and leaves no state a row
    can get stuck in.
    """

    pending = "pending"
    leased = "leased"
    ingested = "ingested"
    abandoned = "abandoned"

    ALL = ("pending", "leased", "ingested", "abandoned")


# Entity
class LogSourceObject(Base):
    __tablename__ = "log_source_objects"

    __table_args__ = (
        # the hot query: "oldest due, unclaimed row" — partial so the index only covers live work.
        Index("ix_log_source_objects_claim", "available_at", "created_at",
              postgresql_where=text("status = 'pending'")),
        # operator/tenant browsing, and the queue-depth guard's count.
        Index("ix_log_source_objects_customer", "customer_code", "created_at"),
        # lease-expiry sweep.
        Index("ix_log_source_objects_lease", "lease_expires_at",
              postgresql_where=text("status = 'leased'")),
        CheckConstraint("status IN ('pending','leased','ingested','abandoned')",
                        name="ck_log_source_objects_status"),
        CheckConstraint("end_offset >= start_offset", name="ck_log_source_objects_offsets"),
        CheckConstraint("attempts >= 0", name="ck_log_source_objects_attempts"),
        CheckConstraint("max_attempts > 0", name="ck_log_source_objects_max_attempts"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # tenant — soft key, matching every other log table.
    customer_code: Mapped[str] = mapped_column(String(64), index=True)

    # --- provenance (must survive SSH source deletion) ---
    source_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)  # soft, no FK
    source_name: Mapped[str] = mapped_column(String(255))
    remote_path: Mapped[str] = mapped_column(String(1024))
    start_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    end_offset: Mapped[int] = mapped_column(BigInteger)
    observed_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    observed_mtime: Mapped[float | None] = mapped_column(Float, nullable=True)
    head_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- where the downloaded bytes live ---
    storage_key: Mapped[str] = mapped_column(String(1024))

    # --- queue state ---
    status: Mapped[str] = mapped_column(String(24), default=SourceObjectStatus.pending,
                                        server_default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    # Server-side only — see the note on LogRegroupPending.available_at. Writing this with the app
    # host's clock and comparing it with the database's makes claiming intermittently wrong.
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()"))
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- outcome ---
    job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)  # soft, transitional
    entries_inserted: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    ingested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    file_deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
