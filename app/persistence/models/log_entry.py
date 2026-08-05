# log_entry.py — Raw Log Entry (one timestamped entry; the lossless source of truth)
#
#   A log_entry is one *timestamped* entry from the M3 WMS log. Because most entries are
#   multi-line (LogAPICall/LogAPIResult/stored-proc/REQUEST BODY bodies continue until the next
#   timestamped line), one row = one logical entry, not one physical line.
#
#   Design intentions:
#   - Written in Stage 1 (parse → insert), per file, idempotently. This table is the source of
#     truth; everything else (log_transactions) is derived from it and can be re-computed anytime.
#   - timestamp is the stream-ordering key Stage 2 uses to merge files and stitch transactions.
#   - transaction_id is set later by Stage 2 (NULL until grouping runs, or for orphan entries
#     before the first REQUEST). SUPERSEDED: the current assignment now lives in
#     log_entry_assignment, which is what makes this table insert-only.
#   - Promoted columns (entry_type, mi_program, mi_transaction, result_status, record_count) make
#     the line-level questions cheap; fields (JSONB) holds parsed Inputs/Outputs/Records and
#     raw_body keeps the literal text.

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Enum, Integer, Text, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogEntryType(str, enum.Enum):
    request = "request"
    request_body = "request_body"
    mi_call = "mi_call"
    mi_result = "mi_result"
    sql = "sql"
    response = "response"
    info = "info"
    error = "error"


# Entity
class LogEntry(Base):
    __tablename__ = "log_entries"

    # content dedup is scoped PER CUSTOMER: two customers can legitimately emit an identical line
    # (e.g. a health-check at the same millisecond), so the dedup key is (customer_code, entry_hash),
    # not entry_hash alone — otherwise the second customer's line would be silently dropped.
    __table_args__ = (
        UniqueConstraint("customer_code", "entry_hash", name="uq_log_entries_customer_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # LEGACY, superseded by log_entry_assignment. The FK was ON DELETE SET NULL, which meant every
    # Stage 2 window delete UPDATED every entry that pointed at those transactions — the implicit half
    # of the write amplification (105.8M updates, 0.0% HOT in production). The constraint is dropped
    # in migration d5b830e14f72 so the raw table can be insert-only; the column stays only until the
    # partitioning pass, which rewrites the table anyway. Nothing reads it.
    transaction_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True, index=True
    )
    # FK kept (ON DELETE CASCADE); its index was dropped as unused/damaged — see migration
    # e2a9c7b41d68. Cascade-deletes of a job now seq-scan (rare/admin-only).
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))
    # tenant (denormalized from the job) — Stage 2 grouping partitions on this and every read filters
    # by it, so thread ids (e.g. [94]) can never cross-stitch between customers.
    customer_code: Mapped[str] = mapped_column(String(64), index=True)

    # --- content dedup key: sha256(raw_body). Unique per (customer_code, entry_hash) so the same log
    #     line is never stored twice for a customer, no matter how many times its file is (re)ingested
    #     or which rotated file it appears in. ---
    # standalone index dropped (unused — the UNIQUE(customer_code, entry_hash) covers dedup); see migration e2a9c7b41d68
    entry_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- provenance / ordering ---
    source_file: Mapped[str] = mapped_column(String(512))
    line_number: Mapped[int | None] = mapped_column(Integer, nullable=True)  # physical line where the entry starts
    seq: Mapped[int | None] = mapped_column(Integer, nullable=True)          # order within the transaction (set by Stage 2)
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # --- log line fields ---
    level: Mapped[str | None] = mapped_column(String(8), nullable=True)
    # thread id from the log header (e.g. "[68]"). NOT a request id, but Stage 2 uses it to
    # demultiplex concurrent requests: one request's internal MI work stays on one thread (~98%),
    # so it's a reliable correlation key where the async REQUEST/RESPONSE bracket lines hop threads.
    thread: Mapped[str | None] = mapped_column(String(16), nullable=True)  # index dropped (unused); see migration e2a9c7b41d68
    # log4net context user from the header prefix "(CPRICE)". Present on EVERY line — including the
    # async RESPONSE line, which has no user in its payload and no ReqID. Stage 2 uses it to attach
    # a response to the oldest open request *for that same user*, so a response can never cross users.
    user_ctx: Mapped[str | None] = mapped_column(String(64), nullable=True)  # index dropped (unused); see migration e2a9c7b41d68
    logger: Mapped[str | None] = mapped_column(String(256), nullable=True)
    method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    entry_type: Mapped[LogEntryType] = mapped_column(Enum(LogEntryType), default=LogEntryType.info)  # index dropped (unused); see migration e2a9c7b41d68

    # --- M3 MI call promoted fields ---
    mi_program: Mapped[str | None] = mapped_column(String(32), nullable=True)      # e.g. MMS200MI (index dropped; migration e2a9c7b41d68)
    mi_transaction: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)  # e.g. LstItmAltUnitMs
    result_status: Mapped[str | None] = mapped_column(Text, nullable=True)                     # "OK" / soft-error text
    record_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- content ---
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    fields: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
