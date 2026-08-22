# analytics_pending_window.py — N1's ticket: "a bounded event-time range of this tenant's
# transactions changed, go re-examine it".
#
#   Mirrors log_regroup_pending FIELD FOR FIELD, and is a SEPARATE table rather than a shared one for
#   two reasons (N1). `consumed_at` is single-consumer: a second consumer stamping it means whichever
#   runs second finds the window closed and skips work it never did. And log_regroup_pending rows are
#   written in Stage 1 per ingested file from log_entries.timestamp, so they describe INGEST ranges and
#   never cover a rebuild that no ingest triggered.
#
#   A3: provably constraint-free. No foreign key, no unique constraint a retry could violate, no
#   trigger — because this row is inserted inside the ingestion transaction, so a failed insert here
#   fails INGESTION. That is a deliberate trade: the alternative is a ticket that can be lost, and a
#   lost ticket is a window that is never re-examined and a total that is silently wrong forever.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class AnalyticsPendingWindow(Base):
    __tablename__ = "analytics_pending_windows"

    __table_args__ = (
        # The hot query is "open tickets for this tenant, due now" — a composite keeps it index-only.
        Index("ix_analytics_pending_customer_consumed", "customer_code", "consumed_at"),
        # The worker's tenant sweep: distinct customer_code over open, due rows.
        Index("ix_analytics_pending_due", "consumed_at", "abandoned_at", "available_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    # Which ingest or rebuild dirtied the window. Nullable: regroup_all has no single job.
    job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    # Event-time bounds of the transactions that changed, already padded by the publisher.
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # NULL = still open. Stamped only after the ENTIRE range has been diffed (invariant 4), so a crash
    # mid-range leaves the ticket open and the work is redone rather than skipped.
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Dead-letter tracking, same policy as the Stage 2 queue via services/queueing/retry_policy.py.
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    abandoned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Backoff gate. NO Python-side default: written and compared by the DATABASE clock only, because
    # clock skew between app host and database makes a fresh row look "not yet due" intermittently.
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()"), nullable=False)
