# log_regroup_pending.py — Dirty time-window marker for scoped Stage 2 regroup
#
#   One row records that a time range of a customer's log stream was touched by an ingest and so
#   needs (re)grouping. It is written in Stage 1 (parse → insert) — one row per ingested file/job —
#   and CONSUMED later by a scoped regroup:
#     - the web-console path consumes pending rows on an explicit POST /logs/regroup/finalize, and
#     - the directory-watcher path consumes them when its incoming queue drains empty.
#
#   Design intentions:
#   - range_start/range_end are the MIN/MAX entry timestamp of the dirtying ingest. They are NOT the
#     window that gets regrouped — finalize PADS them by log_regroup_pad_seconds (≥ the seal window)
#     so a transaction straddling the range boundary is never split (see regroup_window).
#   - Rows are coalesced at finalize time, not on write, so ingestion stays a single cheap INSERT.
#   - consumed_at = NULL means still pending; a regroup stamps it instead of deleting, leaving an
#     audit trail of what was regrouped when.

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class LogRegroupPending(Base):
    __tablename__ = "log_regroup_pending"

    # the hot query is "open pending rows for this customer" — a composite keeps it index-only.
    __table_args__ = (
        Index("ix_log_regroup_pending_customer_consumed", "customer_code", "consumed_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # tenant — every finalize is scoped to one customer, mirroring the rest of the log pipeline.
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    # the ingest job that dirtied this window. Nullable for rows written before this column existed.
    # Lets a per-upload caller answer "did MY upload still leave an open window?" (GET /logs/jobs/{id}
    # → pending_regroup) without conflating other tenants' uploads. Finalize stays tenant-wide.
    job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)
    # min/max entry timestamp of the ingest that dirtied this window (padded later at finalize).
    range_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    range_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # NULL = still pending; set to the regroup time once a scoped regroup has covered this window.
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Dead-letter tracking (see finalize_pending): a window is retried while consumed_at IS NULL AND
    # abandoned_at IS NULL. Each failed finalize attempt bumps `attempts` and records `last_error` /
    # `last_attempt_at`; once attempts reaches settings.log_regroup_max_attempts the window is
    # ABANDONED (abandoned_at set) and no longer retried — so a poison window can't retry forever.
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    abandoned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Backoff gate. A failed window is pushed into the future by the SHARED retry policy
    # (app/services/queueing/retry_policy.py), and the open-window query filters `available_at <=
    # now()`. Without this the window was retried on the very next tick, so all three attempts were
    # spent within seconds — before a transient condition (busy disk, held lock) could clear, which
    # made the retries useless. Defaults to now() so existing rows are immediately eligible.
    # NO Python-side default: available_at is written and compared by the DATABASE clock only.
    # Setting it from the app host would mean writing with one clock and reading with another, and
    # any skew (an app host a few ms ahead of the DB is routine, and containers drift more) makes a
    # freshly-written row look "not yet due" — intermittently, which is the worst kind of bug.
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()"), nullable=False)
