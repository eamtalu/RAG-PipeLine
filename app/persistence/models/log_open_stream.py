"""S4. The grouper's live state, made durable so it survives a process boundary.

`_group` has always held this in memory and thrown it away at the end of every batch, which is why
Stage 2 pads its window wide enough to re-read everything and re-derive from scratch. Persisting it is
what lets a window be narrowed to only the new entries - the whole point of S4.

TWO TABLES, because the state has two distinct shapes:

    log_open_stream       a transaction that is open and may still receive entries, keyed by the
                          (thread, user_ctx) pair that identifies its stream
    log_pending_request   a REQUEST line whose processing thread has not appeared yet, so it belongs
                          to no stream at all

DELIBERATELY NOT PARTITIONED. Both are small self-cleaning working sets - a few hundred rows, deleted
when a stream closes. Same reasoning that keeps `analytics_monthly_rollups` out of `PARTITIONED`:
nothing worth pruning, and partitioning adds planning cost for no gain. Being unpartitioned they need
no grain and no retention policy, so the partitioning tests are untouched.

BUT THEY NEED A REAPER THAT DERIVED STATE NEVER DID (section 18d). `evict_stale` closes a stream when
an ENTRY ARRIVES. A tenant that stops ingesting leaves its streams open forever and the rows leak.
Derived state cannot leak because it is rebuilt from nothing every batch; persisted state can. The TTL
sweep is required rather than optional, and `count(*)` on each table is the only signal there is - a
number that only grows is the alarm, and there is no upstream event to catch it.

The entries themselves are NOT stored here. `log_entry_assignment` already holds which entries belong
to which transaction, keyed by `transaction_id`, so a seeded builder reloads its own members. Storing
them twice would create a second copy that can disagree with the first.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogOpenStream(Base):
    """One transaction that is still open, and the stream it belongs to."""

    __tablename__ = "log_open_stream"
    __table_args__ = (
        # NULLS NOT DISTINCT is load-bearing, not tidiness. `thread` is nullable and `user_ctx` is
        # nullable, and under the default NULL-distinct rule `(NULL, 'amin')` would never conflict
        # with itself - so one stream would accumulate several rows and the lookup would be
        # non-deterministic. Failure mode 5 in the plan's table.
        UniqueConstraint("customer_code", "thread", "user_ctx",
                         name="uq_log_open_stream_key", postgresql_nulls_not_distinct=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: The stream key. Stored RAW, byte for byte as the entry carried it - failure mode 4. If the
    #: writer normalised and the reader did not (or the reverse), the lookup would miss every time and
    #: silently fall back, turning S4 into a slower version of S3 with no error anywhere.
    thread: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_ctx: Mapped[str | None] = mapped_column(String(64), nullable=True)

    #: The open transaction this stream is building. Its entries live in `log_entry_assignment`.
    transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    #: Whether a REQUEST line has already been bound. `_group` uses this to decide whether a pending
    #: GET REQUEST can still be attached once the user becomes known.
    has_request: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                              server_default="false")

    #: The newest entry timestamp in the stream. The GUARD reads this: a stream is only reusable when
    #: `last_entry_ts < lo` and `lo - last_entry_ts < log_open_gap_seconds`. Without the first half a
    #: backfilled window with an older clock would bind to a stream from the future (failure mode 2),
    #: and without the second a long quiet gap would produce one bloated transaction (mode 1).
    last_entry_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: S2's durable stream position, stored as its four comparable parts. A single opaque string would
    #: have needed parsing back into a tuple to compare, and a parse is a second place the ordering can
    #: be got wrong.
    open_ts_is_null: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                                  server_default="false")
    open_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    open_source_file: Mapped[str | None] = mapped_column(String(512), nullable=True)
    open_line_number: Mapped[int | None] = mapped_column(nullable=True)

    #: Whether this is the thread's CURRENT stream, which is what a user-less line inherits. Part of
    #: the state rather than derivable: a thread that flipped A -> B -> A has two open streams and only
    #: one of them is current.
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False,
                                             server_default="false")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))


class LogPendingRequest(Base):
    """A REQUEST line whose processing thread has not appeared yet, so it belongs to no stream."""

    __tablename__ = "log_pending_request"
    __table_args__ = (
        UniqueConstraint("customer_code", "entry_id", name="uq_log_pending_request_entry"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: The entry itself is re-read from `log_entries` by this id rather than copied here. A copy would
    #: be a second version of a row that is already append-only, and the two could disagree.
    entry_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)

    reqid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    req_user: Mapped[str | None] = mapped_column(String(64), nullable=True)
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
