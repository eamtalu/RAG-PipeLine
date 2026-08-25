# log_transaction.py — Derived Transaction (one API request/response cycle)
#
#   A log_transaction is the top-level, queryable unit for the M3 WMS log feature. One row =
#   one API request/response cycle (bracketed by Server.CommonCode.ApiLogHandler REQUEST→RESPONSE).
#   It is NOT written at ingestion time — it is *derived* (Stage 2) from log_entry rows read in
#   timestamp order, which is what lets a single transaction span multiple rotated files.
#
#   Design intentions:
#   - Promoted columns (indexed) hold the common, groupable WMS dimensions so aggregate/filter
#     questions ("count for user on date", "status of reqid X") run without touching log_entry.
#   - attributes (JSONB) is the catch-all for every other extracted request param (the long tail).
#   - flow_id is an unused nullable hook today; Phase 3 fills it to roll transactions up into
#     business flows (log_flow) without any migration churn.
#   - status is three-valued (+ incomplete) so soft M3 results don't get reported as real failures.

import enum
import uuid
from datetime import datetime, date, timezone

from sqlalchemy import (UniqueConstraint, String, DateTime, Date, Enum, Integer, Text, Boolean,
                        ForeignKey, Index, text)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogTransactionStatus(str, enum.Enum):
    success = "success"        # finished, no errors
    soft = "soft"              # M3 returned not-found/needs-value but the app coped
    error = "error"            # a real ERROR-level failure
    incomplete = "incomplete"  # REQUEST seen, RESPONSE not yet ingested (closed by a later Stage-2 pass)


# Entity
class LogTransaction(Base):
    __tablename__ = "log_transactions"

    # composite indexes for the common tenant-scoped filters (every read pins customer_code first).
    __table_args__ = (
        # Identity. NOT a PRIMARY KEY: `started_at` is the partition key and is nullable (a
        # transaction all of whose entries lack a parsable timestamp has none), and a PK would force
        # it NOT NULL. `id` leads so lookups by id alone stay an index scan.
        UniqueConstraint("id", "started_at", name="uq_log_transactions_id",
                         postgresql_nulls_not_distinct=True),
        Index("ix_log_transactions_customer_date", "customer_code", "date"),
        Index("ix_log_transactions_customer_user", "customer_code", "user_name"),
        # S1. The notification cursor reads `customer_code = ? AND updated_at >= ? AND < ? ORDER BY
        # updated_at`, so the index has to match both the filter and the sort (CLAUDE.md rule 4).
        # Before S1 that query was on `created_at` and had no composite index at all; it was fast only
        # because the feed is small and recent.
        Index("ix_log_transactions_customer_updated", "customer_code", "updated_at"),
        # S1. The sealer's own access pattern, and the reason a tick is cheap: `NOT sealed` is 2.1% of
        # rows, so a partial index is roughly fifty times smaller than a full one. Both the sealer's
        # tenant enumeration and its UPDATE filter on exactly this.
        Index("ix_log_transactions_unsealed", "customer_code", "ended_at",
              postgresql_where=text("NOT sealed")),
        # NOTE: `primary_key=True` on the id column below is the ORM's row identity ONLY. The DDL
        # SQLAlchemy would emit from it (`PRIMARY KEY (id)`) is invalid on a partitioned table and is
        # never used — Alembic builds this schema, nothing calls create_all (pinned by a test in
        # tests/test_partitioning_chunk23.py). Identity is enforced in the database by the UNIQUE
        # above. Keeping the ORM key as `id` alone is deliberate: making it (id, key) would force
        # every `db.get(Model, id)` call site to pass a tuple.
        # Range-partitioned by UTC day (see app/persistence/partitioning.py and migration
        # a1f6d70b3e92). Retention is a DROP of the day's partition rather than a DELETE + VACUUM that
        # reads the whole table.
        {"postgresql_partition_by": "RANGE (started_at)"},
    )

    # --- pk / lineage ---
    # id is DETERMINISTIC: uuid5 of the transaction's anchor entry hash (see derive_transactions).
    # So the same transaction keeps the same id across regroups — references stay valid.
    # `primary_key=True` is the ORM's row identity; the DATABASE enforces it via uq_log_transactions_id.
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    # tenant (denormalized from the job/entries) — Stage 2 stamps it; every read & the agent filter by it.
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    # sealed = no new entry can join this transaction (its end is older than the seal window), so
    # incremental Stage 2 never recomputes it. Only the unsealed "live tail" is reprocessed per cycle.
    sealed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False, index=True)
    flow_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True, index=True)  # Phase-3 hook (no FK yet)
    source_file_start: Mapped[str | None] = mapped_column(String(512), nullable=True)
    source_file_end: Mapped[str | None] = mapped_column(String(512), nullable=True)

    # --- time ---
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    date: Mapped[date | None] = mapped_column(Date, nullable=True, index=True)  # derived from started_at for cheap day grouping
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- who ---
    user_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)  # "user" is reserved in Postgres
    user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    employee_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # --- where (org) ---
    company: Mapped[str | None] = mapped_column(String(16), nullable=True)
    warehouse: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    warehouse_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    division: Mapped[str | None] = mapped_column(String(16), nullable=True)
    facility: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # --- where (device) ---
    device_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reqid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)

    # --- what ---
    method: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)        # endpoint = MethodName
    http_method: Mapped[str | None] = mapped_column(String(8), nullable=True)
    endpoint_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    transaction_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    transaction_type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)

    # --- business keys ---
    route: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # 128, not 64: the WMS can send a composite/doubled ItemNumber in the request URL (observed up to
    # 75 chars, e.g. "BEC|V1|...|521BEC|V1|...|521"). At 64 the INSERT raised StringDataRightTruncation
    # and, because Stage 2 finalize is retried from the oldest window, one such row stalled ALL
    # stitching. A generic length guard in _persist (derive_transactions) is the belt-and-suspenders
    # backstop for anything still over the limit.
    item_number: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    delivery_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    picklist_suffix: Mapped[str | None] = mapped_column(String(16), nullable=True)
    order_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    reporting_number: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- outcome ---
    status: Mapped[LogTransactionStatus] = mapped_column(
        Enum(LogTransactionStatus), default=LogTransactionStatus.incomplete, index=True
    )
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    entry_count: Mapped[int] = mapped_column(Integer, default=0)
    mi_program_count: Mapped[int] = mapped_column(Integer, default=0)

    # --- summaries ---
    request_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    response_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- catch-all: every other extracted request param ---
    attributes: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    # S1. When this row was last WRITTEN, whether by construction or by an UPDATE.
    #
    # Equal to `created_at` at birth and backfilled to it for every existing row, so a row nothing ever
    # updates behaves exactly as it did before this column existed.
    #
    # It exists because sealing became an explicit UPDATE. `created_at` is not refreshed by an UPDATE,
    # and the notification cursor only ever moves forward from its stored position — so a row sealed by
    # the sealer would never re-enter the feed and `stability.py`'s "incomplete AND sealed" alert could
    # never fire. The cursor reads THIS column instead (`notifications/cursor.py`).
    #
    # NOT NULL is load-bearing rather than tidy: the cursor's range filter and ORDER BY would silently
    # drop every NULL row from the feed, which is precisely the "nothing absorbs a skip" failure that
    # module's docstring forbids.
    #
    # S3 will make `created_at` genuinely mean "first written" by removing the delete-and-reinsert. At
    # that point this column carries the churn `created_at` carries today, and it is already the one the
    # cursor reads — which is why the cursor was moved here in S1, while the sealer was the only writer
    # of updates and the change could be observed in isolation.
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))

    # S3. The two digests that make a write conditional on the row having actually changed.
    #
    # NULLABLE, and that is the migration strategy rather than an oversight: every existing row has
    # NULL, a NULL never equals a recomputed digest, so the first pass after deploying rewrites each
    # row exactly once and fills them in. A NOT NULL column would have needed a backfill that
    # recomputed the derivation outside the pipeline that owns it.
    #
    # Deliberately NOT indexed. They are only ever read by id for a row the rebuild already has in
    # hand, never searched, and an index on a column that changes on every real write is pure cost -
    # the same reasoning that makes the seal flip a non-HOT update.
    row_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    members_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
