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

from sqlalchemy import String, DateTime, Date, Enum, Integer, Text, Boolean, ForeignKey, Index
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
        Index("ix_log_transactions_customer_date", "customer_code", "date"),
        Index("ix_log_transactions_customer_user", "customer_code", "user_name"),
    )

    # --- pk / lineage ---
    # id is DETERMINISTIC: uuid5 of the transaction's anchor entry hash (see derive_transactions).
    # So the same transaction keeps the same id across regroups — references stay valid.
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
    item_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
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
