# log_entry_assignment.py — which transaction currently owns each raw entry
#
#   One row means: "entry E currently belongs to transaction T at position N".
#
#   Why this table exists. Stage 2 used to write the grouping result back onto log_entries
#   (transaction_id / seq), and clear it again through an ON DELETE SET NULL cascade. Because
#   transaction_id was indexed, every rewrite touched the heap AND the index, and the unsealed tail is
#   regrouped repeatedly before it seals. Measured on production 2026-08-05:
#
#       log_entries: n_tup_upd = 105,838,123   n_tup_hot_upd = 162   -> 0.0% HOT
#
#   ~55 rewrites per row. That is the write amplification, dead-tuple churn and vacuum pressure behind
#   the outage.
#
#   Separating the CURRENT INTERPRETATION from the RAW EVIDENCE fixes it: log_entries becomes
#   insert-only, and the churn moves here — to a small table whose whole purpose is to be replaced.
#
#   Design choices:
#   - entry_id is the PRIMARY KEY, so "at most one current assignment per entry" is a database
#     guarantee rather than a convention the writer has to remember.
#   - BOTH foreign keys CASCADE. Deleting a transaction drops its assignments (what ON DELETE SET NULL
#     used to do, without touching the raw rows); deleting an entry drops its assignment, which keeps
#     the existing purge path working unchanged: jobs -> entries -> assignments.
#   - customer_code is denormalized and stays a SOFT tenant key, consistent with every other log
#     table. It lets tenant-scoped cleanup and consistency checks avoid a join.
#   - There is deliberately no foreign key to an SSH source: deleting a source must never cascade
#     into ingestion evidence.

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class LogEntryAssignment(Base):
    __tablename__ = "log_entry_assignment"

    __table_args__ = (
        # the hot read: "give me this transaction's entries, in order" — index-only for the ids.
        Index("ix_log_entry_assignment_txn", "transaction_id", "seq"),
        # tenant-scoped cleanup and consistency checks.
        Index("ix_log_entry_assignment_customer", "customer_code"),
    )

    # PK, not just a FK: one current assignment per entry, enforced by the database.
    entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_entries.id", ondelete="CASCADE"), primary_key=True)
    transaction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("log_transactions.id", ondelete="CASCADE"), nullable=False)
    # position within the transaction, 0-based, in stream order.
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    # soft tenant key — denormalized so tenant cleanup needs no join.
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    # clock_timestamp(), not now(): now() is transaction_timestamp(), so every row written in one
    # regroup transaction would share an identical stamp and lose the ordering information.
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()"), nullable=False)
