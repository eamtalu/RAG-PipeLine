# idempotency_key.py — server-side Idempotency-Key store (best-practice request de-duplication).
#
#   A client sends an `Idempotency-Key` header on a mutating POST. IdempotencyMiddleware records the
#   key here, runs the handler once, and caches its response. A retry / double-submit carrying the
#   SAME key replays the stored response instead of re-running the side effect. Scope is per tenant
#   (`customer_code`) so keys from different logspaces never collide.
#
#   Lifecycle: row inserted `in_progress` (this is the atomic guard — UNIQUE(customer_code, idem_key)
#   means a concurrent duplicate loses the insert race), then updated to `completed` with the captured
#   response. `expires_at` bounds retention so the table self-cleans (TTL sweep).

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, Integer, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class IdempotencyStatus(str, enum.Enum):
    in_progress = "in_progress"  # first request is running; a duplicate now gets 409
    completed = "completed"      # response captured; a duplicate now gets the replayed response


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    __table_args__ = (
        # the atomic de-dup guard: one row per (tenant, key). A concurrent duplicate INSERT loses.
        UniqueConstraint("customer_code", "idem_key", name="uq_idempotency_customer_key"),
        Index("ix_idempotency_expires_at", "expires_at"),  # cheap TTL sweep
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True)  # tenant scope (X-Customer-Code)
    idem_key: Mapped[str] = mapped_column(String(255))                  # client-supplied Idempotency-Key
    method: Mapped[str] = mapped_column(String(8))                      # operation scope (guards key reuse)
    path: Mapped[str] = mapped_column(String(512))
    # sha256 hex of method|path|body — a key reused with a DIFFERENT request is a client bug (-> 422).
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default=IdempotencyStatus.in_progress.value)

    # captured response (NULL until completed). Only JSON, non-5xx responses are cached.
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_body: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
