# customer.py — Tenant registry (one row per customer "log space")
#
#   A customer must be created here BEFORE any log can be ingested under its code. This is the
#   source of truth for "which tenants exist": ingestion validates against an ACTIVE row, and the
#   frontend lists these rows so a user can pick which tenant's log space to view or ingest into.
#
#   customer_code is the stable slug used everywhere downstream (jobs/log_entries/log_transactions
#   carry it). display_name is the human label shown in the UI. active=false retires a tenant from
#   ingestion + selection without deleting its historical data.
#
#   Each row is one "log space", of one of two KINDS (the frontend's Switch-Logspace palette):
#     - disposable: a throwaway space owning a brand-new customer_code (1:1), created for a debugging
#       session and auto-purged at expires_at (owner_name records who created it).
#     - permanent: an admin-curated space with a human name/description and a live|test environment.
#   Permanent-only columns (name/description/environment) are NULL on disposables and vice versa.

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Boolean, DateTime, Text, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogSpaceKind(str, enum.Enum):
    """Which group a log space belongs to. Defaults to disposable (every pre-existing row is one)."""
    permanent = "permanent"
    disposable = "disposable"


class LogSpaceEnvironment(str, enum.Enum):
    """Admin-set environment of a PERMANENT space. `inactive` is NOT a value here — it is derived from
    active=false, so the palette renders an inactive permanent as INACTIVE regardless of environment."""
    live = "live"
    test = "test"


# Entity
class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), unique=True, index=True)  # stable slug
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # IANA timezone of THIS customer's log server (e.g. "Europe/London", "Europe/Berlin"). Used at
    # ingestion to localize the log's naive wall-clock into a true UTC instant (independent of the
    # ingest host's timezone), and on read to display those instants back in the customer's local time.
    # NULL = not yet configured: behaviour falls back to settings.display_timezone, but the NULL is the
    # detectable "needs attention" signal (ingestion warns; GET /customers reports timezone_set=false).
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Which kind of log space this is. NOT NULL, defaults to disposable so legacy rows + omitted-kind
    # creates keep working unchanged.
    kind: Mapped[LogSpaceKind] = mapped_column(
        Enum(LogSpaceKind), default=LogSpaceKind.disposable, server_default=LogSpaceKind.disposable.value,
        nullable=False,
    )
    # --- permanent-only (NULL on disposables) ---
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)  # human name of a permanent space
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    environment: Mapped[LogSpaceEnvironment | None] = mapped_column(Enum(LogSpaceEnvironment), nullable=True)
    # --- disposable-only (NULL on permanents) ---
    owner_name: Mapped[str | None] = mapped_column(String(128), nullable=True)  # who created the disposable
    # When the disposable auto-expires. NULL = never expires (legacy / omitted-kind creates); the
    # cleanup worker only purges rows with a non-NULL expires_at that is due.
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)

    #: Whether this tenant's notification rules run at all, and whether its queued alerts go out.
    #: Separate from a RULE's own status: this is the subsystem switch for the tenant, and both must
    #: be on. It replaced a single deployment-wide env flag that was read once at process boot, which
    #: no UI could ever have controlled.
    #:
    #: Defaults FALSE, and deliberately so — every existing tenant acquires this column at once, and a
    #: default of true would start alerting for people who never asked for it.
    notifications_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
