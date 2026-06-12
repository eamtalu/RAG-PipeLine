# customer_display_name.py — additional display names (usernames) for a tenant
#
#   The customers table holds ONE row per customer_code (the stable tenant slug used everywhere
#   downstream). A single tenant, however, may be known by more than one human label / username —
#   e.g. customer_code 'mnp' shown as "MNP Ops", "MNP Warehouse", ... This table is the one-to-many
#   side of that: many display names attach to one customer_code without disturbing the tenant key.
#
#   This is purely a UI/labelling concern: nothing downstream (jobs/log_entries/log_transactions)
#   references it, so adding/removing rows here never affects ingestion or querying.

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class CustomerDisplayName(Base):
    __tablename__ = "customer_display_names"
    __table_args__ = (
        # the same label can't be attached twice to the same tenant
        UniqueConstraint("customer_code", "display_name", name="uq_customer_display_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # FK to the tenant's stable slug (customers.customer_code is unique). ON DELETE CASCADE so a
    # removed tenant takes its labels with it.
    customer_code: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("customers.customer_code", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
