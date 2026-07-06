# logspace_presence.py — who is currently "in" a log space
#
#   The Switch-Logspace palette shows presence for a space: the people currently viewing/debugging it.
#   This is a lightweight, self-declared, ephemeral signal (not auth): a user opening a space upserts a
#   row with their name (and an optional note like "debugging 15656"); leaving removes it. Rows are
#   swept after a TTL by the cleanup worker (and filtered out on read) so a crashed/closed client
#   doesn't linger forever.
#
#   De-duped by (customer_code, name): re-opening a space refreshes `since` + `note` rather than
#   stacking duplicate rows for the same person.

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogspacePresence(Base):
    __tablename__ = "logspace_presence"
    __table_args__ = (
        # one presence row per person per space; a repeat open upserts onto this key.
        UniqueConstraint("customer_code", "name", name="uq_logspace_presence"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # FK to the tenant's stable slug (customers.customer_code is unique). ON DELETE CASCADE so purging a
    # log space takes its presence rows with it.
    customer_code: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("customers.customer_code", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)  # who is present (self-declared)
    note: Mapped[str | None] = mapped_column(String(256), nullable=True)  # optional, e.g. "debugging 15656"
    # server-set on insert AND refreshed on every upsert — the "freshness" clock the TTL sweep uses.
    since: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False,
    )
