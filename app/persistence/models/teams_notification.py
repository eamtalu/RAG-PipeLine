# teams_notification.py - what a person is told under the Teams bell (program step 3, slice 4)
#
#   One row per thing worth telling one person, written in the same transaction as what it is
#   about (a routine's run), so it is queued exactly once. The consumer hands queued rows to the
#   edge, which speaks to Microsoft; the row then says what became of it. Kept as a ledger: it
#   is what makes "sent once" true across restarts, and what answers "was I told?".
#
#   customer_code is a real foreign key, as on the access tables.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

#: routine_ran: the person's own routine has run; routine_paused: it paused itself.
KINDS = ("routine_ran", "routine_paused")
#: queued: waiting to be handed to the edge; sent: Microsoft took it; refused: it said no (not
#: tried again); failed: three passing failures; not_configured: the edge has no Microsoft
#: secret; expired: too old to send late.
STATUSES = ("queued", "sent", "refused", "failed", "not_configured", "expired")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TeamsNotification(Base):
    __tablename__ = "teams_notifications"
    __table_args__ = (
        UniqueConstraint("kind", "subject_id", name="uq_teams_notifications_subject"),
        Index("ix_teams_notifications_queue", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    #: The Entra object id of the person told.
    object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    #: What it is about (a routine run's id); with `kind` it is told once.
    subject_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    #: The preview line; Teams shows the first 150 characters.
    text: Mapped[str] = mapped_column(String(150), nullable=False, default="", server_default="")
    #: The page of the app it opens.
    tab: Mapped[str] = mapped_column(String(32), nullable=False, default="home", server_default="home")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued", server_default="queued")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error: Mapped[str | None] = mapped_column(String(300), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
