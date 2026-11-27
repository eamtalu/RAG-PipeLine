# preference.py - what a person or a role wants to see first (program step 2)
#
#   One row per customer, subject and area. A subject is a role ("role", "supervisor") or a
#   person ("user", their Entra object id). An area is a named group of settings with a
#   validated shape (app/services/preferences/areas.py); `settings` holds only the fields that
#   were set, so a field left out falls through to the role's value and then to the built-in
#   default. A preference never widens access: it is narrowed again every time it is read.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

SUBJECT_KINDS = ("role", "user")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Preference(Base):
    __tablename__ = "preferences"
    __table_args__ = (
        UniqueConstraint("customer_code", "subject_kind", "subject_id", "area",
                         name="uq_preferences_subject_area"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(8), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(128), nullable=False)
    area: Mapped[str] = mapped_column(String(32), nullable=False)
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict,
                                           server_default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now,
                                                 nullable=False)
    updated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
