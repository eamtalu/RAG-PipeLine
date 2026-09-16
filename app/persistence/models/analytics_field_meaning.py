"""Chunk 108: what a field MEANS, recorded once per name.

Meaning used to live on `analytics_field_registry`, whose rows are per field PER METHOD. That is
right for a decision (`EmployeeName` may be ticked on picking and not on counting) and wrong for a
meaning: `EmployeeName` means the same thing on all 44 methods that carry it, so describing it meant
typing the same sentence 44 times. Nobody did. Measured before this chunk: 1,492 registry rows on the
live tenant, 0 described, 0 with a unit.

Moving meaning to the NAME turns 1,492 decisions into 176, of which roughly twenty are numbers.

`kind` is the part no amount of looking at the data can supply. Discovery run over the live facts
classified `ItemNumber`, `DeliveryNumber`, `LotNumber`, `UserID` and `DeviceID` as measures: they are
numeric and they repeat, exactly as a quantity does. A delivery number is a name spelled with digits
and nothing in its values says so. One person saying so, once, is what stops an agent adding delivery
numbers together and calling the answer picked quantity.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

#: What a field IS, once somebody has said so. None means nobody has yet, which is not the same as
#: "noise" and must not be shown as though it were.
#:
#: Chunk 109 added `level`, the distinction no amount of looking at the values can supply. A measure
#: is how much HAPPENED and adding two of them is the point; a level is how much there IS at a moment
#: and adding two of them produces a number nothing ever was. Measured on the live tenant: eight
#: on-hand readings of item 104353 add to 41,206 where 427 are on the shelf, and every on-hand reading
#: on the tenant adds to 340,206 where the stock is 18,248 (tmp-live, 16 September 2026; the figures
#: drift as facts arrive, the order of magnitude does not). `level` sits beside `measure` because that
#: is the pair a person confuses.
KINDS = ("measure", "level", "slice", "noise")


class AnalyticsFieldMeaning(Base):
    """One row per field NAME per tenant. Documentation that happens to live in a database."""

    __tablename__ = "analytics_field_meanings"
    __table_args__ = (
        UniqueConstraint("customer_code", "field", name="uq_analytics_field_meanings_field"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: The namespaced name, exactly as it appears in `attributes`: `QuantityPicked`,
    #: `resp.CustomerName`, `rec.STQT`. The same string a metric names after `attr:`.
    field: Mapped[str] = mapped_column(String(128), nullable=False)

    #: A sentence somebody who has never seen the warehouse could read.
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: What the number is in: units, ms, kg. Meaningless for a slice, and left null there.
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    #: `measure`, `level`, `slice` or `noise`. NULL until a person decides, and absent is not "noise".
    kind: Mapped[str | None] = mapped_column(String(16), nullable=True)

    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))
