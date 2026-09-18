"""Chunk 117: settlements, and the one-row-per-key tables they produce.

Two tables, for the same reason the lookups have two. A DEFINITION is a handful of rows a person
edits: which method, which key, what to carry, what to settle and how. A SETTLED ROW is one row per
distinct key, recomputed by the fold whenever a call for that key arrives.

Why this exists is measured at the top of `app/services/analytics/settle.py`: a pick-list release is
confirmed in several calls and its expected quantity is stamped on every one, so nothing folded from
the calls can add it up correctly. Release 540551 picked 9 and summed to 17; across 6,160 releases the
shortfall read -5,576 summed every call and -4,344 settled.

A settled row deliberately has the SHAPE of a fact row: the same typed columns where the settlement
carries them, and one attribute bag holding everything else. That is what lets it be grouped by
warehouse, item, delivery or lot with the machinery that already groups facts, and lets the delivery
lookup turn its delivery number into a customer name exactly as it does for a fact.

Size: one row per release, never per call. On tmp-live that is 6,160 rows against 6,292 calls today
and grows with releases, roughly two thousand a day.
"""

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsSettlement(Base):
    """One declared way of turning many call rows into one row per key. Configuration, never code."""

    __tablename__ = "analytics_settlements"
    __table_args__ = (
        UniqueConstraint("customer_code", "name", name="uq_analytics_settlements_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: Addressed as a source, `settled:<name>`, so it may not contain a colon or a dot.
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    #: `{reads: [...], key: [...], carry: [...], values: [{name, rule, field, statuses, only, left,
    #: right, op, right_value}]}`. One document, read whole and written whole, for the same reason a
    #: metric's `measures` is.
    definition: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")

    #: Off means declared but not maintained and not offered. Settled rows are kept: they are history.
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))


class AnalyticsSettledRow(Base):
    """One row per key of one settlement. Recompute-and-replace: a new call for the key rewrites it."""

    __tablename__ = "analytics_settled_rows"
    __table_args__ = (
        UniqueConstraint("customer_code", "settlement", "key", name="uq_analytics_settled_rows_key"),
        # The two things a reader groups and windows by.
        Index("ix_analytics_settled_rows_when", "customer_code", "settlement", "event_time"),
        Index("ix_analytics_settled_rows_delivery", "customer_code", "settlement", "delivery_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    settlement: Mapped[str] = mapped_column(String(64), nullable=False)

    #: The key's parts joined with a unit separator, so a composite key is one string and the unique
    #: constraint holds. `key_parts` keeps them apart for reading.
    key: Mapped[str] = mapped_column(String(512), nullable=False)
    key_parts: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")

    #: When the key was first seen, so a settled row is placed in time like a fact row. A release
    #: counts in the hour it began.
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    business_date: Mapped[date_type | None] = mapped_column(Date, nullable=True)

    # --- the fact row's typed columns, filled where the settlement carries them ---
    method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    transaction_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    warehouse: Mapped[str | None] = mapped_column(String(64), nullable=True)
    item_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    delivery_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lot_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    #: Every carried field and every settled value, by the names the settlement gives them. A number
    #: is stored as a string exactly as fact attributes are, so the same coercion reads both.
    attributes: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")

    #: How many calls stood behind this row when it was last settled.
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    settled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
