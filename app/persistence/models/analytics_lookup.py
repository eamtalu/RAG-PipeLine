"""Chunk 100: the declared lookups, and the key-to-attribute values harvested for them.

Two tables, because they answer two different questions and change at completely different rates. A
DECLARATION is a handful of rows a person edits. A VALUE is one row per key, per attribute, per period
it was true, filled by the fold as a by-product of a read it already does.

Neither holds anything about a fact. That is deliberate and it is the whole design: the rollup stores
the KEY (a delivery number) and the attribute (a customer name) is resolved when somebody reads. Copying
the value onto every fact would need this map anyway, would make each new lookup a rewrite of history,
and would break the fold's source-fingerprint skip. The reasoning, with its measurements, is at the top
of `app/services/analytics/lookup.py`.

Size, measured on tmp-live: 114 delivery keys, 539 item keys, 123 order keys over two days. The table
grows with distinct ENTITIES, not with records, which is why it stays small while the facts do not.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (BigInteger, Boolean, DateTime, Index, String, Text, UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsLookup(Base):
    """One declared key-to-attributes relationship. Configuration, never code."""

    __tablename__ = "analytics_lookups"
    __table_args__ = (
        UniqueConstraint("customer_code", "name", name="uq_analytics_lookups_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: Addressed as `lookup:<name>.<attribute>`, so it may not contain a dot.
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    #: The field on the FACT that holds the key: a plain column (`delivery_number`) or an `attr:` path.
    #: This is the dimension a rollup must carry for the lookup to be answerable from the stored tier.
    key_field: Mapped[str] = mapped_column(String(128), nullable=False)

    #: `[{name, stable, on_conflict, sources: [{method, key_field, value_field}]}]`.
    #: JSON rather than two more tables because it is read whole, written whole, and never queried
    #: into - the same reasoning that keeps a metric's `measures` a JSON document.
    attributes: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")

    #: Off means declared but not harvested and not offered as a grouping. A lookup is never deleted
    #: on the way to being switched off, because its harvested values are history.
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True,
                                          server_default="true")

    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))


class AnalyticsLookupValue(Base):
    """One value of one attribute of one key, and the period it was true for."""

    __tablename__ = "analytics_lookup_values"
    __table_args__ = (
        UniqueConstraint("customer_code", "lookup", "key", "attribute", "valid_from",
                         name="uq_analytics_lookup_values_key"),
        #: The read path asks for a handful of keys of one lookup at a time, never for a scan.
        Index("ix_analytics_lookup_values_lookup_key", "customer_code", "lookup", "key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    lookup: Mapped[str] = mapped_column(String(64), nullable=False)
    key: Mapped[str] = mapped_column(String(128), nullable=False)
    attribute: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[str] = mapped_column(String(512), nullable=False)

    #: A key's FIRST value is valid from `lookup.BEGINNING`, not from when it was observed. Without
    #: that, a delivery named after its picks would leave those picks permanently unattributed.
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    #: NULL while current. A later, different value closes the previous period and opens its own.
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: `observed` (inferred from traffic) or `imported` (loaded from a master file). Imported beats
    #: observed. Only `observed` is written today; the column exists so the other needs no migration.
    origin: Mapped[str] = mapped_column(String(16), nullable=False, default="observed",
                                        server_default="observed")
    #: Which method said it, so a wrong value can be traced back to where it came from.
    source_method: Mapped[str | None] = mapped_column(String(128), nullable=True)

    observations: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1,
                                              server_default="1")
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
