"""R4. One row per M3 record, for transactions whose `expand` switch is ticked.

A SEPARATE TABLE, and that was the open decision from 18a
--------------------------------------------------------
The alternative was a second row type inside `analytics_facts`. It was measured rather than argued
about: `_read_dirty_facts` selects the whole table with no grain predicate, and `group_fold` has no
notion of grain, so record rows fold into the SAME buckets as their parent transaction. Feeding the
seed definition one transaction plus three of its records inflated the quantity total from 10 to 40 -
**4x, silently**.

Avoiding that would need EVERY definition to carry a grain filter, and forgetting one on any single
definition produces a plausible-looking wrong total. A separate table makes the mistake structurally
impossible instead: the existing fold cannot see these rows at all.

The cost 18a named for this option was "doubles the fold path". That is real and it is the reason the
record-grain fold is NOT built yet - see the module note at the bottom.

Why it is opt-in, with the arithmetic
-------------------------------------
Measured on the deployed database: 3,641,353 records, around 200k a day. At that rate KEEP_FOREVER is
roughly 365M rows over five years, which is why expansion is per transaction rather than global - the
volume is chosen by whoever ticks the switch, not inherited.

Measured on the development database this was built against: 8,614 records in the whole retained
window, average 2.3 per `mi_result` entry, maximum 26. The two numbers are far apart and the smaller
one is NOT evidence that the cost is small - it is evidence that this box holds much less data.

KEEP_FOREVER, matching `analytics_facts`, because the reason to capture a record at all is a question
somebody asks next year and raw entries are gone in 60 days. `_LOUD_EXPANSION` in the capture path
makes a careless tick visible in the log immediately rather than in a disk alert three weeks later.
"""

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import DateTime, Date, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AnalyticsRecordFact(Base):
    """One M3 record from one transaction's `mi_result.records[]`."""

    __tablename__ = "analytics_record_facts"
    __table_args__ = (
        # Identity is (transaction, which record). Deterministic, so a re-expansion of the same
        # transaction replaces rather than duplicates - the same property that makes the transaction
        # grain idempotent. `event_time` is in the key because it is the partition column and is
        # nullable, exactly as on `analytics_facts`.
        UniqueConstraint("source_transaction_id", "record_index", "event_time",
                         name="uq_analytics_record_facts_id", postgresql_nulls_not_distinct=True),
        {"postgresql_partition_by": "RANGE (event_time)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    #: The transaction this record came from. A soft reference rather than a foreign key, matching
    #: `analytics_facts`: the fact tables outlive `log_transactions` by design, so an enforced FK would
    #: make retention's partition drop fail.
    source_transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: Position within `records[]`. Part of the key, because two records of one transaction are
    #: genuinely different observations and can be identical in every field.
    record_index: Mapped[int] = mapped_column(Integer, nullable=False)

    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    business_date: Mapped[date_type | None] = mapped_column(Date, nullable=True, index=True)

    #: Denormalised from the parent so a record metric can filter without a join - the same reasoning
    #: that puts them on `analytics_facts`.
    method: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    transaction_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    mi_program: Mapped[str | None] = mapped_column(String(64), nullable=True)
    mi_transaction: Mapped[str | None] = mapped_column(String(64), nullable=True)

    #: The record's APPROVED scalars, namespaced `rec.` so they can never collide with the transaction
    #: grain's `resp.` or `mi.` keys, and read back as `attr:rec.STQT`.
    attributes: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
                                                 default=lambda: datetime.now(timezone.utc))
