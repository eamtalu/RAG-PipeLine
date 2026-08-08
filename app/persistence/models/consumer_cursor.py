"""Where each incremental reader of `log_transactions` has got to.

One row per consumer, holding a single published number: "everything strictly before this position is
consumed". `position` is a `log_transactions.created_at` — write time, not event time — matching what
every incremental reader cursors on.

This exists for retention, not for the consumers themselves. The partition worker drops days past 60
and already refuses to drop a day Stage 2 has not stitched; a slow READER needs the same protection,
or dropping day 70 while ML is at day 70 destroys that data permanently and ML silently skips it. You
cannot gate on something you cannot see, and you cannot answer "is anything behind?" without one place
to ask.

Deliberately separate from a subsystem's INTERNAL cursors. `notification_rules.cursor_at` tracks each
rule independently, because one rule being replayed must not drag another's position. What it
publishes here is the minimum across those rules — the oldest data the subsystem as a whole still
needs. Internal progress and the external contract are different things and change for different
reasons.

`updated_at` is a heartbeat, and is what separates "behind" from "gone". Without it a consumer that
died three weeks ago is indistinguishable from one that is merely slow, and it would hold retention
hostage forever — filling the disk, which is a worse failure than the data loss it was preventing.
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class ConsumerCursor(Base):
    __tablename__ = "consumer_cursors"

    #: Stable identifier for the reader, e.g. "notifications" or "ml:features-v1". The primary key,
    #: because a consumer has exactly one position — appending would make this a log nobody prunes.
    consumer: Mapped[str] = mapped_column(String(128), primary_key=True)

    #: Everything with a `log_transactions.created_at` STRICTLY BEFORE this has been consumed.
    position: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    #: Heartbeat. A consumer that stops refreshing this is treated as gone rather than slow, so it
    #: cannot block retention indefinitely.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), nullable=False)
