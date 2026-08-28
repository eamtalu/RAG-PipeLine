# log_stream_frontier — the head lane's bookmark (P4, chunk 72).
#
# One row per tenant: "every line with a timestamp before `frontier_ts` has been processed by some
# lane". Both lanes advance it (greatest-wins); nothing may move it backwards, because a late
# backfill window is a normal event and must not make the head lane think history ended earlier
# than it did. The head lane's eligibility test is simply `window_lo >= frontier_ts` - anything
# behind the bookmark belongs to the rebuild lane.
#
# Deliberately tiny and unpartitioned: one row per tenant, updated in place.

from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogStreamFrontier(Base):
    __tablename__ = "log_stream_frontier"

    customer_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    frontier_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))
