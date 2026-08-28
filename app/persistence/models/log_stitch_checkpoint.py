# log_stitch_checkpoint — the head lane's per-tenant checkpoint (P4, chunk 72).
#
# One row per tenant: "every line with a timestamp before `stitched_through` has been processed by
# some lane". The same idea as the SSH file checkpoints one layer down: processed-up-to-here,
# advance-only, and safe to lose (the head lane simply falls back to the rebuild lane until it is
# re-established). Both lanes advance it (greatest-wins); nothing may move it backwards, because a
# late backfill window is a normal event and must not make the head lane think history ended
# earlier than it did. The head lane's eligibility test is simply `window_lo >= stitched_through` -
# anything behind the checkpoint belongs to the rebuild lane.
#
# Deliberately tiny and unpartitioned: one row per tenant, updated in place.

from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class LogStitchCheckpoint(Base):
    __tablename__ = "log_stitch_checkpoint"

    customer_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    stitched_through: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))
