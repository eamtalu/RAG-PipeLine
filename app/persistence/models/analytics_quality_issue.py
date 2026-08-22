# analytics_quality_issue.py — the quarantine. One row per fact that could not be normalised.
#
#   A1: QUARANTINE MUST NEVER HALT A TENANT. Halting on one bad row freezes every metric until a human
#   intervenes, and the row that halts it is by definition the one nobody understands yet. So N2 writes
#   the problem here and the tenant continues.
#
#   That makes this table the only record that a number is incomplete. A total computed while rows sit
#   in quarantine is not wrong so much as unexplained, and the difference matters when someone asks why
#   two figures disagree. Retained a year — long enough to explain a total that was questioned months
#   later, and bounded because a permanently-broken source would otherwise grow it forever.
#
#   Deliberately NOT keyed uniquely on the source transaction: the same row can fail for different
#   reasons across rebuilds, and collapsing those would hide a source that is getting worse rather than
#   staying broken.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class AnalyticsQualityIssue(Base):
    __tablename__ = "analytics_quality_issues"

    __table_args__ = (
        Index("ix_analytics_quality_customer_detected", "customer_code", "detected_at"),
        # "Which reasons are firing, and how often" is the question an operator actually asks.
        Index("ix_analytics_quality_reason", "customer_code", "reason"),
        # Monthly: bounded at a year, and a month is the unit retention drops.
        {"postgresql_partition_by": "RANGE (detected_at)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    #: Partition key, so NOT NULL. Write time rather than event time: a row that could not be
    #: normalised may have no usable event time at all, which is often the reason it is here.
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    #: Which transaction. Both identity columns (F3), and nullable because a row can be unusable
    #: precisely because its identity could not be read.
    source_transaction_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: A short machine-readable code, so the index above can group by it. Free text would make "which
    #: reasons are firing" a full scan and a spelling lottery.
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: What was actually seen, so the issue can be understood after the raw entry is dropped at 60 days.
    #: Without this a year-old quarantine row records only that something went wrong.
    observed: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
