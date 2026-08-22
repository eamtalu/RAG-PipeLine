# analytics_tenant_state.py — one row per tenant, read by the polled status card (F5).
#
#   EXACTLY ONE ROW PER TENANT, AND EXACTLY ONE READ. The browser polls the status endpoint every 2
#   seconds per tab, across four gunicorn workers. The original design computed counts over several
#   tables on each poll; F5's fix is that the worker writes every field it needs into this single row
#   each cycle, so the endpoint is one indexed lookup. A test asserts the endpoint issues one query.
#
#   FRESHNESS NEEDS TWO NUMBERS, NOT ONE (F4). The obvious one — how far behind the projection analytics
#   is — is not enough, because records are not final for 1.7 hours on average. A screen could truthfully
#   say "updated 2 seconds ago" about a number still due to move. So:
#
#     copy freshness  analytics watermark vs source watermark: am I behind?
#     settledness     share of contributing records still unsealed, and the age of the oldest
#
#   A window with unsealed contributors reads as PROVISIONAL, not stale. Those are different words for
#   the user and different actions for an operator, which is why both are stored rather than derived
#   from one.

import uuid
from datetime import datetime, timezone

from sqlalchemy import (BigInteger, DateTime, Integer, Numeric, String, Text, UniqueConstraint)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class AnalyticsTenantState(Base):
    __tablename__ = "analytics_tenant_state"

    __table_args__ = (
        # One row per tenant, enforced. Two rows would make the "exactly one read" contract a lie and
        # the card would show whichever the planner happened to return.
        UniqueConstraint("customer_code", name="uq_analytics_tenant_state_customer"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # --- copy freshness: am I behind the projection? ---
    #: Newest event_time this tenant has folded.
    analytics_watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: Newest started_at the projection holds, as observed at the same moment. Stored rather than read
    #: live so both halves of the comparison come from ONE snapshot; two reads would show a lag that is
    #: really just the gap between them.
    source_watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: The EARLIEST event_time this tenant has folded, so the interface can say where history begins.
    #:
    #: Needed because there is no backfill (correction D8): analytics counts from switch-on, and the
    #: period before that must be LABELLED rather than drawn as zero, since an empty chart reads as "no
    #: activity" when the truth is "not measured". The first implementation reported the analytics
    #: WATERMARK for this, which is the newest folded instant -- so the notice claimed there was no
    #: history before a moment the chart was already plotting data at. It contradicted the chart
    #: directly beneath it.
    #:
    #: Kept as a column rather than computed on read because F5 requires the status endpoint to be
    #: exactly ONE row read, and `min(event_time)` over a 13M-row fact table is not that.
    #:
    #: Moves BACKWARD only, mirroring how `analytics_watermark` moves forward only: folding an older
    #: range legitimately extends history into the past, which is exactly what a late backfill does.
    history_starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    #: F6's retention frontier for THIS tenant: the maximum `log_transactions.created_at` among rows it
    #: has fully processed. A WRITE time, not an event time, matching what `consumer_cursors` positions
    #: mean everywhere else in the codebase (see `NotificationRule.cursor_at`).
    #:
    #: Stored PER TENANT because `consumer_cursors` holds ONE row for the whole consumer, and retention
    #: is global. Publishing each tenant's own frontier into that single row would let a tenant that is
    #: far ahead advance the position past a tenant that is far behind, and the partition worker would
    #: then drop source data the lagging tenant had never read. The published value is the MINIMUM of
    #: this column across tenants -- the same shape `consumer_cursors.notifications_position` already
    #: uses over `NotificationRule.cursor_at`.
    source_write_frontier: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True)

    # --- settledness: is what I folded still due to move? ---
    #: Share of contributing records in the last folded window that were still unsealed, 0..1. NUMERIC
    #: rather than a computed percentage, so the interface chooses the wording.
    unsealed_share: Mapped[object | None] = mapped_column(Numeric(6, 5), nullable=True)
    #: Age of the oldest unsealed contributor. A large value means provisional numbers will keep moving
    #: for a while yet, which is a different message from "briefly provisional".
    oldest_unsealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # --- what the card shows without touching another table (F5) ---
    open_tickets: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    abandoned_tickets: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    quarantined_rows: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    facts_total: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")

    #: A5: ONE authoritative revision per tenant, bumped in the same commit as the work it describes.
    #: Cache validation keys off it, so a revision that moved without the data (or vice versa) would
    #: serve a stale chart that looks fresh.
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")

    #: Last successful cycle, and why the last one failed if it did. Quarantine never halts a tenant
    #: (A1), so a tenant can be simultaneously progressing and reporting an error.
    last_cycle_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))
