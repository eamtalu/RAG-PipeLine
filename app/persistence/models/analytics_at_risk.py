"""Deliveries at risk: five tables around one question, "which deliveries are behind their van".

- a DELIVERY row is the current word on one delivery for one departure: what is picked and loaded,
  which tier it sits in, the thresholds it was judged against, and once the van has gone, how it
  ended. Written by the at-risk worker, except the five `check*` columns, which the API writes when
  a person marks the delivery as checked. The two writers touch disjoint columns.
- a CHECK row is one acknowledgement action, append-only, so "who looked at this and when" survives
  the delivery row being rewritten every minute.
- a ROUTE PROFILE row is what one route's history taught on one day: the lead nine in ten loaded
  deliveries kept before departure. One row per day, so drift is visible.
- a SETTINGS row is the tenant's floors and knobs. Absent means the defaults.
- a TENANT STATE row is one read for `/status`.

All five are tenant-scoped by `customer_code` (soft reference, as everywhere in analytics) and
reference each other by UUID without foreign keys, matching Subsystem 8.
"""

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import Boolean, Date, DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

TIERS = ("none", "watch", "at_risk", "late")
STATUSES = ("open", "closed")
#: `picked_*` are the outcomes of a route without a loading step, where the last pick decides.
OUTCOMES = ("loaded_in_time", "loaded_late", "never_loaded", "picked_in_time", "picked_late", "unknown")
CHECK_ACTIONS = ("checked", "unchecked", "reopened")
THRESHOLD_SOURCES = ("learned", "floor")

#: The floors a tenant starts with. A floor is the LEAST lead a delivery may have before it is
#: flagged; the learned lead only ever raises it. 120 minutes to load and 180 to pick are below every
#: lead the live routes kept over 14 days (the tightest was 201), so on day one they flag only the
#: genuinely late, and once a route has learned its own rhythm they stop mattering.
DEFAULT_LOAD_FLOOR_MIN = 120
DEFAULT_PICK_FLOOR_MIN = 180
DEFAULT_MIN_SAMPLE = 20
DEFAULT_WINDOW_DAYS = 28
DEFAULT_CLOSE_GRACE_MIN = 180
DEFAULT_COVERAGE = "0.900"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AnalyticsAtRiskDelivery(Base):
    """The current word on one delivery for one departure."""

    __tablename__ = "analytics_at_risk_deliveries"
    __table_args__ = (
        UniqueConstraint("customer_code", "delivery_number", "departure_date", name="uq_analytics_at_risk_deliveries_key"),
        # The board and the history: one tenant, a day or a range of days, optionally one tier.
        Index("ix_analytics_at_risk_deliveries_board", "customer_code", "departure_date", "tier"),
        # The close sweep: the open rows whose departure has passed.
        Index("ix_analytics_at_risk_deliveries_open", "customer_code", "departure_at",
              postgresql_where=text("status = 'open'")),
        # The checked view, newest first.
        Index("ix_analytics_at_risk_deliveries_checked", "customer_code", "checked_at",
              postgresql_where=text("checked_at IS NOT NULL")),
        # The profile aggregate: one route's closed deliveries over a window of days.
        Index("ix_analytics_at_risk_deliveries_route", "customer_code", "route", "departure_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    delivery_number: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Tenant-local date of the departure, part of the key. When the WMS moves a departure the open
    #: row is updated in place, so one delivery stays one row.
    departure_date: Mapped[date_type] = mapped_column(Date, nullable=False)
    departure_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    route: Mapped[str | None] = mapped_column(String(32), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    customer_number: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- tier ---
    tier: Mapped[str] = mapped_column(String(16), nullable=False, default="none", server_default="none")
    max_tier: Mapped[str] = mapped_column(String(16), nullable=False, default="none", server_default="none")
    first_flagged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_flagged_tier: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: `[{tier, at, minutes_to_departure, threshold_min, threshold_source}]`, appended on every change.
    tier_history: Mapped[list] = mapped_column(JSONB, nullable=False, default=list, server_default="[]")
    load_threshold_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    load_threshold_source: Mapped[str | None] = mapped_column(String(16), nullable=True)
    pick_threshold_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    pick_threshold_source: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # --- progress ---
    #: From the pick-line lookup. NULL when the lookup never saw this delivery's lines; never zero for unknown.
    lines_expected: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lines_confirmed: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    lines_picked: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    lines_short: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    packages_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    packages_loaded: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_pick_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_load_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: Whether the route has a loading step. False for routes that never scan a load (the BRILA runs);
    #: the last pick then decides the tier and the outcome.
    loading_expected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    # --- outcome ---
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", server_default="open")
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str | None] = mapped_column(String(24), nullable=True)
    #: Departure minus the last load, in minutes; negative when the last package went on after the van should have left.
    outcome_lead_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)

    # --- acknowledgement (API-written) ---
    checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    checked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    check_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: The tier the delivery sat in when it was checked. A later, higher tier re-opens it.
    checked_tier: Mapped[str | None] = mapped_column(String(16), nullable=True)
    reopened_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")

    rule_version: Mapped[str] = mapped_column(String(32), nullable=False)
    last_evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)


class AnalyticsAtRiskCheck(Base):
    """One acknowledgement action. Append-only."""

    __tablename__ = "analytics_at_risk_checks"
    __table_args__ = (
        Index("ix_analytics_at_risk_checks_when", "customer_code", "at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    #: Soft reference to `analytics_at_risk_deliveries.id`.
    delivery_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    delivery_number: Mapped[str] = mapped_column(String(64), nullable=False)
    departure_date: Mapped[date_type] = mapped_column(Date, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    tier: Mapped[str] = mapped_column(String(16), nullable=False)
    #: Who: the web app's self-declared logspace name, or the Teams display name. Text, never authority.
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)


class AnalyticsAtRiskRouteProfile(Base):
    """What one route's closed deliveries taught on one day."""

    __tablename__ = "analytics_at_risk_route_profiles"
    __table_args__ = (
        UniqueConstraint("customer_code", "route", "as_of_date", name="uq_analytics_at_risk_route_profiles_key"),
        Index("ix_analytics_at_risk_route_profiles_latest", "customer_code", "route", "as_of_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    route: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_date: Mapped[date_type] = mapped_column(Date, nullable=False)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False)
    #: Closed deliveries in the window, and those that were loaded at all.
    sample: Mapped[int] = mapped_column(Integer, nullable=False)
    loaded_sample: Mapped[int] = mapped_column(Integer, nullable=False)
    load_lead_p50: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    load_lead_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    pick_lead_p50: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    pick_lead_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    #: The coverage quantiles. NULL below the sample floor, so the floor alone applies.
    learned_load_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    learned_pick_min: Mapped[object | None] = mapped_column(Numeric(10, 2), nullable=True)
    #: Informative: the departure time (HHMM) most of the route's deliveries carried.
    departure_time_mode: Mapped[str | None] = mapped_column(String(4), nullable=True)
    coverage: Mapped[object] = mapped_column(Numeric(4, 3), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(32), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)


class AnalyticsAtRiskSettings(Base):
    """The tenant's floors and knobs. One row, or none for the defaults."""

    __tablename__ = "analytics_at_risk_settings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    load_floor_min: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_LOAD_FLOOR_MIN,
                                                server_default=str(DEFAULT_LOAD_FLOOR_MIN))
    pick_floor_min: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_PICK_FLOOR_MIN,
                                                server_default=str(DEFAULT_PICK_FLOOR_MIN))
    min_sample: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_MIN_SAMPLE,
                                            server_default=str(DEFAULT_MIN_SAMPLE))
    window_days: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_WINDOW_DAYS,
                                             server_default=str(DEFAULT_WINDOW_DAYS))
    close_grace_min: Mapped[int] = mapped_column(Integer, nullable=False, default=DEFAULT_CLOSE_GRACE_MIN,
                                                 server_default=str(DEFAULT_CLOSE_GRACE_MIN))
    coverage: Mapped[object] = mapped_column(Numeric(4, 3), nullable=False, default=DEFAULT_COVERAGE,
                                             server_default=DEFAULT_COVERAGE)
    updated_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)


class AnalyticsAtRiskTenantState(Base):
    """One read for `/status`."""

    __tablename__ = "analytics_at_risk_tenant_state"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_profiled_date: Mapped[date_type | None] = mapped_column(Date, nullable=True)
    open_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now, onupdate=_now)
