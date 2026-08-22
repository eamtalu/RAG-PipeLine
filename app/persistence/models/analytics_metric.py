# analytics_metric.py — the metric registry (N4). What is measured, as DATA rather than code.
#
#   This table is the centre of the design, not an extension. The user chooses what is measured and how
#   it is sliced from the interface, and the measure list is not fixed now — so nothing about dimensions
#   or measures may be hardcoded into a rollup schema. A new metric is a ROW plus a backfill, never a
#   migration.
#
#   Shape follows NotificationRule, which already proved the pattern in this codebase: a JSONB body for
#   the parts that vary, promoted columns for the parts every query filters on, and a
#   draft/active/inactive lifecycle.
#
#   The in-memory twin is `app/services/analytics/definition.py` (MetricDefinition), which owns the
#   validation N4 specifies. Keeping the rules there rather than in a CHECK constraint is deliberate:
#   the interface has to show a user ALL the problems with a definition at once, not fail on the first.

import enum
import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import Date, DateTime, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class MetricStatus(str, enum.Enum):
    """N4's lifecycle. A definition cannot go ACTIVE until its backfill has run, or its chart shows a
    false start date — no history, drawn as though there were none to have."""

    draft = "draft"
    active = "active"
    inactive = "inactive"


# Entity
class AnalyticsMetric(Base):
    __tablename__ = "analytics_metrics"

    __table_args__ = (
        # A tenant cannot have two definitions with the same name: the name is what the interface and
        # the agent tools address a metric by.
        UniqueConstraint("customer_code", "name", name="uq_analytics_metrics_name"),
        # The worker's per-cycle question: which definitions are active for this tenant.
        Index("ix_analytics_metrics_customer_status", "customer_code", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- the definition itself, as data ---
    # JSONB rather than columns, because these are exactly the parts that must not be hardcoded. A user
    # inventing "average duration of ConfirmPickLine by warehouse" writes a row; nothing dispatches on
    # a metric's name anywhere, and no migration is involved.
    #: Fact-row field names to group by. Validated against contract.FACT_FIELDS: a dimension nobody
    #: writes produces a chart that is silently EMPTY rather than an error.
    dimensions: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")
    #: One entry per measure: name, aggregation, field, and which classifications contribute. Each
    #: declares the additive ROLES its rollup rows carry, so a finished answer is never storable.
    measures: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")
    #: Row filter, notably the method allow-list. A quantity measure may only be registered where the
    #: methods actually carry one, and only 3 of 49 do.
    filter: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    #: Which grains to maintain: hourly, daily, weekly, monthly. Weekly has no table — ISO Monday weeks
    #: derive from daily at read time, because a week is not a partition boundary anywhere.
    grains: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")

    # --- lifecycle ---
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default=MetricStatus.draft.value, server_default="draft")
    #: How far back its history has been built. NULL = never backfilled, which is what keeps it out of
    #: `active` and what the interface must show, because a newly defined metric has NO history until
    #: its backfill runs.
    backfilled_through: Mapped[date_type | None] = mapped_column(Date, nullable=True)
    #: Who defined it. There is no authentication in this codebase yet (api/deps.py is a permit-all
    #: placeholder with a TODO), so this is recorded now to be attributable later rather than
    #: retrofitted onto rows that have no author.
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))
