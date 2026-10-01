"""The demand forecast's own three tables: runs, series summaries, and rolling accuracy.

The predictions themselves live in `analytics_predictions` (M1, `analytics_ml.py`), extended with
the metric, grain, quantiles and the actual that scores them. These tables sit around that one:

- a RUN is one nightly (or manual) pass for one tenant, with what it read and what it wrote;
- a SERIES row is the latest word on one forecasted thing (which model, how it classifies, how it
  backtested), so the items list is one indexed page and not a scan of the predictions;
- an ACCURACY row is the rolling out-of-sample score for one series at one horizon, recomputed
  from the scored predictions each night, so a reader never aggregates on the request path.

All three are tenant-scoped by `customer_code` (soft reference, as everywhere in analytics) and
reference each other by UUID without foreign keys, matching Subsystem 8.
"""

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import Date, DateTime, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

RUN_STATUSES = ("queued", "running", "completed", "skipped", "failed")
RUN_TRIGGERS = ("nightly", "manual")
CLASSIFICATIONS = ("smooth", "intermittent", "lumpy", "erratic", "insufficient")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AnalyticsForecastRun(Base):
    """One forecasting pass for one tenant."""

    __tablename__ = "analytics_forecast_runs"
    __table_args__ = (
        Index("ix_analytics_forecast_runs_latest", "customer_code", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    trigger: Mapped[str] = mapped_column(String(16), nullable=False, default="nightly")
    #: The last tenant-local day whose rows the run learned from. Daily targets start the day after.
    as_of_date: Mapped[date_type] = mapped_column(Date, nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    points_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    scored: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: History bounds, the ramp trimmed, series counts, and the warnings a reader should see.
    detail: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)


class AnalyticsForecastSeries(Base):
    """The latest word on one forecasted series: how it classifies and which model it got."""

    __tablename__ = "analytics_forecast_series"
    __table_args__ = (
        UniqueConstraint("customer_code", "metric", "grain", "subject_kind", "subject",
                         name="uq_analytics_forecast_series_key"),
        # The items page: "top N by recent volume" for one (metric, grain, kind) is an index walk.
        Index("ix_analytics_forecast_series_volume", "customer_code", "metric", "grain", "subject_kind",
              "volume_28d", "subject"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    grain: Mapped[str] = mapped_column(String(8), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    subject: Mapped[str] = mapped_column(String(128), nullable=False)
    classification: Mapped[str] = mapped_column(String(16), nullable=False)
    #: The winning candidate, or None when the series was too thin to forecast.
    model: Mapped[str | None] = mapped_column(String(32), nullable=True)
    backtest_wape: Mapped[object | None] = mapped_column(Numeric(10, 6), nullable=True)
    history_days: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_days_28d: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    volume_28d: Mapped[object] = mapped_column(Numeric(20, 6), nullable=False, default=0)
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)


class AnalyticsForecastAccuracy(Base):
    """Rolling out-of-sample accuracy for one series at one horizon."""

    __tablename__ = "analytics_forecast_accuracy"
    __table_args__ = (
        UniqueConstraint("customer_code", "metric", "grain", "subject_kind", "subject", "horizon", "model_version",
                         name="uq_analytics_forecast_accuracy_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    grain: Mapped[str] = mapped_column(String(8), nullable=False)
    subject_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    subject: Mapped[str] = mapped_column(String(128), nullable=False)
    horizon: Mapped[str] = mapped_column(String(16), nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    window_start: Mapped[date_type] = mapped_column(Date, nullable=False)
    window_end: Mapped[date_type] = mapped_column(Date, nullable=False)
    n: Mapped[int] = mapped_column(Integer, nullable=False)
    mae: Mapped[object] = mapped_column(Numeric(20, 6), nullable=False)
    #: Sum of absolute errors over sum of actuals. None when every actual in the window was zero.
    wape: Mapped[object | None] = mapped_column(Numeric(10, 6), nullable=True)
    #: Mean |error|/actual over the non-zero actuals only, and how many of those there were.
    mape: Mapped[object | None] = mapped_column(Numeric(10, 6), nullable=True)
    mape_n: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    #: Mean of predicted minus actual: positive means the forecast runs high.
    bias: Mapped[object] = mapped_column(Numeric(20, 6), nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_now)
