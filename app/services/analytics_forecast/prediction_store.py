"""Writing and reading `analytics_predictions` and `analytics_forecast_series`.

A prediction is keyed by what it is about, how far ahead, and when it is for. Re-running the same
night upserts in place; the next night writes a NEW row for the same target at a shorter horizon.
That is the design, not duplication: it is how "how good were we a week out" can be answered.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal
from typing import Sequence

from sqlalchemy import and_, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_forecast import AnalyticsForecastSeries
from app.persistence.models.analytics_ml import AnalyticsPrediction
from app.services.analytics_forecast import series

BATCH = 1000
#: Hard cap on rows one read may return; a window of 365 days at three grains is far below it.
READ_CAP = 2000


@dataclass(frozen=True)
class PredictionRow:
    metric: str
    grain: str
    subject_kind: str
    subject: str
    horizon: str
    model_version: str
    target_at: datetime
    predicted_at: datetime
    value: Decimal
    p10: Decimal | None
    p90: Decimal | None
    detail: dict = field(default_factory=dict)
    run_id: uuid.UUID | None = None


@dataclass(frozen=True)
class SeriesRow:
    metric: str
    grain: str
    subject_kind: str
    subject: str
    classification: str
    model: str | None
    backtest_wape: Decimal | None
    history_days: int
    active_days_28d: int
    volume_28d: Decimal
    last_run_id: uuid.UUID | None


def _chunks(rows: Sequence, size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


async def upsert(db: AsyncSession, cc: str, rows: Sequence[PredictionRow]) -> int:
    """Insert or replace by the unique key. Returns how many rows were sent."""
    for chunk in _chunks(list(rows), BATCH):
        stmt = pg_insert(AnalyticsPrediction).values([{
            "id": uuid.uuid4(), "customer_code": cc, "metric": r.metric, "grain": r.grain,
            "subject_kind": r.subject_kind, "subject": r.subject, "horizon": r.horizon,
            "model_version": r.model_version, "target_at": r.target_at, "predicted_at": r.predicted_at,
            "value": r.value, "p10": r.p10, "p90": r.p90, "detail": r.detail, "run_id": r.run_id,
            "created_at": r.predicted_at} for r in chunk])
        stmt = stmt.on_conflict_do_update(
            constraint="uq_analytics_predictions_key",
            set_={"predicted_at": stmt.excluded.predicted_at, "value": stmt.excluded.value,
                  "p10": stmt.excluded.p10, "p90": stmt.excluded.p90, "detail": stmt.excluded.detail,
                  "run_id": stmt.excluded.run_id,
                  # a re-run is a new prediction: whatever it was scored against no longer applies
                  "actual": None, "abs_error": None, "scored_at": None})
        await db.execute(stmt)
    return len(rows)


async def latest_per_target(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str, subject: str,
                            start: datetime, end: datetime, horizon: str | None = None,
                            limit: int = READ_CAP) -> list[AnalyticsPrediction]:
    """One row per target in `[start, end)`: the most recently made prediction, or the one made at
    `horizon` when pinned. Walks `ix_analytics_predictions_read`; no sort over the table."""
    q = select(AnalyticsPrediction).where(
        AnalyticsPrediction.customer_code == cc, AnalyticsPrediction.metric == metric,
        AnalyticsPrediction.grain == grain, AnalyticsPrediction.subject_kind == subject_kind,
        AnalyticsPrediction.subject == subject, AnalyticsPrediction.target_at >= start,
        AnalyticsPrediction.target_at < end)
    if horizon is not None:
        q = q.where(AnalyticsPrediction.horizon == horizon)
    q = q.distinct(AnalyticsPrediction.target_at).order_by(
        AnalyticsPrediction.target_at, AnalyticsPrediction.predicted_at.desc()).limit(limit)
    return list((await db.execute(q)).scalars().all())


async def latest_for_subjects(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                              subjects: Sequence[str], start: datetime, end: datetime, horizon: str | None = None,
                              limit: int = READ_CAP) -> list[AnalyticsPrediction]:
    """`latest_per_target` for a page of subjects at once (the items list's "next bucket" column)."""
    if not subjects:
        return []
    q = select(AnalyticsPrediction).where(
        AnalyticsPrediction.customer_code == cc, AnalyticsPrediction.metric == metric,
        AnalyticsPrediction.grain == grain, AnalyticsPrediction.subject_kind == subject_kind,
        AnalyticsPrediction.subject.in_(list(subjects)), AnalyticsPrediction.target_at >= start,
        AnalyticsPrediction.target_at < end)
    if horizon is not None:
        q = q.where(AnalyticsPrediction.horizon == horizon)
    q = q.distinct(AnalyticsPrediction.subject, AnalyticsPrediction.target_at).order_by(
        AnalyticsPrediction.subject, AnalyticsPrediction.target_at, AnalyticsPrediction.predicted_at.desc()).limit(limit)
    return list((await db.execute(q)).scalars().all())


async def first_target(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                       subject: str) -> datetime | None:
    """The earliest instant this series has ever been predicted for: when scoring can first happen."""
    return await db.scalar(select(func.min(AnalyticsPrediction.target_at)).where(
        AnalyticsPrediction.customer_code == cc, AnalyticsPrediction.metric == metric,
        AnalyticsPrediction.grain == grain, AnalyticsPrediction.subject_kind == subject_kind,
        AnalyticsPrediction.subject == subject))


def bucket_close(target_at: datetime, grain: str, tz: tzinfo) -> datetime:
    """The instant the bucket starting at `target_at` is over, on the tenant's clock."""
    local = target_at.astimezone(tz)
    if grain == "hour":
        return target_at + timedelta(hours=1)
    _start, end = series.bucket_bounds(local.date(), grain)
    return series.local_midnight(end + timedelta(days=1), tz)


async def scorable(db: AsyncSession, cc: str, *, since: datetime, until: datetime, tz: tzinfo,
                   limit: int = 50000) -> list[AnalyticsPrediction]:
    """Predictions whose target bucket started at or after `since` and had CLOSED by `until`, so
    the actual is final enough to score. The read is bounded by `ix_analytics_predictions_score`;
    the close test needs the grain and the zone, so it is applied to the rows."""
    q = select(AnalyticsPrediction).where(
        AnalyticsPrediction.customer_code == cc, AnalyticsPrediction.target_at >= since,
        AnalyticsPrediction.target_at < until,
    ).order_by(AnalyticsPrediction.target_at, AnalyticsPrediction.predicted_at).limit(limit)
    rows = (await db.execute(q)).scalars().all()
    return [r for r in rows if bucket_close(r.target_at, r.grain, tz) <= until]


async def write_scores(db: AsyncSession, scores: Sequence[tuple[uuid.UUID, Decimal]], *, scored_at: datetime) -> int:
    """Set the actual, the absolute error and the scoring instant on each prediction by id."""
    for pid, actual in scores:
        await db.execute(update(AnalyticsPrediction).where(AnalyticsPrediction.id == pid).values(
            actual=actual, abs_error=func.abs(AnalyticsPrediction.value - actual), scored_at=scored_at))
    return len(scores)


async def upsert_series(db: AsyncSession, cc: str, rows: Sequence[SeriesRow], *, updated_at: datetime) -> int:
    for chunk in _chunks(list(rows), BATCH):
        stmt = pg_insert(AnalyticsForecastSeries).values([{
            "id": uuid.uuid4(), "customer_code": cc, "metric": r.metric, "grain": r.grain,
            "subject_kind": r.subject_kind, "subject": r.subject, "classification": r.classification,
            "model": r.model, "backtest_wape": r.backtest_wape, "history_days": r.history_days,
            "active_days_28d": r.active_days_28d, "volume_28d": r.volume_28d, "last_run_id": r.last_run_id,
            "updated_at": updated_at} for r in chunk])
        stmt = stmt.on_conflict_do_update(
            constraint="uq_analytics_forecast_series_key",
            set_={c: getattr(stmt.excluded, c) for c in ("classification", "model", "backtest_wape", "history_days",
                                                         "active_days_28d", "volume_28d", "last_run_id", "updated_at")})
        await db.execute(stmt)
    return len(rows)


async def series_page(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                      after: tuple[Decimal, str] | None, limit: int, q: str | None = None) -> list[AnalyticsForecastSeries]:
    """A page of series by recent volume, biggest first, keyset on `(volume_28d DESC, subject)`.
    `limit + 1` rows are returned when more exist, so the caller can say so."""
    stmt = select(AnalyticsForecastSeries).where(
        AnalyticsForecastSeries.customer_code == cc, AnalyticsForecastSeries.metric == metric,
        AnalyticsForecastSeries.grain == grain, AnalyticsForecastSeries.subject_kind == subject_kind)
    if q:
        stmt = stmt.where(AnalyticsForecastSeries.subject.ilike(f"{q}%"))
    if after is not None:
        volume, subject = after
        stmt = stmt.where(and_(AnalyticsForecastSeries.volume_28d <= volume,
                               (AnalyticsForecastSeries.volume_28d < volume) | (AnalyticsForecastSeries.subject > subject)))
    stmt = stmt.order_by(AnalyticsForecastSeries.volume_28d.desc(), AnalyticsForecastSeries.subject).limit(limit + 1)
    return list((await db.execute(stmt)).scalars().all())


async def series_one(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                     subject: str) -> AnalyticsForecastSeries | None:
    return await db.scalar(select(AnalyticsForecastSeries).where(
        AnalyticsForecastSeries.customer_code == cc, AnalyticsForecastSeries.metric == metric,
        AnalyticsForecastSeries.grain == grain, AnalyticsForecastSeries.subject_kind == subject_kind,
        AnalyticsForecastSeries.subject == subject))
