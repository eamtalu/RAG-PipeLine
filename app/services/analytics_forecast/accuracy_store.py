"""Rolling out-of-sample accuracy, recomputed from scored predictions.

One GROUP BY over the scored rows in the window, upserted per series and horizon. Recomputed in
full each night rather than updated incrementally: the window slides, late-settled actuals get
re-scored, and a recomputation cannot drift from its inputs the way a running total can.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Sequence

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_forecast import AnalyticsForecastAccuracy
from app.persistence.models.analytics_ml import AnalyticsPrediction
from app.services.analytics_forecast import series

BATCH = 1000


async def recompute(db: AsyncSession, cc: str, *, window_start: date, window_end: date, model_version: str,
                    tz=timezone.utc) -> int:
    """Score every (metric, grain, kind, subject, horizon) with scored rows whose target starts in
    the window. Returns how many accuracy rows were written."""
    P = AnalyticsPrediction
    since = series.local_midnight(window_start, tz)
    until = series.local_midnight(window_end, tz)
    nonzero = P.actual != 0
    q = select(
        P.metric, P.grain, P.subject_kind, P.subject, P.horizon,
        func.count().label("n"),
        func.avg(P.abs_error).label("mae"),
        func.sum(P.abs_error).label("abs_sum"),
        func.sum(func.abs(P.actual)).label("actual_sum"),
        func.avg(case((nonzero, P.abs_error / func.abs(P.actual)), else_=None)).label("mape"),
        func.count(case((nonzero, 1), else_=None)).label("mape_n"),
        func.avg(P.value - P.actual).label("bias"),
    ).where(P.customer_code == cc, P.model_version == model_version, P.scored_at.isnot(None),
            P.target_at >= since, P.target_at <= until,
    ).group_by(P.metric, P.grain, P.subject_kind, P.subject, P.horizon)
    rows = (await db.execute(q)).mappings().all()
    now = datetime.now(timezone.utc)
    values = []
    for r in rows:
        actual_sum = Decimal(r["actual_sum"] or 0)
        values.append({
            "id": uuid.uuid4(), "customer_code": cc, "metric": r["metric"], "grain": r["grain"],
            "subject_kind": r["subject_kind"], "subject": r["subject"], "horizon": r["horizon"],
            "model_version": model_version, "window_start": window_start, "window_end": window_end,
            "n": int(r["n"]), "mae": Decimal(r["mae"] or 0),
            "wape": (Decimal(r["abs_sum"] or 0) / actual_sum) if actual_sum > 0 else None,
            "mape": Decimal(r["mape"]) if r["mape"] is not None else None, "mape_n": int(r["mape_n"] or 0),
            "bias": Decimal(r["bias"] or 0), "computed_at": now})
    for i in range(0, len(values), BATCH):
        stmt = pg_insert(AnalyticsForecastAccuracy).values(values[i:i + BATCH])
        stmt = stmt.on_conflict_do_update(
            constraint="uq_analytics_forecast_accuracy_key",
            set_={c: getattr(stmt.excluded, c) for c in ("window_start", "window_end", "n", "mae", "wape", "mape",
                                                         "mape_n", "bias", "computed_at")})
        await db.execute(stmt)
    return len(values)


async def read_series(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                      subject: str, model_version: str | None = None, limit: int = 100) -> list[AnalyticsForecastAccuracy]:
    """Every horizon's score for one series, shortest horizon first."""
    A = AnalyticsForecastAccuracy
    q = select(A).where(A.customer_code == cc, A.metric == metric, A.grain == grain,
                        A.subject_kind == subject_kind, A.subject == subject)
    if model_version is not None:
        q = q.where(A.model_version == model_version)
    rows = list((await db.execute(q.limit(limit))).scalars().all())
    rows.sort(key=lambda a: (len(a.horizon), a.horizon))
    return rows


async def read_for_subjects(db: AsyncSession, cc: str, *, metric: str, grain: str, subject_kind: str,
                            horizon: str, subjects: Sequence[str], model_version: str) -> dict[str, AnalyticsForecastAccuracy]:
    """One horizon's score for a page of subjects, by subject."""
    if not subjects:
        return {}
    A = AnalyticsForecastAccuracy
    rows = (await db.execute(select(A).where(
        A.customer_code == cc, A.metric == metric, A.grain == grain, A.subject_kind == subject_kind,
        A.horizon == horizon, A.model_version == model_version, A.subject.in_(list(subjects))))).scalars().all()
    return {a.subject: a for a in rows}
