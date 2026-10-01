"""One tenant, one run: read, compute off the loop, write, score, record.

Five phases, each in its own short session, because the compute in the middle runs in a thread
and a session must never span it (CLAUDE.md rule 6). The write phase holds the tenant's advisory
lock so a manual trigger and the nightly loop cannot interleave their upserts.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.config.database import async_session
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import settle_reads
from app.services.analytics_forecast import MODEL_VERSION, accuracy_store, history_store, plan, prediction_store, run_store
from app.settings import settings

logger = logging.getLogger(__name__)

LOCK_PREFIX = "analytics-forecast:"


def config_from_settings(cc: str) -> plan.ForecastConfig:
    override = (settings.analytics_forecast_history_start or {}).get(cc)
    return plan.ForecastConfig(
        settlement=settings.analytics_forecast_settlement, units_value=settings.analytics_forecast_units_value,
        history_days=settings.analytics_forecast_history_days, min_history_days=settings.analytics_forecast_min_history_days,
        min_days_ets=settings.analytics_forecast_min_days_ets,
        item_daily_min_active_days=settings.analytics_forecast_item_daily_min_active_days,
        max_items=settings.analytics_forecast_max_items, backtest_folds=settings.analytics_forecast_backtest_folds,
        score_lag_hours=settings.analytics_forecast_score_lag_hours,
        accuracy_window_days=settings.analytics_forecast_accuracy_window_days,
        staffing_buffer_pct=settings.analytics_forecast_staffing_buffer_pct,
        horizon_days=settings.analytics_forecast_horizon_days, horizon_weeks=settings.analytics_forecast_horizon_weeks,
        horizon_months=settings.analytics_forecast_horizon_months, heatmap_days=settings.analytics_forecast_heatmap_days,
        history_start=date.fromisoformat(override) if override else None)


async def _begin(cc: str, as_of_date: date, trigger: str, run_id: uuid.UUID | None) -> uuid.UUID | None:
    async with async_session() as db:
        if run_id is None:
            run_id = (await run_store.create(db, cc, as_of_date=as_of_date, trigger=trigger,
                                             model_version=MODEL_VERSION)).id
            await db.commit()
        claimed = await run_store.claim(db, run_id)
        await db.commit()
    return run_id if claimed else None


async def _read(cc: str, as_of_date: date, cfg: plan.ForecastConfig):
    async with async_session() as db:
        tz = ZoneInfo(await get_customer_timezone(db, cc))
        await settle_reads.declared(db, cc, cfg.settlement)  # raises UnknownSettlement, which fails the run
        start = as_of_date - timedelta(days=cfg.history_days - 1)
        bundle = await history_store.read_history(db, cc, cfg.settlement, start=start, end=as_of_date, tz=tz,
                                                  units_value=cfg.units_value, item_cap=cfg.max_items * cfg.history_days)
    return tz, bundle


async def _write(cc: str, out: plan.RunOutput, *, predicted_at: datetime) -> int:
    async with async_session() as db:
        await db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": LOCK_PREFIX + cc})
        written = await prediction_store.upsert(db, cc, out.predictions)
        await prediction_store.upsert_series(db, cc, out.series, updated_at=predicted_at)
        await db.commit()
    return written


async def _score(cc: str, bundle, *, as_of_date: date, tz, cfg: plan.ForecastConfig, now: datetime) -> int:
    """Fill the actual on every prediction whose bucket has closed, then recompute the rolling scores."""
    actuals = plan.Actuals(bundle, as_of=as_of_date, cfg=cfg)
    window_start = as_of_date - timedelta(days=cfg.accuracy_window_days)
    cutoff = now - timedelta(hours=cfg.score_lag_hours)
    async with async_session() as db:
        rows = await prediction_store.scorable(db, cc, since=plan.series.local_midnight(window_start, tz),
                                               until=cutoff, tz=tz)
        scores = []
        for r in rows:
            actual = actuals.value(metric=r.metric, grain=r.grain, subject_kind=r.subject_kind, subject=r.subject,
                                   target_at=r.target_at, tz=tz)
            if actual is not None:
                scores.append((r.id, actual))
        await prediction_store.write_scores(db, scores, scored_at=now)
        await accuracy_store.recompute(db, cc, window_start=window_start, window_end=as_of_date,
                                       model_version=MODEL_VERSION, tz=tz)
        await db.commit()
    return len(scores)


async def _finish(run_id: uuid.UUID, **fields) -> None:
    async with async_session() as db:
        await run_store.finish(db, run_id, **fields)
        await db.commit()


async def run_tenant(cc: str, *, as_of_date: date, trigger: str, run_id: uuid.UUID | None = None,
                     cfg: plan.ForecastConfig | None = None, now: datetime | None = None) -> dict:
    """Forecast one tenant as of `as_of_date` (the last day learned from). Returns a summary; the run
    row carries the same. Never raises for a data problem: those become `skipped` or `failed`."""
    cfg = cfg or config_from_settings(cc)
    now = now or datetime.now(timezone.utc)
    run_id = await _begin(cc, as_of_date, trigger, run_id)
    if run_id is None:
        return {"status": "not_claimed"}
    try:
        tz, bundle = await _read(cc, as_of_date, cfg)
        try:
            out = await asyncio.to_thread(plan.run_all, bundle, as_of=as_of_date, tz=tz, cfg=cfg,
                                          model_version=MODEL_VERSION, run_id=run_id, predicted_at=now)
        except plan.InsufficientHistory as exc:
            detail = {"history": {"start": bundle.start.isoformat(), "end": as_of_date.isoformat()}, "reason": str(exc)}
            await _finish(run_id, status="skipped", error=str(exc), detail=detail)
            return {"status": "skipped", "reason": str(exc), "run_id": str(run_id)}
        written = await _write(cc, out, predicted_at=now)
        scored = await _score(cc, bundle, as_of_date=as_of_date, tz=tz, cfg=cfg, now=now)
        detail = {"history": out.history, "series": out.counts, "warnings": out.warnings,
                  "predictions": written, "scored": scored}
        await _finish(run_id, status="completed", points_written=written, scored=scored, detail=detail)
        return {"status": "completed", "run_id": str(run_id), "points_written": written, "scored": scored,
                "warnings": out.warnings}
    except Exception as exc:  # a failed run is recorded, not raised: the loop must go on to other tenants
        logger.exception("Forecast run %s for %s failed", run_id, cc)
        await _finish(run_id, status="failed", error=f"{type(exc).__name__}: {exc}")
        return {"status": "failed", "run_id": str(run_id), "error": str(exc)}
