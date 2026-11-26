"""One tenant, one run: read, compute off the loop, write, score, record.

Five phases, each in its own short session, because the compute in the middle runs in a thread
and a session must never span it (CLAUDE.md rule 6). The write phase holds the tenant's advisory
lock so a manual trigger and the nightly loop cannot interleave their upserts. Scoring commits
page by page (chunk 225), so no transaction stays open for long and a failed run keeps its pages.
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
#: Predictions read, scored and committed together. It bounds the memory of a page and the work
#: lost when a statement is cancelled.
SCORE_PAGE = 5000


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


def _changed_scores(rows, actuals: plan.Actuals, tz) -> list[tuple[uuid.UUID, object]]:
    """The scores one page needs written: an actual where none is stored, or where it has changed.

    (unscored, actual 90) -> written ; (scored 90, actual 90) -> left
    ; (scored 90, actual 97) -> written ; (no actual in the history read) -> left
    """
    scores = []
    for r in rows:
        actual = actuals.value(metric=r.metric, grain=r.grain, subject_kind=r.subject_kind,
                               subject=r.subject, target_at=r.target_at, tz=tz)
        if actual is not None and (r.scored_at is None or r.actual != actual):
            scores.append((r.id, actual))
    return scores


async def _score(cc: str, bundle, *, as_of_date: date, tz, cfg: plan.ForecastConfig, now: datetime) -> int:
    """Score every closed prediction of the accuracy window, then recompute the rolling scores.

    (window with 12,000 closed, 5,000 unscored) -> 5,000 ; (same night again) -> 0
    """
    actuals = plan.Actuals(bundle, as_of=as_of_date, cfg=cfg)
    window_start = as_of_date - timedelta(days=cfg.accuracy_window_days)
    since = plan.series.local_midnight(window_start, tz)
    cutoff = now - timedelta(hours=cfg.score_lag_hours)
    scored, after = 0, None
    async with async_session() as db:
        while True:
            # Step 1: Read one page of the window, scored or not, so that a day whose lines arrived
            # late is compared again; reading only unscored rows would freeze a stale score.
            rows, after = await prediction_store.scorable_page(
                db, cc, since=since, until=cutoff, tz=tz, after=after, limit=SCORE_PAGE)
            # Step 2: Write only the scores that are new or different, because rewriting the same
            # rows every night is what made the run six hours long and the table bloated.
            scores = _changed_scores(rows, actuals, tz)
            if scores:
                await prediction_store.write_scores(db, scores, scored_at=now)
            # Step 3: Commit each page, otherwise one cancelled statement late in the run would
            # roll back every score before it, as it did on 25 and 26 Nov 2026.
            await db.commit()
            scored += len(scores)
            if after is None:
                break
        # Step 4: Recompute the accuracy once, after the last page, so that it sees every score.
        await accuracy_store.recompute(db, cc, window_start=window_start, window_end=as_of_date,
                                       model_version=MODEL_VERSION, tz=tz)
        await db.commit()
    return scored


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
