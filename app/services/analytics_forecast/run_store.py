"""The run ledger: one row per forecasting pass, claimed by exactly one worker."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_forecast import AnalyticsForecastRun

#: A `queued` or `running` row older than this is a dead process, not a run in progress. A run takes
#: seconds; fifteen minutes is generous. The live incident: two restarts a minute apart killed a
#: worker mid-run and the row blocked the tenant until this window passed.
STALE_AFTER = timedelta(minutes=15)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def create(db: AsyncSession, cc: str, *, as_of_date: date, trigger: str, model_version: str) -> AnalyticsForecastRun:
    run = AnalyticsForecastRun(customer_code=cc, status="queued", trigger=trigger, as_of_date=as_of_date,
                               model_version=model_version)
    db.add(run)
    await db.flush()
    return run


async def claim(db: AsyncSession, run_id: uuid.UUID) -> bool:
    """Move `queued` to `running`. Exactly one caller gets True, whatever the race."""
    result = await db.execute(update(AnalyticsForecastRun).where(
        AnalyticsForecastRun.id == run_id, AnalyticsForecastRun.status == "queued",
    ).values(status="running", started_at=_now()))
    return result.rowcount == 1


async def finish(db: AsyncSession, run_id: uuid.UUID, *, status: str, points_written: int = 0, scored: int = 0,
                 error: str | None = None, detail: dict | None = None) -> None:
    values = {"status": status, "finished_at": _now(), "points_written": points_written, "scored": scored,
              "error": error}
    if detail is not None:
        values["detail"] = detail
    await db.execute(update(AnalyticsForecastRun).where(AnalyticsForecastRun.id == run_id).values(**values))


async def sweep_stale(db: AsyncSession, *, now: datetime | None = None, older_than: timedelta = STALE_AFTER) -> int:
    """Mark every `queued` or `running` run older than the stale window as failed. Called at the
    start of each worker pass, because a process that dies mid-run cannot write its own epitaph."""
    cutoff = (now or _now()) - older_than
    result = await db.execute(update(AnalyticsForecastRun).where(
        AnalyticsForecastRun.status.in_(("queued", "running")), AnalyticsForecastRun.created_at < cutoff,
    ).values(status="failed", finished_at=_now(),
             error=f"orphaned: no process finished this run within {int(older_than.total_seconds() // 60)} minutes"))
    return result.rowcount or 0


async def get(db: AsyncSession, cc: str, run_id: uuid.UUID) -> AnalyticsForecastRun | None:
    return await db.scalar(select(AnalyticsForecastRun).where(
        AnalyticsForecastRun.id == run_id, AnalyticsForecastRun.customer_code == cc))


async def running(db: AsyncSession, cc: str, *, stale_after: timedelta = STALE_AFTER) -> AnalyticsForecastRun | None:
    """The run in progress for this tenant, ignoring one that has been `running` for too long."""
    return await db.scalar(select(AnalyticsForecastRun).where(
        AnalyticsForecastRun.customer_code == cc, AnalyticsForecastRun.status.in_(("queued", "running")),
        AnalyticsForecastRun.created_at >= _now() - stale_after,
    ).order_by(AnalyticsForecastRun.created_at.desc()).limit(1))


async def latest(db: AsyncSession, cc: str) -> AnalyticsForecastRun | None:
    return await db.scalar(select(AnalyticsForecastRun).where(AnalyticsForecastRun.customer_code == cc)
                           .order_by(AnalyticsForecastRun.created_at.desc()).limit(1))


async def latest_completed(db: AsyncSession, cc: str) -> AnalyticsForecastRun | None:
    return await db.scalar(select(AnalyticsForecastRun).where(
        AnalyticsForecastRun.customer_code == cc, AnalyticsForecastRun.status == "completed",
    ).order_by(AnalyticsForecastRun.created_at.desc()).limit(1))


async def done_for(db: AsyncSession, cc: str, as_of_date: date) -> bool:
    """Whether a completed or skipped run already exists for this as-of day."""
    row = await db.scalar(select(AnalyticsForecastRun.id).where(
        AnalyticsForecastRun.customer_code == cc, AnalyticsForecastRun.as_of_date == as_of_date,
        AnalyticsForecastRun.status.in_(("completed", "skipped"))).limit(1))
    return row is not None


async def list_runs(db: AsyncSession, cc: str, *, limit: int) -> list[AnalyticsForecastRun]:
    return list((await db.execute(select(AnalyticsForecastRun).where(AnalyticsForecastRun.customer_code == cc)
                                  .order_by(AnalyticsForecastRun.created_at.desc()).limit(limit))).scalars().all())
