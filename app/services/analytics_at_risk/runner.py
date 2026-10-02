"""One tenant, one pass: read the board, judge it, write it, close what has departed.

Two phases, each in its own short session (CLAUDE.md rule 6). The write phase holds the tenant's
advisory lock so the worker's minute pass and a manual `/evaluate` cannot interleave their upserts.
A failure is written to the tenant's state row and returned, never raised: the loop must go on to
the other tenants.

`profile_tenant` is the daily pass: it learns each route's rhythm from the closed rows and stamps
the state row, so the worker knows the day is done.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text

from app.config.database import async_session
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics_at_risk import (RULE_VERSION, board_store, delivery_store, profile_store, settings_store,
                                            state_store)
from app.settings import settings

logger = logging.getLogger(__name__)

LOCK_PREFIX = "analytics-at-risk:"


async def _lock(db, cc: str) -> None:
    await db.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": LOCK_PREFIX + cc})


async def _fail(cc: str, exc: Exception) -> dict:
    message = f"{type(exc).__name__}: {exc}"
    try:
        async with async_session() as db:
            await state_store.touch(db, cc, last_error=message)
            await db.commit()
    except Exception:  # the state row is the epitaph; failing to write it must not hide the first error
        logger.exception("At-risk: could not record the failure for %s", cc)
    return {"status": "failed", "error": message}


async def evaluate_tenant(cc: str, *, now: datetime | None = None) -> dict:
    """Evaluate one tenant's board as of `now`. Returns the pass's counts, or `skipped` when the tenant
    has switched the feature off, or `failed` with the error also written to the state row."""
    now = now or datetime.now(timezone.utc)
    try:
        async with async_session() as db:
            tz = ZoneInfo(await get_customer_timezone(db, cc))
            cfg = await settings_store.effective(db, cc)
            if not cfg.enabled:
                return {"status": "skipped", "reason": "disabled in settings"}
            profiles = await profile_store.latest(db, cc)
            board = await board_store.read_states(
                db, cc, now=now, tz=tz, settlement_name=settings.analytics_at_risk_settlement,
                pick_settlement=settings.analytics_at_risk_pick_settlement,
                lookup_name=settings.analytics_at_risk_pick_line_lookup,
                lookback_hours=settings.analytics_at_risk_board_lookback_hours, facts_cap=settings.analytics_at_risk_facts_cap)
        thresholds_for = profile_store.thresholds_for(profiles, cfg)
        by_number = {s.delivery_number: s for s in board.states}
        async with async_session() as db:
            await _lock(db, cc)
            stats = await delivery_store.apply(db, cc, board.states, now=now, tz=tz, thresholds_for=thresholds_for,
                                               rule_version=RULE_VERSION)
            closed = await delivery_store.close_due(db, cc, by_number, now=now, grace=timedelta(minutes=cfg.close_grace_min), tz=tz)
            swept = await delivery_store.sweep(db, cc, now=now)
            open_rows = len(await delivery_store.open_rows(db, cc))
            await state_store.touch(db, cc, last_evaluated_at=now, open_rows=open_rows, last_error=None)
            await db.commit()
        if board.overflow:
            logger.warning("At-risk %s: the facts read hit its cap of %d rows; packages or loads may be missing",
                           cc, settings.analytics_at_risk_facts_cap)
        return {"status": "completed", **stats.as_dict(), "closed": closed, "swept": swept, "open_rows": open_rows,
                "overflow": board.overflow, "unreadable_departures": board.unreadable_departures}
    except Exception as exc:
        logger.exception("At-risk evaluation for %s failed", cc)
        return await _fail(cc, exc)


async def profile_tenant(cc: str, *, as_of: date, now: datetime | None = None) -> dict:
    """Learn every route's rhythm from the closed rows ending on `as_of`, and stamp the state row."""
    now = now or datetime.now(timezone.utc)
    try:
        async with async_session() as db:
            tz = ZoneInfo(await get_customer_timezone(db, cc))
            cfg = await settings_store.effective(db, cc)
        async with async_session() as db:
            await _lock(db, cc)
            rows = await profile_store.compute(db, cc, as_of=as_of, settings=cfg, rule_version=RULE_VERSION, now=now, tz=tz)
            await state_store.touch(db, cc, last_profiled_date=as_of)
            await db.commit()
        return {"status": "completed", "as_of": as_of.isoformat(), "routes": len(rows)}
    except Exception as exc:
        logger.exception("At-risk profile for %s as of %s failed", cc, as_of)
        return await _fail(cc, exc)
