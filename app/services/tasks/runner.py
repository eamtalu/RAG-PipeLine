"""Fires the tasks whose time has come (stage D, step 10). Every tick: claim the due rows, write
the ledger row first (so a crash or a second consumer never fires the same planned moment twice),
skip the ones made pointless (an advisory resolved, a delivery gone), re-run the attached
question with the owner's own access, deliver (the person's row, and the customer's channel for a
schedule aimed there), book the next firing; and evaluate the condition schedules against the
site's current figures. A task missed by more than the catch-up window is noted, not fired.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.task import TeamsTask, TeamsTaskRun
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.notifications.events import NotificationEvent
from app.services.tasks import specs, store
from app.services.tasks import mirror as tasks_mirror
from app.services.groups import store as groups_store
from app.services.groups.store import Route
from app.services.teams.mirror import ChangeTracker, MirrorWriter

logger = logging.getLogger(__name__)

#: Runs the task's question as its owner: (task) -> {"answer": str, "blocks": [...], "withheld": bool}
RunQuery = Callable[[TeamsTask], Awaitable[dict[str, Any]]]
#: Reads the current figures of a site for the condition schedules: (customer, site_id) -> fill rate 0..1 or None
ReadFillRate = Callable[[str, str | None], Awaitable[float | None]]


async def is_pointless(db: AsyncSession, task: TeamsTask) -> str | None:
    """Why a reminder should be skipped, from its skip_if, or None to fire it.

    (skip_if advisory a-1, resolved) -> "the advisory is resolved" ; (open) -> None ; (no skip_if) -> None
    """
    rule = task.skip_if or {}
    if rule.get("kind") == "advisory":
        try:
            key = uuid.UUID(str(rule.get("advisory_id")))
        except ValueError:
            return None
        row = await db.scalar(select(Advisory).where(Advisory.customer_code == task.customer_code, Advisory.id == key))
        if row is not None and row.status in ("resolved", "dismissed"):
            return f"the advisory is {row.status}"
        return None
    if rule.get("kind") == "delivery":
        try:
            departure = date.fromisoformat(str(rule.get("departure_date")))
        except (TypeError, ValueError):
            return None
        row = await db.scalar(select(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == task.customer_code,
            AnalyticsAtRiskDelivery.delivery_number == str(rule.get("delivery_number")),
            AnalyticsAtRiskDelivery.departure_date == departure))
        if row is not None and row.status == "closed":
            return f"delivery {row.delivery_number} has gone"
        return None
    return None


def notification_for(task: TeamsTask, text: str, blocks: list[dict], *, route: Route | None = None) -> NotificationEvent:
    """The channel message for a schedule aimed at a group (step 12: its group's channels).

    (schedule "Short products list" to group Stock on ch1) -> event schedule:<id>:<planned>, target ch1
    """
    return NotificationEvent(
        event_type="schedule", customer_code=task.customer_code, severity="info", title=task.title,
        summary=text[:2000] or None, dedup_key=f"task:{task.id}:{task.next_fire_at.isoformat() if task.next_fire_at else ''}",
        target_channel_ids=list(route.channel_ids) if route is not None else None,
        payload={"task_id": str(task.id), "owner_object_id": task.owner_object_id, "show_as": task.show_as,
                 "blocks": blocks[:4], "site_id": task.site_id,
                 "groups": list(route.group_names) if route is not None else []},
    )


async def fire(db: AsyncSession, task: TeamsTask, *, now: datetime, tz: ZoneInfo, run_query: RunQuery | None,
               enqueue, catch_up: timedelta) -> TeamsTaskRun:
    """Fire one due task: the ledger row, the skip check, the question, the delivery, the rebooking."""
    planned = task.next_fire_at or now
    run = TeamsTaskRun(id=uuid.uuid4(), task_id=task.id, customer_code=task.customer_code, planned_at=planned,
                       started_at=now)
    db.add(run)
    await db.flush()  # the unique (task, planned) row is the lock against a second firing
    try:
        # Step 1: A firing far past its time is a miss, not a late message, so a Monday restart
        # never sends Friday's list
        if now - planned > catch_up:
            run.outcome, run.text = "missed", f"missed by {int((now - planned).total_seconds() // 3600)} h"
        else:
            reason = await is_pointless(db, task)
            if reason:
                run.outcome, run.text = "skipped", f"skipped: {reason}"
            else:
                text, blocks = task.note or "", []
                if task.query_text and run_query is not None:
                    answer = await run_query(task)
                    text = str(answer.get("answer") or "")
                    blocks = list(answer.get("blocks") or [])
                run.outcome, run.text, run.blocks = "sent", text, blocks
                # Step 1b: A schedule aimed at a group goes to that group's channels; a group
                # that is gone or switched off means nothing is posted (the ledger still says sent)
                if task.kind == "schedule" and task.target == "channel" and enqueue is not None:
                    route = await groups_store.group_route(db, task.customer_code, task.target_group_id)
                    if route.channel_ids:
                        await enqueue(notification_for(task, text, blocks, route=route))
    except Exception as exc:  # noqa: BLE001 - the ledger keeps the reason
        logger.exception("task %s failed to fire", task.id)
        run.outcome, run.error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
    run.finished_at = datetime.now(timezone.utc)
    task.last_result = {"at": now.isoformat(), "outcome": run.outcome, "text": run.text,
                        "blocks": run.blocks or [], "error": run.error}
    # Step 2: Book the next firing from the planned time, not from now, so a late tick does not
    # drift the clock
    store.advance(task, fired_at=planned if planned > now - catch_up else now, tz=tz)
    await db.flush()
    return run


async def fill_rate_of(snapshot: dict | None) -> float | None:
    """The fill rate 0..1 from a site Home snapshot's rail, or None.

    >>> import asyncio
    >>> asyncio.run(fill_rate_of({"rail": {"fill_rate": "89.6%"}})), asyncio.run(fill_rate_of(None))
    (0.896, None)
    """
    if not snapshot:
        return None
    raw = str((snapshot.get("rail") or {}).get("fill_rate") or "").rstrip("%")
    try:
        return float(Decimal(raw) / 100) if raw else None
    except InvalidOperation:
        return None


async def check_conditions(db: AsyncSession, customer_code: str, *, now: datetime, read_fill_rate: ReadFillRate | None) -> int:
    """Book a firing now for every condition schedule whose figure crosses its line and whose
    cooldown has passed; how many were booked."""
    if read_fill_rate is None:
        return 0
    booked = 0
    figures: dict[str | None, float | None] = {}
    for task in await store.condition_tasks(db, customer_code):
        if task.site_id not in figures:
            figures[task.site_id] = await read_fill_rate(customer_code, task.site_id)
        if not specs.holds(task.when, figures[task.site_id]):
            continue
        cooldown = timedelta(minutes=int(task.when.get("cooldown_minutes") or 60))
        if task.last_fired_at is not None and now - task.last_fired_at < cooldown:
            continue
        task.next_fire_at = now
        task.note = (task.note or "")  # the message carries the figure through the note below
        task.last_result = {**(task.last_result or {}), "figure": figures[task.site_id]}
        booked += 1
    await db.flush()
    return booked


async def tick(mirror: MirrorWriter | None, *, run_query: RunQuery | None, enqueue=None,
               read_fill_rate: ReadFillRate | None = None, tracker: ChangeTracker | None = None,
               catch_up_hours: float = 6.0, session_factory=None, now: datetime | None = None,
               only: list[str] | None = None) -> dict[str, int]:
    """One tick: conditions, then everything due, then the rows of the owners touched.
    Fired runs per customer."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    fired: dict[str, int] = {}
    touched: set[tuple[str, str]] = set()
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                await check_conditions(db, code, now=now, read_fill_rate=read_fill_rate)
                await db.commit()
            except Exception:
                logger.exception("condition schedules of %s not checked", code)
                await db.rollback()
        # Step 1: Claim and fire inside one transaction per task, so a crash mid-way leaves the
        # ledger row and the unique constraint refuses a second attempt at that planned time
        for task in await store.claim_due(db, now=now):
            if task.customer_code not in codes:
                continue
            tz = ZoneInfo(await get_customer_timezone(db, task.customer_code))
            await fire(db, task, now=now, tz=tz, run_query=run_query, enqueue=enqueue,
                       catch_up=timedelta(hours=catch_up_hours))
            fired[task.customer_code] = fired.get(task.customer_code, 0) + 1
            touched.add((task.customer_code, task.owner_object_id))
        await db.commit()
        if mirror is not None:
            for code, owner in sorted(touched):
                try:
                    await tasks_mirror.mirror_user(db, mirror, code, owner, tracker=tracker, now=now)
                except Exception:
                    logger.exception("tasks of %s not mirrored after firing", owner)
    return fired
