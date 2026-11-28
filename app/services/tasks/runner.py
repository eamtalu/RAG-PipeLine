"""Fires the tasks whose time has come (stage D, step 10). Every tick: claim the due rows, write
the ledger row first (so a crash or a second consumer never fires the same planned moment twice),
skip the ones made pointless (an advisory resolved, a delivery gone), re-run the attached
question with the owner's own access, deliver (the person's row, and the customer's channel for a
schedule aimed there), book the next firing. A task missed by more than the catch-up window is
noted, not fired. Conditions ("when fill rate drops below") are watches since step 14.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.task import TeamsTask, TeamsTaskRun
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services import access
from app.services.notifications.events import NotificationEvent
from app.services.tasks import store
from app.services.tasks import mirror as tasks_mirror
from app.services.groups import store as groups_store
from app.services.groups.store import Route
from app.services.teams import bell
from app.services.teams.mirror import ChangeTracker, MirrorWriter
from app.settings import settings

logger = logging.getLogger(__name__)

#: Runs the task's question as its owner: (task) -> {"answer": str, "blocks": [...], "withheld": bool}
RunQuery = Callable[[TeamsTask], Awaitable[dict[str, Any]]]


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
                       started_at=now, trigger="time")
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
                    run.trace_id = answer.get("trace_id")
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
    task.last_result = _last_result(run, now)
    # Step 2: Book the next firing from the planned time, not from now, so a late tick does not
    # drift the clock
    store.advance(task, fired_at=planned if planned > now - catch_up else now, tz=tz)
    # Step 3: Count failures in a row; at the limit the routine pauses itself and says why,
    # otherwise a broken question would fail at every firing for ever with nobody told
    had_reason = task.pause_reason is not None
    _count_failure(task, run, now=now)
    # Step 4: Tell the owner under their Teams bell when they asked for it: that it has run,
    # or that it paused itself. Written with the run, so it is queued exactly once.
    await _ring(db, task, run, has_just_paused=not had_reason and task.pause_reason is not None,
                now=now)
    await db.flush()
    return run


async def _ring(db: AsyncSession, task: TeamsTask, run: TeamsTaskRun, *, has_just_paused: bool,
                now: datetime) -> None:
    """Queue the bell for a routine's owner, when the routine is theirs to be told about.

    (ticked, it ran) -> "Open picks" with the answer's first line ; (it paused itself) -> the
    reason ; (not ticked) -> nothing

    The bell is the owner's own choice and independent of where the result goes: a routine
    can post its result to a group's channel and ring its owner's bell as well.
    """
    if not task.notify_bell:
        return
    if has_just_paused:
        await bell.queue(db, task.customer_code, object_id=task.owner_object_id, kind="routine_paused",
                         subject_id=str(run.id), title=task.title, text=task.pause_reason or "",
                         tab="routines", now=now)
    elif run.outcome == "sent":
        await bell.queue(db, task.customer_code, object_id=task.owner_object_id, kind="routine_ran",
                         subject_id=str(run.id), title=task.title, text=run.text or "", tab="routines",
                         now=now)


def _count_failure(task: TeamsTask, run: TeamsTaskRun, *, now: datetime) -> None:
    """Keep the count of failed runs in a row, and pause the routine when it reaches the limit.

    (2 failed, this one failed, limit 3) -> paused with the reason ; (this one sent) -> count 0 ;
    (a missed or skipped run) -> the count is left as it is
    """
    if run.outcome == "sent":
        task.failed_in_a_row = 0
        return
    if run.outcome != "failed":
        return
    task.failed_in_a_row = (task.failed_in_a_row or 0) + 1
    limit = settings.teams_tasks_pause_after_failures
    if task.failed_in_a_row >= limit and task.status == "active":
        why = (run.error or "").split(": ", 1)[-1] or "the run failed"
        store.pause_by_system(task, f"Paused after {limit} failed runs in a row: {why}", now=now)


def _last_result(run: TeamsTaskRun, now: datetime) -> dict[str, Any]:
    return {"at": now.isoformat(), "outcome": run.outcome, "text": run.text, "blocks": run.blocks or [],
            "error": run.error, "trigger": run.trigger, "note": run.note, "by_name": run.by_name}


async def run_by_hand(db: AsyncSession, task: TeamsTask, *, actor_object_id: str, actor_name: str | None,
                      now: datetime, run_query: RunQuery | None) -> TeamsTaskRun:
    """Run a routine now because the person asked (step 15): the ledger row says so, the question
    is answered as the owner, the clock is left alone (the next planned run still comes), and
    nothing is posted to a channel (the answer is theirs to read here).

    (check "short products today") -> run(trigger hand, outcome sent, the answer) ; (an action) -> run with its note
    """
    run = TeamsTaskRun(id=uuid.uuid4(), task_id=task.id, customer_code=task.customer_code, planned_at=now,
                       started_at=now, trigger="hand", by_object_id=actor_object_id, by_name=actor_name)
    db.add(run)
    await db.flush()
    try:
        text, blocks = task.note or "", []
        if task.query_text:
            if run_query is None:
                raise RuntimeError("no question runner is wired here")
            answer = await run_query(task)
            text = str(answer.get("answer") or "")
            blocks = list(answer.get("blocks") or [])
            run.trace_id = answer.get("trace_id")
        run.outcome, run.text, run.blocks = "sent", text, blocks
    except Exception as exc:  # noqa: BLE001 - the ledger keeps the reason
        logger.exception("task %s failed to run by hand", task.id)
        run.outcome, run.error = "failed", f"{type(exc).__name__}: {exc}"[:2000]
    run.finished_at = datetime.now(timezone.utc)
    task.last_result, task.updated_at = _last_result(run, now), now
    await db.flush()
    return run


async def complete(db: AsyncSession, task: TeamsTask, *, actor_object_id: str, actor_name: str | None,
                   outcome: str, note: str | None, now: datetime) -> TeamsTaskRun:
    """Record the person's own outcome of a routine (step 15): done or skipped, with their note.
    A one-off is finished by it; a repeating routine keeps its clock, the run is just logged.

    (once action, "done", "moved Dan to Route 14") -> run(trigger tick, done) and the task done ;
    (weekday check, "skipped", "bank holiday") -> run logged, the task still active
    """
    if outcome not in ("done", "skipped"):
        raise ValueError("outcome must be done or skipped")
    run = TeamsTaskRun(id=uuid.uuid4(), task_id=task.id, customer_code=task.customer_code, planned_at=now,
                       started_at=now, finished_at=now, trigger="tick", by_object_id=actor_object_id,
                       by_name=actor_name, outcome=outcome, text=(note or "").strip() or None, note=(note or "").strip() or None)
    db.add(run)
    task.last_result, task.updated_at = _last_result(run, now), now
    if task.when.get("kind") != "recurring":
        task.status, task.done_at, task.next_fire_at = "done", now, None
    await db.flush()
    return run


async def purge_once(*, retention_days: int, session_factory=None, now: datetime | None = None) -> int:
    """Drop the runs older than the retention (step 15); how many went."""
    from app.config.database import async_session

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    async with factory() as db:
        gone = await store.purge_runs_before(db, now - timedelta(days=retention_days))
        await db.commit()
    return gone



async def tick(mirror: MirrorWriter | None, *, run_query: RunQuery | None, enqueue=None,
               tracker: ChangeTracker | None = None,
               catch_up_hours: float = 6.0, session_factory=None, now: datetime | None = None,
               only: list[str] | None = None) -> dict[str, int]:
    """One tick: everything due, then the rows of the owners touched. Fired runs per customer."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    fired: dict[str, int] = {}
    touched: set[tuple[str, str]] = set()
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        # Step 1: Claim and fire inside one transaction per task, so a crash mid-way leaves the
        # ledger row and the unique constraint refuses a second attempt at that planned time
        for task in await store.claim_due(db, now=now):
            if task.customer_code not in codes:
                continue
            # A person who no longer has access gets no answers: their routine is paused once,
            # not left firing a refusal at every planned time
            owner = await access.access_record_for(db, task.customer_code, task.owner_object_id)
            if owner is None or not owner.is_active:
                store.pause_by_system(
                    task, f"Paused: {task.owner_name or 'its owner'} no longer has access", now=now)
                touched.add((task.customer_code, task.owner_object_id))
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
