"""The teams_tasks table (stage D, step 10): making a task, listing a person's, claiming the ones
due, moving one on after a firing, and the changes a person makes (pause, resume, run now, done,
stop, delete, edit). Used by runner.py, commands.py and mirror.py.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.task import SHOW_AS, TARGETS, TASK_KINDS, TeamsTask, TeamsTaskRun
from app.services.tasks import specs

ACTIONS = ("pause", "resume", "run_now", "done", "stop", "delete", "edit")
EDITABLE = ("title", "note", "when", "query_text", "skip_if", "target", "show_as", "until", "site_id", "target_group_id")


async def create(db: AsyncSession, customer_code: str, *, owner_object_id: str, owner_name: str | None,
                 kind: str, title: str, when: dict[str, Any], tz: ZoneInfo, now: datetime,
                 note: str | None = None, site_id: str | None = None, query_text: str | None = None,
                 skip_if: dict | None = None, target: str = "me", show_as: str = "table",
                 until: datetime | None = None, created_via: str = "tab", source: dict | None = None,
                 target_group_id=None) -> TeamsTask:
    """A new task with its first firing booked; ValueError for a spec or field that is wrong.

    (reminder "Check the 9 short lines", once 17:45) -> TeamsTask(next_fire_at 17:45 UTC)
    """
    # Step 1: Refuse a task that could never fire or names a target that does not exist, so the
    # tab never lists something the loop will ignore
    if kind not in TASK_KINDS:
        raise ValueError("kind must be reminder or schedule")
    if not title.strip():
        raise ValueError("a task needs a title")
    if target not in TARGETS or show_as not in SHOW_AS:
        raise ValueError("target must be me or channel; show_as table, chart or summary")
    spec = specs.validate(when)
    first = specs.next_fire(spec, now, tz)
    if first is None:
        raise ValueError("that time has already passed")
    if kind == "schedule" and not (query_text or "").strip():
        raise ValueError("a schedule needs the question it runs")
    task = TeamsTask(id=uuid.uuid4(), customer_code=customer_code, owner_object_id=owner_object_id,
                     owner_name=owner_name, kind=kind, title=title.strip()[:200], note=(note or None),
                     site_id=site_id, when=spec, status="active", next_fire_at=first,
                     query_text=(query_text or "").strip() or None, skip_if=skip_if, target=target,
                     show_as=show_as, until=until, created_via=created_via, source=dict(source or {}),
                     target_group_id=_uuid_or_none(target_group_id), created_at=now, updated_at=now)
    db.add(task)
    await db.flush()
    return task


async def get(db: AsyncSession, customer_code: str, task_id: str) -> TeamsTask | None:
    try:
        key = uuid.UUID(str(task_id))
    except ValueError:
        return None
    return await db.scalar(select(TeamsTask).where(TeamsTask.customer_code == customer_code, TeamsTask.id == key))


async def list_for_owner(db: AsyncSession, customer_code: str, owner_object_id: str, *,
                         limit: int = 200) -> list[TeamsTask]:
    """One person's tasks, newest first, every status (the tab filters)."""
    return list((await db.execute(
        select(TeamsTask).where(TeamsTask.customer_code == customer_code,
                                TeamsTask.owner_object_id == owner_object_id)
        .order_by(TeamsTask.created_at.desc()).limit(limit))).scalars())


async def claim_due(db: AsyncSession, *, now: datetime, limit: int = 50) -> list[TeamsTask]:
    """The active tasks whose time has come, locked for this transaction so a second consumer
    cannot fire them too (SKIP LOCKED); oldest due first."""
    return list((await db.execute(
        select(TeamsTask).where(TeamsTask.status == "active", TeamsTask.next_fire_at.is_not(None),
                                TeamsTask.next_fire_at <= now)
        .order_by(TeamsTask.next_fire_at).limit(limit).with_for_update(skip_locked=True))).scalars())



def advance(task: TeamsTask, *, fired_at: datetime, tz: ZoneInfo) -> None:
    """Book the next firing after one, or close the task when there is none (a once reminder is
    done, a once schedule or one past its end date is stopped).

    (reminder once) -> status done ; (schedule weekdays) -> next_fire_at next weekday
    """
    task.last_fired_at, task.fire_count, task.updated_at = fired_at, task.fire_count + 1, fired_at
    following = specs.next_fire(task.when, fired_at, tz)
    if task.until is not None and following is not None and following > task.until:
        following = None
    if following is None:
        task.status = "done" if task.kind == "reminder" else "stopped"
        task.done_at = fired_at
        task.next_fire_at = None
        return
    task.next_fire_at = following


def apply(task: TeamsTask, action: str, fields: dict[str, Any], *, now: datetime, tz: ZoneInfo) -> str:
    """One change a person makes to their task; the line that says what happened.

    (active schedule, "pause") -> "paused · <title>" ; ("run_now") -> next_fire_at now ;
    ("edit", {"when": ...}) -> rebooked
    """
    if action not in ACTIONS:
        raise ValueError(f"unknown task action {action!r}")
    task.updated_at = now
    if action == "pause":
        task.status, task.next_fire_at = "paused", None
        return f"paused · {task.title}"
    if action == "resume":
        task.status = "active"
        task.next_fire_at = specs.next_fire(task.when, now, tz)
        if task.next_fire_at is None:
            raise ValueError("that time has already passed; edit the task first")
        return f"resumed · {task.title}"
    if action == "run_now":
        if task.status != "active":
            raise ValueError("resume the task before running it")
        task.next_fire_at = now
        return f"running now · {task.title}"
    if action == "done":
        task.status, task.done_at, task.next_fire_at = "done", now, None
        return f"done · {task.title}"
    if action in ("stop", "delete"):
        task.status, task.done_at, task.next_fire_at = "stopped", now, None
        return f"{'deleted' if action == 'delete' else 'stopped'} · {task.title}"
    # Step 1: An edit takes only the fields a person may change, and rebooks the firing when the
    # time changed, otherwise a schedule would keep its old clock
    for name in EDITABLE:
        if name in fields:
            value = fields[name]
            if name == "when":
                value = specs.validate(value or {})
            if name == "until":
                value = datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None
            if name == "title" and not str(value or "").strip():
                raise ValueError("a task needs a title")
            if name == "target_group_id":
                value = _uuid_or_none(value)
            setattr(task, name, value.strip()[:200] if name == "title" else value)
    if "when" in fields and task.status == "active":
        task.next_fire_at = specs.next_fire(task.when, now, tz)
    return f"changed · {task.title}"


async def recent_runs(db: AsyncSession, task_id: uuid.UUID, *, limit: int = 5) -> list[TeamsTaskRun]:
    return list((await db.execute(
        select(TeamsTaskRun).where(TeamsTaskRun.task_id == task_id)
        .order_by(TeamsTaskRun.planned_at.desc()).limit(limit))).scalars())


def task_payload(task: TeamsTask, tz: ZoneInfo) -> dict:
    """One task as the tab draws it."""
    return {
        "id": str(task.id), "kind": task.kind, "title": task.title, "note": task.note, "site_id": task.site_id,
        "status": task.status, "when": dict(task.when), "when_text": specs.when_text(task.when, tz),
        "next_fire_at": _iso(task.next_fire_at), "last_fired_at": _iso(task.last_fired_at),
        "fire_count": task.fire_count, "query_text": task.query_text, "skip_if": task.skip_if,
        "target": task.target, "target_group_id": str(task.target_group_id) if task.target_group_id else None,
        "show_as": task.show_as, "until": _iso(task.until),
        "created_via": task.created_via, "source": dict(task.source or {}), "last_result": task.last_result,
        "created_at": _iso(task.created_at), "updated_at": _iso(task.updated_at), "done_at": _iso(task.done_at),
        "can_undo_until": _iso(task.created_at + timedelta(minutes=10)) if task.status == "active" else None,
    }


def _uuid_or_none(value):
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except ValueError:
        raise ValueError("target_group_id is not a group id")


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None
