"""What a person does with their tasks (stage D, step 10), arriving as the `task_create` and
`task_update` commands on the question queue: make a reminder or a schedule (from the Tasks
tab, from an advisory, or from a confirmed Ask form), and pause, resume, run now, finish, stop,
delete or edit one of their own. The actor's record decides (an active record; a schedule needs
`ask`, because it runs questions); every change is one audit line; the person's `USER#<id>#TASKS`
row is mirrored at once. Used by app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import audit, records, users
from app.services.access.commands import CommandOutcome, Refused, receipt
from app.services.tasks import forms, runner, store
from app.services.tasks import mirror as tasks_mirror
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings
from app.settings import settings

logger = logging.getLogger(__name__)

FORM_IDS = ("new_reminder", "new_schedule")


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None, run_query: runner.RunQuery | None = None) -> CommandOutcome:
    """Run one task command for the job's sender; the line the page shows, with a receipt.
    `run_query` answers a routine's question for "Run now" (step 15); without it a check cannot run.

    (task_create reminder) -> "added to your routines · Fri 13 Nov, 17:45 · Check…" + receipt with Undo ;
    (task_update pause by another person) -> Refused
    """
    command = job.command
    if command is None or command.kind not in ("task_create", "task_update"):
        raise ValueError("the job carries no task command")
    now = now or datetime.now(timezone.utc)
    actor = users.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides, so a forged job cannot make tasks for someone
        # else or run questions a role may not ask
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        tz = ZoneInfo(await get_customer_timezone(db, job.customer_code))
        payload = dict(command.payload)
        if command.kind == "task_create":
            text, task, undo = await _create(db, job, record, actor, payload, now, tz)
        else:
            text, task, undo = await _update(db, job, record, actor, payload, now, tz, run_query=run_query)
        await db.commit()
        # Step 2: Mirror the person's row now; a failed mirror is a line, the loop rewrites it
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await tasks_mirror.mirror_user(db, writer, job.customer_code, actor.object_id, now=now)
            except Exception:
                logger.warning("tasks of %s not mirrored after %s", actor.object_id, command.kind, exc_info=True)
        return CommandOutcome(text, (receipt(text, reference=str(task.id), now=now, undo=undo),))


async def sendable_groups(db: AsyncSession, customer_code: str, object_id: str) -> list[tuple[str, str]]:
    """The groups a schedule of this person may post to: every enabled group of the customer, as
    (id, name) pairs by name (a schedule posts with its owner's access either way)."""
    from app.services.groups import store as groups_store

    return [(str(v.group.id), v.group.name) for v in await groups_store.list_groups(db, customer_code, include_disabled=False)]


async def _create(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor,
                  payload: dict, now: datetime, tz: ZoneInfo):
    # Step 1: A confirmed Ask form arrives as its field values under "form"; read it into the
    # same fields the Tasks tab sends, so one path makes every task
    form_id = payload.get("form_id")
    if form_id in FORM_IDS:
        values = dict(payload.get("values") or {})
        fields = (forms.reminder_from_form(values, tz=tz) if form_id == "new_reminder"
                  else forms.schedule_from_form(values, tz=tz, groups=await sendable_groups(db, job.customer_code, actor.object_id)))
        payload = {**payload, **fields, "via": payload.get("via") or "ask"}
    kind = str(payload.get("kind") or "reminder")
    site_id = payload.get("site_id") or job.site_id
    # Step 2: A schedule runs questions, so it needs the ask capability at its site; a reminder
    # with a question attached too
    if kind == "schedule" or payload.get("query_text"):
        decision = records.decide(record, "ask", site_id)
        if not decision.is_allowed:
            raise Refused(decision.reason or "you cannot schedule questions")
    until = payload.get("until")
    try:
        task = await store.create(
            db, job.customer_code, owner_object_id=actor.object_id, owner_name=actor.name, kind=kind,
            title=str(payload.get("title") or ""), when=dict(payload.get("when") or {}), tz=tz, now=now,
            note=payload.get("note"), site_id=site_id, query_text=payload.get("query_text"),
            skip_if=payload.get("skip_if"), target=str(payload.get("target") or "me"),
            show_as=str(payload.get("show_as") or "table"), target_group_id=payload.get("target_group_id"),
            until=datetime.fromisoformat(str(until).replace("Z", "+00:00")) if until else None,
            created_via=str(payload.get("via") or "tab"), source=payload.get("source") or {})
    except ValueError as exc:
        raise Refused(str(exc))
    # step 15: one word for both kinds, the clock says which it is
    text = f"added to your routines · {store.task_payload(task, tz)['when_text']} · {task.title}"
    await audit.record_event(
        db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name, action=f"task.created.{kind}",
        decision="allowed", subject_kind="task", subject_id=str(task.id), site_id=site_id, request_id=job.job_id,
        text=f"{actor.name or actor.object_id} set {text}", detail={"when": task.when, "via": task.created_via}, at=now)
    return text, task, {"task_id": str(task.id), "action": "delete"}


async def _update(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor,
                  payload: dict, now: datetime, tz: ZoneInfo, *, run_query: runner.RunQuery | None = None):
    task = await store.get(db, job.customer_code, str(payload.get("task_id") or ""))
    if task is None:
        raise Refused("that task no longer exists")
    if task.owner_object_id != actor.object_id:
        raise Refused("that task is not yours")
    action = str(payload.get("action") or "")
    if action not in store.ACTIONS:
        raise Refused(f"unknown task action {action!r}")
    # Step 0: The routine actions of step 15 write a ledger row or make a new task instead of
    # changing the row; each is one audit line like the rest
    if action in ("run_now", "done", "skip", "plan_again", "copy"):
        return await _routine_action(db, job, record, actor, task, action, payload, now, tz, run_query=run_query)
    # Step 1: A delete from a receipt's Undo is only good while the window is open; a delete from
    # the Tasks tab is a stop and is always allowed
    if action == "delete" and payload.get("via") == "undo":
        if now - task.created_at > timedelta(minutes=settings.teams_undo_minutes):
            raise Refused("the undo window has passed; stop it from the Tasks tab instead")
    # Step 2: An edit from the tab's dialog arrives as the form's values; read it the way a new
    # one is read, so the two paths cannot drift
    fields = dict(payload.get("fields") or {})
    if fields.get("form_id") in FORM_IDS:
        values = dict(fields.get("values") or {})
        parsed = (forms.reminder_from_form(values, tz=tz) if fields["form_id"] == "new_reminder"
                  else forms.schedule_from_form(values, tz=tz, groups=await sendable_groups(db, job.customer_code, actor.object_id)))
        fields = {k: v for k, v in parsed.items() if k in store.EDITABLE}
    try:
        text = store.apply(task, action, fields, now=now, tz=tz)
    except ValueError as exc:
        raise Refused(str(exc))
    await audit.record_event(
        db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name, action=f"task.{action}",
        decision="allowed", subject_kind="task", subject_id=str(task.id), site_id=task.site_id, request_id=job.job_id,
        text=f"{actor.name or actor.object_id}: {text}", at=now)
    return text, task, None


async def _routine_action(db: AsyncSession, job: QuestionJob, record: records.AccessRecord, actor: users.Actor,
                          task, action: str, payload: dict, now: datetime, tz: ZoneInfo, *,
                          run_query: runner.RunQuery | None):
    """The step 15 actions on one routine: run it by hand, tick it done or skipped with a note,
    plan it again at a new time, or copy it as a new one.

    (check, "run_now") -> "ran · Short products list · GINGER was short…" ; ("done", note) -> "done · …"
    """
    undo = None
    if action == "run_now":
        # Step 1: A by-hand run of a question needs the ask capability like a schedule does, and
        # the runner; the clock is untouched, the answer is the receipt's line
        if task.status not in ("active", "paused"):
            raise Refused("that routine has ended; plan it again first")
        if task.query_text:
            decision = records.decide(record, "ask", task.site_id)
            if not decision.is_allowed:
                raise Refused(decision.reason or "you cannot run questions")
            if run_query is None:
                raise Refused("questions cannot be run from here; try again in a moment")
        run = await runner.run_by_hand(db, task, actor_object_id=actor.object_id, actor_name=actor.name, now=now,
                                       run_query=run_query)
        first_line = (run.text or run.error or "").strip().split("\n", 1)[0][:160]
        text = f"ran · {task.title}" + (f" · {first_line}" if first_line else "")
        if run.outcome == "failed":
            text = f"could not run · {task.title} · {first_line}"
        detail = {"run_id": str(run.id), "outcome": run.outcome}
    elif action in ("done", "skip"):
        note = str(payload.get("note") or "").strip()[:500] or None
        run = await runner.complete(db, task, actor_object_id=actor.object_id, actor_name=actor.name,
                                    outcome="done" if action == "done" else "skipped", note=note, now=now)
        word = "done" if action == "done" else "skipped"
        text = f"{word} · {task.title}" + (f" · {note}" if note else "")
        detail = {"run_id": str(run.id), "note": note, "still_active": task.status == "active"}
    elif action == "plan_again":
        # Step 2: Plan again is a new routine with the same content on a new time, so the old
        # one's history stays whole; the receipt's Undo deletes the new one
        when = payload.get("when")
        if not when:
            raise Refused("plan_again needs the new time")
        try:
            task = await store.copy_from(db, task, overrides={"when": when}, tz=tz, now=now, created_via="plan_again")
        except ValueError as exc:
            raise Refused(str(exc))
        text = f"planned again · {store.task_payload(task, tz)['when_text']} · {task.title}"
        undo = {"task_id": str(task.id), "action": "delete"}
        detail = {"from_task_id": task.source.get("task_id"), "when": task.when}
    else:
        # Step 3: Copy as new takes the dialog's values the way an edit does, or plain fields
        fields = dict(payload.get("fields") or {})
        if fields.get("form_id") in FORM_IDS:
            values = dict(fields.get("values") or {})
            fields = (forms.reminder_from_form(values, tz=tz) if fields["form_id"] == "new_reminder"
                      else forms.schedule_from_form(values, tz=tz, groups=await sendable_groups(db, job.customer_code, actor.object_id)))
        fields = {k: v for k, v in fields.items() if k in store.EDITABLE}
        try:
            task = await store.copy_from(db, task, overrides=fields, tz=tz, now=now, created_via="copy")
        except ValueError as exc:
            raise Refused(str(exc))
        text = f"copied · {store.task_payload(task, tz)['when_text']} · {task.title}"
        undo = {"task_id": str(task.id), "action": "delete"}
        detail = {"from_task_id": task.source.get("task_id"), "fields": sorted(fields)}
    await audit.record_event(
        db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name, action=f"task.{action}",
        decision="allowed", subject_kind="task", subject_id=str(task.id), site_id=task.site_id, request_id=job.job_id,
        text=f"{actor.name or actor.object_id}: {text}", detail=detail, at=now)
    return text, task, undo

