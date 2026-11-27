"""Actions a Teams job can carry instead of a question (chunk 150).

The at-risk check is answered here; every other family (access, advisories, tasks, groups, the
watchlist) lives with its own service and is routed to it.
The consumer hands the whole job here; this module opens its own short session, calls the same
store function the web API uses, and returns the one-line confirmation the tab shows. A refused
command raises, and the consumer turns that into a short error sentence.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from app.config.database import async_session
from app.services.analytics_at_risk import check_store
from app.services import access
from app.services.advisories import actions as advisory_actions
from app.services.ask import demo as demo_actions
from app.services.ask import forms as ask_forms
from app.services.tasks import commands as task_commands
from app.services.groups import commands as group_commands
from app.services.watchlist import commands as watch_commands
from app.services.preferences import commands as preference_commands
from app.services.teams.contracts import ACCESS_COMMAND_KINDS, QuestionJob


def _departure_date(text: str | None) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ValueError(f"departure_date {text!r} is not YYYY-MM-DD")


def unwrap_form(job: QuestionJob) -> QuestionJob:
    """The job with a confirmed Ask form turned into its command's own fields, for the families
    whose forms Ask offers (access, groups), so those services never know about Ask (chunk 219).

    (access_set_user {form_id: change_user, values: {...}}) -> access_set_user {object_id, role, ...} ;
    (watch_add {...}, or no form) -> the job unchanged
    """
    command = job.command
    if command is None or not (command.kind in group_commands.KINDS or command.kind.startswith("access_")):
        return job
    command = command.model_copy(update={"payload": ask_forms.unwrap(dict(command.payload))})
    return job.model_copy(update={"command": command})


async def run_command(job: QuestionJob, *, run_query=None) -> str:
    """Route one command to its service; `run_query` lets a routine's "Run now" answer its question
    (step 15; the consumer passes its agent runner)."""
    command = job.command
    if command is None:
        raise ValueError("the job carries no command")
    job = unwrap_form(job)
    command = job.command
    # a person's preferences (program step 2) live with the preferences service
    if command.kind in preference_commands.KINDS:
        return await preference_commands.run(job)  # a CommandOutcome: the line
    # the watchlist (step 14) lives with the watchlist service
    if command.kind in watch_commands.KINDS:
        return await watch_commands.run(job)  # a CommandOutcome: the line, a receipt or a choice
    # the groups and channels (step 12) live with the groups service
    if command.kind in group_commands.KINDS:
        return await group_commands.run(job)  # a CommandOutcome: the line and the receipt
    # the Tasks tab's actions (step 10) live with the tasks service
    if command.kind in ("task_create", "task_update"):
        return await task_commands.run(job, run_query=run_query)  # a CommandOutcome: the line and the receipt
    # the Advisories tab's actions (step 9) live with the advisories service
    if command.kind == "advisory_act":
        return await advisory_actions.run(job)  # a CommandOutcome: the line and the receipt
    # a confirmed demo form (step 18) is recorded as a demo and changes nothing
    if command.kind == "demo_act":
        return await demo_actions.run(job)  # a CommandOutcome: the demo line and its receipt
    # the Admin tab's actions (step 7c) live with the access service
    if command.kind in ACCESS_COMMAND_KINDS:
        return await access.run_admin_command(job)  # a CommandOutcome: the line and the receipt
    if not command.delivery_number:
        raise ValueError("an at-risk command names a delivery_number")
    actor = (command.by or job.sender_name or check_store.DEFAULT_ACTOR).strip()[:128]
    departure_date = _departure_date(command.departure_date)
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        if command.kind == "at_risk_check":
            row = await check_store.check(db, job.customer_code, command.delivery_number, actor=actor, note=command.note,
                                          now=now, departure_date=departure_date)
            await db.commit()
            return f"checked · delivery {row.delivery_number} · {actor}"
        if command.kind == "at_risk_uncheck":
            row = await check_store.uncheck(db, job.customer_code, command.delivery_number, actor=actor, now=now,
                                            departure_date=departure_date)
            await db.commit()
            return f"check removed · delivery {row.delivery_number} · {actor}"
    raise ValueError(f"unknown command {command.kind!r}")
