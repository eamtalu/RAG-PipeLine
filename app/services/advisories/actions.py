"""What a person does with an advisory (stage D, step 9): acknowledge, assign, snooze, dismiss or
resolve, arriving as the `advisory_act` command on the question queue. The actor's own record
decides (they must see the advisory's site); every action is one audit line; the ADVISORIES row
is mirrored at once so the tab shows the change on its next poll. Used by
app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.services.access import audit, records, users
from app.services.access.commands import CommandOutcome, Refused, receipt
from app.services.advisories import mirror as advisories_mirror
from app.services.advisories import store
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings

logger = logging.getLogger(__name__)

ACTIONS = ("acknowledge", "assign", "snooze", "dismiss", "resolve")
DEFAULT_SNOOZE_MINUTES = 60
MAX_SNOOZE_MINUTES = 7 * 24 * 60
#: Dismissing or resolving on the warehouse's behalf is a supervisor's call; acknowledging,
#: assigning and snoozing need only a view of the site.
CAPABILITY = {"acknowledge": "view_own_site", "assign": "view_own_site", "snooze": "view_own_site",
              "dismiss": "run_actions", "resolve": "run_actions"}


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Apply one advisory action for the job's sender; the line the page shows, with a receipt.

    (supervisor BRI: acknowledge a BRI advisory) -> "acknowledged · Route R14 ..." ;
    (picker: dismiss) -> Refused ; (snooze until=2026-11-13T12:00Z) -> "snoozed until 12:00"
    """
    command = job.command
    if command is None or command.kind != "advisory_act":
        raise ValueError("the job carries no advisory action")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    action = str(payload.get("action") or "")
    if action not in ACTIONS:
        raise ValueError(f"unknown advisory action {action!r}")
    actor = users.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides, scoped to the advisory's site, so a forged job
        # cannot act on a site its sender cannot see
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        row = await store.get(db, job.customer_code, str(payload.get("advisory_id") or ""))
        if row is None:
            raise Refused("that advisory no longer exists")
        decision = records.decide(record, CAPABILITY[action], row.site_id)
        if not decision.is_allowed:
            raise Refused(decision.reason or f"{action} is not allowed for you")
        # Step 2: Apply the action and write its audit line in the same transaction
        text = _apply(row, action, payload, actor, now)
        await audit.record_event(
            db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
            action=f"advisory.{action}", capability_code=CAPABILITY[action], decision="allowed",
            subject_kind="advisory", subject_id=str(row.id), site_id=row.site_id, text=text,
            detail={"title": row.title, "assignee": row.assignee_object_id, "until": _iso(row.snoozed_until)}, at=now)
        await db.commit()
        # Step 3: Mirror now; a failed mirror is a line, the loop rewrites within seconds
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await advisories_mirror.mirror_customer(db, writer, job.customer_code, now=now)
            except Exception:
                logger.warning("advisories for %s not mirrored after %s", job.customer_code, action, exc_info=True)
        return CommandOutcome(text, (receipt(text, reference=str(row.id), now=now),))


def _apply(row, action: str, payload: dict, actor: users.Actor, now: datetime) -> str:
    """Change the row for one action; the one line that says what happened.

    (open row, "acknowledge") -> "acknowledged · <title>" and status acknowledged
    """
    row.updated_at = now
    if action == "acknowledge":
        row.status, row.acknowledged_by, row.snoozed_until = "acknowledged", actor.object_id, None
        return f"acknowledged · {row.title}"
    if action == "assign":
        assignee = payload.get("assignee") or {}
        object_id = str(assignee.get("object_id") or payload.get("assignee_object_id") or actor.object_id)
        name = assignee.get("name") or payload.get("assignee_name") or (actor.name if object_id == actor.object_id else None)
        row.assignee_object_id, row.assignee_name = object_id, name
        if row.status == "open":
            row.status, row.acknowledged_by = "acknowledged", actor.object_id
        return f"assigned to {name or object_id} · {row.title}"
    if action == "snooze":
        until = _until(payload.get("until"), payload.get("minutes"), now)
        row.status, row.snoozed_until = "snoozed", until
        return f"snoozed until {until.strftime('%H:%M')} UTC · {row.title}"
    row.status, row.resolved_at, row.resolved_reason = ("dismissed" if action == "dismiss" else "resolved"), now, actor.object_id
    row.snoozed_until = None
    return f"{row.status} · {row.title}"


def _until(until, minutes, now: datetime) -> datetime:
    """The snooze end: an explicit ISO moment, else minutes from now, capped at a week.

    ("2026-11-13T12:00:00+00:00", None, 09:00) -> 12:00 ; (None, None, 09:00) -> 10:00
    """
    if until:
        try:
            parsed = datetime.fromisoformat(str(until).replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("until is not an ISO datetime")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = now + timedelta(minutes=int(minutes or DEFAULT_SNOOZE_MINUTES))
    return min(max(parsed, now + timedelta(minutes=1)), now + timedelta(minutes=MAX_SNOOZE_MINUTES))


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None
