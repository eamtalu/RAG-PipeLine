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
#: How many advisories one command may act on (step 17: a group or a selection at once).
MAX_BATCH = 50
DEFAULT_SNOOZE_MINUTES = 60
MAX_SNOOZE_MINUTES = 7 * 24 * 60
#: Dismissing or resolving on the warehouse's behalf is a supervisor's call; acknowledging,
#: assigning and snoozing need only a view of the site.
CAPABILITY = {"acknowledge": "view_own_site", "assign": "view_own_site", "snooze": "view_own_site",
              "dismiss": "run_actions", "resolve": "run_actions"}


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Apply one advisory action for the job's sender, to one advisory or to a batch of up to 50
    (step 17); the line the page shows, with a receipt.

    (supervisor BRI: acknowledge a BRI advisory) -> "acknowledged · Route R14 ..." ;
    (picker: dismiss) -> Refused ; (snooze until=2026-11-13T12:00Z) -> "snoozed until 12:00" ;
    (advisory_ids of 3, one outside the sites) -> "acknowledged 2 · 1 not yours to change"
    """
    command = job.command
    if command is None or command.kind != "advisory_act":
        raise ValueError("the job carries no advisory action")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    action = str(payload.get("action") or "")
    if action not in ACTIONS:
        raise ValueError(f"unknown advisory action {action!r}")
    ids = batch_ids(payload)
    actor = users.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides, scoped to each advisory's site, so a forged job
        # cannot act on a site its sender cannot see; in a batch the refused ones are counted,
        # not fatal, because a selection may straddle sites
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        done: list = []
        refused: list[str] = []
        for advisory_id in ids:
            row = await store.get(db, job.customer_code, advisory_id)
            if row is None:
                refused.append("that advisory no longer exists")
                continue
            decision = records.decide(record, CAPABILITY[action], row.site_id)
            if not decision.is_allowed:
                refused.append(decision.reason or f"{action} is not allowed for you")
                continue
            # Step 2: Apply the action and write its audit line, one per advisory, in one transaction
            text = _apply(row, action, payload, actor, now)
            await audit.record_event(
                db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
                action=f"advisory.{action}", capability_code=CAPABILITY[action], decision="allowed",
                subject_kind="advisory", subject_id=str(row.id), site_id=row.site_id, text=text,
                detail={"title": row.title, "assignee": row.assignee_object_id, "until": _iso(row.snoozed_until)}, at=now)
            done.append((row, text))
        if not done:
            raise Refused(refused[0] if refused else "nothing to change")
        await db.commit()
        # Step 3: Mirror now; a failed mirror is a line, the loop rewrites within seconds
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await advisories_mirror.mirror_customer(db, writer, job.customer_code, now=now)
            except Exception:
                logger.warning("advisories for %s not mirrored after %s", job.customer_code, action, exc_info=True)
        if len(ids) == 1:
            row, text = done[0]
            return CommandOutcome(text, (receipt(text, reference=str(row.id), now=now),))
        text = batch_text(action, len(done), len(refused))
        return CommandOutcome(text, (receipt(text, reference=f"{len(done)} advisories", now=now),))


def batch_ids(payload: dict) -> list[str]:
    """The advisory ids a command names: one `advisory_id`, or `advisory_ids` of up to 50.

    >>> batch_ids({"advisory_id": "a"}), batch_ids({"advisory_ids": ["a", "b", "a"]})
    (['a'], ['a', 'b'])
    >>> batch_ids({})
    Traceback (most recent call last):
    ...
    ValueError: the command names no advisory
    """
    many = payload.get("advisory_ids")
    if isinstance(many, (list, tuple)) and many:
        ids = list(dict.fromkeys(str(i) for i in many if i))
        if len(ids) > MAX_BATCH:
            raise ValueError(f"at most {MAX_BATCH} advisories at once")
        if ids:
            return ids
    one = str(payload.get("advisory_id") or "")
    if not one:
        raise ValueError("the command names no advisory")
    return [one]


def batch_text(action: str, done: int, refused: int) -> str:
    """The one line of a batch: what happened to how many, and how many were not the actor's to change.

    >>> batch_text("acknowledge", 12, 0), batch_text("dismiss", 11, 1), batch_text("snooze", 2, 2)
    ('acknowledged 12', 'dismissed 11 · 1 not yours to change', 'snoozed 2 · 2 not yours to change')
    """
    past = {"acknowledge": "acknowledged", "assign": "assigned", "snooze": "snoozed",
            "dismiss": "dismissed", "resolve": "resolved"}[action]
    text = f"{past} {done}"
    return text if refused == 0 else f"{text} · {refused} not yours to change"


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
