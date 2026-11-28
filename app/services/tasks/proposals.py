"""A routine set up for a colleague (program step 3, slice 3).

Proposing writes one row per colleague and nothing else: no routine exists and nothing runs.
The colleague accepts, which makes a routine they own (so every run reads with their access,
never the sender's), or declines. The sender withdraws one that was not answered. Used by
commands.py, which checks who is asking, and by mirror.py for the person's row.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.task import SHOW_AS, TASK_KINDS, TeamsTask, TeamsTaskProposal
from app.services import access
from app.services.tasks import specs, store

#: Colleagues one proposal may go to; a whole site at once is not this slice.
MAX_RECEIVERS = 20
#: What a proposal carries of a routine; the site and the owner come from who accepts it.
FIELDS = ("kind", "title", "when", "query_text", "note", "show_as", "until")
#: How many of the sender's past proposals the row lists.
SENT_LISTED = 30


def clean_fields(payload: dict[str, Any], *, now: datetime, tz: ZoneInfo) -> dict[str, Any]:
    """The routine a proposal carries, checked as a routine is; ValueError naming what is wrong.

    ({"kind": "schedule", "title": "Open picks", "query_text": "…", "when": weekdays 14:00})
    -> those fields with the spec validated ; (a channel target) -> ValueError ;
    (a schedule without its question) -> ValueError ; (a once time already past) -> ValueError
    """
    kind = str(payload.get("kind") or "schedule")
    if kind not in TASK_KINDS:
        raise ValueError("kind must be reminder or schedule")
    title = str(payload.get("title") or "").strip()[:200]
    if not title:
        raise ValueError("a routine needs a title")
    if str(payload.get("target") or "me") != "me":
        raise ValueError("a routine for a colleague is shown to them in the app; "
                         "a channel belongs to a group")
    show_as = str(payload.get("show_as") or "table")
    if show_as not in SHOW_AS:
        raise ValueError("show_as must be table, chart or summary")
    query = str(payload.get("query_text") or "").strip()
    if kind == "schedule" and not query:
        raise ValueError("a schedule needs the question it runs")
    spec = specs.validate(dict(payload.get("when") or {}))
    if specs.next_fire(spec, now, tz) is None:
        raise ValueError("that time has already passed")
    return {"kind": kind, "title": title, "when": spec, "query_text": query or None,
            "note": payload.get("note") or None, "show_as": show_as,
            "until": payload.get("until") or None}


async def propose(db: AsyncSession, customer_code: str, *, sender: access.Actor,
                  receiver_ids: list[str], fields: dict[str, Any],
                  now: datetime) -> list[TeamsTaskProposal]:
    """One waiting proposal per colleague; ValueError for nobody, oneself, a stranger, too many,
    or the same routine already waiting for one of them.

    (Ada, ["u-sam", "u-pat"], Open picks) -> two rows, status waiting
    """
    # Step 1: Check every colleague before writing any row, so a proposal is never half sent
    wanted = list(dict.fromkeys(str(r).strip() for r in receiver_ids if str(r).strip()))
    if not wanted:
        raise ValueError("pick at least one colleague")
    if sender.object_id in wanted:
        raise ValueError("a routine for yourself is made directly, not proposed")
    if len(wanted) > MAX_RECEIVERS:
        raise ValueError(f"a proposal goes to at most {MAX_RECEIVERS} colleagues at once")
    people = {u.object_id: u for u in await access.list_users(db, customer_code)
              if u.object_id and u.status == "active"}
    for receiver in wanted:
        if receiver not in people:
            raise ValueError("there is no such person with access for this customer")
    waiting = (await db.execute(select(TeamsTaskProposal).where(
        TeamsTaskProposal.customer_code == customer_code,
        TeamsTaskProposal.sender_object_id == sender.object_id,
        TeamsTaskProposal.status == "waiting",
        TeamsTaskProposal.receiver_object_id.in_(wanted)))).scalars().all()
    for row in waiting:
        # the same routine, not just any proposal: two different ones may wait side by side
        if (row.fields.get("title"), row.fields.get("when")) == (fields["title"], fields["when"]):
            raise ValueError("this routine is already waiting for "
                             f"{row.receiver_name or row.receiver_object_id}")
    # Step 2: One row per colleague, each answered on its own
    made = [TeamsTaskProposal(
        id=uuid.uuid4(), customer_code=customer_code, sender_object_id=sender.object_id,
        sender_name=sender.name, receiver_object_id=receiver,
        receiver_name=people[receiver].display_name, fields=dict(fields), status="waiting",
        created_at=now) for receiver in wanted]
    db.add_all(made)
    await db.flush()
    return made


async def get(db: AsyncSession, customer_code: str, proposal_id: str) -> TeamsTaskProposal | None:
    try:
        key = uuid.UUID(str(proposal_id))
    except ValueError:
        return None
    return await db.scalar(select(TeamsTaskProposal).where(
        TeamsTaskProposal.customer_code == customer_code, TeamsTaskProposal.id == key))


def _refuse_answered(proposal: TeamsTaskProposal) -> None:
    if proposal.status != "waiting":
        raise ValueError(f"that proposal was already {proposal.status}")


async def accept(db: AsyncSession, proposal: TeamsTaskProposal, *, site_id: str | None,
                 tz: ZoneInfo, now: datetime) -> TeamsTask:
    """Make the receiver's own routine from a waiting proposal; ValueError when it was answered
    or its time has passed since.

    (waiting, Sam accepts on Brighton) -> a task owned by Sam, site BRI, proposed_by the sender
    """
    _refuse_answered(proposal)
    fields = dict(proposal.fields)
    until = fields.pop("until", None)
    task = await store.create(
        db, proposal.customer_code, owner_object_id=proposal.receiver_object_id,
        owner_name=proposal.receiver_name, tz=tz, now=now, site_id=site_id, target="me",
        until=datetime.fromisoformat(str(until).replace("Z", "+00:00")) if until else None,
        created_via="proposal", source={"proposal_id": str(proposal.id)}, **fields)
    task.proposed_by, task.proposed_by_name = proposal.sender_object_id, proposal.sender_name
    proposal.status, proposal.task_id, proposal.answered_at = "accepted", task.id, now
    await db.flush()
    return task


async def close(db: AsyncSession, proposal: TeamsTaskProposal, status: str, *,
                now: datetime) -> None:
    """Decline or withdraw a waiting proposal; ValueError when it was answered already."""
    _refuse_answered(proposal)
    proposal.status, proposal.answered_at = status, now
    await db.flush()


async def for_person(db: AsyncSession, customer_code: str, object_id: str, *,
                     tz: ZoneInfo) -> dict[str, list[dict]]:
    """What waits for one person and what they proposed, as their row carries it.

    -> {"waiting": [{id, title, from_name, when_text, …}], "sent": [{id, to_name, status, …}]}
    """
    waiting = (await db.execute(select(TeamsTaskProposal).where(
        TeamsTaskProposal.customer_code == customer_code,
        TeamsTaskProposal.receiver_object_id == object_id,
        TeamsTaskProposal.status == "waiting").order_by(TeamsTaskProposal.created_at))).scalars()
    sent = (await db.execute(select(TeamsTaskProposal).where(
        TeamsTaskProposal.customer_code == customer_code,
        TeamsTaskProposal.sender_object_id == object_id)
        .order_by(TeamsTaskProposal.created_at.desc()).limit(SENT_LISTED))).scalars()
    return {"waiting": [_payload(row, tz) for row in waiting],
            "sent": [_payload(row, tz) for row in sent]}


def _payload(row: TeamsTaskProposal, tz: ZoneInfo) -> dict:
    fields = dict(row.fields or {})
    return {
        "id": str(row.id), "status": row.status, "kind": fields.get("kind"),
        "title": fields.get("title"), "query_text": fields.get("query_text"),
        "note": fields.get("note"), "show_as": fields.get("show_as"),
        "when": fields.get("when"), "when_text": specs.when_text(fields.get("when") or {}, tz),
        "from_object_id": row.sender_object_id, "from_name": row.sender_name,
        "to_object_id": row.receiver_object_id, "to_name": row.receiver_name,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "answered_at": row.answered_at.isoformat() if row.answered_at else None,
    }
