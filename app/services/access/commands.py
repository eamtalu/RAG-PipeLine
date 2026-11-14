"""The Admin tab's writes (stage D, step 7c), arriving as commands on the question queue: create
or change a user, deactivate one, set a matrix cell, decide a request, or ask for more access
for oneself. The actor's own record decides first (the job's claims are never trusted); an
action the matrix marks "with approval" becomes a request instead of acting; everything is
audited by the service it calls, and the customer's access rows are mirrored at once.
Used by app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.services.access import approvals, records, users
from app.services.access import matrix as matrix_file
from app.services.access import mirror as access_mirror
from app.services.ask import forms as ask_forms
from app.services.teams.contracts import ACCESS_COMMAND_KINDS, QuestionJob, ReceiptBlock
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings
from app.settings import settings

logger = logging.getLogger(__name__)


class Refused(ValueError):
    """The actor may not do this; the sentence is what the page shows."""


@dataclass(frozen=True)
class CommandOutcome:
    """What a command answers with: one line, and the blocks the page draws (a receipt)."""

    text: str
    blocks: tuple[dict, ...] = ()


def receipt(text: str, *, status: str = "done", reference: str | None = None, undo: dict | None = None,
            now: datetime | None = None, minutes: int | None = None) -> dict:
    """A receipt block (step 8): what happened, its reference, and the undo while it is open.

    >>> r = receipt("Request sent", status="needs_approval", reference="USR-1", undo={"reference": "USR-1"},
    ...             now=datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc), minutes=10)
    >>> r["type"], r["undo_until"], r["undo"]
    ('receipt', '2026-11-13T09:10:00Z', {'reference': 'USR-1'})
    """
    now = now or datetime.now(timezone.utc)
    minutes = settings.teams_undo_minutes if minutes is None else minutes
    return ReceiptBlock(text=text, reference=reference, status=status,
                        undo_until=(now + timedelta(minutes=minutes)) if undo else None,
                        undo=undo).model_dump(mode="json")


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Run one access command for the job's sender; the one line the page shows, with a receipt
    block where the action can be undone.

    (admin: access_set_user u-9 supervisor BRI) -> "created Jo Bloggs as Supervisor for BRI" + receipt ;
    (supervisor: the same) -> "sent for approval · USR-3" + receipt ; (picker: the same) -> Refused
    """
    command = job.command
    if command is None or command.kind not in ACCESS_COMMAND_KINDS:
        raise ValueError("the job carries no access command")
    now = now or datetime.now(timezone.utc)
    actor = users.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides, so a forged job cannot act beyond its sender
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        # a confirmed form from Ask (step 18) arrives as {form_id, values, via}; its own fields come out
        payload = ask_forms.unwrap(dict(command.payload))
        handler = _HANDLERS[command.kind]
        outcome = await handler(db, job, record, actor, payload, now)
        if isinstance(outcome, str):
            outcome = CommandOutcome(outcome)
        await db.commit()
        # Step 2: Mirror now, so the Admin tab sees the change on its next poll rather than on
        # the next sweep; a failed mirror is a line, the sweep retries within a minute
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await access_mirror.mirror_customer(db, writer, job.customer_code, now=now)
                await db.commit()
            except Exception:
                logger.warning("access rows for %s not mirrored after %s", job.customer_code,
                               command.kind, exc_info=True)
    return outcome


async def _set_user(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                    actor: users.Actor, payload: dict, now: datetime) -> str:
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("object_id is required")
    # the confirmed form (step 8) names one site and a shift; the Admin tab names site_ids
    site_ids = payload.get("site_ids")
    if site_ids is None and payload.get("site_id"):
        site_ids = [payload["site_id"]]
    note = " · ".join(p for p in (str(payload.get("shift") or "").strip(), str(payload.get("note") or "").strip()) if p) or None
    change = users.UserChange(
        display_name=payload.get("display_name"), email=payload.get("email") or None,
        role_code=payload.get("role"), all_sites=payload.get("all_sites"),
        site_ids=tuple(site_ids) if site_ids is not None else None,
        capabilities=payload.get("capabilities"), status=payload.get("status"),
        starts_on=_date(payload.get("starts_on")), note=note)
    existing = await users.get_user(db, job.customer_code, object_id)
    decision = records.decide(record, "manage_users")
    # Step 1: "With approval" queues the whole change for an Ops manager; nothing is applied
    if decision.outcome == "needs_approval":
        kind = "create_user" if existing is None else "change_role"
        title = (f"New user: {change.display_name or object_id}" if existing is None
                 else f"Change {existing.display_name}")
        row = await approvals.request(
            db, job.customer_code, kind=kind, requested_by=actor.object_id,
            requested_by_name=actor.name, requested_via=payload.get("via") or "admin", title=title,
            detail=_detail(change), payload={**payload, "object_id": object_id, "site_ids": list(site_ids or [])},
            now=now)
        return CommandOutcome(
            f"sent for approval · {row.reference}",
            (receipt("Request sent to the Ops managers", status="needs_approval", reference=row.reference,
                     undo={"reference": row.reference}, now=now),))
    if not decision.is_allowed:
        raise Refused(decision.reason)
    if existing is None:
        user = await users.create_user(db, job.customer_code, object_id, change=change,
                                       actor=actor, now=now)
        text = f"created {user.display_name} as {_role_name(user.role_code)}{_sites_of(change, site_ids)}"
        return CommandOutcome(text, (receipt(text[0].upper() + text[1:], undo={"object_id": object_id}, now=now),))
    user = await users.update_user(db, job.customer_code, object_id, change=change, actor=actor,
                                   now=now)
    return f"updated {user.display_name}"


async def _deactivate_user(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                           actor: users.Actor, payload: dict, now: datetime) -> str:
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("object_id is required")
    if object_id == actor.object_id:
        raise Refused("you cannot deactivate yourself")
    decision = records.decide(record, "manage_users")
    if not decision.is_allowed:
        raise Refused(decision.reason)
    user = await users.update_user(db, job.customer_code, object_id,
                                   change=users.UserChange(status="deactivated"), actor=actor, now=now)
    return f"deactivated {user.display_name}"


async def _set_matrix_cell(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                           actor: users.Actor, payload: dict, now: datetime) -> str:
    # the matrix is the Admin's alone: it changes what every other role may do
    if record.role_code != "admin":
        raise Refused("only an Admin may change the matrix")
    role, capability, state = (str(payload.get(k) or "") for k in ("role", "capability", "state"))
    await records.set_matrix_cell(db, job.customer_code, role, capability, state, actor=actor)
    return f"{capability} for {_role_name(role)}: {state.replace('_', ' ')}"


async def _decide(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                  actor: users.Actor, payload: dict, now: datetime) -> str:
    decision = records.decide(record, "approve")
    if not decision.is_allowed:
        raise Refused(decision.reason)
    reference = str(payload.get("reference") or "")
    approve = bool(payload.get("approve"))
    row = await approvals.decide(db, job.customer_code, reference, approve=approve,
                                 decided_by=actor.object_id, decided_by_name=actor.name,
                                 changes=payload.get("changes") or None, note=payload.get("note"),
                                 now=now)
    return f"{row.status} {row.reference} · {row.title}"


async def _request(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                   actor: users.Actor, payload: dict, now: datetime) -> str:
    # Step 1: A person asks for one capability or for more sites, for themselves only
    capability = payload.get("capability")
    site_ids = payload.get("site_ids")
    if capability:
        if capability not in matrix_file.load().capability_codes:
            raise ValueError(f"unknown capability {capability!r}")
        if record.state(capability) == "allowed":
            return f"you already have {_capability_name(capability)}"
        row = await approvals.request(
            db, job.customer_code, kind="grant_capability", requested_by=actor.object_id,
            requested_by_name=actor.name, requested_via="admin",
            title=f"{_capability_name(capability)}: {record.display_name or actor.object_id}",
            detail=payload.get("reason"), payload={"capability": capability}, now=now)
    elif site_ids:
        row = await approvals.request(
            db, job.customer_code, kind="add_sites", requested_by=actor.object_id,
            requested_by_name=actor.name, requested_via="admin",
            title=f"More sites ({', '.join(site_ids)}): {record.display_name or actor.object_id}",
            detail=payload.get("reason"), payload={"site_ids": list(site_ids)}, now=now)
    else:
        raise ValueError("a request names a capability or site_ids")
    return f"requested · {row.reference} · waiting for an Ops manager"


async def _undo(db: AsyncSession, job: QuestionJob, record: records.AccessRecord,
                actor: users.Actor, payload: dict, now: datetime) -> str:
    """A receipt's Undo: a request of one's own is withdrawn, a user one created is deactivated,
    within the undo window of the action."""
    window = timedelta(minutes=settings.teams_undo_minutes)
    if payload.get("reference"):
        row = await approvals.withdraw(db, job.customer_code, str(payload["reference"]), by=actor.object_id,
                                       by_name=actor.name, now=now)
        if now - row.created_at > window:
            raise Refused("the undo window has passed; ask an Ops manager to decline it")
        return f"withdrawn · {row.reference}"
    if payload.get("object_id"):
        user = await users.get_user(db, job.customer_code, str(payload["object_id"]))
        if user is None or user.created_by != actor.object_id:
            raise Refused("that user was not created by you")
        if now - user.created_at > window:
            raise Refused("the undo window has passed; deactivate the user from the Admin tab instead")
        await users.update_user(db, job.customer_code, user.object_id,
                                change=users.UserChange(status="deactivated"), actor=actor, now=now)
        return f"undone · {user.display_name} deactivated"
    raise ValueError("an undo names a reference or an object_id")


_HANDLERS = {
    "access_set_user": _set_user,
    "access_deactivate_user": _deactivate_user,
    "access_set_matrix_cell": _set_matrix_cell,
    "access_decide": _decide,
    "access_request": _request,
    "access_undo": _undo,
}


def _role_name(code: str) -> str:
    return next((r.name for r in matrix_file.load().roles if r.code == code), code)


def _capability_name(code: str) -> str:
    return next((c.name for c in matrix_file.load().capabilities if c.code == code), code)


def _sites_of(change: users.UserChange, site_ids) -> str:
    if change.all_sites:
        return " for all sites"
    return f" for {', '.join(site_ids)}" if site_ids else ""


def _detail(change: users.UserChange) -> str | None:
    parts = [p for p in (_role_name(change.role_code) if change.role_code else None,
                         ", ".join(change.site_ids) if change.site_ids else ("all sites" if change.all_sites else None),
                         change.note) if p]
    return " · ".join(parts) or None


def _date(value) -> date | None:
    return date.fromisoformat(value) if value else None
