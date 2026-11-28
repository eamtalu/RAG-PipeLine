"""What a person does with their preferences, arriving as commands on the question queue.

`preference_set` saves some fields of one area and `preference_reset` forgets an area. Without
a target both are about the sender's own preferences. A payload may name one target instead,
for a sender who may manage users: a `role` (its defaults, and what "Default" shows on a page),
`customer` (one value for everyone, the line search range) or, for a reset only, a `user`
(another person's saved views). The sender's own access record decides; a change for anyone
but the sender is one audit line; the rows of everyone whose resolved values moved are
mirrored at once. Used by app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.tenant_site import TenantSite
from app.services import access
from app.services.preferences import areas, store
from app.services.preferences import mirror as prefs_mirror
from app.services.teams.command_outcome import CommandOutcome, Refused
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings
from app.settings import settings as app_settings

logger = logging.getLogger(__name__)

KINDS = ("preference_set", "preference_reset")
ROLE_CAPABILITY = "manage_users"
RESET = "preference_reset"

_NOT_ALLOWED = {
    areas.ROLE: "a role's defaults are set by people who may manage users",
    areas.CUSTOMER: "the customer's defaults are set by people who may manage users",
    areas.USER: "another person's views are reset by people who may manage users"}


@dataclass(frozen=True)
class _Subject:
    """Whose preferences a command is about, and what to call them in the result line."""

    kind: str
    id: str
    name: str
    #: False when the sender changes someone else's: a role's, the customer's, a person's.
    is_own: bool


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Run one preference command for the job's sender; the line the page shows.

    (preference_set advisories {use, views}) -> "Saved" ; (a wrong value) -> Refused
    naming the field ; (a role's, the customer's or another person's without the capability)
    -> Refused
    """
    command = job.command
    if command is None or command.kind not in KINDS:
        raise ValueError("the job carries no preference command")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    area = str(payload.get("area") or "")
    async with (session_factory or async_session)() as db:
        # Step 1: The sender's own record decides, so a forged job cannot write for someone
        # else: the subject is the sender unless they may manage users and name a target.
        record = await access.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        subject = await _subject_of(db, record, payload, command.kind)
        # Step 2: Check the values before anything is stored, so a refusal leaves no row.
        try:
            _check_level(area, subject)
            if command.kind == RESET:
                await store.reset(db, job.customer_code, subject.kind, subject.id, area)
                text = _reset_text(area, subject)
            else:
                settings = dict(payload.get("settings") or {})
                await _check_site(db, record, area, settings, is_own=subject.is_own)
                stored = await store.save(db, job.customer_code, subject.kind, subject.id, area,
                                          settings, by=record.object_id, now=now)
                text = _saved_text(area, subject, stored)
        except ValueError as exc:
            raise Refused(str(exc)) from None
        # Only a change for someone else is audited: it moves what other people see. A person
        # re-sorting their own list is not an access event and would bury the Admin's list.
        if not subject.is_own:
            await access.record_audit_event(
                db, job.customer_code, actor_object_id=record.object_id,
                actor_name=job.sender_name, action=command.kind, decision="allowed",
                subject_kind=f"preference_{subject.kind}", subject_id=subject.id,
                site_id=job.site_id, request_id=job.job_id,
                text=f"{job.sender_name or record.object_id}: {text}",
                detail={"area": area}, at=now)
        await db.commit()
        # Step 3: Mirror everyone whose resolved values moved, so their apps follow at once.
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            for object_id in await _affected(db, job.customer_code, subject):
                try:
                    await prefs_mirror.mirror_user(db, writer, job.customer_code, object_id,
                                                   now=now)
                except Exception:
                    logger.warning("preferences of %s not mirrored after %s", object_id,
                                   command.kind, exc_info=True)
    return CommandOutcome(text)


async def _subject_of(db: AsyncSession, record: access.AccessRecord, payload: dict,
                      kind: str) -> _Subject:
    """Whose preferences the command is about, refused unless the sender may act for them.

    (no target) -> the sender ; ({"role": "picker"}, an Admin) -> the role Picker ;
    ({"user": "u-9"}, a set) -> Refused ; (two targets, or a target without the capability)
    -> Refused
    """
    targets = {areas.ROLE: payload.get("role"), areas.USER: payload.get("user"),
               areas.CUSTOMER: record.customer_code if payload.get("customer") else None}
    named = [(level, str(value)) for level, value in targets.items() if value]
    if not named:
        return _Subject(areas.USER, record.object_id, record.display_name, True)
    if len(named) > 1:
        raise Refused("name one of role, user or customer, not several")
    level, target = named[0]
    if not access.decide(record, ROLE_CAPABILITY).is_allowed:
        raise Refused(_NOT_ALLOWED[level])
    if level == areas.CUSTOMER:
        return _Subject(level, target, "the whole customer", False)
    if level == areas.ROLE:
        # A default for a role nobody can have would sit in the table unseen for ever
        names = await access.role_names(db, record.customer_code)
        if target not in names:
            raise Refused(f"there is no role {target} for this customer")
        return _Subject(level, target, names[target], False)
    if kind != RESET:
        raise Refused("another person's views can only be reset, not written")
    person = await access.get_user(db, record.customer_code, target)
    if person is None:
        raise Refused("there is no such person for this customer")
    return _Subject(level, target, person.display_name or target, False)


def _check_level(area: str, subject: _Subject) -> None:
    """Refuse an area at a level this slice does not offer.

    ("ask", a role) -> ValueError ; ("ask", the customer) -> fine ; ("app", the customer) ->
    ValueError ; anything else is left to the area's own holder rule
    """
    if area in areas.CUSTOMER_AREAS and subject.kind != areas.CUSTOMER:
        raise ValueError(f"{_CUSTOMER_WORDS[area]} is set for the whole customer")
    if subject.kind == areas.CUSTOMER and area not in areas.CUSTOMER_AREAS:
        raise ValueError("only the line search range and the number of routines are set "
                         "for the whole customer")
    if subject.kind == areas.USER and not subject.is_own and areas.page_of(area) != area:
        raise ValueError("only a person's saved views of a page are reset for them")


_CUSTOMER_WORDS = {"ask": "the line search range", "routines": "the number of routines"}


def _saved_text(area: str, subject: _Subject, stored: dict) -> str:
    """The one line the page shows after a save.

    ("ask", {"line_search_days": 10}) -> "Line search now covers the last 10 days" ;
    ("advisories_default", the role Picker) -> "Default view of advisories set for Picker"
    """
    if area == "ask":
        return f"Line search now covers the last {stored.get('line_search_days')} days"
    if area == "routines":
        return f"Each person can now have up to {stored.get('max_active')} active routines"
    if area in areas.ROLE_DEFAULT_AREAS:
        return f"Default view of {areas.page_of(area)} set for {subject.name}"
    return f"Saved · {_title(area)}"


def _reset_text(area: str, subject: _Subject) -> str:
    """The one line the page shows after a reset."""
    if area == "ask":
        return f"Line search covers the last {app_settings.log_search_default_days} days again"
    if area == "routines":
        return (f"Each person can have up to {app_settings.teams_tasks_max_active} "
                "active routines again")
    if area in areas.ROLE_DEFAULT_AREAS:
        return (f"Default view of {areas.page_of(area)} for {subject.name} "
                "is the built-in one again")
    if subject.kind == areas.USER and not subject.is_own:
        return f"{subject.name}'s {_title(area)} views were reset"
    return f"{_title(area)} back to the default"


def _title(area: str) -> str:
    return area.replace("_", " ")


async def _check_site(db: AsyncSession, record: access.AccessRecord, area: str, settings: dict,
                      *, is_own: bool) -> None:
    """Refuse a default site that does not exist, or that the person choosing it may not open.

    (own, "LON", a Brighton person) -> ValueError ; (a role's default, "LON") -> fine, each
    person's value is narrowed when it is read
    """
    site = settings.get("default_site") if area == "app" else None
    if site is None:
        return
    known = set((await db.execute(select(TenantSite.site_id).where(
        TenantSite.customer_code == record.customer_code))).scalars())
    if site != access.ALL_SITES and site not in known:
        raise ValueError(f"default_site: {site} is not a site of this customer")
    if is_own and not record.may_see(site):
        raise ValueError(f"default_site: {site} is not one of your sites")


async def _affected(db: AsyncSession, customer_code: str, subject: _Subject) -> list[str]:
    """Whose row to mirror: the one person for a person's change, everyone of the role for a
    role's, everyone active for the customer's."""
    if subject.kind == areas.USER:
        return [subject.id]
    return [u.object_id for u in await access.list_users(db, customer_code)
            if u.object_id and u.status == "active"
            and (subject.kind == areas.CUSTOMER or u.role_code == subject.id)]
