"""What a person does with their preferences, arriving as commands on the question queue.

`preference_set` saves some fields of one area and `preference_reset` forgets an area, for the
sender's own preferences, or for a role's defaults when the payload names a `role` and the
sender may manage users. The sender's own access record decides; every change is one audit
line; the rows of everyone whose resolved values moved are mirrored at once. Used by
app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

KINDS = ("preference_set", "preference_reset")
ROLE_CAPABILITY = "manage_users"


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Run one preference command for the job's sender; the line the page shows.

    (preference_set advisories_view {sort_by: due}) -> "Saved" ; (a wrong value) -> Refused
    naming the field ; (role defaults without the capability) -> Refused
    """
    command = job.command
    if command is None or command.kind not in KINDS:
        raise ValueError("the job carries no preference command")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    area, role = str(payload.get("area") or ""), payload.get("role")
    async with (session_factory or async_session)() as db:
        # Step 1: The sender's own record decides, so a forged job cannot write for someone
        # else: the subject is always the sender, or a role when the sender may manage users.
        record = await access.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        if role is not None and not access.decide(record, ROLE_CAPABILITY).is_allowed:
            raise Refused("a role's defaults are set by people who may manage users")
        subject = ("role", str(role)) if role is not None else ("user", record.object_id)
        # Step 2: Check the values before anything is stored, so a refusal leaves no row.
        try:
            if command.kind == "preference_reset":
                await store.reset(db, job.customer_code, *subject, area)
                text = f"{_title(area)} back to the default"
            else:
                settings = dict(payload.get("settings") or {})
                await _check_site(db, record, area, settings, is_own=role is None)
                await store.save(db, job.customer_code, *subject, area, settings,
                                 by=record.object_id, now=now)
                text = f"Saved · {_title(area)}"
        except ValueError as exc:
            raise Refused(str(exc)) from None
        await access.record_audit_event(
            db, job.customer_code, actor_object_id=record.object_id, actor_name=job.sender_name,
            action=command.kind, decision="allowed", subject_kind=f"preference_{subject[0]}",
            subject_id=subject[1], site_id=job.site_id, request_id=job.job_id,
            text=f"{job.sender_name or record.object_id}: {text}", detail={"area": area}, at=now)
        await db.commit()
        # Step 3: Mirror everyone whose resolved values moved, so their apps follow at once.
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            for object_id in await _affected(db, job.customer_code, record, role):
                try:
                    await prefs_mirror.mirror_user(db, writer, job.customer_code, object_id,
                                                   now=now)
                except Exception:
                    logger.warning("preferences of %s not mirrored after %s", object_id,
                                   command.kind, exc_info=True)
    return CommandOutcome(text)


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


async def _affected(db: AsyncSession, customer_code: str, record: access.AccessRecord,
                    role) -> list[str]:
    """Whose row to mirror: the sender for their own change, everyone of the role for a role's."""
    if role is None:
        return [record.object_id]
    return [u.object_id for u in await access.list_users(db, customer_code)
            if u.role_code == role and u.object_id and u.status == "active"]
