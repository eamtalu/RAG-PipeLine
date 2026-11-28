"""What people do with groups and channels (stage D, step 12), arriving as commands on the
question queue: an Admin or an Ops manager (`configure_agents`) makes, changes or deletes a group,
adds a channel by its webhook, tests it or deletes it; anyone active joins or leaves a group.
A group is about some sites (program step 3) and nobody gives it more sites than they may see
themselves. Every change is one audit line; the GROUPS rows are mirrored at once. Used by
app/services/teams/commands.py.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.tenant_site import TenantSite
from app.services import access
from app.services.teams.command_outcome import CommandOutcome, Refused, receipt
from app.services.groups import routing, store
from app.services.groups import mirror as groups_mirror
from app.services.notifications import channel_secrets
from app.services.notifications.channels import get_channel
from app.services.notifications.events import NotificationEvent
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings

logger = logging.getLogger(__name__)

KINDS = ("group_set", "group_delete", "group_join", "group_leave", "channel_set", "channel_delete", "channel_test")
MANAGING = ("group_set", "group_delete", "channel_set", "channel_delete", "channel_test")


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Run one group or channel command for the job's sender; the line the page shows.

    (ops manager: group_set Stock) -> "group Stock saved · 1 channel · 2 members" ;
    (picker: group_set) -> Refused ; (anyone: group_join) -> "joined Stock"
    """
    command = job.command
    if command is None or command.kind not in KINDS:
        raise ValueError("the job carries no group command")
    now = now or datetime.now(timezone.utc)
    actor = access.Actor(job.sender_object_id, job.sender_name)
    factory = session_factory or async_session
    async with factory() as db:
        # Step 1: The actor's own record decides: managing needs configure_agents, which Admins and
        # Ops managers hold; joining and leaving need an active record only
        record = await access.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        if command.kind in MANAGING:
            decision = access.decide(record, "configure_agents")
            if not decision.is_allowed:
                raise Refused(decision.reason or "managing groups and channels needs configure_agents")
        # a confirmed Ask form (step 18) was unwrapped into its own fields by the router
        payload = dict(command.payload)
        handler = _HANDLERS[command.kind]
        try:
            if command.kind == "group_set":
                # resolved here, because which sites a group may get depends on who asks
                payload["_sites"] = await _wanted_sites(db, record, payload)
            text, subject_kind, subject_id, detail = await handler(db, job, actor, payload, now)
        except ValueError as exc:
            raise Refused(str(exc))
        await access.record_audit_event(
            db, job.customer_code, actor_object_id=actor.object_id, actor_name=actor.name, action=f"{command.kind}",
            capability_code="configure_agents" if command.kind in MANAGING else None, decision="allowed",
            subject_kind=subject_kind, subject_id=subject_id, request_id=job.job_id,
            text=f"{actor.name or actor.object_id}: {text}", detail=detail, at=now)
        await db.commit()
        # Step 2: Mirror now, so the Admin tab sees the change on its next poll
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await groups_mirror.mirror_customer(db, writer, job.customer_code, now=now, people=[actor.object_id])
            except Exception:
                logger.warning("group rows for %s not mirrored after %s", job.customer_code, command.kind, exc_info=True)
        return CommandOutcome(text, (receipt(text, reference=subject_id, now=now),))


async def _wanted_sites(db: AsyncSession, record: access.AccessRecord, payload: dict) -> store.GroupSites | None:
    """The sites a group_set asks for, refused when they reach past the person asking; None to
    leave an existing group's sites as they are.

    (Bea of BRI, new group, nothing named) -> BRI ; (Bea, site_ids [BRI, LON]) -> ValueError ;
    (Bea, an all-sites group, nothing named) -> None ; (Olu of all sites, all_sites) -> all
    """
    group_id, all_sites, site_ids = payload.get("group_id"), payload.get("all_sites"), payload.get("site_ids")
    # Step 1: Nothing named keeps what the group has; a new group starts with its maker's own
    # reach, never wider, so a form that does not know about sites cannot open one up
    if all_sites is None and site_ids is None:
        if group_id:
            return None
        return store.GroupSites.of(record.all_sites, record.site_ids)
    wanted = store.GroupSites.of(bool(all_sites), [str(s).strip() for s in (site_ids or []) if str(s).strip()])
    current = await store.sites_of(db, record.customer_code, group_id) if group_id else None
    # Step 2: What the group already has may stay, because keeping it widens nothing; only
    # what is added must be inside the asker's own sites
    if wanted.all_sites:
        if not record.all_sites and not (current is not None and current.all_sites):
            raise ValueError("a group about every site is made by someone who may see every site")
        return wanted
    if not wanted.site_ids:
        raise ValueError("a group needs at least one site, or every site")
    known = {site.site_id: site.active for site in (await db.execute(select(TenantSite).where(
        TenantSite.customer_code == record.customer_code))).scalars()}
    kept = set(current.site_ids) if current is not None and not current.all_sites else set()
    for site in wanted.site_ids:
        if site in kept:
            continue
        if site not in known:
            raise ValueError(f"{site} is not a site of this customer")
        if not known[site]:
            raise ValueError(f"{site} is retired; a group is about sites in use")
        if not record.may_see(site):
            raise ValueError(f"{site} is not one of your sites")
    return wanted


async def _group_set(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    group = await store.set_group(
        db, job.customer_code, group_id=payload.get("group_id"), name=str(payload.get("name") or ""),
        topics=payload.get("topics"), min_severity=payload.get("min_severity"), channel_ids=payload.get("channel_ids"),
        member_object_ids=payload.get("members"), actor_object_id=actor.object_id, now=now,
        max_per_hour=payload.get("max_per_hour"), sites=payload.get("_sites"))
    sites = await store.sites_of(db, job.customer_code, group.id) or store.GroupSites()
    shown = routing.display_name(group.name, sites.all_sites, sites.site_ids)
    channels, members = len(payload.get("channel_ids") or []), len(payload.get("members") or [])
    text = (f"group {shown} saved · {channels} channel{'s' if channels != 1 else ''} · "
            f"{members} member{'s' if members != 1 else ''} · {group.max_per_hour} card{'s' if group.max_per_hour != 1 else ''} an hour")
    return text, "group", str(group.id), {"topics": group.topics, "min_severity": group.min_severity,
                                           "max_per_hour": group.max_per_hour,
                                           "all_sites": sites.all_sites, "site_ids": list(sites.site_ids)}


async def _group_delete(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    group = await store.delete_group(db, job.customer_code, str(payload.get("group_id") or ""))
    return f"group {group.name} deleted", "group", str(group.id), None


async def _group_join(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    group = await store.set_membership(db, job.customer_code, str(payload.get("group_id") or ""), actor.object_id,
                                       join=True, actor_object_id=actor.object_id, now=now)
    return f"joined {group.name}", "group", str(group.id), None


async def _group_leave(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    group = await store.set_membership(db, job.customer_code, str(payload.get("group_id") or ""), actor.object_id,
                                       join=False, actor_object_id=actor.object_id, now=now)
    return f"left {group.name}", "group", str(group.id), None


async def _channel_set(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    """Add a Teams channel by its webhook, or rename one / replace its webhook; the webhook goes
    into the secret store, never into the row."""
    name = str(payload.get("name") or "").strip()[:128]
    if not name:
        raise ValueError("a channel needs a name")
    channel_type = str(payload.get("channel_type") or "teams").strip().lower()
    if get_channel(channel_type) is None:
        raise ValueError(f"no adapter for channel type {channel_type!r}")
    webhook = str(payload.get("webhook_url") or "").strip()
    existing = None
    if payload.get("channel_id"):
        try:
            key = uuid.UUID(str(payload["channel_id"]))
        except ValueError:
            raise ValueError("channel_id is not a channel id")
        existing = await db.get(CustomerNotificationChannel, key)
        if existing is None or existing.customer_code != job.customer_code:
            raise ValueError("that channel no longer exists")
    if existing is None and not webhook.startswith("https://"):
        raise ValueError("a new channel needs its webhook URL (https://…)")
    channel_id = existing.id if existing is not None else uuid.uuid4()
    config = await channel_secrets.seal_config(
        db, customer_code=job.customer_code, channel_id=channel_id,
        config={"webhook_url": webhook} if webhook else {"webhook_url_set": True},
        previous=existing.config if existing is not None else None)
    if existing is None:
        db.add(CustomerNotificationChannel(id=channel_id, customer_code=job.customer_code, channel_type=channel_type,
                                           name=name, config=config, enabled=True))
    else:
        existing.name, existing.config, existing.enabled = name, config, bool(payload.get("enabled", existing.enabled))
    await db.flush()
    return f"channel {name} saved", "channel", str(channel_id), {"channel_type": channel_type}


async def _channel_delete(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    try:
        key = uuid.UUID(str(payload.get("channel_id") or ""))
    except ValueError:
        raise ValueError("channel_id is not a channel id")
    row = await db.get(CustomerNotificationChannel, key)
    if row is None or row.customer_code != job.customer_code:
        raise ValueError("that channel no longer exists")
    await channel_secrets.delete_for_channel(db, row)
    await db.delete(row)
    await db.flush()
    return f"channel {row.name} deleted", "channel", str(key), None


async def _channel_test(db: AsyncSession, job: QuestionJob, actor: access.Actor, payload: dict, now: datetime):
    """Send one test card straight to the channel, as the ops API's test does."""
    try:
        key = uuid.UUID(str(payload.get("channel_id") or ""))
    except ValueError:
        raise ValueError("channel_id is not a channel id")
    row = await db.get(CustomerNotificationChannel, key)
    if row is None or row.customer_code != job.customer_code:
        raise ValueError("that channel no longer exists")
    adapter = get_channel(row.channel_type)
    if adapter is None:
        raise ValueError(f"no adapter for channel type {row.channel_type!r}")
    event = NotificationEvent(event_type="test", customer_code=job.customer_code, severity="info",
                              title=f"eSmart Eye test from {actor.name or actor.object_id}",
                              summary="If you can read this, the channel is wired.", dedup_key=f"test:{key}:{uuid.uuid4()}")
    try:
        await adapter.send(event, await channel_secrets.reveal_config(db, row))
    except Exception as exc:  # noqa: BLE001 - the person reads the reason
        raise ValueError(f"the test card did not land: {exc}")
    return f"test card sent to {row.name}", "channel", str(key), None


_HANDLERS = {
    "group_set": _group_set,
    "group_delete": _group_delete,
    "group_join": _group_join,
    "group_leave": _group_leave,
    "channel_set": _channel_set,
    "channel_delete": _channel_delete,
    "channel_test": _channel_test,
}
