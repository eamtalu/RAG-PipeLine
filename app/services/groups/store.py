"""The notification groups tables (stage D, step 12): making, changing and removing groups, their
channels and their members, and the one lookup the dispatchers make: which channels and which
people take a finding of a topic and severity. Used by commands.py, mirror.py and the advisories
and tasks runners.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import (
    NotificationGroup,
    NotificationGroupChannel,
    NotificationGroupMember,
)
from app.services.groups import routing


@dataclass(frozen=True)
class Route:
    """Where a finding goes: the groups that took it, their channels, and their members."""

    group_ids: tuple[str, ...] = ()
    group_names: tuple[str, ...] = ()
    channel_ids: tuple[str, ...] = ()
    member_object_ids: tuple[str, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.group_ids


@dataclass
class GroupView:
    """A group with what hangs off it, as the store returns it."""

    group: NotificationGroup
    channel_ids: list[uuid.UUID] = field(default_factory=list)
    member_object_ids: list[str] = field(default_factory=list)


async def list_groups(db: AsyncSession, customer_code: str, *, include_disabled: bool = True) -> list[GroupView]:
    """Every group of a customer with its channels and members, by name."""
    q = select(NotificationGroup).where(NotificationGroup.customer_code == customer_code)
    if not include_disabled:
        q = q.where(NotificationGroup.enabled.is_(True))
    groups = list((await db.execute(q.order_by(NotificationGroup.name))).scalars())
    if not groups:
        return []
    ids = [g.id for g in groups]
    channels = (await db.execute(select(NotificationGroupChannel).where(NotificationGroupChannel.group_id.in_(ids)))).scalars()
    members = (await db.execute(select(NotificationGroupMember).where(NotificationGroupMember.group_id.in_(ids)))).scalars()
    views = {g.id: GroupView(group=g) for g in groups}
    for c in channels:
        views[c.group_id].channel_ids.append(c.channel_id)
    for m in members:
        views[m.group_id].member_object_ids.append(m.object_id)
    return [views[g.id] for g in groups]


async def get_group(db: AsyncSession, customer_code: str, group_id: str) -> NotificationGroup | None:
    try:
        key = uuid.UUID(str(group_id))
    except ValueError:
        return None
    return await db.scalar(select(NotificationGroup).where(
        NotificationGroup.customer_code == customer_code, NotificationGroup.id == key))


async def customer_channels(db: AsyncSession, customer_code: str) -> list[CustomerNotificationChannel]:
    return list((await db.execute(
        select(CustomerNotificationChannel).where(CustomerNotificationChannel.customer_code == customer_code)
        .order_by(CustomerNotificationChannel.name))).scalars())


async def set_group(db: AsyncSession, customer_code: str, *, group_id: str | None, name: str, topics, min_severity,
                    channel_ids, member_object_ids, actor_object_id: str, now: datetime) -> NotificationGroup:
    """Make or change a group with its channels and members; ValueError for a bad field or a
    channel that is not the customer's.

    (None, "Stock", ["advisory:stock"], "medium", [ch1], ["u-1"]) -> a new group ; (id, ...) -> changed
    """
    # Step 1: Check every field before touching anything, so a half-made group never lingers
    clean_name = name.strip()[:80]
    if not clean_name:
        raise ValueError("a group needs a name")
    clean_topics = routing.clean_topics(topics)
    severity = routing.clean_severity(min_severity)
    own = {c.id for c in await customer_channels(db, customer_code)}
    wanted_channels: list[uuid.UUID] = []
    for raw in channel_ids or []:
        try:
            key = uuid.UUID(str(raw))
        except ValueError:
            raise ValueError(f"channel {raw!r} is not a channel id")
        if key not in own:
            raise ValueError(f"channel {raw!r} is not one of this customer's channels")
        wanted_channels.append(key)
    members = sorted({str(m).strip() for m in (member_object_ids or []) if str(m).strip()})
    # Step 2: Write the group, then its channels and members afresh, so an edit is one shape
    if group_id:
        group = await get_group(db, customer_code, group_id)
        if group is None:
            raise ValueError("that group no longer exists")
        group.name, group.topics, group.min_severity, group.updated_at = clean_name, clean_topics, severity, now
        await db.execute(delete(NotificationGroupChannel).where(NotificationGroupChannel.group_id == group.id))
        await db.execute(delete(NotificationGroupMember).where(NotificationGroupMember.group_id == group.id))
    else:
        clash = await db.scalar(select(NotificationGroup).where(
            NotificationGroup.customer_code == customer_code, NotificationGroup.name == clean_name))
        if clash is not None:
            raise ValueError(f"a group named {clean_name!r} already exists")
        group = NotificationGroup(id=uuid.uuid4(), customer_code=customer_code, name=clean_name, topics=clean_topics,
                                  min_severity=severity, enabled=True, created_by=actor_object_id,
                                  created_at=now, updated_at=now)
        db.add(group)
        await db.flush()
    db.add_all([NotificationGroupChannel(group_id=group.id, channel_id=c) for c in wanted_channels])
    db.add_all([NotificationGroupMember(group_id=group.id, object_id=m, added_by=actor_object_id, added_at=now)
                for m in members])
    await db.flush()
    return group


async def delete_group(db: AsyncSession, customer_code: str, group_id: str) -> NotificationGroup:
    """Remove a group for good (its channels and members go with it); ValueError when unknown."""
    group = await get_group(db, customer_code, group_id)
    if group is None:
        raise ValueError("that group no longer exists")
    await db.delete(group)
    await db.flush()
    return group


async def set_membership(db: AsyncSession, customer_code: str, group_id: str, object_id: str, *, join: bool,
                         actor_object_id: str, now: datetime) -> NotificationGroup:
    """A person joins or leaves a group; ValueError when the group is unknown."""
    group = await get_group(db, customer_code, group_id)
    if group is None:
        raise ValueError("that group no longer exists")
    row = await db.scalar(select(NotificationGroupMember).where(
        NotificationGroupMember.group_id == group.id, NotificationGroupMember.object_id == object_id))
    if join and row is None:
        db.add(NotificationGroupMember(group_id=group.id, object_id=object_id, added_by=actor_object_id, added_at=now))
    if not join and row is not None:
        await db.delete(row)
    await db.flush()
    return group


async def routes_for(db: AsyncSession, customer_code: str, topic: str, severity: str) -> Route:
    """The channels and the people that take a finding of this topic and severity, across every
    enabled group; empty when no group takes it (so nothing goes out).

    ("advisory:stock", "high" with a Stock group on ch1 with u-1) -> Route(("<id>",), ("Stock",), ("ch1",), ("u-1",))
    """
    groups = [v for v in await list_groups(db, customer_code, include_disabled=False)
              if routing.admits(list(v.group.topics or []), v.group.min_severity, topic, severity)]
    channels: list[str] = []
    members: list[str] = []
    for v in groups:
        channels.extend(str(c) for c in v.channel_ids if str(c) not in channels)
        members.extend(m for m in v.member_object_ids if m not in members)
    return Route(group_ids=tuple(str(v.group.id) for v in groups), group_names=tuple(v.group.name for v in groups),
                 channel_ids=tuple(channels), member_object_ids=tuple(members))


async def group_route(db: AsyncSession, customer_code: str, group_id) -> Route:
    """One named group's channels and members (a schedule's target), or empty when it is gone or off."""
    if not group_id:
        return Route()
    views = [v for v in await list_groups(db, customer_code, include_disabled=False) if str(v.group.id) == str(group_id)]
    if not views:
        return Route()
    v = views[0]
    return Route((str(v.group.id),), (v.group.name,), tuple(str(c) for c in v.channel_ids), tuple(v.member_object_ids))


async def groups_of(db: AsyncSession, customer_code: str, object_id: str) -> list[NotificationGroup]:
    """The groups a person belongs to, by name."""
    rows = (await db.execute(
        select(NotificationGroup).join(NotificationGroupMember, NotificationGroupMember.group_id == NotificationGroup.id)
        .where(NotificationGroup.customer_code == customer_code, NotificationGroupMember.object_id == object_id)
        .order_by(NotificationGroup.name))).scalars()
    return list(rows)


def group_payload(view: GroupView, channel_names: dict[uuid.UUID, str]) -> dict:
    """One group as the Admin tab draws it."""
    g = view.group
    return {
        "id": str(g.id), "name": g.name, "topics": list(g.topics or []), "min_severity": g.min_severity,
        "enabled": g.enabled, "created_by": g.created_by,
        "channels": [{"id": str(c), "name": channel_names.get(c, "?")} for c in view.channel_ids],
        "members": list(view.member_object_ids),
        "created_at": g.created_at.astimezone(timezone.utc).isoformat() if g.created_at else None,
        "updated_at": g.updated_at.astimezone(timezone.utc).isoformat() if g.updated_at else None,
    }
