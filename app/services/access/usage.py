"""How the app is used, as access control knows it (step 18's usage tool): who has access, who was
active in a window and in which role, and which audited actions happened. The usage tool in
app/services/analytics_agent/usage_tools.py adds the questions from the conversation turns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import AuditEvent, TeamsUser


@dataclass(frozen=True)
class PeopleActivity:
    """The people of a customer against a window: everyone active, those seen, by role, and names."""

    with_access: int
    active_in_window: int
    by_role: dict[str, int] = field(default_factory=dict)
    names: dict[str, str] = field(default_factory=dict)  # object id -> display name


async def people_activity(db: AsyncSession, customer_code: str, *, since: datetime) -> PeopleActivity:
    """Everyone with active access and who of them asked since `since`, counted by role.

    ("acme" with 5 active, 1 seen since Monday) -> PeopleActivity(5, 1, {"supervisor": 1}, names)
    """
    people = list((await db.execute(select(TeamsUser).where(TeamsUser.customer_code == customer_code))).scalars())
    active = [u for u in people if u.status == "active"]
    seen = [u for u in active if u.last_active_at is not None and u.last_active_at >= since]
    by_role: dict[str, int] = {}
    for u in seen:
        by_role[u.role_code] = by_role.get(u.role_code, 0) + 1
    return PeopleActivity(with_access=len(active), active_in_window=len(seen), by_role=by_role,
                          names={u.object_id: u.display_name for u in people if u.object_id})


async def audit_action_counts(db: AsyncSession, customer_code: str, *, since: datetime,
                              limit: int) -> list[tuple[str, int]]:
    """The audited actions since `since` that were not refused, most frequent first.

    ("acme", since Monday, 10) -> [("advisory.acknowledge", 4), ("access.user_created", 1)]
    """
    rows = await db.execute(
        select(AuditEvent.action, func.count())
        .where(AuditEvent.customer_code == customer_code, AuditEvent.at >= since, AuditEvent.decision != "refused")
        .group_by(AuditEvent.action).order_by(func.count().desc()).limit(limit))
    return [(a, n) for a, n in rows.all()]


async def audit_actor_counts(db: AsyncSession, customer_code: str, *, since: datetime, action: str,
                             limit: int) -> list[tuple[str, str | None, int]]:
    """Who did one audited action since `since`, most often first: (object id, name, count).

    ("acme", since Monday, "advisory.acknowledge", 5) -> [("u-sup", "Sam", 3)]
    """
    rows = await db.execute(
        select(AuditEvent.actor_object_id, AuditEvent.actor_name, func.count())
        .where(AuditEvent.customer_code == customer_code, AuditEvent.at >= since, AuditEvent.action == action)
        .group_by(AuditEvent.actor_object_id, AuditEvent.actor_name).order_by(func.count().desc()).limit(limit))
    return [(oid, name, n) for oid, name, n in rows.all()]
