"""The usage tool (stage D, step 18): how the app is used, from what it already records: who was
active and when (teams_users), the questions asked (teams_conversation_turns), the actions run and
the advisories acknowledged (audit_events). Offered only to a record that may manage users or
approve, since it names people. Every figure is a count the tables gave.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import AuditEvent, TeamsUser
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.services.access import records

APP_USAGE = "app_usage"
USAGE_TOOL_NAMES = frozenset({APP_USAGE})
MAX_DAYS = 90
TOP = 10

USAGE_TOOLS: list[dict[str, Any]] = [
    {
        "name": APP_USAGE,
        "description": (
            "How eSmart Eye itself is used over the last days: how many people were active and by role, "
            "how many questions were asked each day and who asked the most, which actions were run and "
            "how often, and which advisories were acknowledged and by whom. Call it for questions about "
            "the app's users, traffic or activity (\"how many people used it in the last 7 days\"); it is "
            "not for the warehouse's own figures. Answer only with the counts it returns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"days": {"type": "integer", "description": "How many days back, 1 to 90; 7 when not said."}},
            "additionalProperties": False,
        },
    }
]


def may_read(record: records.AccessRecord | None) -> bool:
    """Whether a record may read usage: it manages users or approves.

    (admin) -> True ; (supervisor) -> False ; (None) -> False
    """
    if record is None or not record.is_active:
        return False
    return records.decide(record, "manage_users").is_allowed or records.decide(record, "approve").is_allowed


async def usage_rows(db: AsyncSession, customer_code: str, *, days: int, now: datetime) -> dict[str, Any]:
    """The counts for the window: active people by role, questions per day, top askers, actions,
    acknowledgements.

    ("acme", 7 days) -> {"days": 7, "people": {...}, "questions_per_day": [...], "top_askers": [...], ...}
    """
    since = now - timedelta(days=days)
    # Step 1: Who was active, by role, against everyone who has access, so "12 of 30" reads at once
    people = list((await db.execute(select(TeamsUser).where(TeamsUser.customer_code == customer_code))).scalars())
    active = [u for u in people if u.status == "active"]
    seen = [u for u in active if u.last_active_at is not None and u.last_active_at >= since]
    by_role: dict[str, int] = {}
    for u in seen:
        by_role[u.role_code] = by_role.get(u.role_code, 0) + 1
    names = {u.object_id: u.display_name for u in people}
    # Step 2: The questions, per day and per person, from the stored turns
    per_day = await db.execute(
        select(func.date_trunc("day", TeamsConversationTurn.created_at).label("day"), func.count())
        .where(TeamsConversationTurn.customer_code == customer_code, TeamsConversationTurn.role == "user",
               TeamsConversationTurn.created_at >= since)
        .group_by("day").order_by("day"))
    per_person = await db.execute(
        select(TeamsConversationTurn.sender_object_id, func.count())
        .where(TeamsConversationTurn.customer_code == customer_code, TeamsConversationTurn.role == "user",
               TeamsConversationTurn.created_at >= since, TeamsConversationTurn.sender_object_id.is_not(None))
        .group_by(TeamsConversationTurn.sender_object_id).order_by(func.count().desc()).limit(TOP))
    # Step 3: The actions and the acknowledgements, from the audit
    by_action = await db.execute(
        select(AuditEvent.action, func.count())
        .where(AuditEvent.customer_code == customer_code, AuditEvent.at >= since, AuditEvent.decision != "refused")
        .group_by(AuditEvent.action).order_by(func.count().desc()).limit(TOP * 2))
    acknowledged = await db.execute(
        select(AuditEvent.actor_object_id, AuditEvent.actor_name, func.count())
        .where(AuditEvent.customer_code == customer_code, AuditEvent.at >= since,
               AuditEvent.action == "advisory.acknowledge")
        .group_by(AuditEvent.actor_object_id, AuditEvent.actor_name).order_by(func.count().desc()).limit(TOP))
    return {
        "days": days, "since": since.isoformat(),
        "people": {"with_access": len(active), "active_in_window": len(seen),
                   "by_role": [{"role": r, "active": n} for r, n in sorted(by_role.items())]},
        "questions_per_day": [{"day": d.date().isoformat(), "questions": n} for d, n in per_day.all()],
        "top_askers": [{"name": names.get(oid, oid), "questions": n} for oid, n in per_person.all()],
        "actions": [{"action": a, "count": n} for a, n in by_action.all()],
        "advisories_acknowledged": [{"name": name or names.get(oid, oid), "count": n}
                                    for oid, name, n in acknowledged.all()],
    }


async def run_usage_tool(db: AsyncSession, customer_code: str, actor_object_id: str, name: str,
                         args: Mapping[str, Any]) -> str:
    """Run the usage tool for the asker: the counts as JSON, or the problem.

    ("app_usage", {"days": 7}) -> '{"days": 7, "people": {...}, ...}' ; (a supervisor) -> problems
    """
    if name != APP_USAGE:
        return json.dumps({"problems": [f"unknown usage tool {name!r}"]})
    record = await records.access_record_for(db, customer_code, actor_object_id)
    if not may_read(record):
        return json.dumps({"problems": ["usage figures are for people who manage users or approve requests"]})
    try:
        days = max(1, min(MAX_DAYS, int(args.get("days") or 7)))
    except (TypeError, ValueError):
        days = 7
    return json.dumps(await usage_rows(db, customer_code, days=days, now=datetime.now(timezone.utc)), default=str)
