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

from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.services import access

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


def may_read(record: access.AccessRecord | None) -> bool:
    """Whether a record may read usage: it manages users or approves.

    (admin) -> True ; (supervisor) -> False ; (None) -> False
    """
    if record is None or not record.is_active:
        return False
    return access.decide(record, "manage_users").is_allowed or access.decide(record, "approve").is_allowed


async def usage_rows(db: AsyncSession, customer_code: str, *, days: int, now: datetime) -> dict[str, Any]:
    """The counts for the window: active people by role, questions per day, top askers, actions,
    acknowledgements.

    ("acme", 7 days) -> {"days": 7, "people": {...}, "questions_per_day": [...], "top_askers": [...], ...}
    """
    since = now - timedelta(days=days)
    # Step 1: Who was active, by role, against everyone who has access, so "12 of 30" reads at once;
    # access control owns those tables, so it counts them (chunk 219)
    people = await access.people_activity(db, customer_code, since=since)
    names = people.names
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
    # Step 3: The actions and the acknowledgements, from the audit log access control keeps
    by_action = await access.audit_action_counts(db, customer_code, since=since, limit=TOP * 2)
    acknowledged = await access.audit_actor_counts(db, customer_code, since=since,
                                                   action="advisory.acknowledge", limit=TOP)
    return {
        "days": days, "since": since.isoformat(),
        "people": {"with_access": people.with_access, "active_in_window": people.active_in_window,
                   "by_role": [{"role": r, "active": n} for r, n in sorted(people.by_role.items())]},
        "questions_per_day": [{"day": d.date().isoformat(), "questions": n} for d, n in per_day.all()],
        "top_askers": [{"name": names.get(oid, oid), "questions": n} for oid, n in per_person.all()],
        "actions": [{"action": a, "count": n} for a, n in by_action],
        "advisories_acknowledged": [{"name": name or names.get(oid, oid), "count": n}
                                    for oid, name, n in acknowledged],
    }


async def run_usage_tool(db: AsyncSession, customer_code: str, actor_object_id: str, name: str,
                         args: Mapping[str, Any], *, now: datetime | None = None) -> str:
    """Run the usage tool for the asker: the counts as JSON, or the problem.

    ("app_usage", {"days": 7}) -> '{"days": 7, "people": {...}, ...}' ; (a supervisor) -> problems
    """
    if name != APP_USAGE:
        return json.dumps({"problems": [f"unknown usage tool {name!r}"]})
    record = await access.access_record_for(db, customer_code, actor_object_id)
    if not may_read(record):
        return json.dumps({"problems": ["usage figures are for people who manage users or approve requests"]})
    try:
        days = max(1, min(MAX_DAYS, int(args.get("days") or 7)))
    except (TypeError, ValueError):
        days = 7
    now = now or datetime.now(timezone.utc)
    return json.dumps(await usage_rows(db, customer_code, days=days, now=now), default=str)
