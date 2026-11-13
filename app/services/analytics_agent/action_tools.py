"""The action tools (stage D, step 8): how the model proposes an action for the person to confirm
(creating a user, a reminder, a schedule), and since step 14c acts on the watchlist (watch_tools.py). The model never creates anything; it calls
`propose_create_user` with what the person said, the runner reads the actor's own record (may
they manage users, and does the matrix want an approval), the customer's sites and the roles, and
answers with the form block, pre-filled, that the page shows for confirmation. The confirmed form
travels back as the `access_set_user` command, which checks, queues, audits and answers with a
receipt. A person who may not create users gets `problems`, as the data tools give.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.access import matrix as matrix_file
from app.services.access import records
from app.services.teams.contracts import FormBlock, FormField
from app.services.teams.sites import list_sites

from app.services.analytics_agent.watch_tools import WATCH_TOOL_NAMES, WATCH_TOOLS, run_watch_tool

PROPOSE_CREATE_USER = "propose_create_user"
PROPOSE_REMINDER = "propose_reminder"
PROPOSE_SCHEDULE = "propose_schedule"
ACTION_TOOL_NAMES = frozenset({PROPOSE_CREATE_USER, PROPOSE_REMINDER, PROPOSE_SCHEDULE}) | WATCH_TOOL_NAMES

SHIFTS = ["Night · 22:00 – 06:00", "Day · 06:00 – 14:00", "Late · 14:00 – 22:00"]

ACTION_TOOLS: list[dict[str, Any]] = [
    {
        "name": PROPOSE_CREATE_USER,
        "description": (
            "Propose a new user of this app for the person to confirm, when they ask to create, add or "
            "set up a user, account or login for someone (a picker, a supervisor, a colleague). Call it "
            "with what they said: the person's name, and the role, site, shift or email when given. "
            "Never say the user was created: the form you get back is what the person confirms, and "
            "the answer says what happens next (done, or sent for approval). Then write one sentence "
            "inviting them to check the form and confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "display_name": {"type": "string", "description": "The new person's full name."},
                "role": {
                    "type": "string",
                    "description": "picker | shift_lead | supervisor | ops_manager | executive | admin; "
                    "picker when not said.",
                },
                "site_id": {"type": "string", "description": "The site id, e.g. BRI; the asker's site when not said."},
                "email": {"type": "string", "description": "Work email when given."},
                "shift": {"type": "string", "description": "night | day | late, when said."},
                "starts_on": {"type": "string", "description": "YYYY-MM-DD when a start date was said."},
                "note": {"type": "string", "description": "Anything else said about the person, briefly."},
            },
            "required": ["display_name"],
            "additionalProperties": False,
        },
        "side_effects": False,
    },
    {
        "name": PROPOSE_REMINDER,
        "description": (
            "Propose a reminder for the person to confirm, when they ask to be reminded of something at a "
            "time (\"remind me at 5 to check Route 14\", \"tomorrow at 9 remind me to plan cover\"). Call "
            "it with what they said: what to be reminded of, the time, the day when said, and whether it "
            "repeats. The form that comes back is what they confirm; never say the reminder is set. If "
            "their earlier question should come back with the reminder, pass it as attach_question."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "What to be reminded of, in their words."},
                "time": {"type": "string", "description": "The clock time, e.g. 17:45 or 5pm."},
                "date": {"type": "string", "description": "YYYY-MM-DD when a day was said (tomorrow, Monday)."},
                "repeat": {"type": "string", "description": "once | daily | weekdays; once when not said."},
                "attach_question": {"type": "string",
                                    "description": "A question to re-run when it goes off, when one fits."},
                "note": {"type": "string", "description": "Anything else said, briefly."},
            },
            "required": ["title"],
            "additionalProperties": False,
        },
        "side_effects": False,
    },
    {
        "name": PROPOSE_SCHEDULE,
        "description": (
            "Propose a schedule for the person to confirm, when they ask for something to be sent or run "
            "automatically on a rhythm (\"send me the short products list every weekday at 4pm\", \"every "
            "Monday at 8 post the deliveries graph in the channel\"). Not for a figure crossing a line "
            "(\"tell me if fill rate drops below 97%\" is a watch, not a schedule). Call it with what they "
            "said. The form that comes back is what they confirm; never say the schedule is set."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "what": {"type": "string", "description": "The question to run, in their words."},
                "how_often": {"type": "string",
                              "description": "weekdays | daily | weekly | once."},
                "weekday": {"type": "string", "description": "The day for a weekly one, e.g. Monday."},
                "time": {"type": "string", "description": "The clock time, e.g. 16:00 or 4pm."},
                "send_to": {"type": "string", "description": "me | channel; me when not said."},
                "show_as": {"type": "string", "description": "table | graph | summary; table when not said."},
                "until": {"type": "string", "description": "YYYY-MM-DD when an end was said."},
            },
            "required": ["what"],
            "additionalProperties": False,
        },
        "side_effects": False,
    },
    *WATCH_TOOLS,
]


def shift_text(raw: str | None) -> str | None:
    """The shift as the form offers it, from a word the model passed.

    >>> shift_text("night"), shift_text("Late shift"), shift_text(None)
    ('Night · 22:00 – 06:00', 'Late · 14:00 – 22:00', None)
    """
    if not raw:
        return None
    word = raw.strip().lower()
    for option in SHIFTS:
        if option.split(" ")[0].lower() in word:
            return option
    return None


async def create_user_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                           args: Mapping[str, Any]) -> FormBlock:
    """The pre-filled New user form for the actor, or ValueError with the reason they may not.

    (admin, {"display_name": "Dan Roberts", "shift": "night"}) -> FormBlock(requires_approval False,
    role picker, site the asker's, note "Night · 22:00 – 06:00") ; (picker) -> ValueError
    """
    # Step 1: The actor's own record decides whether a form is offered at all, and whether the
    # confirmed form will wait for an Ops manager
    record = await records.access_record_for(db, customer_code, actor_object_id)
    decision = records.decide(record, "manage_users")
    if decision.outcome == "refused":
        raise ValueError(f"you cannot create users: {decision.reason}")
    requires_approval = decision.outcome == "needs_approval"
    sites = await list_sites(db, customer_code)
    m = matrix_file.load()
    role = str(args.get("role") or "picker").strip().lower().replace(" ", "_")
    if role not in m.role_codes:
        role = "picker"
    site_id = str(args.get("site_id") or actor_site_id or (sites[0].site_id if sites else "")).strip()
    if sites and site_id not in {s.site_id for s in sites}:
        site_id = sites[0].site_id
    shift = shift_text(args.get("shift"))
    note_parts = [p for p in (shift, str(args.get("note") or "").strip() or None) if p]
    fields = [
        FormField(name="object_id", label="Entra object id", kind="text", required=True,
                  placeholder="from the person's Microsoft 365 account"),
        FormField(name="display_name", label="Full name", kind="text", required=True,
                  value=str(args.get("display_name") or "").strip()[:128]),
        FormField(name="email", label="Work email", kind="text", required=False,
                  value=(str(args.get("email") or "").strip() or None), placeholder="name@company.com"),
        FormField(name="role", label="Role", kind="select", options=list(m.role_codes), value=role),
        FormField(name="site_id", label="Site", kind="select", options=[s.site_id for s in sites], value=site_id or None,
                  required=bool(sites)),
        FormField(name="shift", label="Shift", kind="select", options=SHIFTS, value=shift, required=False),
        FormField(name="starts_on", label="Start date", kind="date", required=False,
                  value=(str(args.get("starts_on") or "").strip() or None)),
        FormField(name="note", label="Note", kind="text", required=False,
                  value=(" · ".join(note_parts) or None), placeholder="handheld only, covers Route 14…"),
    ]
    role_name = next((r.name for r in m.roles if r.code == role), role)
    return FormBlock(
        form_id="create_user",
        title="New user",
        fields=fields,
        requires_approval=requires_approval,
        submit_label="Create & send for approval" if requires_approval else "Create user",
        note=f"{role_name}: {_role_note(role)}" + (" Needs approval from an Ops manager." if requires_approval else ""),
    )


def _role_note(role: str) -> str:
    return {
        "picker": "can see their own site and ask with approval; cannot run actions, approve or export.",
        "shift_lead": "can see their own site and ask; actions need approval.",
        "supervisor": "can see their own site, ask and run actions; all sites, users and export need approval.",
        "ops_manager": "can see all sites, run actions, manage users, approve and configure.",
        "executive": "can see all sites and approve; read-only, no actions.",
        "admin": "can do everything, including the matrix.",
    }.get(role, "")


async def run_action_tool(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                          name: str, args: Mapping[str, Any], *, actor_name: str | None = None) -> str:
    """Run one action tool: the form block as JSON under "block", or the problems the model can fix.

    ("propose_create_user", {"display_name": "Dan"}) -> '{"shown": "form", "block": {...}}' ;
    (a picker asking) -> '{"problems": ["you cannot create users: …"]}'
    """
    if name in WATCH_TOOL_NAMES:
        return await run_watch_tool(db, customer_code, actor_object_id, actor_site_id, name, args, actor_name=actor_name)
    if name in (PROPOSE_REMINDER, PROPOSE_SCHEDULE):
        return await _propose_task(db, customer_code, actor_object_id, actor_site_id, name, args)
    if name != PROPOSE_CREATE_USER:
        return json.dumps({"problems": [f"unknown action tool {name!r}"]})
    if not str(args.get("display_name") or "").strip():
        return json.dumps({"problems": ["display_name is required: the new person's full name"]})
    try:
        block = await create_user_form(db, customer_code, actor_object_id, actor_site_id, args)
    except ValueError as exc:
        return json.dumps({"problems": [str(exc)]})
    return json.dumps({"shown": "form", "requires_approval": block.requires_approval,
                       "block": block.model_dump(mode="json")})


async def _propose_task(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                        name: str, args: Mapping[str, Any]) -> str:
    """The New reminder or New schedule form (step 10) for the actor, or the problems.

    ("propose_schedule", {"what": "short products list", "how_often": "weekdays", "time": "4pm"})
    -> '{"shown": "form", "block": {"form_id": "new_schedule", ...}}'
    """
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    from app.persistence.repositories.customer_repository import get_customer_timezone
    from app.services.tasks import forms

    record = await records.access_record_for(db, customer_code, actor_object_id)
    if record is None or not record.is_active:
        return json.dumps({"problems": ["you have no active access record"]})
    if name == PROPOSE_SCHEDULE and not str(args.get("what") or "").strip():
        return json.dumps({"problems": ["what is required: the question to run"]})
    if name == PROPOSE_REMINDER and not str(args.get("title") or "").strip():
        return json.dumps({"problems": ["title is required: what to be reminded of"]})
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    now = datetime.now(timezone.utc)
    if name == PROPOSE_REMINDER:
        block = forms.reminder_form(args, now=now, tz=tz, last_question=None)
    else:
        from app.services.tasks.commands import sendable_groups

        sites = await list_sites(db, customer_code)
        site_name = next((s.name for s in sites if s.site_id == actor_site_id), None)
        block = forms.schedule_form(args, now=now, tz=tz, last_question=None, site_name=site_name,
                                    groups=await sendable_groups(db, customer_code, actor_object_id))
    return json.dumps({"shown": "form", "requires_approval": False, "block": block.model_dump(mode="json")})
