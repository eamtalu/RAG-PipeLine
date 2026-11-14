"""The registry of actions Ask can run (stage D, step 18): each one entry, from the words the
model reads to the form the person confirms and the command it becomes. Real actions get one
`propose_<id>` tool each, so the model sees plainly named tools with typed arguments; the demo
actions share one `demo_action` tool whose description says what it is, so a demo can never be
taken for a real change. A required argument carries its own question, asked back when it is
missing. Used by app/services/analytics_agent/action_tools.py (the tools), the slash commands,
the catalogue row and the demo command.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.access import records
from app.services.ask import forms
from app.services.teams.contracts import FormBlock

REAL, DEMO = "real", "demo"
DEMO_TOOL = "demo_action"
Builder = Callable[[AsyncSession, str, str, str | None, Mapping[str, Any]], Awaitable[FormBlock]]


@dataclass(frozen=True)
class ActionSpec:
    """One action: its id, its slash command, what the model reads, its arguments (each required
    one with the question to ask when it is missing), the form it proposes, the capability it
    needs, the command a confirmed form becomes, whether it is real or a demo, and the words of
    a demo receipt."""

    id: str
    label: str
    slash: str
    description: str
    args: dict[str, dict[str, Any]]
    required: tuple[str, ...]
    form_id: str
    capability: str
    command_kind: str
    builder: Builder
    mode: str = REAL
    #: The demo receipt's words after "Demo · nothing was changed · ", with the form's values.
    summary: str = ""
    #: The question asked back for each required argument that is missing.
    asks: dict[str, str] = field(default_factory=dict)

    @property
    def tool_name(self) -> str:
        return f"propose_{self.id}"


def _arg(description: str, kind: str = "string") -> dict[str, Any]:
    return {"type": kind, "description": description}


ACTIONS: tuple[ActionSpec, ...] = (
    ActionSpec(
        id="create_user", label="Create a user", slash="/create-user",
        description=(
            "Propose a new user of this app for the person to confirm, when they ask to create, add or "
            "set up a user, account or login for someone (a picker, a supervisor, a colleague). Call it "
            "with what they said: the person's name, and the role, site, shift or email when given. "
            "Never say the user was created: the form you get back is what the person confirms, and "
            "the answer says what happens next (done, or sent for approval). Then write one sentence "
            "inviting them to check the form and confirm."
        ),
        args={
            "display_name": _arg("The new person's full name."),
            "role": _arg(
                "picker | shift_lead | supervisor | ops_manager | executive | admin; picker when not said."
            ),
            "site_id": _arg("The site id, e.g. BRI; the asker's site when not said."),
            "email": _arg("Work email when given."),
            "shift": _arg("night | day | late, when said."),
            "starts_on": _arg("YYYY-MM-DD when a start date was said."),
            "note": _arg("Anything else said about the person, briefly."),
        },
        required=("display_name",), asks={"display_name": "Who is the new user? Their full name, please."},
        form_id="create_user", capability="manage_users", command_kind="access_set_user",
        builder=forms.create_user_form,
    ),
    ActionSpec(
        id="change_user", label="Change a user", slash="/change-user",
        description=(
            "Propose a change to an existing user of this app for the person to confirm, when they ask "
            "to make someone a different role, move them to another site, give them all sites, or "
            "deactivate or remove them (\"make Sam a supervisor of Brighton\", \"deactivate Lee\"). Call "
            "it with the person's name as said and the change. Never say it is done: the form that "
            "comes back is what they confirm."
        ),
        args={
            "who": _arg("The person's name, as said."),
            "role": _arg("The new role code when a role was said."),
            "site_id": _arg("The site id when a site was said."),
            "status": _arg("deactivated when they asked to deactivate or remove; active otherwise."),
            "note": _arg("Anything else said, briefly."),
        },
        required=("who",), asks={"who": "Which user should I change? Their name, please."},
        form_id="change_user", capability="manage_users", command_kind="access_set_user",
        builder=forms.change_user_form,
    ),
    ActionSpec(
        id="decide_request", label="Decide a request", slash="/approve",
        description=(
            "Propose a decision on a waiting access request for the person to confirm, when they ask to "
            "approve or decline a request, often by its reference (\"approve USR-2041\"). Call it with "
            "the reference when given and whether they said approve or decline. Never say it is decided: "
            "the form that comes back is what they confirm."
        ),
        args={
            "reference": _arg("The request's reference, e.g. USR-2041, when said."),
            "decision": _arg("approve | decline."),
            "note": _arg("A note to the requester, when said."),
        },
        required=(), form_id="decide_request", capability="approve", command_kind="access_decide",
        builder=forms.decide_request_form,
    ),
    ActionSpec(
        id="set_group", label="Notification group", slash="/group",
        description=(
            "Propose a notification group for the person to confirm, when they ask for advisories or "
            "schedules to be sent somewhere (\"send high stock advisories to the stock-control channel\", "
            "\"make a group for deliveries\"). Call it with the group's name when said, the topics "
            "(delivery, stock, location, schedule), the lowest severity, and the channel's name. Never "
            "say it is set: the form that comes back is what they confirm."
        ),
        args={
            "name": _arg("The group's name when said."),
            "topics": {"type": "array", "items": {"type": "string"},
                       "description": "Any of delivery, stock, location, schedule."},
            "min_severity": _arg("high | medium | low."),
            "channel": _arg("The channel's name when said."),
        },
        required=(), form_id="set_group", capability="configure_agents", command_kind="group_set",
        builder=forms.set_group_form,
    ),
    ActionSpec(
        id="reminder", label="Reminder", slash="/remind-me",
        description=(
            "Propose a reminder for the person to confirm, when they ask to be reminded of something at a "
            "time (\"remind me at 5 to check Route 14\", \"tomorrow at 9 remind me to plan cover\"). Call "
            "it with what they said: what to be reminded of, the time, the day when said, and whether it "
            "repeats. The form that comes back is what they confirm; never say the reminder is set. If "
            "their earlier question should come back with the reminder, pass it as attach_question."
        ),
        args={
            "title": _arg("What to be reminded of, in their words."),
            "time": _arg("The clock time, e.g. 17:45 or 5pm."),
            "date": _arg("YYYY-MM-DD when a day was said (tomorrow, Monday)."),
            "repeat": _arg("once | daily | weekdays; once when not said."),
            "attach_question": _arg("A question to re-run when it goes off, when one fits."),
            "note": _arg("Anything else said, briefly."),
        },
        required=("title",), asks={"title": "What should I remind you of?"},
        form_id="new_reminder", capability="ask", command_kind="task_create", builder=forms.reminder_form_for,
    ),
    ActionSpec(
        id="schedule", label="Schedule", slash="/schedule",
        description=(
            "Propose a schedule for the person to confirm, when they ask for something to be sent or run "
            "automatically on a rhythm (\"send me the short products list every weekday at 4pm\", \"every "
            "Monday at 8 post the deliveries graph in the channel\"). Not for a figure crossing a line "
            "(\"tell me if fill rate drops below 97%\" is a watch, not a schedule). Call it with what they "
            "said. The form that comes back is what they confirm; never say the schedule is set."
        ),
        args={
            "what": _arg("The question to run, in their words."),
            "how_often": _arg("weekdays | daily | weekly | once."),
            "weekday": _arg("The day for a weekly one, e.g. Monday."),
            "time": _arg("The clock time, e.g. 16:00 or 4pm."),
            "send_to": _arg("me | channel; me when not said."),
            "show_as": _arg("table | graph | summary; table when not said."),
            "until": _arg("YYYY-MM-DD when an end was said."),
        },
        required=("what",), asks={"what": "What should I send, and how often?"},
        form_id="new_schedule", capability="ask", command_kind="task_create", builder=forms.schedule_form_for,
    ),
    ActionSpec(
        id="add_device", label="Add a handheld", slash="/add-device", mode=DEMO,
        description="add a handheld scanner and say who holds it (\"add a handheld for Fiona Nachon\")",
        args={"device_name": _arg("The handheld's name or number when said."),
              "login": _arg("The handheld login when said, e.g. FNACHONLEO."),
              "holder": _arg("Who holds it, as said."), "site_id": _arg("The site id when said.")},
        required=("holder",), asks={"holder": "Who will hold the handheld?"},
        form_id="add_device", capability="run_actions", command_kind="demo_act",
        builder=forms.add_device_form, summary="would add handheld {device_name} for {holder} at {site_id}",
    ),
    ActionSpec(
        id="reassign_picker", label="Move a picker", slash="/reassign", mode=DEMO,
        description="move a picker to another route (\"move Dan to Route 14\")",
        args={"picker": _arg("The picker's name, as said."),
              "from_route": _arg("The route they leave, when said."),
              "to_route": _arg("The route they go to.")},
        required=("picker", "to_route"),
        asks={"picker": "Which picker should move?", "to_route": "Which route should they move to?"},
        form_id="reassign_picker", capability="run_actions", command_kind="demo_act",
        builder=forms.reassign_picker_form, summary="would move {picker} to route {to_route}",
    ),
    ActionSpec(
        id="substitute_item", label="Substitute an item", slash="/substitute", mode=DEMO,
        description=(
            "substitute one item for another on the releases "
            "(\"substitute 104568 with 104570 today\")"
        ),
        args={"item": _arg("The item number that is short."),
              "substitute": _arg("The item that replaces it."),
              "note": _arg("A note for the pickers, when said.")},
        required=("item",), asks={"item": "Which item should be substituted?"},
        form_id="substitute_item", capability="run_actions", command_kind="demo_act",
        builder=forms.substitute_item_form,
        summary="would substitute item {item} with {substitute} on {releases} releases",
    ),
)

_BY_ID = {a.id: a for a in ACTIONS}
_BY_TOOL = {a.tool_name: a for a in ACTIONS if a.mode == REAL}
_BY_SLASH = {a.slash: a for a in ACTIONS}
DEMO_IDS = tuple(a.id for a in ACTIONS if a.mode == DEMO)
ACTION_FORM_IDS = frozenset(a.form_id for a in ACTIONS)


def spec_of(action_id: str) -> ActionSpec | None:
    """The spec of an id, or None.

    >>> spec_of("add_device").mode, spec_of("nope")
    ('demo', None)
    """
    return _BY_ID.get(action_id)


def spec_of_slash(slash: str) -> ActionSpec | None:
    """The spec a slash command names, or None.

    >>> spec_of_slash("/remind-me").id, spec_of_slash("/nope")
    ('reminder', None)
    """
    return _BY_SLASH.get(slash)


def tool_specs() -> list[dict[str, Any]]:
    """The tools the model is offered: one per real action, and one `demo_action` for every demo.

    >>> names = [t["name"] for t in tool_specs()]
    >>> names[:3], names[-1]
    (['propose_create_user', 'propose_change_user', 'propose_decide_request'], 'demo_action')
    """
    out: list[dict[str, Any]] = []
    for a in ACTIONS:
        if a.mode != REAL:
            continue
        out.append({
            "name": a.tool_name, "description": a.description,
            "input_schema": {"type": "object", "properties": dict(a.args), "required": list(a.required),
                             "additionalProperties": False},
            "side_effects": False,
        })
    demos = [a for a in ACTIONS if a.mode == DEMO]
    if demos:
        lines = "; ".join(f"{a.id}: {a.description}" for a in demos)
        out.append({
            "name": DEMO_TOOL,
            "description": (
                "DEMO ONLY: nothing is changed; returns a sample form for the person to see what this action "
                "would look like. Call it when the person asks for one of these, with what they said as "
                f"fields: {lines}. The form says it is a demo; say so too, and never say it was done."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "action_id": {"type": "string", "enum": [a.id for a in demos],
                                  "description": "Which demo action."},
                    "fields": {"type": "object", "description": "What the person said, by field name.",
                               "additionalProperties": True},
                },
                "required": ["action_id"],
                "additionalProperties": False,
            },
            "side_effects": False,
        })
    return out


def tool_names() -> frozenset[str]:
    """Every tool name this registry offers."""
    return frozenset(t["name"] for t in tool_specs())


def actions_payload() -> list[dict[str, Any]]:
    """The actions as the catalogue row lists them, for the command chips and the Browse sheet."""
    return [{"id": a.id, "label": a.label, "slash": a.slash, "form_id": a.form_id, "needs": a.capability,
             "mode": a.mode, "command_kind": a.command_kind} for a in ACTIONS]


def demo_text(action_id: str, values: Mapping[str, Any]) -> str:
    """The demo receipt's words, from the spec's summary and the confirmed values.

    >>> demo_text("add_device", {"device_name": "HH-0421", "holder": "Fiona Nachon", "site_id": "BRI"})
    'Demo · nothing was changed · would add handheld HH-0421 for Fiona Nachon at BRI'
    >>> demo_text("nope", {})
    'Demo · nothing was changed'
    """
    spec = _BY_ID.get(action_id)
    if spec is None or spec.mode != DEMO:
        return "Demo · nothing was changed"

    class _Blank(dict):
        def __missing__(self, key):
            return "…"

    filled = _Blank({k: (v or "…") for k, v in values.items()})
    return "Demo · nothing was changed · " + spec.summary.format_map(filled)


def missing_of(spec: ActionSpec, args: Mapping[str, Any]) -> str | None:
    """The first required argument the call left out, or None.

    >>> (missing_of(spec_of("reassign_picker"), {"picker": "Dan"}),
    ...  missing_of(spec_of("reminder"), {"title": "x"}))
    ('to_route', None)
    """
    return next((r for r in spec.required if not str(args.get(r) or "").strip()), None)


async def run(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
              name: str, args: Mapping[str, Any]) -> str:
    """Run one action tool: the form block as JSON under "block"; the question to ask back under
    "ask" when something required is missing; or the problems the model can fix.

    ("propose_create_user", {"display_name": "Dan"}) -> '{"shown": "form", "block": {...}}' ;
    ("propose_reminder", {}) -> '{"problems": ["title is required…"],
    "ask": "What should I remind you of?"}' ;
    ("demo_action", {"action_id": "add_device", "fields": {...}}) -> '{"shown": "form", "demo": true, ...}' ;
    (a picker asking) -> '{"problems": ["you cannot do that: …"]}'
    """
    # Step 1: Find the spec the tool names, and for the demo tool the action it was called for
    if name == DEMO_TOOL:
        spec = _BY_ID.get(str(args.get("action_id") or ""))
        if spec is None or spec.mode != DEMO:
            known = ", ".join(DEMO_IDS)
            problem = f"unknown demo action {args.get('action_id')!r}; one of {known}"
            return json.dumps({"problems": [problem]})
        args = dict(args.get("fields") or {})
    else:
        spec = _BY_TOOL.get(name)
        if spec is None:
            return json.dumps({"problems": [f"unknown action tool {name!r}"]})
    # Step 2: The actor's record decides before any form is drawn, so a refused person reads the
    # reason instead of a form they could not confirm; a demo still needs run_actions. The site
    # is not part of this gate because the form's own site field is what the command checks
    # (a BRI supervisor may propose a LON user and have it approved), as the step 8 tools did
    record = await records.access_record_for(db, customer_code, actor_object_id)
    if record is None or not record.is_active:
        return json.dumps({"problems": ["you have no active access record"]})
    decision = records.decide(record, spec.capability)
    if decision.outcome == "refused":
        return json.dumps({"problems": [f"you cannot do that: {decision.reason}"]})
    # Step 3: Something required and not said is asked back as one question, so a command typed
    # without it ("/remind me") turns into a short exchange rather than an empty form
    missing = missing_of(spec, args)
    if missing:
        ask = spec.asks.get(missing) or f"What is the {missing.replace('_', ' ')}?"
        problem = f"{missing} is required: {spec.args[missing]['description']}"
        return json.dumps({"problems": [problem], "ask": ask})
    # Step 4: Build the form from what was said and the records; the builder may still refuse
    try:
        block = await spec.builder(db, customer_code, actor_object_id, actor_site_id, args)
    except ValueError as exc:
        return json.dumps({"problems": [str(exc)]})
    block = block.model_copy(update={"command_kind": spec.command_kind, "mode": spec.mode})
    return json.dumps({"shown": "form", "demo": spec.mode == DEMO,
                       "requires_approval": block.requires_approval,
                       "block": block.model_dump(mode="json")})
