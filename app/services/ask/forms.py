"""The forms Ask proposes and the person confirms (stage D, step 8 the user form; step 10 the
reminder and schedule forms; step 18 the user change, the decision, the group, and the demo
forms). Each builder pre-fills a FormBlock from what the model understood and the records; each
confirmed form is turned back into its command's own payload by `payload_from_form`, so the
services never learn the forms' field names. Used by app/services/ask/actions.py and the command
handlers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.notification_group import SEVERITIES, TOPICS as GROUP_TOPICS
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import approvals, matrix as matrix_file, records, users
from app.services.groups import store as groups_store
from app.services.teams.contracts import FormBlock, FormField
from app.services.teams.sites import list_sites

SHIFTS = ["Night · 22:00 – 06:00", "Day · 06:00 – 14:00", "Late · 14:00 – 22:00"]
DEMO_NOTE = "Demo: this is not connected to the warehouse yet; confirming records nothing."
TOPIC_WORDS = {"advisory:delivery": "Deliveries", "advisory:stock": "Stock", "advisory:location": "Locations",
               "schedule": "Schedules"}

#: The form ids this module builds and reads back; a command payload carrying one is unwrapped.
FORM_IDS = frozenset({"create_user", "new_reminder", "new_schedule", "change_user", "decide_request", "set_group",
                      "add_device", "reassign_picker", "substitute_item"})


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


def role_note(role: str) -> str:
    """What a role may do, in one line, for the form's note.

    >>> role_note("executive")
    'can see all sites and approve; read-only, no actions.'
    """
    return {
        "picker": "can see their own site and ask with approval; cannot run actions, approve or export.",
        "shift_lead": "can see their own site and ask; actions need approval.",
        "supervisor": "can see their own site, ask and run actions; all sites, users and export need approval.",
        "ops_manager": "can see all sites, run actions, manage users, approve and configure.",
        "executive": "can see all sites and approve; read-only, no actions.",
        "admin": "can do everything, including the matrix.",
    }.get(role, "")


def _text(value: Any, limit: int = 128) -> str | None:
    words = str(value or "").strip()
    return words[:limit] or None


async def _tz(db: AsyncSession, customer_code: str) -> ZoneInfo:
    return ZoneInfo(await get_customer_timezone(db, customer_code))


# ==================================================================== the user forms (step 8, step 18)
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
    note_parts = [p for p in (shift, _text(args.get("note"))) if p]
    fields = [
        FormField(name="object_id", label="Entra object id", kind="text", required=True,
                  placeholder="from the person's Microsoft 365 account"),
        FormField(name="display_name", label="Full name", kind="text", required=True,
                  value=str(args.get("display_name") or "").strip()[:128]),
        FormField(name="email", label="Work email", kind="text", required=False,
                  value=_text(args.get("email")), placeholder="name@company.com"),
        FormField(name="role", label="Role", kind="select", options=list(m.role_codes),
                  labels=[r.name for r in m.roles], value=role),
        FormField(name="site_id", label="Site", kind="select", options=[s.site_id for s in sites],
                  labels=[s.name for s in sites], value=site_id or None, required=bool(sites)),
        FormField(name="shift", label="Shift", kind="select", options=SHIFTS, value=shift, required=False),
        FormField(name="starts_on", label="Start date", kind="date", required=False,
                  value=_text(args.get("starts_on"))),
        FormField(name="note", label="Note", kind="text", required=False,
                  value=(" · ".join(note_parts) or None), placeholder="handheld only, covers Route 14…"),
    ]
    role_name = next((r.name for r in m.roles if r.code == role), role)
    return FormBlock(
        form_id="create_user", title="New user", fields=fields, requires_approval=requires_approval,
        submit_label="Create & send for approval" if requires_approval else "Create user",
        note=f"{role_name}: {role_note(role)}" + (" Needs approval from an Ops manager." if requires_approval else ""),
    )


def _pick_user(people: list, who: str | None):
    """The user whose name contains the words, or None."""
    words = (who or "").strip().lower()
    if not words:
        return None
    return next((u for u in people if words in (u.display_name or "").lower()), None)


async def change_user_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                           args: Mapping[str, Any]) -> FormBlock:
    """The Change a user form: who (named in words), their role, site and whether they stay active.

    (admin, {"who": "Sam", "role": "supervisor", "site_id": "BRI"}) -> form with Sam Super chosen ;
    ({"who": "Lee", "status": "deactivated"}) -> form with Lee London and Deactivated
    """
    record = await records.access_record_for(db, customer_code, actor_object_id)
    decision = records.decide(record, "manage_users")
    if decision.outcome == "refused":
        raise ValueError(f"you cannot change users: {decision.reason}")
    requires_approval = decision.outcome == "needs_approval"
    people = [u for u in await users.list_users(db, customer_code) if u.object_id != actor_object_id]
    if not people:
        raise ValueError("there is nobody else to change yet")
    m = matrix_file.load()
    role_names = {r.code: r.name for r in m.roles}
    sites = await list_sites(db, customer_code)
    chosen = _pick_user(people, args.get("who"))
    role = str(args.get("role") or (chosen.role_code if chosen else "") or "").strip().lower().replace(" ", "_")
    status = "deactivated" if str(args.get("status") or "").lower().startswith("deactiv") else "active"
    site_id = str(args.get("site_id") or "").strip()
    fields = [
        FormField(name="object_id", label="Who", kind="select", options=[u.object_id for u in people],
                  labels=[f"{u.display_name} · {role_names.get(u.role_code, u.role_code)}" for u in people],
                  value=chosen.object_id if chosen else None),
        FormField(name="role", label="Role", kind="select", options=list(m.role_codes),
                  labels=[r.name for r in m.roles], value=role if role in m.role_codes else None, required=False),
        FormField(name="site_id", label="Site", kind="select", options=[s.site_id for s in sites],
                  labels=[s.name for s in sites], value=site_id if site_id in {s.site_id for s in sites} else None,
                  required=False),
        FormField(name="status", label="Access", kind="select", options=["active", "deactivated"],
                  labels=["Active", "Deactivated"], value=status),
        FormField(name="note", label="Note", kind="text", required=False, value=_text(args.get("note"))),
    ]
    return FormBlock(
        form_id="change_user", title="Change a user", fields=fields, requires_approval=requires_approval,
        submit_label="Send for approval" if requires_approval else "Save changes",
        note=("Only what you fill in changes; the rest stays as it is."
              + (" Needs approval from an Ops manager." if requires_approval else "")),
    )


async def decide_request_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                              args: Mapping[str, Any]) -> FormBlock:
    """The Decide a request form: which waiting request, approve or decline, a note.

    (ops manager, {"reference": "USR-2041", "decision": "approve"}) -> form with USR-2041 chosen ;
    (nothing waiting) -> ValueError
    """
    record = await records.access_record_for(db, customer_code, actor_object_id)
    decision = records.decide(record, "approve")
    if not decision.is_allowed:
        raise ValueError(f"you cannot decide requests: {decision.reason}")
    waiting = await approvals.pending(db, customer_code)
    if not waiting:
        raise ValueError("nothing is waiting for a decision")
    wanted = str(args.get("reference") or "").strip().upper()
    chosen = next((a for a in waiting if a.reference.upper() == wanted), None) or (waiting[0] if len(waiting) == 1 else None)
    verdict = "decline" if str(args.get("decision") or "").lower().startswith("dec") else "approve"
    fields = [
        FormField(name="reference", label="Request", kind="select", options=[a.reference for a in waiting],
                  labels=[f"{a.reference} · {a.title} · by {a.requested_by_name or a.requested_by}" for a in waiting],
                  value=chosen.reference if chosen else None),
        FormField(name="decision", label="Decision", kind="select", options=["approve", "decline"],
                  labels=["Approve", "Decline"], value=verdict),
        FormField(name="note", label="Note to the requester", kind="text", required=False, value=_text(args.get("note"))),
    ]
    return FormBlock(form_id="decide_request", title="Decide a request", fields=fields,
                     submit_label="Approve" if verdict == "approve" else "Decline",
                     note="The requester is told the decision and your note.")


async def set_group_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                         args: Mapping[str, Any]) -> FormBlock:
    """The notification group form: a new group or an existing one, what it takes, from which
    severity, to which channel, and its cap.

    (ops manager, {"name": "Stock", "topics": ["stock"], "min_severity": "high", "channel": "stock-control"})
    -> form with Stock, Stock ticked, High, the channel chosen ; (picker) -> ValueError
    """
    record = await records.access_record_for(db, customer_code, actor_object_id)
    decision = records.decide(record, "configure_agents")
    if not decision.is_allowed:
        raise ValueError(f"you cannot change notification groups: {decision.reason}")
    groups = await groups_store.list_groups(db, customer_code)
    channels = await groups_store.customer_channels(db, customer_code)
    name = _text(args.get("name"), 80) or ""
    existing = next((g for g in groups if g.group.name.lower() == name.lower()), None) if name else None
    wanted_topics = {str(t).lower() for t in (args.get("topics") or [])}
    wanted_channel = str(args.get("channel") or "").strip().lower()
    channel = next((c for c in channels if wanted_channel and wanted_channel in c.name.lower()), None)
    severity = str(args.get("min_severity") or (existing.group.min_severity if existing else "medium")).lower()
    fields = [
        FormField(name="group_id", label="Group", kind="select", options=["", *(str(g.group.id) for g in groups)],
                  labels=["New group", *(g.group.name for g in groups)],
                  value=str(existing.group.id) if existing else "", required=False),
        FormField(name="name", label="Name", kind="text", value=name or None, placeholder="Stock control"),
        *[FormField(name=f"topic_{code.split(':')[-1]}", label=f"Takes {TOPIC_WORDS[code].lower()}", kind="checkbox",
                    required=False,
                    value="true" if (code.split(":")[-1] in wanted_topics
                                     or (existing is not None and code in (existing.group.topics or []))) else "false")
          for code in GROUP_TOPICS],
        FormField(name="min_severity", label="From severity", kind="select", options=list(SEVERITIES),
                  labels=["High only", "Medium and up", "Everything"], value=severity if severity in SEVERITIES else "medium"),
        FormField(name="channel_id", label="Posts to", kind="select", options=["", *(str(c.id) for c in channels)],
                  labels=["In the app only", *(c.name for c in channels)], value=str(channel.id) if channel else "",
                  required=False),
        FormField(name="max_per_hour", label="Cards an hour", kind="number", required=False,
                  value=str(existing.group.max_per_hour) if existing else "5"),
    ]
    return FormBlock(form_id="set_group", title="Notification group", fields=fields,
                     submit_label="Save group", note="People join a group themselves on the Advisories tab.")


# ==================================================================== the reminder and schedule forms (step 10)
async def reminder_form_for(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                            args: Mapping[str, Any]) -> FormBlock:
    """The New reminder form for the actor.

    ({"title": "check Route 14", "time": "5pm"}) -> FormBlock(form_id "new_reminder", …)
    """
    from app.services.tasks import forms as task_forms

    return task_forms.reminder_form(args, now=datetime.now(timezone.utc), tz=await _tz(db, customer_code), last_question=None)


async def schedule_form_for(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                            args: Mapping[str, Any]) -> FormBlock:
    """The New schedule form for the actor, with the groups it may post to.

    ({"what": "short products list", "how_often": "weekdays", "time": "4pm"}) -> FormBlock(form_id "new_schedule", …)
    """
    from app.services.tasks import forms as task_forms
    from app.services.tasks.commands import sendable_groups

    sites = await list_sites(db, customer_code)
    site_name = next((s.name for s in sites if s.site_id == actor_site_id), None)
    return task_forms.schedule_form(args, now=datetime.now(timezone.utc), tz=await _tz(db, customer_code),
                                    last_question=None, site_name=site_name,
                                    groups=await sendable_groups(db, customer_code, actor_object_id))


# ==================================================================== the demo forms (step 18)
async def add_device_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                          args: Mapping[str, Any]) -> FormBlock:
    """The demo Add a handheld form: the device, its login, who holds it, the site. Nothing is kept.

    ({"holder": "Fiona Nachon", "login": "FNACHONLEO"}) -> form with those filled
    """
    sites = await list_sites(db, customer_code)
    site_id = str(args.get("site_id") or actor_site_id or (sites[0].site_id if sites else "")).strip()
    fields = [
        FormField(name="device_name", label="Handheld", kind="text", value=_text(args.get("device_name"), 64),
                  placeholder="HH-0421"),
        FormField(name="login", label="Handheld login", kind="text", required=False, value=_text(args.get("login"), 64),
                  placeholder="FNACHONLEO"),
        FormField(name="holder", label="Who holds it", kind="text", value=_text(args.get("holder"), 128),
                  placeholder="the picker's name"),
        FormField(name="site_id", label="Site", kind="select", options=[s.site_id for s in sites],
                  labels=[s.name for s in sites], value=site_id if site_id in {s.site_id for s in sites} else None,
                  required=bool(sites)),
    ]
    return FormBlock(form_id="add_device", title="Add a handheld", fields=fields, submit_label="Add handheld",
                     note=DEMO_NOTE, mode="demo")


async def reassign_picker_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                               args: Mapping[str, Any]) -> FormBlock:
    """The demo Move a picker form: who, from which route, to which, until when. Nothing is kept.

    ({"picker": "Dan", "to_route": "R14"}) -> form with those filled
    """
    fields = [
        FormField(name="picker", label="Picker", kind="text", value=_text(args.get("picker"))),
        FormField(name="from_route", label="From route", kind="text", required=False, value=_text(args.get("from_route"), 32)),
        FormField(name="to_route", label="To route", kind="text", value=_text(args.get("to_route"), 32), placeholder="R14"),
        FormField(name="until", label="Until", kind="select", options=["end_of_shift", "end_of_day", "until_told"],
                  labels=["End of this shift", "End of today", "Until I say"], value="end_of_shift"),
    ]
    return FormBlock(form_id="reassign_picker", title="Move a picker", fields=fields, submit_label="Move picker",
                     note=DEMO_NOTE, mode="demo")


async def substitute_item_form(db: AsyncSession, customer_code: str, actor_object_id: str, actor_site_id: str | None,
                               args: Mapping[str, Any]) -> FormBlock:
    """The demo Substitute an item form: the item, what replaces it, on which releases. Nothing is kept.

    ({"item": "104568", "substitute": "104570"}) -> form with those filled
    """
    fields = [
        FormField(name="item", label="Item", kind="text", value=_text(args.get("item"), 32), placeholder="104568"),
        FormField(name="substitute", label="Replace it with", kind="text", value=_text(args.get("substitute"), 32)),
        FormField(name="releases", label="On", kind="select", options=["today", "tomorrow", "both"],
                  labels=["Today's releases", "Tomorrow's releases", "Both"], value="today"),
        FormField(name="note", label="Note for the pickers", kind="text", required=False, value=_text(args.get("note"))),
    ]
    return FormBlock(form_id="substitute_item", title="Substitute an item", fields=fields, submit_label="Substitute",
                     note=DEMO_NOTE, mode="demo")


# ==================================================================== reading a confirmed form back
def payload_from_form(form_id: str, values: Mapping[str, Any]) -> dict[str, Any]:
    """The command's own payload from a confirmed form's values.

    >>> payload_from_form("decide_request", {"reference": "USR-1", "decision": "approve", "note": "ok"})
    {'reference': 'USR-1', 'approve': True, 'note': 'ok'}
    >>> payload_from_form("set_group", {"group_id": "", "name": "Stock", "topic_stock": "true", "topic_delivery": "false",
    ...                                  "min_severity": "high", "channel_id": "", "max_per_hour": "3"})
    {'group_id': None, 'name': 'Stock', 'topics': ['advisory:stock'], 'min_severity': 'high', 'channel_ids': [], 'max_per_hour': 3}
    >>> payload_from_form("change_user", {"object_id": "u-2", "role": "supervisor", "site_id": "BRI", "status": "active"})
    {'object_id': 'u-2', 'role': 'supervisor', 'site_ids': ['BRI'], 'status': 'active'}
    """
    v = {k: (val.strip() if isinstance(val, str) else val) for k, val in dict(values).items()}
    if form_id == "decide_request":
        return {"reference": v.get("reference") or "", "approve": v.get("decision") == "approve", "note": v.get("note") or None}
    if form_id == "set_group":
        topics = [code for code in GROUP_TOPICS if str(v.get(f"topic_{code.split(':')[-1]}") or "").lower() == "true"]
        cap = v.get("max_per_hour")
        return {"group_id": v.get("group_id") or None, "name": v.get("name") or "", "topics": topics,
                "min_severity": v.get("min_severity") or "medium",
                "channel_ids": [v["channel_id"]] if v.get("channel_id") else [],
                "max_per_hour": int(cap) if str(cap or "").isdigit() else None}
    if form_id == "change_user":
        out: dict[str, Any] = {"object_id": v.get("object_id") or ""}
        if v.get("role"):
            out["role"] = v["role"]
        if v.get("site_id"):
            out["site_ids"] = [v["site_id"]]
        out["status"] = v.get("status") or "active"
        if v.get("note"):
            out["note"] = v["note"]
        return out
    return dict(v)


def unwrap(payload: Mapping[str, Any]) -> dict[str, Any]:
    """A command payload as its handler reads it: a confirmed form ({form_id, values, via}) is
    turned into the command's own fields; anything else passes through.

    >>> unwrap({"form_id": "decide_request", "values": {"reference": "USR-1", "decision": "decline"}, "via": "ask"})
    {'reference': 'USR-1', 'approve': False, 'note': None, 'via': 'ask'}
    >>> unwrap({"object_id": "u-1", "role": "picker"})
    {'object_id': 'u-1', 'role': 'picker'}
    """
    form_id = str(payload.get("form_id") or "")
    if form_id not in FORM_IDS or not isinstance(payload.get("values"), Mapping):
        return dict(payload)
    out = payload_from_form(form_id, payload["values"])
    if payload.get("via"):
        out["via"] = payload["via"]
    return out
