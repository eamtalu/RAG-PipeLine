"""The two forms of screen 04 (stage D, step 10): the New reminder and New schedule the assistant
proposes for the person to confirm, built from what they said, and the reading of a confirmed
form back into a task. Used by the action tools (proposing) and the task commands (confirming);
the Tasks tab's own dialogs send the same field names.
"""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from app.services.teams.contracts import FormBlock, FormField

REPEATS = ["Just this once", "Every day at this time", "Every weekday at this time"]
HOW_OFTEN = ["Every weekday (Mon – Fri)", "Every day", "Once a week", "Once only", "When fill rate drops below…"]
WEEKDAY_OPTIONS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
SEND_TO = ["Me, in the app", "The customer's channel"]
SHOW_AS = ["A table", "A graph", "A short summary"]
SHOW_AS_CODES = {"A table": "table", "A graph": "chart", "A short summary": "summary"}
SEND_TO_CODES = {"Me, in the app": "me", "The customer's channel": "channel"}


def clock(text: str | None, default: str) -> str:
    """A clock time as HH:MM from what the model or the person wrote.

    >>> clock("4pm", "08:00"), clock("17:45", "08:00"), clock("quarter to six", "08:00"), clock(None, "08:00")
    ('16:00', '17:45', '08:00', '08:00')
    """
    if not text:
        return default
    raw = text.strip().lower()
    m = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", raw)
    if not m:
        return default
    hours, minutes, ampm = int(m.group(1)), int(m.group(2) or 0), m.group(3)
    if ampm == "pm" and hours < 12:
        hours += 12
    if ampm == "am" and hours == 12:
        hours = 0
    if hours > 23 or minutes > 59:
        return default
    return f"{hours:02d}:{minutes:02d}"


def reminder_form(args: Mapping[str, Any], *, now: datetime, tz: ZoneInfo, last_question: str | None) -> FormBlock:
    """The New reminder form, pre-filled from what the person said.

    ({"title": "check the 9 short lines", "time": "17:45"}, now 17:16) -> FormBlock(when today 17:45,
    repeat once, attached question the last one asked)
    """
    local = now.astimezone(tz)
    when_time = clock(args.get("time"), (local + timedelta(minutes=30)).strftime("%H:%M"))
    day = str(args.get("date") or "").strip() or local.date().isoformat()
    if day == local.date().isoformat() and time.fromisoformat(when_time) <= local.time():
        day = (local.date() + timedelta(days=1)).isoformat()  # "at 9" said at 17:00 means tomorrow
    repeat = str(args.get("repeat") or "once").lower()
    repeat_text = REPEATS[1] if "day" in repeat and "week" not in repeat else REPEATS[2] if "week" in repeat else REPEATS[0]
    attach = str(args.get("attach_question") or "").strip() or (last_question if args.get("bring_figures", True) else None)
    fields = [
        FormField(name="title", label="Remind me to", kind="text", required=True,
                  value=str(args.get("title") or "").strip()[:200]),
        FormField(name="for_whom", label="For", kind="select", options=["Me only (others later)"],
                  value="Me only (others later)"),
        FormField(name="date", label="On", kind="date", required=True, value=day),
        FormField(name="time", label="At", kind="text", required=True, value=when_time, placeholder="HH:MM"),
        FormField(name="repeat", label="Repeat", kind="select", options=REPEATS, value=repeat_text),
        FormField(name="attach_question", label="Bring the latest figures for", kind="text", required=False,
                  value=attach, placeholder="a question to re-run when it goes off"),
        FormField(name="note", label="Note", kind="text", required=False,
                  value=str(args.get("note") or "").strip() or None),
    ]
    return FormBlock(form_id="new_reminder", title="New reminder", fields=fields, submit_label="Set reminder",
                     note="It arrives in the app with the latest figures when a question is attached; change it any time in Tasks.")


def schedule_form(args: Mapping[str, Any], *, now: datetime, tz: ZoneInfo, last_question: str | None,
                  site_name: str | None) -> FormBlock:
    """The New schedule form, pre-filled from what the person said.

    ({"what": "the short products list", "how_often": "weekdays", "time": "4pm"}) -> FormBlock(
    weekdays at 16:00, to me, as a table)
    """
    often = str(args.get("how_often") or "").lower()
    how = (HOW_OFTEN[4] if "below" in often or "drops" in often or "when" in often
           else HOW_OFTEN[3] if "once" in often and "week" not in often
           else HOW_OFTEN[2] if "week" in often and "day" not in often
           else HOW_OFTEN[1] if "every day" in often or "daily" in often
           else HOW_OFTEN[0])
    weekday = str(args.get("weekday") or "Monday").strip().capitalize()
    if weekday not in WEEKDAY_OPTIONS:
        weekday = "Monday"
    target = SEND_TO[1] if "channel" in str(args.get("send_to") or "").lower() else SEND_TO[0]
    show = (SHOW_AS[1] if "graph" in str(args.get("show_as") or "").lower() or "chart" in str(args.get("show_as") or "").lower()
            else SHOW_AS[2] if "summary" in str(args.get("show_as") or "").lower() else SHOW_AS[0])
    what = str(args.get("what") or "").strip() or (last_question or "")
    fields = [
        FormField(name="what", label="What to send", kind="text", required=True, value=what[:200],
                  placeholder="the question to run, in your words"),
        FormField(name="how_often", label="How often", kind="select", options=HOW_OFTEN, value=how),
        FormField(name="weekday", label="Which day (once a week)", kind="select", options=WEEKDAY_OPTIONS,
                  value=weekday, required=False),
        FormField(name="time", label="At what time", kind="text", required=False,
                  value=clock(args.get("time"), "08:00"), placeholder="HH:MM"),
        FormField(name="threshold", label="Fill rate below (%)", kind="number", required=False,
                  value=str(args.get("threshold") or "").strip() or None),
        FormField(name="send_to", label="Send it to", kind="select", options=SEND_TO, value=target),
        FormField(name="show_as", label="Show it as", kind="select", options=SHOW_AS, value=show),
        FormField(name="until", label="Keep going until", kind="date", required=False,
                  value=str(args.get("until") or "").strip() or None, placeholder="leave empty to keep going"),
    ]
    where = f" ({site_name})" if site_name else ""
    return FormBlock(form_id="new_schedule", title="New schedule", fields=fields, submit_label="Schedule it",
                     note=f"It runs with your access and only ever shows what you are allowed to see{where}; change or pause it any time in Tasks.")


def reminder_from_form(values: Mapping[str, Any], *, tz: ZoneInfo) -> dict[str, Any]:
    """A confirmed New reminder form as the task_create payload's fields.

    >>> r = reminder_from_form({"title": "Check the 9 short lines", "date": "2026-11-13", "time": "17:45",
    ...                         "repeat": "Just this once"}, tz=ZoneInfo("Europe/London"))
    >>> r["kind"], r["when"]
    ('reminder', {'kind': 'once', 'at': '2026-11-13T17:45:00+00:00'})
    """
    title = str(values.get("title") or "").strip()
    day = date.fromisoformat(str(values.get("date") or "").strip())
    at = time.fromisoformat(clock(values.get("time"), "08:00"))
    repeat = str(values.get("repeat") or REPEATS[0])
    if repeat == REPEATS[1]:
        when: dict[str, Any] = {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4, 5, 6], "time": at.strftime("%H:%M")}
    elif repeat == REPEATS[2]:
        when = {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": at.strftime("%H:%M")}
    else:
        when = {"kind": "once", "at": datetime.combine(day, at, tzinfo=tz).isoformat()}
    return {"kind": "reminder", "title": title, "when": when,
            "query_text": str(values.get("attach_question") or "").strip() or None,
            "note": str(values.get("note") or "").strip() or None}


def schedule_from_form(values: Mapping[str, Any], *, tz: ZoneInfo) -> dict[str, Any]:
    """A confirmed New schedule form as the task_create payload's fields.

    >>> s = schedule_from_form({"what": "short products list", "how_often": "Every weekday (Mon – Fri)",
    ...                         "time": "16:00", "send_to": "Me, in the app", "show_as": "A table"},
    ...                        tz=ZoneInfo("Europe/London"))
    >>> s["kind"], s["when"], s["target"], s["show_as"]
    ('schedule', {'kind': 'recurring', 'weekdays': [0, 1, 2, 3, 4], 'time': '16:00'}, 'me', 'table')
    """
    what = str(values.get("what") or "").strip()
    how = str(values.get("how_often") or HOW_OFTEN[0])
    at = clock(values.get("time"), "08:00")
    if how == HOW_OFTEN[4]:
        raw = str(values.get("threshold") or "").strip().rstrip("%")
        value = float(raw) / 100 if raw else 0.97
        when: dict[str, Any] = {"kind": "condition", "metric": "fill_rate", "op": "<", "value": value}
    elif how == HOW_OFTEN[3]:
        day = date.fromisoformat(str(values.get("until") or "").strip()) if values.get("until") else None
        day = day or date.today()
        when = {"kind": "once", "at": datetime.combine(day, time.fromisoformat(at), tzinfo=tz).isoformat()}
    elif how == HOW_OFTEN[2]:
        weekday = str(values.get("weekday") or "Monday")
        when = {"kind": "recurring", "weekdays": [WEEKDAY_OPTIONS.index(weekday) if weekday in WEEKDAY_OPTIONS else 0],
                "time": at}
    elif how == HOW_OFTEN[1]:
        when = {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4, 5, 6], "time": at}
    else:
        when = {"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": at}
    until = None
    if values.get("until") and how != HOW_OFTEN[3]:
        until = datetime.combine(date.fromisoformat(str(values["until"]).strip()), time(23, 59), tzinfo=tz).isoformat()
    return {"kind": "schedule", "title": what[:200], "when": when, "query_text": what,
            "target": SEND_TO_CODES.get(str(values.get("send_to")), "me"),
            "show_as": SHOW_AS_CODES.get(str(values.get("show_as")), "table"), "until": until}
