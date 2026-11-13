"""When a task fires (stage D, step 10): the spec a task carries, the next moment it is due in
the customer's time zone, and the words the Tasks tab shows for it. Pure functions; the runner
and the forms use them. Weekdays are Monday 0 to Sunday 6, as Python counts them.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

WEEKDAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
WEEKDAYS = (0, 1, 2, 3, 4)
EVERY_DAY = (0, 1, 2, 3, 4, 5, 6)


def parse_time(text: str) -> time:
    """A clock time from "16:00", "4pm", "17:45" or "quarter to six" is not handled: HH:MM only.

    >>> parse_time("16:00"), parse_time("7:05")
    (datetime.time(16, 0), datetime.time(7, 5))
    """
    hours, minutes = text.strip().split(":")
    return time(int(hours), int(minutes))


def next_fire(spec: dict[str, Any], after: datetime, tz: ZoneInfo) -> datetime | None:
    """The first moment strictly after `after` the spec fires, in UTC; None when it never will.

    >>> tz = ZoneInfo("Europe/London")
    >>> after = datetime(2026, 11, 13, 15, 0, tzinfo=ZoneInfo("UTC"))  # Friday 15:00 UTC
    >>> next_fire({"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"}, after, tz).isoformat()
    '2026-11-13T16:00:00+00:00'
    >>> next_fire({"kind": "recurring", "weekdays": [0], "time": "08:00"}, after, tz).isoformat()
    '2026-11-16T08:00:00+00:00'
    >>> next_fire({"kind": "once", "at": "2026-11-13T17:45:00+00:00"}, after, tz).isoformat()
    '2026-11-13T17:45:00+00:00'
    >>> next_fire({"kind": "once", "at": "2026-11-13T14:00:00+00:00"}, after, tz) is None
    True
    """
    kind = spec.get("kind")
    if kind == "once":
        at = datetime.fromisoformat(str(spec["at"]).replace("Z", "+00:00"))
        if at.tzinfo is None:
            at = at.replace(tzinfo=tz)
        return at.astimezone(ZoneInfo("UTC")) if at > after else None
    if kind == "recurring":
        weekdays = sorted(set(int(d) for d in spec.get("weekdays") or WEEKDAYS))
        at = parse_time(str(spec.get("time") or "08:00"))
        local = after.astimezone(tz)
        # Step 1: Walk day by day from today for at most a fortnight, so a spec with one weekday
        # resolves without arithmetic on week boundaries or clock changes
        for days_ahead in range(0, 15):
            day = (local + timedelta(days=days_ahead)).date()
            if day.weekday() not in weekdays:
                continue
            candidate = datetime.combine(day, at, tzinfo=tz)
            if candidate > local:
                return candidate.astimezone(ZoneInfo("UTC"))
        return None
    return None


def when_text(spec: dict[str, Any], tz: ZoneInfo) -> str:
    """The spec in the Tasks tab's words.

    >>> tz = ZoneInfo("Europe/London")
    >>> when_text({"kind": "recurring", "weekdays": [0, 1, 2, 3, 4], "time": "16:00"}, tz)
    'Every weekday at 16:00'
    >>> when_text({"kind": "recurring", "weekdays": [0], "time": "08:00"}, tz)
    'Every Monday at 08:00'
    >>> when_text({"kind": "recurring", "weekdays": [0, 1, 2, 3, 4, 5, 6], "time": "07:00"}, tz)
    'Every day at 07:00'
    >>> when_text({"kind": "once", "at": "2026-11-13T17:45:00+00:00"}, tz)
    'Fri 13 Nov, 17:45'
    """
    kind = spec.get("kind")
    if kind == "once":
        at = datetime.fromisoformat(str(spec["at"]).replace("Z", "+00:00")).astimezone(tz)
        return f"{WEEKDAY_NAMES[at.weekday()]} {at.day} {at.strftime('%b')}, {at.strftime('%H:%M')}"
    if kind == "recurring":
        weekdays = tuple(sorted(set(int(d) for d in spec.get("weekdays") or WEEKDAYS)))
        at = str(spec.get("time") or "08:00")
        if weekdays == EVERY_DAY:
            return f"Every day at {at}"
        if weekdays == WEEKDAYS:
            return f"Every weekday at {at}"
        if len(weekdays) == 1:
            full = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
            return f"Every {full[weekdays[0]]} at {at}"
        return f"Every {', '.join(WEEKDAY_NAMES[d] for d in weekdays)} at {at}"
    return "Unscheduled"


def validate(spec: dict[str, Any]) -> dict[str, Any]:
    """The spec cleaned and checked, or ValueError with what is wrong.

    >>> validate({"kind": "recurring", "weekdays": ["0", 4], "time": "16:00"})
    {'kind': 'recurring', 'weekdays': [0, 4], 'time': '16:00'}
    >>> validate({"kind": "weekly"})
    Traceback (most recent call last):
    ...
    ValueError: when.kind must be once or recurring
    """
    kind = spec.get("kind")
    if kind == "once":
        try:
            at = datetime.fromisoformat(str(spec.get("at") or "").replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("when.at must be an ISO datetime")
        return {"kind": "once", "at": at.isoformat()}
    if kind == "recurring":
        try:
            weekdays = sorted({int(d) for d in (spec.get("weekdays") or WEEKDAYS)})
            parse_time(str(spec.get("time") or ""))
        except ValueError:
            raise ValueError("when.weekdays must be 0..6 and when.time HH:MM")
        if not weekdays or any(d < 0 or d > 6 for d in weekdays):
            raise ValueError("when.weekdays must be 0..6")
        return {"kind": "recurring", "weekdays": weekdays, "time": str(spec["time"]).strip()}
    raise ValueError("when.kind must be once or recurring")

