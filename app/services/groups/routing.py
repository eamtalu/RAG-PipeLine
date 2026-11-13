"""Which groups take a finding (stage D, step 12): the topic of an advisory, and whether a group's
topics and severity line admit it; and the cap's clock hour and bounds (step 13). Pure; the store
looks the groups up and the runners call these.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from app.persistence.models.notification_group import SEVERITIES, TOPICS

RANK = {s: i for i, s in enumerate(SEVERITIES)}  # high 0, medium 1, low 2
CAP_MIN, CAP_MAX, CAP_DEFAULT = 1, 60, 5
WINDOW = timedelta(hours=1)


def topic_of(agent: str) -> str:
    """The topic an advisory of one agent belongs to.

    >>> topic_of("stock"), topic_of("delivery")
    ('advisory:stock', 'advisory:delivery')
    """
    return f"advisory:{agent}"


def admits(group_topics: list[str], min_severity: str, topic: str, severity: str) -> bool:
    """Whether a group with these topics and this line takes a finding of that topic and severity.

    >>> admits(["advisory:stock"], "medium", "advisory:stock", "high")
    True
    >>> admits(["advisory:stock"], "high", "advisory:stock", "medium")
    False
    >>> admits(["advisory:stock"], "low", "advisory:delivery", "high")
    False
    >>> admits(["schedule"], "low", "schedule", "info")
    True
    """
    if topic not in group_topics:
        return False
    if topic == "schedule":
        return True
    return RANK.get(severity, 99) <= RANK.get(min_severity, -1)


def clean_topics(topics) -> list[str]:
    """The known topics among those given, in the canonical order; ValueError when none is known.

    >>> clean_topics(["schedule", "advisory:stock", "weather"])
    ['advisory:stock', 'schedule']
    """
    wanted = {str(t) for t in (topics or [])}
    kept = [t for t in TOPICS if t in wanted]
    if not kept:
        raise ValueError(f"topics must name at least one of {', '.join(TOPICS)}")
    return kept


def clean_severity(value) -> str:
    """The severity line, or ValueError.

    >>> clean_severity("MEDIUM"), clean_severity(None)
    ('medium', 'high')
    """
    text = str(value or "high").strip().lower()
    if text not in SEVERITIES:
        raise ValueError("min_severity must be high, medium or low")
    return text


def clean_cap(value) -> int:
    """Cards a group may send per clock hour, 1 to 60 (5 when not given); ValueError otherwise.

    >>> clean_cap(None), clean_cap("3"), clean_cap(60)
    (5, 3, 60)
    """
    if value is None or value == "":
        return CAP_DEFAULT
    try:
        cap = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"max_per_hour must be a number between {CAP_MIN} and {CAP_MAX}")
    if not CAP_MIN <= cap <= CAP_MAX:
        raise ValueError(f"max_per_hour must be between {CAP_MIN} and {CAP_MAX}")
    return cap


def window_start(now: datetime) -> datetime:
    """The start of the clock hour a moment falls in; the cap counts cards per clock hour.

    >>> from datetime import timezone
    >>> window_start(datetime(2026, 11, 13, 15, 10, 30, tzinfo=timezone.utc)).isoformat()
    '2026-11-13T15:00:00+00:00'
    """
    return now.replace(minute=0, second=0, microsecond=0)
