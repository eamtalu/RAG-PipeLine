"""Which groups take a finding (stage D, step 12): the topic of an advisory, and whether a group's
topics and severity line admit it. Pure; the store looks the groups up and the runners call these.
"""

from __future__ import annotations

from app.persistence.models.notification_group import SEVERITIES, TOPICS

RANK = {s: i for i, s in enumerate(SEVERITIES)}  # high 0, medium 1, low 2


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
