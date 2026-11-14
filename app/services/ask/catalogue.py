"""The Ask catalogue (stage D, step 18): the topics a person can start from and their starter
questions, as data in one place. The edge draws the topic cards and the typed suggestions from
it, "What can I ask you about?" is answered from it without the model, and a topic a record may
not use is never sent to them. Adding a starter is one line here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from app.services.access import records

#: Bumped when the words change, so the mirror rewrites every customer's row.
CATALOGUE_VERSION = "18.1"

QUESTION, ACTION, DEMO = "question", "action", "demo"


@dataclass(frozen=True)
class QuestionSpec:
    """One starter: the sentence a person taps, what tapping it does, and who may."""

    text: str
    kind: str = QUESTION  # question | action | demo
    needs: str | None = None  # a capability code, or None for everyone with access


@dataclass(frozen=True)
class TopicSpec:
    """One card: its words, its icon, who may see it, and its starters."""

    id: str
    label: str
    blurb: str
    icon: str
    questions: tuple[QuestionSpec, ...]
    needs: str | None = None


TOPICS: tuple[TopicSpec, ...] = (
    TopicSpec(
        "stock", "Stock and locations", "What was short, where picks found nothing, and why.", "stock",
        (
            QuestionSpec("Which products were short today, and by how many units?"),
            QuestionSpec("Which items were short on several releases this week?"),
            QuestionSpec("Which locations turned up empty today?"),
            QuestionSpec("Which zones did pickers pick from today?"),
            QuestionSpec("Why is location M02B empty?"),
        ),
    ),
    TopicSpec(
        "deliveries", "Deliveries and vans",
        "What left, what is at risk, and which vans run late.", "delivery",
        (
            QuestionSpec("How many deliveries and lines were picked today?"),
            QuestionSpec("Which deliveries are at risk of leaving late today?"),
            QuestionSpec("Which vans are usually ready late?"),
            QuestionSpec("Which deliveries were missed this week?"),
            QuestionSpec("Show deliveries per day this week as a chart"),
        ),
    ),
    TopicSpec(
        "pickers", "Pickers", "Who picked what, how fast, and where it went wrong.", "picker",
        (
            QuestionSpec("Which pickers picked the most units today?"),
            QuestionSpec("Who had the most zero-picks today?"),
            QuestionSpec("How long does a line take per picker today?"),
            QuestionSpec("Show lines picked per hour today as a chart"),
        ),
    ),
    TopicSpec(
        "planning", "Planning ahead", "What is coming, when it peaks, and what is already at risk.", "plan",
        (
            QuestionSpec("What is coming tomorrow?"),
            QuestionSpec("Which day is busiest this week?"),
            QuestionSpec("When is tonight's peak, and how many pickers does it need?"),
            QuestionSpec("How accurate has the forecast been?"),
            QuestionSpec("Which deliveries are already at risk for tomorrow?"),
        ),
    ),
    TopicSpec(
        "watch", "Watch and remind", "Keep an eye on something, or have it sent to you.", "watch",
        (
            QuestionSpec("Watch route 14", ACTION),
            QuestionSpec("Tell me if fill rate drops below 95%", ACTION),
            QuestionSpec("Remind me at 5 to check Route 14", ACTION),
            QuestionSpec("Every weekday at 4pm send me the short products list", ACTION),
        ),
    ),
    TopicSpec(
        "usage", "How the app is used", "Who uses eSmart Eye, how often, and what they ask.", "usage",
        (
            QuestionSpec("How many people used eSmart Eye in the last 7 days?"),
            QuestionSpec("How many questions were asked each day this week?"),
            QuestionSpec("Who asked the most questions this week?"),
            QuestionSpec("Which advisories were acknowledged this week, and by whom?"),
        ),
        needs="approve",
    ),
    TopicSpec(
        "admin", "Admin actions", "Users, approvals and notification groups, from a sentence.", "admin",
        (
            QuestionSpec("Create a user for Dan Roberts", ACTION, "manage_users"),
            QuestionSpec("Make Sam Super a supervisor of Brighton", ACTION, "manage_users"),
            QuestionSpec("Deactivate Lee London", ACTION, "manage_users"),
            QuestionSpec("Approve the waiting request USR-2041", ACTION, "approve"),
            QuestionSpec("Send high stock advisories to the stock-control channel", ACTION, "configure_agents"),
            QuestionSpec("Add a handheld for Fiona Nachon", DEMO, "run_actions"),
            QuestionSpec("Move a picker to Route 14", DEMO, "run_actions"),
            QuestionSpec("Substitute item 104568 on today's releases", DEMO, "run_actions"),
        ),
        needs="run_actions",
    ),
)

_CAPABILITY_WORDS = re.compile(
    r"(what|which)\b.*\b(can|could|do|does|should)\b.*\b(ask|do|help|answer|know|say|try)\b"
    r"|^\s*(help|hi|hello|hey)\b|\bwhat (else|questions)\b|\bhow (do|can) i use\b",
    re.I,
)
_DATA_WORDS = re.compile(
    r"\b(short|picked|delivery|deliveries|route|zone|location|stock|fill rate|forecast|van|picker|item)\b", re.I)


def is_capability_question(text: str) -> bool:
    """Whether a message asks what Ask can do rather than for data, so the catalogue answers it.

    >>> is_capability_question("What can I ask you about?"), is_capability_question("help")
    (True, True)
    >>> is_capability_question("What was short today?"), is_capability_question("which pickers do best")
    (False, False)
    """
    words = (text or "").strip()
    if not words or _DATA_WORDS.search(words):
        return False
    return bool(_CAPABILITY_WORDS.search(words))


def may_use(record: records.AccessRecord | None, needs: str | None) -> bool:
    """Whether a record may see something that needs a capability: "no" hides it, "with approval"
    keeps it (the form then says so), and no requirement means everyone with access.

    >>> may_use(None, None), may_use(None, "approve")
    (True, False)
    """
    if needs is None:
        return True
    if record is None or not record.is_active:
        return False
    return record.state(needs) != "no"


def visible_topics(record: records.AccessRecord | None) -> list[TopicSpec]:
    """The topics and starters a record may use, in the catalogue's order; a topic left with no
    starters is dropped.

    (a picker) -> the four data topics and Watch and remind; no usage, no admin ; (an admin) -> all seven
    """
    out: list[TopicSpec] = []
    for topic in TOPICS:
        if not may_use(record, topic.needs):
            continue
        questions = tuple(q for q in topic.questions if may_use(record, q.needs))
        if questions:
            out.append(TopicSpec(topic.id, topic.label, topic.blurb, topic.icon, questions, topic.needs))
    return out


def topic_payload(topic: TopicSpec) -> dict[str, Any]:
    """One topic as the row and the block carry it."""
    return {
        "id": topic.id, "label": topic.label, "blurb": topic.blurb, "icon": topic.icon, "needs": topic.needs,
        "questions": [{"text": q.text, "kind": q.kind, "needs": q.needs} for q in topic.questions],
    }


def topics_payload() -> list[dict[str, Any]]:
    """Every topic, unfiltered, for the row the edge filters per record."""
    return [topic_payload(t) for t in TOPICS]


def topics_block(record: records.AccessRecord | None) -> dict[str, Any]:
    """The "What can I ask" answer as a block: the record's topics with their starters.

    (a picker) -> {"type": "topics", "topics": [stock, deliveries, pickers, planning, watch]}
    """
    return {"type": "topics", "topics": [topic_payload(t) for t in visible_topics(record)]}


def capability_answer(record: records.AccessRecord | None) -> tuple[str, dict[str, Any]]:
    """The sentence and the block that answer "What can I ask you about?".

    (an admin) -> ("I can help with stock and locations, … Pick a topic below, or just ask…", the block)
    """
    block = topics_block(record)
    names = [t["label"].lower() for t in block["topics"]]
    listed = ", ".join(names[:-1]) + (f" and {names[-1]}" if len(names) > 1 else (names[0] if names else ""))
    text = (f"I can help with {listed}. Pick a topic below, or just ask in your own words."
            if names else "I can answer questions about this warehouse once your access is set up.")
    return text, block


def check() -> list[str]:
    """What is wrong with the catalogue, for the test that keeps it honest: duplicate ids, a
    capability the matrix does not know, an empty topic.

    (the shipped catalogue) -> []
    """
    from app.services.access import matrix as matrix_file

    known = {c.code for c in matrix_file.load().capabilities}
    problems: list[str] = []
    ids = [t.id for t in TOPICS]
    if len(ids) != len(set(ids)):
        problems.append("duplicate topic ids")
    for topic in TOPICS:
        if not topic.questions:
            problems.append(f"{topic.id}: no starters")
        for needs in (topic.needs, *(q.needs for q in topic.questions)):
            if needs is not None and needs not in known:
                problems.append(f"{topic.id}: unknown capability {needs!r}")
        for q in topic.questions:
            if q.kind not in (QUESTION, ACTION, DEMO):
                problems.append(f"{topic.id}: unknown kind {q.kind!r}")
    return problems
