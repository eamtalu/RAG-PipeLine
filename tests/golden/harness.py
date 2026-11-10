"""Reads the golden files and checks a run against a case; shared by the test and the script.

A case is one question with the scripted model's turns and what the run must give. The test in
`tests/test_golden_questions_chunk165.py` runs every case with the scripted model and checks all
of its expectations; `scripts/golden_questions.py` runs the same questions against the real model
and checks only the expectations that do not depend on the model's exact words.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml
from langchain_core.messages import AIMessage

from app.agent.contracts import AgentResult

GOLDEN_DIR = Path(__file__).parent
PROFILES = ("analytics", "logspace")

#: the expectation keys a case may carry; the loader refuses any other
EXPECT_KEYS = frozenset(
    {
        "stop_reason",
        "tools",
        "declined",
        "retries",
        "evidence",
        "withheld_figures",
        "answer_contains",
        "answer_excludes",
        "allow_failed_calls",
    }
)
CASE_KEYS = frozenset({"name", "question", "history", "gate", "replies", "expect"})


@dataclass(frozen=True, slots=True)
class Case:
    profile: str
    name: str
    question: str
    history: tuple[dict, ...]
    gate: bool
    replies: tuple[dict, ...]
    expect: Mapping[str, Any]


def load_cases(profile: str) -> list[Case]:
    """The cases of `tests/golden/<profile>.yaml`, refused with a named reason when malformed.

    ("analytics") -> [Case(name="which_products_were_short", ...), ...] ; (a case with an unknown
    expect key) -> ValueError naming the case and the key
    """
    raw = yaml.safe_load((GOLDEN_DIR / f"{profile}.yaml").read_text())
    if raw.get("profile") != profile:
        raise ValueError(f"{profile}.yaml: profile must be {profile!r}")
    cases: list[Case] = []
    for entry in raw["cases"]:
        where = f"{profile}.yaml case {entry.get('name')!r}"
        unknown = sorted(set(entry) - CASE_KEYS)
        if unknown:
            raise ValueError(f"{where}: unknown keys {unknown}")
        expect = entry["expect"]
        bad = sorted(set(expect) - EXPECT_KEYS)
        if bad:
            raise ValueError(f"{where}: unknown expect keys {bad}")
        if "stop_reason" not in expect or "tools" not in expect:
            raise ValueError(f"{where}: expect needs stop_reason and tools")
        for reply in entry.get("replies", ()):
            if set(reply) not in ({"text"}, {"tool", "args"}, {"tool"}):
                raise ValueError(f"{where}: a reply is {{text}} or {{tool, args}}, not {reply}")
        cases.append(
            Case(
                profile=profile,
                name=entry["name"],
                question=entry["question"],
                history=tuple(entry.get("history") or ()),
                gate=bool(entry.get("gate", False)),
                replies=tuple(entry.get("replies") or ()),
                expect=expect,
            )
        )
    return cases


def scripted_replies(case: Case) -> list[AIMessage]:
    """The case's replies as the messages a scripted model returns, in order.

    (replies [{tool: trace, args: {...}}, {text: "Done."}]) -> [AIMessage(tool_calls=[...]),
    AIMessage("Done.")]
    """
    out: list[AIMessage] = []
    for i, reply in enumerate(case.replies, start=1):
        if "text" in reply:
            out.append(AIMessage(content=str(reply["text"])))
        else:
            call = {"id": f"g{i}", "name": reply["tool"], "args": dict(reply.get("args") or {})}
            out.append(AIMessage(content="", tool_calls=[call]))
    return out


def mismatches(case: Case, result: AgentResult, *, scripted: bool) -> list[str]:
    """Every expectation the result misses, as sentences; empty when the run is as expected.

    (case expecting end_turn, result declined) -> ["stop_reason: expected 'end_turn', got
    'declined'"] ; scripted=False -> only the model-independent checks (stop reason, decline, the
    tools as "must include", the forbidden text)
    """
    expect = case.expect
    out: list[str] = []
    called = [t.tool for t in result.tool_calls]
    # Step 1: The checks that hold whatever model answered, because the guards and the tools
    # decide them: how the run stopped, why it declined, what it called, what it must not say.
    if result.stop_reason != expect["stop_reason"]:
        out.append(f"stop_reason: expected {expect['stop_reason']!r}, got {result.stop_reason!r}")
    if result.declined != expect.get("declined"):
        out.append(f"declined: expected {expect.get('declined')!r}, got {result.declined!r}")
    if scripted:
        if called != list(expect["tools"]):
            out.append(f"tools: expected {list(expect['tools'])}, got {called}")
    else:
        missing = [t for t in expect["tools"] if t not in called]
        if missing:
            out.append(f"tools: {missing} were not called; called {called}")
    for text in expect.get("answer_excludes") or ():
        if text in result.answer:
            out.append(f"answer must not contain {text!r}")
    if not scripted:
        return out
    # Step 2: The checks that only hold with the scripted replies, because they depend on the
    # exact words: retries, the evidence, the figures withheld, the answer's fragments, and that
    # every tool call succeeded unless the case says a failed call is part of the story.
    if result.retries != int(expect.get("retries", 0)):
        out.append(f"retries: expected {expect.get('retries', 0)}, got {result.retries}")
    if "evidence" in expect and (result.evidence is not None) != bool(expect["evidence"]):
        out.append(f"evidence: expected {'some' if expect['evidence'] else 'none'}")
    if "withheld_figures" in expect and list(result.withheld_figures) != list(
        expect["withheld_figures"]
    ):
        out.append(
            f"withheld_figures: expected {expect['withheld_figures']}, "
            f"got {list(result.withheld_figures)}"
        )
    for text in expect.get("answer_contains") or ():
        if text not in result.answer:
            out.append(f"answer must contain {text!r}")
    if not expect.get("allow_failed_calls") and result.trace is not None:
        failed = [c["name"] for c in result.trace.tool_calls if not c["ok"]]
        if failed:
            out.append(f"tool calls failed: {failed}")
    return out
