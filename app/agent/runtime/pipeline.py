"""The guards as a pipeline the profile picks from, by name and in order (step 14).

Two stages. A pre-guard looks at the question and the earlier turns before any model call and may
decline the run; a post-guard looks at a draft and may report a problem the runtime answers with
one nudged retry, then a withheld answer. The first decline or problem of a stage wins and the
later guards of that stage do not run, which is what the runtime did when the guards were
hard-wired. Every verdict is recorded on the run's trace with its stage and timing.

Secret redaction is not a guard here on purpose: it runs on every tool result and every answer
whatever the profile lists, because a profile must not be able to switch a safety invariant off.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Protocol, Sequence

from app.agent.presentation import blocks as answer_blocks
from app.agent.presentation import evidence
from app.agent.profiles import Profile
from app.agent.runtime.guards import OUT_OF_SCOPE, TopicGate, looks_like_manipulation
from app.agent.runtime.trace import RunTrace

GUARD_NAMES = ("manipulation", "topic_gate", "fabrication", "ungrounded")
PRE_GUARDS = frozenset({"manipulation", "topic_gate"})
MANIPULATION_PREFIX = "I can't change how I work, show my instructions or read another logspace. "


@dataclass(frozen=True, slots=True)
class Decline:
    """A pre-guard's verdict: why the run stops and the fixed answer the person gets."""

    reason: str  # "manipulation" | "off_topic"
    answer: str


@dataclass(frozen=True, slots=True)
class Problem:
    """A post-guard's verdict: what is wrong with the draft and, for a fabrication, the answer."""

    kind: str  # "fabricated" | "ungrounded"
    figures: tuple[str, ...] = ()
    fallback: str | None = None


@dataclass(frozen=True, slots=True)
class Draft:
    """What the post-guards look at: the draft, the tool trace behind it and the given text."""

    answer: str
    trace: tuple[dict, ...]
    question: str
    given: tuple[str, ...]
    schema_tools: frozenset[str]


class PreGuard(Protocol):
    name: str

    async def inspect(self, question: str, history: Sequence[dict]) -> Decline | None:
        """A decline when the question must not reach the model; None to let it through."""
        ...


class PostGuard(Protocol):
    name: str

    def inspect(self, draft: Draft) -> Problem | None:
        """A problem when the draft must not be answered as it is; None when it is fine."""
        ...


NO_FIGURES = "I could not get the data for that question, so I have no figures to give."
ASK_AGAIN = " Ask again naming the field or window, or ask me to describe what is available."


def _failed(t: dict) -> bool:
    return t.get("result", t["result_preview"]).lstrip().startswith('{"error"')


def looks_fabricated(
    answer: str, trace: list[dict], *, given: list[str], schema_tools: frozenset[str]
) -> str | None:
    """A guard the model cannot talk its way past. On the first live Teams run an 8B model was
    refused for a field that does not exist, and instead of retrying with the lookup it answered
    with "Customer A … Customer J" and round numbers, every one invented. An answer that carries
    figures while no data tool returned a result without an error is replaced by an honest one.

    Dates, times, percentages and small counts are not data figures, nor is a number the
    instructions themselves contain: `given` (the instructions and tool text) and the schema tools'
    results are the only pool.

    A draft that called no tool at all also loses the date, time and share exemptions
    (`evidence.stale_markers`): on 11 November 2026 a repeated Teams question was answered by
    copying the earlier turn's lead-in and freshness note, "as of 14:09 UTC, about 73%
    provisional", which carries no data figure and so passed. Small counts stay exempt, so a
    description in words ("12 tools cover the last 24 hours") still passes."""
    # Step 1: With no tool call behind it, a date, a time or a share in the draft can only have
    # been copied from an earlier turn, so the exemptions below would wave the copy through.
    if not trace and evidence.stale_markers(answer, list(given)):
        return NO_FIGURES + ASK_AGAIN
    pool = list(given) + [
        t.get("result", t["result_preview"])
        for t in trace
        if t["tool"] in schema_tools and not _failed(t)
    ]
    if not evidence.ungrounded(answer, pool):
        return None
    if any(t["tool"] not in schema_tools and not _failed(t) for t in trace):
        return None
    problems = [t.get("result", t["result_preview"]) for t in trace if _failed(t)]
    why = ""
    if problems:
        try:
            last = json.loads(problems[-1])
            why = " " + "; ".join(last.get("problems") or [last.get("error", "")])
        except Exception:  # noqa: BLE001 - the preview may be cut mid-JSON
            why = ""
    return NO_FIGURES + why + ASK_AGAIN


class ManipulationGuard:
    """Declines, in code, an attempt to change the rules, see the prompt or reach another tenant."""

    name = "manipulation"

    def __init__(self, decline_text: str):
        self.answer = MANIPULATION_PREFIX + decline_text

    async def inspect(self, question: str, history: Sequence[dict]) -> Decline | None:
        return Decline("manipulation", self.answer) if looks_like_manipulation(question) else None


class TopicGateGuard:
    """One short model call: declines a question that is not about the agent's data."""

    name = "topic_gate"

    def __init__(self, gate: TopicGate, decline_text: str):
        self.gate = gate
        self.answer = decline_text

    async def inspect(self, question: str, history: Sequence[dict]) -> Decline | None:
        if await self.gate.classify(question, list(history)) == OUT_OF_SCOPE:
            return Decline("off_topic", self.answer)
        return None


class SkippedGuard:
    """A guard the profile lists that cannot run this time (a test model without a gate)."""

    def __init__(self, name: str):
        self.name = name

    async def inspect(self, question: str, history: Sequence[dict]) -> Decline | None:
        return None


def _checked_text(draft: Draft) -> str:
    """The draft plus every number its chart and table calls carry, so a drawn figure is held to
    the same rule as a written one (step 5).

    (answer "3 short", show_chart values [41, 3]) -> "3 short\\n41 3"
    """
    drawn = answer_blocks.presentation_figures(list(draft.trace))
    return f"{draft.answer}\n{drawn}" if drawn else draft.answer


class FabricationGuard:
    """Figures with no successful data call behind them are replaced by an honest answer. A
    presentation call is not a data call: a chart drawn with no data fetched is fabricated."""

    name = "fabrication"

    def inspect(self, draft: Draft) -> Problem | None:
        fallback = looks_fabricated(
            _checked_text(draft),
            answer_blocks.data_trace(list(draft.trace)),
            given=list(draft.given),
            schema_tools=draft.schema_tools,
        )
        return Problem("fabricated", fallback=fallback) if fallback else None


class UngroundedGuard:
    """Every figure in the draft, written or drawn, must be in a data tool result, or in the
    instructions when no tool returned anything (a description of what is available)."""

    name = "ungrounded"

    def inspect(self, draft: Draft) -> Problem | None:
        results = [
            t["result"]
            for t in answer_blocks.data_trace(list(draft.trace))
            if not t["result"].lstrip().startswith('{"error"')
        ]
        figures = evidence.ungrounded(
            _checked_text(draft), results or list(draft.given), draft.question
        )
        return Problem("ungrounded", figures=tuple(figures)) if figures else None


def unknown_guards(names: Sequence[str]) -> list[str]:
    """The names a profile lists that no guard has.

    >>> unknown_guards(["manipulation", "nope"])
    ['nope']
    """
    return [n for n in names if n not in GUARD_NAMES]


@dataclass(frozen=True, slots=True)
class GuardPipeline:
    """The profile's guards, built for one run, in the profile's order."""

    pre: tuple[PreGuard, ...]
    post: tuple[PostGuard, ...]

    async def before(
        self, question: str, history: Sequence[dict], trace: RunTrace
    ) -> Decline | None:
        """The first decline of the pre-guards, each verdict recorded; None when all pass.

        (guards [a declines, b], "q") -> a's Decline, b never runs ; (all pass) -> None
        """
        for guard in self.pre:
            started = time.monotonic()
            decline = await guard.inspect(question, history)
            ms = int((time.monotonic() - started) * 1000)
            if isinstance(guard, SkippedGuard):
                trace.record_guard(name=guard.name, stage="pre", verdict="skipped", ms=ms)
            elif decline is not None:
                trace.record_guard(
                    name=guard.name, stage="pre", verdict="decline", ms=ms, detail=decline.reason
                )
                return decline
            else:
                trace.record_guard(name=guard.name, stage="pre", verdict="pass", ms=ms)
        return None

    def after(self, draft: Draft, trace: RunTrace) -> Problem | None:
        """The first problem of the post-guards, each verdict recorded; None when all pass.

        (draft with invented figures) -> Problem("fabricated", fallback=...) ; (fine) -> None
        """
        for guard in self.post:
            started = time.monotonic()
            problem = guard.inspect(draft)
            ms = int((time.monotonic() - started) * 1000)
            if problem is not None:
                trace.record_guard(
                    name=guard.name, stage="post", verdict="problem", ms=ms, detail=problem.kind
                )
                return problem
            trace.record_guard(name=guard.name, stage="post", verdict="pass", ms=ms)
        return None


def build_pipeline(
    names: Sequence[str], *, profile: Profile, gate: TopicGate | None
) -> GuardPipeline:
    """The profile's guards for one run; `topic_gate` without a gate is listed as skipped.

    (("manipulation", "topic_gate", "fabrication", "ungrounded"), gate) -> 2 pre, 2 post ;
    (same, gate=None) -> the topic gate is a SkippedGuard ; ("nope",) -> ValueError
    """
    # Step 1: Refuse a name no guard has, so that a profile typo fails loudly rather than
    # silently running with one guard fewer.
    bad = unknown_guards(names)
    if bad:
        raise ValueError(f"unknown guard(s) {bad}; the guards are {list(GUARD_NAMES)}")
    pre: list[PreGuard] = []
    post: list[PostGuard] = []
    # Step 2: Build each guard in the profile's order, because the order is the run order and
    # the first verdict of a stage wins.
    for name in names:
        if name == "manipulation":
            pre.append(ManipulationGuard(profile.decline))
        elif name == "topic_gate":
            pre.append(TopicGateGuard(gate, profile.decline) if gate else SkippedGuard(name))
        elif name == "fabrication":
            post.append(FabricationGuard())
        elif name == "ungrounded":
            post.append(UngroundedGuard())
    return GuardPipeline(pre=tuple(pre), post=tuple(post))
