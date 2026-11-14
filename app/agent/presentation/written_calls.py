"""Presentation calls the model wrote out as text instead of calling (step 19).

On 14 November the live model drew its chart with a real `chart_from_data` call and then wrote
`state_findings findings: [...] reasoning: [...]` and `suggest_followups items: [...]` as prose, so
the reader saw raw arguments where the findings should have been. This module lifts such passages
off the answer: each becomes a synthetic trace entry (`written: True`) that the lift validates and
turns into a block exactly as a real call, and the figure guards read as they read a real call.
Pure, doctested.
"""

from __future__ import annotations

import json
import re

from app.services.analytics_agent.presentation_tools import PRESENTATION_TOOL_NAMES

#: a line that opens a written-out call: the tool name alone, or followed by its first argument
_OPENER = re.compile(r"^\s*(?P<tool>[a-z_]+)(?:\s*[:(]?\s*(?P<rest>[a-z_]+\s*[:=].*))?\s*$")
#: an argument line: `key: value` or `key = value` or `key=value`
_ARG = re.compile(r"^\s*(?P<key>[a-z_]+)\s*[:=]\s*(?P<value>.*)$")
#: several arguments on one line: `title: "..." kind: "bar" measure: "picked"`
_INLINE_ARG = re.compile(r"(?P<key>[a-z_]+)\s*[:=]\s*(?P<value>\"(?:[^\"\\\\]|\\\\.)*\"|\[[^\]]*\]|[^\s]+)")


def _parse_value(raw: str):
    """A written value as the argument it means: JSON when it reads as JSON, else the bare words.

    >>> _parse_value('["a", "b"]'), _parse_value('"bar"'), _parse_value("picked"), _parse_value("")
    (['a', 'b'], 'bar', 'picked', '')
    """
    text = raw.strip().rstrip(",")
    if not text:
        return ""
    if text[0] in "[{\"":
        try:
            return json.loads(text)
        except ValueError:
            return text.strip("\"")
    return text


def _args_of(line: str) -> dict:
    """Every `key: value` pair on one line.

    >>> _args_of('title: "Top 10" kind: "bar" measure: "picked"')
    {'title': 'Top 10', 'kind': 'bar', 'measure': 'picked'}
    """
    return {m.group("key"): _parse_value(m.group("value")) for m in _INLINE_ARG.finditer(line)}


def lift_written_calls(answer: str, known: frozenset[str] = PRESENTATION_TOOL_NAMES) -> tuple[str, list[dict]]:
    """The answer without the written-out calls, and those calls as synthetic trace entries.

    >>> text, calls = lift_written_calls('Lead.\\n\\nsuggest_followups\\nitems: ["Why?"]\\n\\nMore.')
    >>> text, [(c["tool"], c["input"], c["written"]) for c in calls]
    ('Lead.\\n\\nMore.', [('suggest_followups', {'items': ['Why?']}, True)])
    >>> lift_written_calls("No calls here, just 3 words.")[1]
    []
    """
    kept: list[str] = []
    calls: list[dict] = []
    lines = answer.split("\n")
    i = 0
    while i < len(lines):
        opener = _OPENER.match(lines[i])
        tool = opener.group("tool") if opener else None
        if tool not in known:
            kept.append(lines[i])
            i += 1
            continue
        # Step 1: Gather the call's arguments from the opener's tail and the `key: value` lines that
        # follow, until a blank line or a line that is neither, so the sentences after it stay
        args: dict = _args_of(opener.group("rest") or "")
        i += 1
        while i < len(lines):
            line = lines[i]
            arg = _ARG.match(line)
            if not line.strip() or arg is None or arg.group("key") in known:
                break
            args.update(_args_of(line) or {arg.group("key"): _parse_value(arg.group("value"))})
            i += 1
        if args:
            calls.append({"tool": tool, "input": args, "result": "{}", "result_preview": "{}", "written": True})
        else:
            kept.append(lines[i - 1])  # a bare tool name with nothing after it is just a word
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(kept)).strip()
    return text, calls
