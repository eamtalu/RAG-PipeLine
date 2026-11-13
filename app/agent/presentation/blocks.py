"""Typed answer blocks lifted off the tool trace (stage D, step 5).

The model asks for a chart, a table or follow-ups by calling the presentation tools; this module
turns those calls, in order, into the blocks the wire contract carries, validated once more and
capped. A call the tool already refused (its result names `problems`) is not a block. The guards
read the same calls: every number in a chart or table must appear in a data tool's result.
"""

from __future__ import annotations

import json
import logging
import re

from app.agent.presentation.entities import entities_from_trace
from app.services.analytics_agent.action_tools import ACTION_TOOL_NAMES
from app.services.analytics_agent.presentation_tools import PRESENTATION_TOOL_NAMES, block_from_call

#: The calls that draw rather than read: the presentation tools and the action tools (step 8).
DRAWING_TOOL_NAMES = PRESENTATION_TOOL_NAMES | ACTION_TOOL_NAMES

logger = logging.getLogger(__name__)

MAX_BLOCKS = 6
_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def is_presentation_call(call: dict) -> bool:
    """Whether a trace entry is a presentation call rather than a data call.

    >>> is_presentation_call({"tool": "show_chart"}), is_presentation_call({"tool": "aggregate_releases"})
    (True, False)
    """
    return call.get("tool") in DRAWING_TOOL_NAMES


def data_trace(trace: list[dict]) -> list[dict]:
    """The data calls only, so a presentation call never counts as evidence of a data call.

    >>> [t["tool"] for t in data_trace([{"tool": "show_chart"}, {"tool": "list_releases"}])]
    ['list_releases']
    """
    return [t for t in trace if not is_presentation_call(t)]


def _refused(call: dict) -> bool:
    return str(call.get("result", call.get("result_preview", ""))).lstrip().startswith('{"problems"')


def blocks_from_trace(trace: list[dict]) -> list[dict]:
    """The blocks the presentation calls describe, in order, at most MAX_BLOCKS, one per kind
    (the last chart wins, the last table wins, the last follow-ups win), plus the entities the
    data calls named (step 14c).

    >>> blocks_from_trace([{"tool": "suggest_followups", "input": {"items": ["Why?"]}, "result": "{}"}])
    [{'type': 'followups', 'items': ['Why?']}]
    >>> blocks_from_trace([{"tool": "aggregate_releases", "input": {}, "result": "{}"}])
    []
    """
    by_type: dict[str, dict] = {}
    for call in trace:
        if not is_presentation_call(call) or _refused(call):
            continue
        try:
            if call["tool"] in ACTION_TOOL_NAMES:
                # an action tool's block is in its result: the runner built it from the records
                block = json.loads(call.get("result") or "{}").get("block")
                if not block:
                    continue
            else:
                block = block_from_call(call["tool"], call.get("input") or {})
        except Exception as exc:  # noqa: BLE001 - a bad call is dropped and logged, never raised
            logger.warning("agent.block_dropped tool=%s reason=%s", call["tool"], str(exc)[:160])
            continue
        by_type[block["type"]] = block
    # the watchable things the data calls named (step 14c), read from the results, never the text
    entities = entities_from_trace(trace)
    if entities is not None and "entities" not in by_type:
        by_type["entities"] = entities
    ordered = [by_type[k] for k in ("entities", "table", "chart", "form", "receipt", "choice", "followups") if k in by_type]
    return ordered[:MAX_BLOCKS]


def presentation_figures(trace: list[dict]) -> str:
    """Every number a presentation call carries, as one text the figure guards can read.

    >>> presentation_figures([{"tool": "show_chart", "input": {"labels": ["Mon"], "series_values": [[41, 3.5]]}}])
    '41 3.5'
    """
    numbers: list[str] = []
    for call in trace:
        if not is_presentation_call(call):
            continue
        args = call.get("input") or {}
        for key in ("series_values", "rows"):
            for item in args.get(key) or []:
                for cell in item if isinstance(item, (list, tuple)) else [item]:
                    numbers.extend(_NUMBER.findall(str(cell)))
    return " ".join(numbers)
