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

from app.agent.presentation.charts import (
    auto_chart,
    auto_heatmap,
    auto_kpis,
    auto_people_table,
    auto_story_table,
    usable_result,
)
from app.agent.presentation.entities import entities_from_trace
from app.services.analytics_agent.action_tools import ACTION_TOOL_NAMES
from app.services.analytics_agent.presentation_tools import (
    CHART_FROM_DATA,
    PRESENTATION_TOOL_NAMES,
    block_from_call,
)

#: The calls that draw rather than read: the presentation tools and the action tools (step 8).
DRAWING_TOOL_NAMES = PRESENTATION_TOOL_NAMES | ACTION_TOOL_NAMES

logger = logging.getLogger(__name__)

MAX_BLOCKS = 8  # equal to the wire contract's cap; one block per kind keeps the bound
#: The blocks in reading order: who it is about, the key figures, the picture, the records, what
#: it means, then the things to act on and the follow-ups.
ORDER = ("entities", "kpis", "chart", "heatmap", "table", "findings", "form", "receipt", "choice", "followups")
#: The presentation arguments whose words the figure guards read (step 19), as whole strings so
#: `evidence.figures` keeps its own comma, date and ordinal handling.
TEXT_KEYS = ("values", "findings", "reasoning", "recommendations", "reasons", "limitations")
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


def blocks_from_trace(trace: list[dict], *, only_grounded: bool = False) -> list[dict]:
    """The blocks the presentation calls describe, in order, at most MAX_BLOCKS, one per kind
    (the last chart wins, the last table wins, the last follow-ups win), plus the entities the
    data calls named (step 14c). With `only_grounded`, only the blocks whose numbers were copied
    from a data result (a `chart_from_data` chart, the shape the lift draws from the data, the
    entities), the ones a withheld answer may still carry (step 19).

    >>> blocks_from_trace([{"tool": "suggest_followups", "input": {"items": ["Why?"]}, "result": "{}"}])
    [{'type': 'followups', 'items': ['Why?']}]
    >>> blocks_from_trace([{"tool": "aggregate_releases", "input": {}, "result": "{}"}])
    []
    >>> blocks_from_trace([{"tool": "suggest_followups", "input": {"items": ["Why?"]}, "result": "{}"}], only_grounded=True)
    []
    """
    by_type: dict[str, dict] = {}
    latest: tuple[str, dict] | None = None  # the last usable data result so far (step 19)
    latest_asked_sort = False  # whether the model asked that result for a ranking
    called: set[str] = set()  # the data tools that answered before this point, for a finding's source
    for call in trace:
        if not is_presentation_call(call):
            usable = usable_result(call)
            if usable is not None:
                latest = (str(call.get("tool")), usable)
                latest_asked_sort = bool((call.get("input") or {}).get("sort"))
            if not _refused(call) and not str(call.get("result", "")).lstrip().startswith('{"error"'):
                called.add(str(call.get("tool")))
            continue
        if _refused(call) or (only_grounded and call["tool"] != CHART_FROM_DATA):
            continue
        try:
            if call["tool"] in ACTION_TOOL_NAMES:
                # an action tool's block is in its result: the runner built it from the records
                block = json.loads(call.get("result") or "{}").get("block")
                if not block:
                    continue
            else:
                block = block_from_call(call["tool"], call.get("input") or {}, data=latest, called=frozenset(called))
        except Exception as exc:  # noqa: BLE001 - a bad call is dropped and logged, never raised
            logger.warning("agent.block_dropped tool=%s reason=%s", call["tool"], str(exc)[:160])
            continue
        by_type[block["type"]] = block
    # The shape the data has wins (step 19, fix of 14 November): a day-by-hour grid is always a
    # heat map, replacing a model's line of hours; a trend over time always gets its chart, beside
    # a model's table; a ranking or a single total is drawn only when the model drew nothing.
    # These come from the data result itself, so the guards hold and a withheld answer keeps them.
    if latest is not None:
        heatmap = auto_heatmap(*latest)
        if heatmap is not None:
            by_type.pop("chart", None)
            by_type["heatmap"] = heatmap.model_dump(mode="json")
        elif "chart" not in by_type and "heatmap" not in by_type:
            # with a model's table only a trend earns a chart; without one, a ranking does too
            chart = auto_chart(*latest, asked_sort=latest_asked_sort and "table" not in by_type)
            if chart is not None:
                by_type["chart"] = chart.model_dump(mode="json")
    if latest is not None and "kpis" not in by_type and not any(k in by_type for k in ("chart", "table", "heatmap")):
        kpis = auto_kpis(*latest)
        if kpis is not None:
            by_type["kpis"] = kpis.model_dump(mode="json")
    # who was active and when (fix of 14 November): the people from the tool's own list, when the
    # model wrote them as bullets instead of drawing a table; the totals above stay beside it
    if latest is not None and "table" not in by_type:
        # ...and the story of one delivery (step 21): its transactions in time order
        table = auto_people_table(*latest) or auto_story_table(*latest)
        if table is not None:
            by_type["table"] = table.model_dump(mode="json")
    # the watchable things the data calls named (step 14c), read from the results, never the text
    entities = entities_from_trace(trace)
    if entities is not None and "entities" not in by_type:
        by_type["entities"] = entities
    ordered = [by_type[k] for k in ORDER if k in by_type]
    return ordered[:MAX_BLOCKS]


def _items(value) -> list:
    """An argument as a list: a list or tuple as it is, nothing as empty, any single value as one
    item, so a model that sends `rows: 5` is read, never crashes the run.

    >>> _items([1, 2]), _items(None), _items(5), _items("x")
    ([1, 2], [], [5], ['x'])
    """
    if value is None or value == "":
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


def presentation_figures(trace: list[dict]) -> str:
    """Every number a presentation call carries, as one text the figure guards can read.

    >>> presentation_figures([{"tool": "show_chart", "input": {"labels": ["Mon"], "series_values": [[41, 3.5]]}}])
    '41 3.5'
    >>> presentation_figures([{"tool": "show_kpis", "input": {"labels": ["Lines"], "values": ["1,375"]}}])
    '\\n1,375'
    """
    numbers: list[str] = []
    lines: list[str] = []
    for call in trace:
        if not is_presentation_call(call):
            continue
        args = call.get("input")
        if not isinstance(args, dict):
            continue
        # chart values are numbers the model typed: each is checked exactly
        for item in _items(args.get("series_values")):
            for cell in _items(item):
                numbers.extend(_NUMBER.findall(str(cell)))
        # table cells are words, one per line, so "1,928" stays one figure and a date or a time is
        # read as one (fix of 14 November: "1,928" was checked as "1" and "928" and withheld)
        for row in _items(args.get("rows")):
            lines.extend(str(cell) for cell in _items(row) if str(cell).strip())
        # the words of the key figures and the findings (step 19) go through whole, one per line
        for key in TEXT_KEYS:
            lines.extend(str(x) for x in _items(args.get(key)) if str(x).strip())
    return " ".join(numbers) + ("\n" + "\n".join(lines) if lines else "")


def grounded_blocks_from_trace(trace: list[dict]) -> list[dict]:
    """The blocks a withheld answer may still carry: those drawn from the data itself.

    >>> grounded_blocks_from_trace([{"tool": "show_table", "input": {"title": "t", "columns": ["a"], "rows": [["1"]]}, "result": "{}"}])
    []
    """
    return blocks_from_trace(trace, only_grounded=True)
