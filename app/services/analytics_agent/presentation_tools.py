"""The presentation tools (stage D, step 5): how the model asks for a chart, a table or follow-ups.

The model never emits HTML. After it has fetched data it may call `show_chart`, `show_table` or
`suggest_followups` with the numbers exactly as the data tools returned them; the arguments are
schema-checked by the model library, validated here against the wire contract, and lifted off the
trace into typed blocks by `app/agent/presentation/blocks.py`. Arguments are flat (lists of
strings and lists of numbers), because tool calling on the models in use is most reliable that
way. A call that fails validation answers with `problems`, as the data tools do, so the model can
correct it; nothing is drawn from it.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from pydantic import ValidationError

from app.services.teams.contracts import ChartBlock, ChartSeries, EvidenceColumn, FollowupsBlock, TableBlock

SHOW_CHART = "show_chart"
SHOW_TABLE = "show_table"
SUGGEST_FOLLOWUPS = "suggest_followups"
PRESENTATION_TOOL_NAMES = frozenset({SHOW_CHART, SHOW_TABLE, SUGGEST_FOLLOWUPS})

PRESENTATION_TOOLS: list[dict[str, Any]] = [
    {
        "name": SHOW_CHART,
        "description": (
            "Draw a chart in the answer from numbers a data tool returned in THIS turn. Call it after the "
            "data call, at most once per answer, then write your sentences. kind: 'line' for a trend over "
            "days or hours, 'bar' to compare groups (items, routes, pickers), 'pie' for shares of a whole "
            "with 2 to 8 parts. labels are the x axis (or the pie slices); series_names and series_values "
            "are the lines or bar groups, one list of numbers per series, each the same length as labels. "
            "Pass the numbers exactly as the tool returned them; never total, round or invent one."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["bar", "line", "pie"]},
                "title": {"type": "string", "description": "Short title, e.g. 'Lines per day · last 7 days'."},
                "labels": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 48},
                "series_names": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 6},
                "series_values": {
                    "type": "array",
                    "items": {"type": "array", "items": {"type": "number"}},
                    "minItems": 1,
                    "maxItems": 6,
                    "description": "One list of numbers per series name, each as long as labels.",
                },
                "unit": {"type": "string", "description": "What the numbers are: lines, units, %, deliveries."},
                "stacked": {"type": "boolean", "description": "Stack the bars (parts of a whole per label)."},
            },
            "required": ["kind", "title", "labels", "series_names", "series_values"],
            "additionalProperties": False,
        },
    },
    {
        "name": SHOW_TABLE,
        "description": (
            "Show a table in the answer from rows a data tool returned in THIS turn, when the rows are the "
            "answer (a list of releases, pickers, customers). At most 25 rows and 12 columns; every row has "
            "one cell per column, cells as text exactly as the tool gave them. Call it at most once per answer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "columns": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 12},
                "rows": {"type": "array", "items": {"type": "array", "items": {"type": "string"}}, "maxItems": 25},
            },
            "required": ["title", "columns", "rows"],
            "additionalProperties": False,
        },
    },
    {
        "name": SUGGEST_FOLLOWUPS,
        "description": (
            "Offer up to four short follow-up questions the person might ask next, as buttons under the "
            "answer. Call it last, at most once; each item is a question in the person's words."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"items": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 4}},
            "required": ["items"],
            "additionalProperties": False,
        },
    },
]


def chart_from_args(args: Mapping[str, Any]) -> ChartBlock:
    """The chart block a `show_chart` call describes, validated by the wire contract.

    ({"kind": "bar", "title": "t", "labels": ["a"], "series_names": ["s"], "series_values": [[1]]}) -> ChartBlock
    """
    names = list(args.get("series_names") or [])
    values = list(args.get("series_values") or [])
    if len(names) != len(values):
        raise ValueError(f"{len(names)} series names for {len(values)} value lists")
    return ChartBlock(
        kind=str(args.get("kind", "")), title=str(args.get("title", "")),
        labels=[str(x) for x in args.get("labels") or []],
        series=[ChartSeries(name=str(n), values=[float(v) for v in vals]) for n, vals in zip(names, values)],
        unit=args.get("unit") or None, stacked=bool(args.get("stacked", False)))


def table_from_args(args: Mapping[str, Any]) -> TableBlock:
    """The table block a `show_table` call describes, numbers right-aligned.

    ({"title": "t", "columns": ["item", "short"], "rows": [["A", "1.5"]]}) -> TableBlock
    """
    rows = [[str(c) for c in row] for row in args.get("rows") or []]
    columns = [str(c) for c in args.get("columns") or []]
    aligns = ["right" if rows and all(_numeric(r[i]) for r in rows if i < len(r)) else "left"
              for i in range(len(columns))]
    return TableBlock(title=str(args.get("title", "")),
                      columns=[EvidenceColumn(name=c, align=a) for c, a in zip(columns, aligns)], rows=rows)


def followups_from_args(args: Mapping[str, Any]) -> FollowupsBlock:
    return FollowupsBlock(items=[str(x) for x in args.get("items") or []])


def _numeric(cell: str) -> bool:
    """Whether a cell reads as a number.

    >>> _numeric("1,250.5"), _numeric("97.6%"), _numeric("GINGER")
    (True, True, False)
    """
    return bool(cell) and all(ch in "0123456789.,%-" for ch in cell.strip())


def block_from_call(name: str, args: Mapping[str, Any]) -> dict:
    """The validated block for one presentation call, as a plain dict; raises on a bad call.

    ("show_chart", {...}) -> {"type": "chart", ...} ; ("show_table", {...}) -> {"type": "table", ...}
    """
    if name == SHOW_CHART:
        return chart_from_args(args).model_dump(mode="json")
    if name == SHOW_TABLE:
        return table_from_args(args).model_dump(mode="json")
    if name == SUGGEST_FOLLOWUPS:
        return followups_from_args(args).model_dump(mode="json")
    raise ValueError(f"unknown presentation tool {name!r}")


async def run_presentation_tool(name: str, args: Mapping[str, Any]) -> str:
    """What the model is told back: an acknowledgement, or the problems with the call.

    ("show_chart", valid) -> '{"shown": "chart", "points": 7}' ; (series too short) -> '{"problems": [...]}'
    """
    try:
        block = block_from_call(name, args)
    except (ValidationError, ValueError, TypeError) as exc:
        problems = [e.get("msg", str(e)) for e in exc.errors()] if isinstance(exc, ValidationError) else [str(exc)]
        return json.dumps({"problems": problems[:3]})
    if block["type"] == "chart":
        return json.dumps({"shown": "chart", "points": len(block["labels"])})
    if block["type"] == "table":
        return json.dumps({"shown": "table", "rows": len(block["rows"])})
    return json.dumps({"shown": "followups", "items": len(block["items"])})
