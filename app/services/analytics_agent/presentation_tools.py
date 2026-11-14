"""The presentation tools (stage D, step 5): how the model asks for a chart, a table or follow-ups.

The model never emits HTML. After it has fetched data it may call `chart_from_data` (step 19: the
numbers are copied from the data call's own result at answer time), `show_chart`, `show_table` or
`suggest_followups` with the numbers exactly as the data tools returned them; the arguments are
schema-checked by the model library, validated here against the wire contract, and lifted off the
trace into typed blocks by `app/agent/presentation/blocks.py`. Arguments are flat (lists of
strings and lists of numbers), because tool calling on the models in use is most reliable that
way. A call that fails validation answers with `problems`, as the data tools do, so the model can
correct it; nothing is drawn from it.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from pydantic import ValidationError

from app.services.teams.contracts import (
    ChartBlock,
    ChartSeries,
    EvidenceColumn,
    FindingsBlock,
    FollowupsBlock,
    KpisBlock,
    TableBlock,
)

SHOW_CHART = "show_chart"
SHOW_TABLE = "show_table"
SUGGEST_FOLLOWUPS = "suggest_followups"
CHART_FROM_DATA = "chart_from_data"
SHOW_KPIS = "show_kpis"
STATE_FINDINGS = "state_findings"
CHART_KINDS = ("auto", "bar", "line", "pie")
TONES = ("good", "medium", "high", "neutral")
PRESENTATION_TOOL_NAMES = frozenset(
    {SHOW_CHART, SHOW_TABLE, SUGGEST_FOLLOWUPS, CHART_FROM_DATA, SHOW_KPIS, STATE_FINDINGS}
)

logger = logging.getLogger(__name__)

PRESENTATION_TOOLS: list[dict[str, Any]] = [
    {
        "name": CHART_FROM_DATA,
        "description": (
            "Draw a chart from the rows the last data call in this turn returned: you name the measure, "
            "the numbers are copied from that result, never retyped. measure: a value the rows carry "
            "(picked, expected, shortfall, units_short, rows, calls, deliveries, refused, late_min, held, "
            "missed) or 'counts' / 'summary' for the at-risk totals. kind 'auto' picks line for a trend "
            "over days or hours, bar for groups, pie for 2 to 8 shares; or name one. At most once per "
            "answer, then write your sentences."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "measure": {"type": "string", "maxLength": 40, "description": "The value the rows carry to draw."},
                "kind": {"type": "string", "enum": list(CHART_KINDS), "description": "auto unless the person asked for one."},
                "title": {"type": "string", "maxLength": 120, "description": "Optional; a title is made from the data when absent."},
            },
            "required": ["measure"],
            "additionalProperties": False,
        },
    },
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
        "name": SHOW_KPIS,
        "description": (
            "Show 1 to 6 key numbers as cards when the answer is a few figures with no groups (a total, a "
            "count, a rate). Each value exactly as a tool returned it, with its unit. Call it at most once, "
            "then write your sentences."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "labels": {"type": "array", "items": {"type": "string", "maxLength": 40}, "minItems": 1, "maxItems": 6},
                "values": {"type": "array", "items": {"type": "string", "maxLength": 24}, "minItems": 1, "maxItems": 6,
                           "description": "One per label, as the tool returned it, e.g. '312' or '88%'."},
                "captions": {"type": "array", "items": {"type": "string", "maxLength": 80}, "maxItems": 6,
                             "description": "One line under each value, e.g. the window or the comparison; optional."},
                "tones": {"type": "array", "items": {"type": "string", "enum": list(TONES)}, "maxItems": 6,
                          "description": "good, medium, high or neutral per value; optional."},
                "title": {"type": "string", "maxLength": 120},
            },
            "required": ["labels", "values"],
            "additionalProperties": False,
        },
    },
    {
        "name": STATE_FINDINGS,
        "description": (
            "State what the data shows: each finding a fact a tool returned in this turn, with its reasoning "
            "(what it is compared with, why it matters) and the data tool it came from; a recommendation only "
            "when the data points to one, with its reason; a limitation when the window, the sample or a failed "
            "call narrows what can be said. Call it once, after the data and the chart or table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "findings": {"type": "array", "items": {"type": "string", "maxLength": 300}, "minItems": 1, "maxItems": 6},
                "reasoning": {"type": "array", "items": {"type": "string", "maxLength": 300}, "maxItems": 6,
                              "description": "One per finding, or empty."},
                "sources": {"type": "array", "items": {"type": "string", "maxLength": 40}, "maxItems": 6,
                            "description": "The data tool each finding comes from, one per finding, or empty."},
                "recommendations": {"type": "array", "items": {"type": "string", "maxLength": 200}, "maxItems": 4},
                "reasons": {"type": "array", "items": {"type": "string", "maxLength": 200}, "maxItems": 4,
                            "description": "One per recommendation, or empty."},
                "limitations": {"type": "array", "items": {"type": "string", "maxLength": 200}, "maxItems": 4},
            },
            "required": ["findings"],
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


def _aligned(args: Mapping[str, Any], key: str, count: int, what: str) -> list[str | None]:
    """A parallel list padded with None, or a ValueError when it is neither empty nor as long.

    ({"captions": ["a"]}, "captions", 2) -> ["a", None] ; ({"captions": ["a", "b", "c"]}, 2) -> ValueError
    """
    values = [str(x) for x in args.get(key) or []]
    if len(values) > count:
        raise ValueError(f"{len(values)} {key} for {count} {what}")
    return values + [None] * (count - len(values))


def kpis_from_args(args: Mapping[str, Any]) -> KpisBlock:
    """The key-figure cards a `show_kpis` call describes, validated by the wire contract.

    ({"labels": ["Lines"], "values": ["312"]}) -> KpisBlock(items=[{label "Lines", value "312"}]) ;
    (two labels, three values) -> ValueError
    """
    labels = [str(x) for x in args.get("labels") or []]
    values = [str(x) for x in args.get("values") or []]
    if len(labels) != len(values):
        raise ValueError(f"{len(labels)} labels for {len(values)} values")
    captions = _aligned(args, "captions", len(labels), "labels")
    tones = _aligned(args, "tones", len(labels), "labels")
    return KpisBlock(
        title=str(args.get("title") or "").strip() or None,
        items=[
            {"label": lab, "value": val, "caption": cap or None, "tone": tone or None}
            for lab, val, cap, tone in zip(labels, values, captions, tones)
        ],
    )


def findings_from_args(args: Mapping[str, Any], called: frozenset[str] | None = None) -> FindingsBlock:
    """The findings block a `state_findings` call describes; with `called` (the data tools that ran
    before it), a finding sourced to a tool that did not run is dropped, and none left is an error.

    ({"findings": ["x"], "sources": ["aggregate_releases"]}, called={"aggregate_releases"}) -> FindingsBlock ;
    (sources ["at_risk_board"], called={"aggregate_releases"}) -> ValueError
    """
    texts = [str(x) for x in args.get("findings") or []]
    reasoning = _aligned(args, "reasoning", len(texts), "findings")
    sources = _aligned(args, "sources", len(texts), "findings")
    findings = []
    for text, why, source in zip(texts, reasoning, sources):
        if called is not None and source and source not in called:
            logger.warning("agent.finding_dropped source=%s reason=not_called", source)
            continue
        findings.append({"text": text, "reasoning": why or None, "source": source or None})
    if texts and not findings:
        raise ValueError("no finding names a tool that was called in this turn")
    recommendations = [str(x) for x in args.get("recommendations") or []]
    reasons = _aligned(args, "reasons", len(recommendations), "recommendations")
    return FindingsBlock(
        findings=findings,
        recommendations=[{"text": t, "reason": r or None} for t, r in zip(recommendations, reasons)],
        limitations=[str(x) for x in args.get("limitations") or []],
    )


def _numeric(cell: str) -> bool:
    """Whether a cell reads as a number.

    >>> _numeric("1,250.5"), _numeric("97.6%"), _numeric("GINGER")
    (True, True, False)
    """
    return bool(cell) and all(ch in "0123456789.,%-" for ch in cell.strip())


def block_from_call(
    name: str,
    args: Mapping[str, Any],
    *,
    data: tuple[str, dict] | None = None,
    called: frozenset[str] | None = None,
) -> dict:
    """The validated block for one presentation call, as a plain dict; raises on a bad call. `data`
    is the last usable data call of the turn (tool, result), which `chart_from_data` draws from.

    ("show_chart", {...}) -> {"type": "chart", ...} ; ("show_table", {...}) -> {"type": "table", ...} ;
    ("chart_from_data", {"measure": "deliveries"}, data=("aggregate_releases", {...})) -> {"type": "chart", ...}
    """
    if name == CHART_FROM_DATA:
        # imported here: the presentation package imports this module for the lift (no cycle)
        from app.agent.presentation import charts

        if data is None:
            raise ValueError("chart_from_data needs a data call before it in this turn")
        return charts.chart_from_result(data[0], data[1], args).model_dump(mode="json")
    if name == SHOW_KPIS:
        return kpis_from_args(args).model_dump(mode="json")
    if name == STATE_FINDINGS:
        return findings_from_args(args, called).model_dump(mode="json")
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
    # Step 1: A chart from the data is drawn at answer time from the trace, so the call itself
    # only checks its shape and acknowledges; a measure the rows lack drops the chart at the lift
    if name == CHART_FROM_DATA:
        measure = str(args.get("measure") or "").strip()
        kind = str(args.get("kind") or "auto")
        if not measure:
            return json.dumps({"problems": ["measure is required: the value the rows carry to draw"]})
        if kind not in CHART_KINDS:
            return json.dumps({"problems": [f"kind must be one of {', '.join(CHART_KINDS)}"]})
        return json.dumps({"shown": "chart", "measure": measure, "note": "drawn from the last data call at answer time"})
    try:
        block = block_from_call(name, args)
    except (ValidationError, ValueError, TypeError) as exc:
        problems = [e.get("msg", str(e)) for e in exc.errors()] if isinstance(exc, ValidationError) else [str(exc)]
        return json.dumps({"problems": problems[:3]})
    if block["type"] == "chart":
        return json.dumps({"shown": "chart", "points": len(block["labels"])})
    if block["type"] == "table":
        return json.dumps({"shown": "table", "rows": len(block["rows"])})
    if block["type"] == "followups":
        return json.dumps({"shown": "followups", "items": len(block["items"])})
    return json.dumps({"shown": block["type"]})
