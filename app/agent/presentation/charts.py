"""The chart chooser (step 19): a chart drawn from a data tool's own result, never from numbers the
model retyped.

The model calls `chart_from_data` naming a measure and, at most, a kind; at answer time the lift
(`blocks.py`) hands this module the last usable data result of the turn, and the labels and values
are copied from it. The kind follows the shape of the data: a time bucket is a trend (line), groups
compare (bar), a few shares of one whole divide (pie). Pure: no I/O, doctested.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from app.agent.presentation.evidence import _cell, _label
from app.services.teams.contracts import ChartBlock, ChartSeries, HeatmapBlock, KpisBlock

TIME_BUCKETS = ("day", "hour", "hour_start", "week", "business_date", "month")
#: A measure the model may name that the rows spell differently: (row key, sign).
ALIASES: dict[str, tuple[str, int]] = {
    "units_short": ("shortfall", -1),
    "lines": ("rows", 1),
    "releases": ("rows", 1),
    "units_picked": ("picked", 1),
    "units_expected": ("expected", 1),
}
UNITS = {
    "users": "users",
    "devices": "devices",
    "calls": "calls",
    "errors": "calls",
    "shortfall": "units",
    "units_short": "units",
    "picked": "units",
    "expected": "units",
    "units_picked": "units",
    "units_expected": "units",
    "rows": "releases",
    "lines": "releases",
    "releases": "releases",
    "deliveries": "deliveries",
    "calls": "calls",
    "late_min": "min",
    "held": "deliveries",
    "missed": "deliveries",
}
MAX_PIE = 8
MAX_POINTS = 48
#: The result keys that make a data call usable as the source of a chart.
SOURCE_KEYS = ("rows", "counts", "summary", "vans", "matrix")
#: The tools whose grouped rows the automatic shape may draw.
GROUPED_TOOLS = ("aggregate_releases", "wms_activity")


def last_data_result(trace: list[dict]) -> tuple[str, dict] | None:
    """The last successful data call whose parsed result carries rows, counts, a summary or vans;
    a schema call, a failed call or a presentation call is skipped.

    >>> last_data_result([{"tool": "aggregate_releases", "result": '{"rows": [1]}'},
    ...                   {"tool": "describe_releases", "result": '{"fields": []}'}])[0]
    'aggregate_releases'
    >>> last_data_result([{"tool": "aggregate_releases", "result": '{"error": "x"}'}]) is None
    True
    """
    found = None
    for call in trace:
        usable = usable_result(call)
        if usable is not None:
            found = (str(call.get("tool")), usable)
    return found


def usable_result(call: dict) -> dict | None:
    """The parsed result of one call when it can source a chart, else None.

    >>> usable_result({"tool": "t", "result": '{"counts": {"missed": 2}}'})
    {'counts': {'missed': 2}}
    >>> usable_result({"tool": "t", "result": '{"problems": ["x"]}'}) is None
    True
    """
    raw = call.get("result")
    if not isinstance(raw, str) or not raw.lstrip().startswith("{"):
        return None
    try:
        result = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(result, dict) or "problems" in result or "error" in result:
        return None
    return result if any(result.get(k) for k in SOURCE_KEYS) else None


def _number(value: Any, measure: str) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError(f"the rows carry no number for {measure!r}")
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        raise ValueError(f"the rows carry no number for {measure!r}") from None


def series_from_result(tool: str, result: dict, measure: str) -> tuple[list[str], list[float], str]:
    """The labels and values a measure gives over a result, and what the labels are: "time" (a
    trend), "group" (a comparison) or "share" (parts of one whole); ValueError naming the measure
    when the result does not carry it.

    >>> series_from_result("aggregate_releases", {"group_by": ["day"], "rows": [
    ...     {"dimensions": ["2026-09-17"], "deliveries": 2}, {"dimensions": ["2026-09-18"], "deliveries": 1}]},
    ...     "deliveries")
    (['2026-09-17', '2026-09-18'], [2.0, 1.0], 'time')
    >>> series_from_result("aggregate_releases", {"group_by": ["item_number"], "rows": [
    ...     {"dimensions": ["100230"], "shortfall": -10}]}, "units_short")
    (['100230'], [10.0], 'group')
    >>> series_from_result("at_risk_history", {"counts": {"missed": 2, "fine": 5}}, "counts")
    (['missed', 'fine'], [2.0, 5.0], 'share')
    """
    key, sign = ALIASES.get(measure, (measure, 1))
    # Step 1: A keyed count map (at-risk history counts, the board's summary) is a set of shares,
    # because its values divide one whole
    for map_key in ("counts", "summary"):
        if measure == map_key and isinstance(result.get(map_key), dict):
            pairs = [(str(k), _number(v, measure)) for k, v in result[map_key].items() if k != "checked"]
            return [k for k, _ in pairs], [v for _, v in pairs], "share"
    # Step 2: Vans by departure day are a trend, by route a comparison; the label is the one that
    # varies so a reader can tell the points apart
    if isinstance(result.get("vans"), list) and result["vans"]:
        vans = result["vans"]
        if not all(isinstance(v, dict) and key in v for v in vans):
            raise ValueError(f"the vans carry no {measure!r}; they carry {sorted(vans[0]) if isinstance(vans[0], dict) else []}")
        days = {str(v.get("day")) for v in vans}
        routes = {str(v.get("route")) for v in vans}
        if len(days) > 1 and len(routes) == 1:
            labels, kind = [str(v.get("day")) for v in vans], "time"
        elif len(days) == 1:
            labels, kind = [str(v.get("route")) for v in vans], "group"
        else:
            labels, kind = [f"{v.get('day')} {v.get('route')}" for v in vans], "time"
        return labels, [sign * _number(v[key], measure) for v in vans], kind
    # Step 3: Grouped rows (aggregate_releases): the first dimension labels the points and a time
    # bucket makes them a trend; a single total has no points and belongs on a KPI card
    rows = result.get("rows")
    group_by = [str(g) for g in result.get("group_by") or []]
    if not isinstance(rows, list) or not rows:
        raise ValueError("the result has no rows to draw")
    if not group_by or not all(isinstance(r, dict) and r.get("dimensions") for r in rows):
        raise ValueError("the result is one total with no groups; show it with show_kpis instead")
    if not all(key in r for r in rows):
        carried = sorted(k for k in rows[0] if k != "dimensions")
        raise ValueError(f"the rows carry no {measure!r}; they carry {carried}")
    labels = [_cell(r["dimensions"][0]) for r in rows]
    values = [sign * _number(r[key], measure) for r in rows]
    return labels, values, "time" if group_by[0] in TIME_BUCKETS else "group"


def choose_kind(label_kind: str, values: list[float], asked: str = "auto") -> str:
    """The chart kind for the data's shape, or the asked one when the contract would accept it.

    >>> choose_kind("time", [1, 2, 3]), choose_kind("group", [5, 2]), choose_kind("share", [3, 1, 2])
    ('line', 'bar', 'pie')
    >>> choose_kind("share", [1] * 9), choose_kind("time", [4]), choose_kind("group", [-3, 2], "pie"), choose_kind("time", [1, 2], "bar")
    ('bar', 'bar', 'bar', 'bar')
    """
    pie_ok = 2 <= len(values) <= MAX_PIE and all(v >= 0 for v in values)
    if asked in ("bar", "line"):
        return asked
    if asked == "pie":
        return "pie" if pie_ok else "bar"
    if label_kind == "time" and len(values) > 1:
        return "line"
    if label_kind == "share" and pie_ok:
        return "pie"
    return "bar"


def chart_title(tool: str, result: dict, measure: str, label_kind: str) -> str:
    """A title a reader can place: the measure, how the points are cut, and the window.

    >>> chart_title("aggregate_releases", {"group_by": ["day"], "window": {"start": "2026-09-11T00:00:00+00:00", "end": "2026-09-19T00:00:00+00:00"}}, "deliveries", "time")
    'Deliveries per day · 2026-09-11 to 2026-09-19'
    >>> chart_title("at_risk_history", {"range": {"start": "2026-09-01", "end": "2026-09-18"}}, "counts", "share")
    'Deliveries by outcome · 2026-09-01 to 2026-09-18'
    """
    words = measure.replace("_", " ")
    if measure in ("counts", "summary"):
        head = "Deliveries by outcome"
    elif result.get("vans"):
        head = f"{words.capitalize()} by {'day' if label_kind == 'time' else 'route'}"
    else:
        group_by = [str(g) for g in result.get("group_by") or []]
        cut = _label(group_by[0]) if group_by else ""
        head = f"{words.capitalize()} {'per' if label_kind == 'time' else 'by'} {cut}".strip()
    window = result.get("window") or result.get("range") or {}
    start, end = str(window.get("start") or "")[:10], str(window.get("end") or "")[:10]
    return f"{head} · {start} to {end}" if start and end else head


def chart_from_result(tool: str, result: dict, args: Mapping[str, Any]) -> ChartBlock:
    """The chart block a `chart_from_data` call draws from a data result, validated by the contract.

    ("aggregate_releases", {group_by [day], rows ...}, {"measure": "deliveries"}) -> ChartBlock(kind "line", ...) ;
    (measure the rows lack) -> ValueError naming what they carry
    """
    measure = str(args.get("measure") or "").strip().lower().replace(" ", "_")
    if not measure:
        raise ValueError("measure is required: the value the rows carry to draw")
    labels, values, label_kind = series_from_result(tool, result, measure)
    kind = choose_kind(label_kind, values, str(args.get("kind") or "auto").strip().lower())
    title = str(args.get("title") or "").strip() or chart_title(tool, result, measure, label_kind)
    # Step 1: A series longer than the contract allows keeps its newest points and says so in the
    # title, because cutting the tail silently drew the first 48 of 336 hours (fix of 14 November)
    total = len(labels)
    if total > MAX_POINTS:
        note = f" (latest {MAX_POINTS} of {total})"
        labels, values = labels[-MAX_POINTS:], values[-MAX_POINTS:]
        title = title[: 120 - len(note)] + note
    return ChartBlock(
        kind=kind,
        title=title[:120],
        labels=labels,
        series=[ChartSeries(name=measure.replace("_", " "), values=values)],
        unit=UNITS.get(measure),
    )


#: The measures an automatic chart prefers when the result is not ranked by one, in order.
AUTO_MEASURES = ("users", "deliveries", "picked", "rows")
MIN_TREND_POINTS = 3
MAX_AUTO_GROUPS = 12
#: The words a key figure wears when Python draws it from a single total.
KPI_WORDS = {
    "rows": "Releases",
    "deliveries": "Deliveries",
    "picked": "Units picked",
    "expected": "Units expected",
    "shortfall": "Shortfall",
    "refused": "Refused calls",
}


def auto_measure(result: dict) -> str | None:
    """The measure an automatic chart draws: what the rows are ranked by, else the first of
    deliveries, picked, rows the rows carry.

    >>> auto_measure({"sort": {"by": "units_short"}, "rows": [{"dimensions": ["a"], "shortfall": -1}]})
    'units_short'
    >>> auto_measure({"sort": {"by": "day"}, "rows": [{"dimensions": ["d"], "rows": 3, "deliveries": 1}]})
    'deliveries'
    """
    rows = result.get("rows") or []
    first = rows[0] if rows and isinstance(rows[0], dict) else {}
    by = str((result.get("sort") or {}).get("by") or "")
    key = ALIASES.get(by, (by, 1))[0]
    if by and by not in TIME_BUCKETS and key in first:
        return by
    return next((m for m in AUTO_MEASURES if m in first), None)


def auto_chart(tool: str, result: dict, *, asked_sort: bool = False) -> ChartBlock | None:
    """The chart Python draws when the model drew none and the data has a shape: a trend of three
    or more points, or a ranking the model asked for (`asked_sort`) of 2 to 12 groups; None for
    anything else, because a plain list of groups is not a comparison the person asked to see.

    >>> auto_chart("aggregate_releases", {"group_by": ["day"], "sort": {"by": "day", "dir": "asc"}, "rows": [
    ...     {"dimensions": [f"2026-09-1{i}"], "rows": i, "deliveries": 1} for i in range(6, 9)]}).kind
    'line'
    >>> auto_chart("aggregate_releases", {"group_by": [], "rows": [{"rows": 4}]}) is None
    True
    """
    if tool not in GROUPED_TOOLS or result.get("matrix"):
        return None
    rows = result.get("rows") or []
    group_by = [str(g) for g in result.get("group_by") or []]
    if not group_by or not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        return None
    is_trend = group_by[0] in TIME_BUCKETS
    is_ranked = asked_sort and bool((result.get("sort") or {}).get("by")) and not is_trend
    if is_trend and len(rows) < MIN_TREND_POINTS:
        return None
    if not is_trend and (not is_ranked or not 2 <= len(rows) <= MAX_AUTO_GROUPS):
        return None
    measure = auto_measure(result)
    if measure is None:
        return None
    try:
        return chart_from_result(tool, result, {"measure": measure, "kind": "auto"})
    except (ValueError, TypeError):
        return None


def auto_kpis(tool: str, result: dict) -> KpisBlock | None:
    """The key figures Python draws from a single total (an aggregate with no groups): the
    releases, deliveries, units picked and expected, the shortfall, as the row carries them.

    >>> auto_kpis("aggregate_releases", {"group_by": [], "rows": [{"rows": 4, "picked": 17, "deliveries": 2}]}).items[1].value
    '2'
    >>> auto_kpis("aggregate_releases", {"group_by": ["day"], "rows": [{"dimensions": ["d"], "rows": 4}]}) is None
    True
    """
    if tool == "wms_activity" and isinstance(result.get("total"), dict):
        total = result["total"]
        items = [
            {"label": words, "value": _cell(total[key])[:24]}
            for key, words in (("users", "People active"), ("devices", "Devices"), ("calls", "Transactions"), ("errors", "Errors"))
            if key in total and total[key] is not None
        ]
        window = result.get("window") or {}
        start, end = str(window.get("start") or "")[:10], str(window.get("end") or "")[:10]
        if items and start and end:
            items[0]["caption"] = start if start == end else f"{start} to {end}"
        return KpisBlock(items=items[:6]) if items else None
    if tool != "aggregate_releases" or result.get("group_by"):
        return None
    rows = result.get("rows") or []
    if len(rows) != 1 or not isinstance(rows[0], dict):
        return None
    row = rows[0]
    items = [
        {"label": words, "value": _cell(row[key])[:24]}
        for key, words in KPI_WORDS.items()
        if key in row and row[key] is not None and _cell(row[key]) != ""
    ]
    if not items:
        return None
    window = result.get("window") or {}
    start, end = str(window.get("start") or "")[:10], str(window.get("end") or "")[:10]
    if start and end:
        items[0]["caption"] = start if start == end else f"{start} to {end}"
    return KpisBlock(items=items[:6])


def auto_heatmap(tool: str, result: dict) -> HeatmapBlock | None:
    """The heat map a result's day-by-hour matrix draws (step 20), validated by the contract.

    >>> auto_heatmap("wms_activity", {"matrix": {"rows": ["Fri 18 Sep"], "columns": ["00", "01"], "values": [[0, 3]],
    ...     "unit": "users", "scale": [1, 2, 4, 9], "outside": None}, "window": {"start": "2026-09-18T00:00:00+01:00", "end": "2026-09-19T00:00:00+01:00"}}).title
    'Active users by hour · 2026-09-18 to 2026-09-19'
    >>> auto_heatmap("wms_activity", {"rows": []}) is None
    True
    """
    matrix = result.get("matrix")
    if not isinstance(matrix, dict) or not matrix.get("rows"):
        return None
    window = result.get("window") or {}
    start, end = str(window.get("start") or "")[:10], str(window.get("end") or "")[:10]
    title = "Active users by hour" + (f" · {start} to {end}" if start and end else "")
    try:
        return HeatmapBlock(
            title=title[:120],
            rows=[str(r)[:40] for r in matrix["rows"]][:62],
            columns=[str(c)[:16] for c in matrix.get("columns") or []][:48],
            values=[[float(v) for v in row] for row in matrix.get("values") or []][:62],
            unit=matrix.get("unit") or "users",
            scale=matrix.get("scale") or [1, 2, 4, 9],
            outside=matrix.get("outside"),
        )
    except (ValueError, TypeError):
        return None
