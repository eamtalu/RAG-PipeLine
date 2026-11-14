"""Fix after D20 (chunk 197), from the live tmp-live answers of 14 November 2026:

- 21:17: a table cell "1,928" was read by the figure guard as "1" and "928", so a correct answer
  was withheld (the chart only showed because the withheld path keeps the data's own chart);
- 21:27: the model drew a table for users per day, and a table switched the trend chart off;
- 21:19: the model drew `chart_from_data` on hourly data, which switched the heat map off and drew
  the first 48 of 336 hours as a line.

The data's shape now wins for trends and day-by-hour grids, a formatted number is one number, and
a chart longer than the contract allows keeps its newest points and says so."""

import json

from app.agent.presentation import blocks, charts
from app.agent.runtime.pipeline import Draft, UngroundedGuard

# the daily result as wms_activity returned it on tmp-live (14 days, users per day; 2026-11-01..14)
DAYS = ["2026-11-%02d" % d for d in range(1, 15)]
USERS = [3, 15, 12, 15, 12, 11, 9, 5, 13, 12, 13, 11, 12, 9]
CALLS = [1928, 8733, 6456, 8642, 7939, 11278, 5037, 2450, 7504, 7642, 7947, 7372, 8727, 6286]
DAILY = {
    "group_by": ["day"], "grain": "day", "by": "none",
    "window": {"start": "2026-11-01T00:00:00+00:00", "end": "2026-11-14T21:27:00+00:00", "timezone": "Europe/London"},
    "rows": [{"dimensions": [d], "users": u, "devices": u, "calls": c, "errors": 1, "first_at": f"{d} 00:00", "last_at": f"{d} 23:59"}
             for d, u, c in zip(DAYS, USERS, CALLS)],
    "total": {"users": 22, "devices": 14, "calls": 97813, "errors": 45},
}
# the hourly result: 14 days x 24 hours, with the matrix the heat map is drawn from
HOURLY = {
    **DAILY, "group_by": ["hour"], "grain": "hour",
    "rows": [{"dimensions": [f"{d} {h:02d}:00"], "users": 1, "devices": 1, "calls": 1, "errors": 0} for d in DAYS for h in range(24)],
    "matrix": {"rows": [f"Day {d[-2:]}" for d in DAYS], "days": DAYS, "columns": ["%02d" % h for h in range(24)],
               "values": [[(h % 5) for h in range(24)] for _ in DAYS], "unit": "users", "scale": [1, 2, 4, 9], "outside": None},
}
# the model's table at 21:17 (counts with thousands separators) and at 21:27 (without)
TABLE_COMMAS = {"title": "Users active per day · last 14 days", "columns": ["Date", "Users", "Devices", "Calls", "First", "Last"],
                "rows": [[d, str(u), str(u), f"{c:,}", "00:00", "23:59"] for d, u, c in zip(DAYS, USERS, CALLS)]}
TABLE_PLAIN = {**TABLE_COMMAS, "rows": [[d, str(u), str(u), str(c), "00:00", "23:59"] for d, u, c in zip(DAYS, USERS, CALLS)]}


def _call(tool, result=None, **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result) if isinstance(result, dict) else (result or "{}")}


def _draft(trace):
    return Draft(answer="15 users were active on 2026-11-02.", trace=tuple(trace), question="How many different users were active each day in the last 2 weeks?",
                 given=("",), schema_tools=frozenset())


# ==================================================== 1. a formatted number is one number


def test_the_live_table_with_thousands_separators_is_grounded():
    trace = [_call("wms_activity", DAILY, grain="day", days=14), _call("show_table", **TABLE_COMMAS)]
    assert UngroundedGuard().inspect(_draft(trace)) is None


def test_an_invented_formatted_number_in_a_table_is_still_caught():
    rows = [r[:] for r in TABLE_COMMAS["rows"]]
    rows[0][3] = "9,999"
    problem = UngroundedGuard().inspect(_draft([_call("wms_activity", DAILY), _call("show_table", **{**TABLE_COMMAS, "rows": rows})]))
    assert problem is not None and problem.figures == ("9,999",)


def test_chart_values_are_still_checked_exactly():
    trace = [_call("wms_activity", DAILY), _call("show_chart", kind="line", title="t", labels=DAYS[:2], series_names=["users"], series_values=[[3, 999]])]
    problem = UngroundedGuard().inspect(_draft(trace))
    assert problem is not None and problem.figures == ("999",)


def test_the_figure_text_keeps_chart_numbers_and_table_cells_as_lines():
    text = blocks.presentation_figures([_call("show_chart", series_values=[[41, 3.5]]), _call("show_table", rows=[["GINGER", "1,928"]])])
    assert text == "41 3.5\nGINGER\n1,928"


# ==================================================== 2. the data's shape wins for a trend


def test_a_trend_gets_its_chart_even_when_the_model_drew_a_table():
    out = blocks.blocks_from_trace([_call("wms_activity", DAILY, grain="day", days=14), _call("show_table", **TABLE_PLAIN),
                                    _call("state_findings", findings=["15 users were active on 2026-11-02"], sources=["wms_activity"])])
    assert [b["type"] for b in out] == ["chart", "table", "findings"]
    assert out[0]["kind"] == "line" and out[0]["labels"] == DAYS and out[0]["series"][0]["values"] == [float(u) for u in USERS]


def test_a_model_chart_of_the_trend_is_kept_and_not_doubled():
    out = blocks.blocks_from_trace([_call("wms_activity", DAILY), _call("chart_from_data", measure="calls"), _call("show_table", **TABLE_PLAIN)])
    assert [b["type"] for b in out] == ["chart", "table"] and out[0]["series"][0]["name"] == "calls"


def test_a_ranking_with_the_models_table_stays_a_table():
    ranked = {"group_by": ["item_number"], "sort": {"by": "units_short", "dir": "desc"},
              "rows": [{"dimensions": ["100230"], "shortfall": -10, "rows": 2}, {"dimensions": ["104568"], "shortfall": -1, "rows": 1}]}
    out = blocks.blocks_from_trace([_call("aggregate_releases", ranked, group_by=["item_number"], sort="units_short"),
                                    _call("show_table", title="t", columns=["item", "short"], rows=[["100230", "10"], ["104568", "1"]])])
    assert [b["type"] for b in out] == ["entities", "table"]


def test_a_two_point_series_with_a_table_gets_no_chart():
    short = {**DAILY, "rows": DAILY["rows"][:2]}
    out = blocks.blocks_from_trace([_call("wms_activity", short), _call("show_table", title="t", columns=["d"], rows=[["2026-11-01"]])])
    assert [b["type"] for b in out] == ["table"]


def test_a_withheld_answer_keeps_the_trend_chart():
    trace = [_call("wms_activity", DAILY), _call("show_table", **TABLE_PLAIN)]
    assert [b["type"] for b in blocks.grounded_blocks_from_trace(trace)] == ["chart"]


# ==================================================== 3. the day-by-hour grid is always a heat map


def test_an_hourly_grid_is_a_heat_map_even_when_the_model_drew_a_chart_of_hours():
    out = blocks.blocks_from_trace([_call("wms_activity", HOURLY, grain="hour", days=14), _call("chart_from_data", measure="users"),
                                    _call("state_findings", findings=["Busiest at 04:00"], sources=["wms_activity"])])
    assert [b["type"] for b in out] == ["heatmap", "findings"]
    assert len(out[0]["rows"]) == 14 and len(out[0]["columns"]) == 24


def test_an_hourly_grid_beside_the_models_table_is_still_a_heat_map():
    out = blocks.blocks_from_trace([_call("wms_activity", HOURLY), _call("show_table", title="t", columns=["h"], rows=[["09"]])])
    assert [b["type"] for b in out] == ["heatmap", "table"]


# ==================================================== 4. a long series keeps its newest points


def test_a_series_longer_than_the_contract_keeps_its_newest_points_and_says_so():
    long_days = {**DAILY, "rows": [{"dimensions": [f"d{i:03d}"], "users": i} for i in range(60)]}
    chart = charts.chart_from_result("wms_activity", long_days, {"measure": "users"})
    assert len(chart.labels) == 48 and chart.labels[0] == "d012" and chart.labels[-1] == "d059"
    assert chart.series[0].values[-1] == 59.0 and chart.title.endswith("(latest 48 of 60)")
