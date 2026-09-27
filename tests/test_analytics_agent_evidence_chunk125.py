"""Chunk 125: figures are checked against tool results, and ranked tables come from the rows.

Pure tests over text. The two failures they encode happened on the live tenant with an 8B
model: a table of "Customer A … J" with round numbers that no tool ever returned, and a
server-sorted top-5 copied out in the wrong order with rows missing.
"""

import json

from app.services.analytics_agent import evidence as ev

RESULT = json.dumps({
    "group_by": ["item_number", "lookup:item description.ItemDescription"], "sort": {"by": "units_short", "dir": "desc"},
    "window": {"start": "2026-09-20T00:00:00+00:00", "end": "2026-09-27T23:59:59+00:00"}, "total_rows": 93,
    "rows": [
        {"dimensions": ["104607", "POTATO AGRIA"], "rows": 25, "shortfall": "-809", "picked": "225", "expected": "1034"},
        {"dimensions": ["100622", "MILK SEMI SKIMMED _2ltr"], "rows": 38, "shortfall": "-329", "picked": "0", "expected": "329"},
        {"dimensions": ["101998", "MELON WATER"], "rows": 19, "shortfall": "-100.000001", "picked": "93.333", "expected": "193.333"},
        {"dimensions": ["104516", "CABBAGE WHITE"], "rows": 7, "shortfall": "-92.584545", "picked": "200", "expected": "292.585"},
    ]})


def test_figures_are_the_numbers_a_reader_takes_as_data():
    text = "1. **POTATO AGRIA** - 809 units short across 25 releases on 2026-09-20 at 06:42, 87.5% exact, 1,034 expected"
    assert ev.figures(text) == ["809", "25", "87.5%", "1,034"]


def test_an_invented_answer_names_every_figure_it_could_not_find():
    answer = "Top customers: Customer A -1,200 units, Customer B -950 units. The grain is across 103 releases."
    assert ev.ungrounded(answer, [RESULT], "Top 10 customers by units short this week") == ["-1,200", "-950", "103"]


def test_a_faithful_answer_has_nothing_ungrounded():
    answer = ("Across 93 releases this week POTATO AGRIA was 809 units short (25 releases, 1,034 expected, 225 picked), "
              "MILK 329, MELON WATER 100, CABBAGE WHITE 92.58; the top 5 asked for.")
    assert ev.ungrounded(answer, [RESULT], "top 5 shorted products this week") == []


def test_rounding_percentages_small_counts_and_the_question_are_allowed():
    assert ev.ungrounded("MELON WATER 100.0 and CABBAGE 92.6; 11.5% of lines; 3 pickers; top 10", [RESULT], "top 10") == []
    assert ev.ungrounded("that is 27.3% short", [RESULT]) == []
    assert ev.ungrounded("about 2,000 units", [RESULT]) == ["2,000"]


def test_the_ranked_table_is_rendered_from_the_rows_in_the_servers_order():
    trace = [{"tool": "describe_releases", "input": {}, "result": "{}"},
             {"tool": "aggregate_releases", "input": {"sort": "shortfall", "dir": "asc"}, "result": RESULT}]
    table = ev.render(trace)
    assert table.startswith("From the data: 4 of 4 group(s), sorted by units short desc, 2026-09-20 to 2026-09-27; 93 releases across the groups returned.")
    lines = table.splitlines()
    assert lines[2] == "| item number | ItemDescription | releases | units short |"
    assert lines[4] == "| 104607 | POTATO AGRIA | 25 | 809 |"
    assert lines[5] == "| 100622 | MILK SEMI SKIMMED _2ltr | 38 | 329 |"
    assert lines[7] == "| 104516 | CABBAGE WHITE | 7 | 92.58 |"


def test_no_table_without_a_sort_or_without_rows():
    assert ev.render([{"tool": "aggregate_releases", "input": {}, "result": RESULT}]) is None
    assert ev.render([{"tool": "aggregate_releases", "input": {"sort": "rows"}, "result": '{"rows": []}'}]) is None
    assert ev.render([{"tool": "aggregate_releases", "input": {"sort": "rows"}, "result": '{"error": "x"}'}]) is None


def test_a_listing_is_rendered_too():
    listing = json.dumps({"total": 6, "rows": [{"key": ["550671"], "user_name": "SGIAMPORCA", "item_number": "104607",
                                                 "attributes": {"expected": "10", "picked": "9", "duration_s": "750.819"},
                                                 "looked_up": {"item description.ItemDescription": "POTATO AGRIA"}}]})
    table = ev.render([{"tool": "list_releases", "input": {"where": ["duration_s>300"]}, "result": listing}])
    assert table.startswith("From the data: 1 of 6 matching release(s).")
    assert "| 550671 | SGIAMPORCA | 104607 | POTATO AGRIA | 10 | 9 | 750.82 |" in table


def test_the_models_own_table_and_ranked_list_are_dropped_and_the_sentences_kept():
    text = ("Here are the top 5:\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n1. **LA PIAZZA** - Shortfall: -2 units\n"
            "2. **VIOS** - Shortfall: -1 unit\n\nThese had the largest shortfalls.")
    assert ev.strip_tables(text) == "Here are the top 5:\n\nThese had the largest shortfalls."


def test_the_table_shows_the_n_the_model_asked_for_else_ten():
    trace = [{"tool": "aggregate_releases", "input": {"sort": "shortfall", "dir": "asc", "limit": 2}, "result": RESULT}]
    assert ev.render(trace).count("\n| ") == 3          # header + 2 rows
    trace[0]["input"].pop("limit")
    assert ev.render(trace).count("\n| ") == 5          # header + all 4 rows (fewer than ten)


def test_the_same_rows_come_out_as_data_for_a_card():
    trace = [{"tool": "aggregate_releases", "input": {"sort": "units_short", "limit": 2}, "result": RESULT}]
    data = ev.structured(trace, link="https://eye.example/matrix/releases")
    assert data["title"] == "Top 2 by units short per item number and ItemDescription"
    assert data["columns"] == [{"name": "item number", "align": "left"}, {"name": "ItemDescription", "align": "left"},
                               {"name": "releases", "align": "right"}, {"name": "units short", "align": "right"}]
    assert data["rows"] == [["104607", "POTATO AGRIA", "25", "809"], ["100622", "MILK SEMI SKIMMED _2ltr", "38", "329"]]
    assert data["facts"] == {"sorted by": "units short, biggest first", "groups": "2 of 4",
                             "grain": "93 releases across the groups returned", "window": "2026-09-20 to 2026-09-27"}
    assert data["link"] == "https://eye.example/matrix/releases"
    # the markdown table and the data table show the same cells
    assert "| 104607 | POTATO AGRIA | 25 | 809 |" in ev.render(trace)


def test_a_day_trend_is_rendered_without_a_sort_with_the_deliveries_column():
    result = json.dumps({"group_by": ["day"], "sort": {"by": "day", "dir": "asc"}, "total_rows": 3,
                         "rows": [{"dimensions": ["2026-09-25"], "rows": 1, "deliveries": 1}, {"dimensions": ["2026-09-26"], "rows": 2, "deliveries": 2}]})
    table = ev.render([{"tool": "aggregate_releases", "input": {"group_by": ["day"]}, "result": result}])
    assert table.splitlines()[2] == "| day | releases | deliveries |"
    assert table.splitlines()[4] == "| 2026-09-25 | 1 | 1 |"
    data = ev.structured([{"tool": "aggregate_releases", "input": {"group_by": ["day"]}, "result": result}])
    assert data["columns"][-1] == {"name": "deliveries", "align": "right"} and data["rows"][1] == ["2026-09-26", "2", "2"]
