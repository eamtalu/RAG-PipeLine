"""Chunk 132: the logspace evidence table carries only columns with something in them."""

import json

from app.services.logspace_agent.evidence import render


def _agg(rows, group_by=("user",)):
    return [{"tool": "aggregate", "input": {}, "result": json.dumps({"group_by": list(group_by), "rows": rows})}]


def test_an_all_zero_errors_column_and_an_all_zero_sum_are_left_out():
    table = render(_agg([{"user": "A", "count": 12, "errors": 0, "sum:QuantityPicked": 0},
                         {"user": "B", "count": 7, "errors": 0, "sum:QuantityPicked": 0}]))
    head = table.splitlines()[0]
    assert head == "| user | count |"


def test_columns_with_values_stay():
    table = render(_agg([{"user": "A", "count": 3, "errors": 1, "sum:QuantityPicked": 9}]))
    assert table.splitlines()[0] == "| user | count | errors | QuantityPicked |"


def test_a_trace_table_links_each_request():
    rows = [{"time": "06:01:00", "method": "ConfirmPickLine", "status": "success", "reqid": "R1",
             "link": "http://eye/?date=2026-09-30&reqid=R1", "QuantityPicked": "3.0"}]
    table = render([{"tool": "trace", "input": {}, "result": json.dumps({"total": 1, "transactions": rows})}])
    assert "[R1](http://eye/?date=2026-09-30&reqid=R1)" in table and "| 3 |" in table
