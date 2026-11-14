"""Hotfix after D20 (chunk 196): a presentation or action call whose argument is a single value
where a list was expected (`rows: 5`, `values: "88%"`, `findings: "x"`) must be read as one
item by the figure guards, never crash the run. On 14 November 2026 the live score run died on
`TypeError: 'int' object is not iterable` in `presentation_figures`."""

from app.agent.presentation import blocks
from app.agent.runtime.pipeline import Draft, UngroundedGuard


def _call(tool, **inp):
    return {"tool": tool, "input": inp, "result": "{}"}


def test_a_scalar_where_a_list_was_expected_is_read_as_one_item():
    trace = [
        _call("show_table", title="t", columns=["a"], rows=5),
        _call("show_kpis", labels="Lines", values="1,375"),
        _call("state_findings", findings="Picked 17 units", limitations=None),
        _call("show_chart", series_values=7),
    ]
    text = blocks.presentation_figures(trace)
    assert text.split("\n")[0] == "5 7"
    assert "1,375" in text and "Picked 17 units" in text


def test_a_call_whose_input_is_not_a_mapping_is_skipped():
    assert blocks.presentation_figures([{"tool": "show_kpis", "input": "values: 3"}]) == ""


def test_the_guard_runs_on_such_a_draft_and_names_the_invented_figure():
    data = {"tool": "aggregate_releases", "input": {}, "result": '{"rows": [{"rows": 4, "picked": 17}]}'}
    draft = Draft(answer="Picked today.", trace=(data, _call("show_kpis", labels="Units", values=9999)),
                  question="how many?", given=("",), schema_tools=frozenset())
    problem = UngroundedGuard().inspect(draft)
    assert problem is not None and problem.figures == ("9999",)
    # and the lift drops the malformed block rather than failing
    assert [b["type"] for b in blocks.blocks_from_trace([data, _call("show_kpis", labels="Units", values=9999)])] in ([], ["kpis"])


def test_the_block_builders_read_a_single_string_as_one_item_not_one_per_letter():
    from app.services.analytics_agent import presentation_tools as pt

    findings = pt.findings_from_args({"findings": "Picked 17 units", "limitations": "One day"})
    assert [f.text for f in findings.findings] == ["Picked 17 units"] and findings.limitations == ["One day"]
    kpis = pt.kpis_from_args({"labels": "Lines", "values": "4"})
    assert [(i.label, i.value) for i in kpis.items] == [("Lines", "4")]
    assert pt.followups_from_args({"items": "Why?"}).items == ["Why?"]
    table = pt.table_from_args({"title": "t", "columns": "item", "rows": ["GINGER"]})
    assert table.rows == [["GINGER"]]
