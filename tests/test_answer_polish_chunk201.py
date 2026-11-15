"""Step 23 (chunk 201), from the live answers of 15 November 2026 on tmp-live:

- "How long does a line take per picker today?" found one picker settled (a Sunday) and drew a
  one-bar chart: a chart needs at least two points;
- the same answer listed its follow-up questions as text lines as well as calling
  suggest_followups: the lines that repeat the chips are taken off the text;
- its bullets explained the method instead of stating facts, and an answer about delivery 33330
  said "9 packages" where the data has 8 packages and 9 photos: the policy says a bullet carries a
  fact from the data and each count goes by its own label."""

import json

import pytest

from app.agent.presentation import blocks, charts
from app.agent.presentation.blocks import strip_followup_lines
from app.agent.profiles import load_profile
from app.services.analytics_agent import presentation_tools as pt

ONE_GROUP = {"group_by": ["user_name"], "sort": {"by": "median_duration", "dir": "asc"},
             "rows": [{"dimensions": ["BCHAM"], "rows": 264, "median_duration": 59.5}]}
FOLLOWUPS = ["How does that compare to yesterday?", "Which picker was fastest today?",
             "What was the average pick rate per hour?", "Show me the breakdown by zone"]
# the live answer of 15 November, run 1 (the text before the rendered table)
LIVE = ("The median time per pick-list line today was 59.5 seconds for picker BCHAM, across 264 releases.\n\n"
        "- This measurement is specific to picker BCHAM.\n\n"
        "How does that compare to yesterday?  \n"
        "Which picker was fastest today?  \n"
        "What was the average pick rate per hour?  \n"
        "Show me the breakdown by zone")


def _call(tool, result="{}", **inp):
    return {"tool": tool, "input": inp, "result": json.dumps(result) if isinstance(result, dict) else result}


# ==================================================== 1. a chart needs two points


def test_a_chart_from_data_with_one_point_is_not_drawn():
    with pytest.raises(ValueError, match="at least two points"):
        charts.chart_from_result("aggregate_releases", ONE_GROUP, {"measure": "median_duration"})
    drawn = blocks.blocks_from_trace([_call("aggregate_releases", ONE_GROUP, group_by=["user_name"], sort="median_duration"),
                                      _call("chart_from_data", measure="median_duration")])
    assert "chart" not in [b["type"] for b in drawn]


async def test_a_typed_chart_with_one_point_is_refused_with_a_reason():
    one = {"kind": "bar", "title": "t", "labels": ["BCHAM"], "series_names": ["s"], "series_values": [[59.5]]}
    answer = json.loads(await pt.run_presentation_tool("show_chart", one))
    assert answer["problems"] == ["a chart needs at least two points; say the one value in words or show_kpis"]
    assert blocks.blocks_from_trace([_call("show_chart", **one)]) == []
    two = {**one, "labels": ["A", "B"], "series_values": [[1, 2]]}
    assert json.loads(await pt.run_presentation_tool("show_chart", two)) == {"shown": "chart", "points": 2}


# ==================================================== 2. follow-ups written as text are taken off


def test_the_live_answers_follow_up_lines_are_taken_off_when_the_chips_carry_them():
    trace = [_call("aggregate_releases", ONE_GROUP), _call("suggest_followups", items=FOLLOWUPS)]
    assert strip_followup_lines(LIVE, trace) == (
        "The median time per pick-list line today was 59.5 seconds for picker BCHAM, across 264 releases.\n\n"
        "- This measurement is specific to picker BCHAM.")


def test_a_question_in_the_text_stays_when_no_chips_carry_it():
    text = "15 users were active on 2026-11-02.\n\nDo you want the same for last month?"
    assert strip_followup_lines(text, [_call("wms_activity", {"rows": [1]})]) == text
    # with chips, a question line that is not one of them stays unless it ends a run of questions
    trace = [_call("suggest_followups", items=["Show by route"])]
    assert strip_followup_lines(text, trace) == text


def test_a_bulleted_or_numbered_follow_up_list_that_repeats_the_chips_is_taken_off_too():
    text = "Lead.\n\n- Fact 1\n\nYou could ask:\n- How does that compare to yesterday?\n- Which picker was fastest today?"
    trace = [_call("suggest_followups", items=FOLLOWUPS)]
    assert strip_followup_lines(text, trace) == "Lead.\n\n- Fact 1"


# ==================================================== 3. the policy: facts, and each count by its label


def test_the_policy_asks_for_facts_in_bullets_and_each_count_by_its_label():
    prompt = " ".join(load_profile("analytics").system_prompt.split())
    assert "each a fact from the data, not a note on the method" in prompt
    assert "Name each count by its own label" in prompt
    assert "packages are not photos" in prompt
    assert "never also write them in the text" in prompt


def test_the_live_heading_and_rule_before_the_follow_ups_go_with_them():
    # the live answer of 14 November to "Show active users per hour for the last 2 weeks"
    text = ("The data only covers the last 14 days.\n\n---\n\n**Follow-up questions:**\n\n"
            "- Show active users per day for the last 2 weeks  \n- Which days had the highest user activity?")
    trace = [_call("suggest_followups", items=["Show active users per day for the last 2 weeks",
                                               "Which days had the highest user activity?"])]
    assert strip_followup_lines(text, trace) == "The data only covers the last 14 days."
    heading = "Lead.\n\n## Follow-ups\n- Which days had the highest user activity?"
    assert strip_followup_lines(heading, trace) == "Lead."
    # a rule that separates two parts of the answer stays when no follow-ups come after it
    kept = "Part one.\n\n---\n\nPart two."
    assert strip_followup_lines(kept, trace) == kept
