"""Step 24 (chunk 202), the root causes found after the live check of 15 November 2026:

1. A run of exactly ten tool rounds that then answered was reported as out of tool calls and its
   finished answer was thrown away: LangGraph needs one more step than `max_iterations * 2 + 1`.
2. The model wrote its full answer beside its suggest_followups call and then closed with a remark
   ("The follow-up options are ready…"); only the remark was kept.
3. The figure guard read "1,042" in a tool result as 1 and 42, read the year of a written date as
   a figure, and never checked a figure written right before a full stop.
4. A bulleted lead-in ("- Suggested follow-ups:") and a typed "[Line chart: …]" placeholder stayed.
5. Asked "by how many units", the model added up the shortfall itself; the tool now returns the
   sum over every group as `overall`."""

import json

import pytest
from langchain_core.messages import AIMessage

from app.agent.presentation import evidence
from app.agent.presentation.blocks import drop_followup_lead, strip_followup_lines
from app.agent.presentation.written_calls import strip_placeholders
from app.agent.profiles import load_profile
from app.agent.runtime.grounded import answer_from_draft
from app.config.database import async_session
from app.services.analytics_agent import tools as agent_tools
from tests.agent_helpers import analytics_runtime, ask
from tests.golden import plant
from tests.test_analytics_agent_chunk124 import _scripted

CC = "test_chunk202"


@pytest.fixture(autouse=True)
async def clean():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    yield
    await plant.wipe_analytics(CC)


def _rounds(n: int) -> list[AIMessage]:
    return [AIMessage(content="", tool_calls=[{"id": f"c{i}", "name": "describe_releases", "args": {}}]) for i in range(n)]


# ==================================================== 1. ten tool rounds and an answer is a finished run


async def test_ten_tool_rounds_then_an_answer_ends_the_turn():
    model = _scripted(*_rounds(10), AIMessage(content="The releases are described."))
    result = await ask(analytics_runtime(CC, model=model), "what is available?")
    assert result["stop_reason"] == "end_turn" and result["answer"] == "The releases are described."


async def test_an_eleventh_tool_round_is_still_out_of_tool_calls():
    model = _scripted(*_rounds(11), AIMessage(content="The releases are described."))
    result = await ask(analytics_runtime(CC, model=model), "what is available?")
    assert result["stop_reason"] == "max_iterations"


# ==================================================== 2. the answer written beside the last call


ANSWER = "Delivery 33330 was picked with 12 lines, 9 exact and 3 over."
FOLLOWUPS = {"id": "f", "name": "suggest_followups", "args": {"items": ["Who picked it?"]}}
DATA = {"id": "d", "name": "delivery_story", "args": {"value": "33330"}}


def test_a_closing_remark_gives_way_to_the_answer_written_beside_the_follow_up_call():
    new = [AIMessage(content="", tool_calls=[DATA]),
           AIMessage(content=ANSWER, tool_calls=[FOLLOWUPS]),
           AIMessage(content="The follow-up options are ready. Let me know which one you'd like to explore.")]
    assert answer_from_draft(new) == ANSWER
    # an empty closing message too
    assert answer_from_draft(new[:2] + [AIMessage(content="")]) == ANSWER


def test_the_final_message_wins_whenever_it_carries_figures_or_nothing_else_was_written_after_the_data():
    repeated = [AIMessage(content="", tool_calls=[DATA]), AIMessage(content=ANSWER + " More detail here.", tool_calls=[FOLLOWUPS]),
                AIMessage(content="Delivery 33330: 12 lines.")]
    assert answer_from_draft(repeated) == "Delivery 33330: 12 lines."
    # words beside a DATA call are narration, never the answer
    narrated = [AIMessage(content="Let me read the 3 days of releases first, then the 2 routes.", tool_calls=[DATA]),
                AIMessage(content="No products were short today.")]
    assert answer_from_draft(narrated) == "No products were short today."
    assert answer_from_draft([AIMessage(content="Plain answer.")]) == "Plain answer."


async def test_a_scripted_run_keeps_the_answer_written_beside_the_follow_up_call():
    await plant.plant_analytics(CC)
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}]),
        AIMessage(content="Across 3 releases, two items were short: 100230 by 10 units, 104568 by 1.",
                  tool_calls=[{"id": "c2", "name": "suggest_followups", "args": {"items": ["Who picked them?"]}}]),
        AIMessage(content="The follow-up options are ready."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert result["stop_reason"] == "end_turn" and result["answer"].startswith("Across 3 releases, two items were short")


# ==================================================== 3. the guard reads numbers as a person writes them


FORECAST = json.dumps({"days": [{"day": "Wed 18", "range_text": "777 – 1,042"}, {"day": "Fri 20", "range_text": "788 – 1,048"}],
                       "week": {"value": "5,231"}, "lines_expected_text": "957"})


def test_a_thousands_figure_in_a_result_is_read_whole():
    pool = evidence.numbers_in_results([FORECAST])
    assert {1042, 1048, 5231, 957} <= pool
    text = "Tomorrow, Monday 16 November 2026, about 957 lines are expected. The week brings 5,231 lines; Wednesday could reach 1,042 and Friday 1,048."
    assert evidence.ungrounded(text, [FORECAST]) == []


def test_the_year_of_a_written_date_is_not_a_figure():
    assert evidence.figures("On 16 November 2026, 957 lines.") == ["957"]
    assert evidence.figures("On November 16, 2026 and in November 2026 and on Monday 16th Nov 2026: 957") == ["957"]
    # a year on its own is still a figure
    assert evidence.figures("In 2026 we had 957 lines") == ["2026", "957"]


def test_a_figure_before_a_full_stop_is_checked():
    assert evidence.ungrounded("Short by 5,999.", ["{}"]) == ["5,999"]
    assert evidence.ungrounded("The median was 7.5.", ["{}"]) == ["7.5"]
    assert evidence.ungrounded("The median was 7.5.", ['{"median": 7.5}']) == []
    # versions, addresses and dotted keys are still not figures
    assert evidence.figures("Build 1.2.3 on 10.0.0.1 and key 4.5.6") == []
    assert evidence.figures("957, 1,042 and 1,048") == ["957", "1,042", "1,048"]


# ==================================================== 4. leftover lines


CHIPS = [{"tool": "suggest_followups", "input": {"items": ["Show lines picked per day", "Which day had the most units?"]}, "result": "{}"}]


def test_a_bulleted_lead_in_goes_with_the_follow_ups_and_a_fact_bullet_stays():
    text = "Deliveries ranged from 30 to 112.\n\n- Fact one\n\n- Suggested follow-ups:\n- Show lines picked per day\n- Which day had the most units?"
    assert strip_followup_lines(text, CHIPS) == "Deliveries ranged from 30 to 112.\n\n- Fact one"
    kept = "Lead.\n\n- Over-picks:\n  - 104441 at 4.08 against 4\n\n- Show lines picked per day"
    assert strip_followup_lines(kept, CHIPS) == "Lead.\n\n- Over-picks:\n  - 104441 at 4.08 against 4"


def test_a_typed_block_placeholder_line_is_removed_and_a_bracket_in_a_sentence_stays():
    text = "Lead.\n\n[Line chart: Deliveries per day · last 7 days]\n\n- Fact\n[Table]\n**[Bar chart]**"
    assert strip_placeholders(text) == "Lead.\n\n- Fact"
    sentence = "Item 104441 [milk] was over-picked; see the chart [below] for the trend."
    assert strip_placeholders(sentence) == sentence


# ==================================================== 5. the tool returns the total over every group


async def test_a_grouped_aggregate_returns_the_sum_over_every_group():
    await plant.plant_analytics(CC)
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], "where": ["shortfall<0"], "limit": 1, **plant.window()}, CC)
        single = await agent_tools.aggregate_releases(db, {"where": ["shortfall<0"], **plant.window()}, CC)
    assert out["groups"] == 1 and out["truncated"] is True  # only the top group came back...
    overall = out["overall"]  # ...but the overall sum covers both items
    assert overall["rows"] == 3 and overall["shortfall"] == "-11" and overall["deliveries"] == 2  # 27907 and 25810
    assert None not in overall.values()  # a value with no sum (a timestamp) is left out, not null
    assert "overall" not in single  # one total with no groups is already the overall


async def test_a_scripted_answer_quoting_the_overall_shortfall_is_not_withheld():
    await plant.plant_analytics(CC)
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **plant.window()}}]),
        AIMessage(content="Two items were short by 11 units in all: 100230 by 10, 104568 by 1."),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short, and by how many units?")
    assert result["stop_reason"] == "end_turn" and result["retries"] == 0


async def test_the_result_tells_the_model_where_the_total_is_and_the_prompt_does_not():
    # in the system prompt or the tool description the same sentence made the model answer "what
    # is available?" without calling a tool (8 of 8 live runs), so it travels with the result
    await plant.plant_analytics(CC)
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], **plant.window()}, CC)
    assert out["overall_note"].startswith("overall = the counts and sums over ALL groups; quote it for a total")
    aggregate = next(t for t in agent_tools.RELEASE_TOOLS if t["name"] == "aggregate_releases")
    assert "overall" not in aggregate["description"]
    assert "overall" not in " ".join(load_profile("analytics").system_prompt.split())


async def test_a_follow_up_lead_left_alone_after_the_table_stripper_is_removed_and_a_plain_lead_stays():
    # four live answers of the week ended "Suggested follow-ups:" above the rendered table, which
    # is attached to a ranked result
    await plant.plant_analytics(CC)
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"],
                                                    "sort": "units_short", **plant.window()}}]),
        AIMessage(content="Two items were short, across 3 releases.\n\nSuggested follow-ups:\n"
                          + "\n".join(f"- question {i}?" for i in range(5))),
    )
    result = await ask(analytics_runtime(CC, model=model), "which products were short?")
    assert "From the data:" in result["answer"]
    body = result["answer"].split("From the data:")[0]
    assert "Suggested follow-ups" not in body and body.strip() == "Two items were short, across 3 releases."
    assert drop_followup_lead("The products short today are:") == "The products short today are:"


async def test_a_grouped_aggregate_with_no_rows_has_an_overall_of_zeros_and_no_nulls():
    # live at 00:20 on 16 November: "today" had no settled rows and every sum came back null
    await plant.plant_analytics(CC)
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], "where": ["shortfall<-1000"], **plant.window()}, CC)
    assert out["groups"] == 0 and out["overall"] == {"rows": 0, "deliveries": 0}


# ==================================================== 6. the turn ends after the follow-ups


FINISH = "this was the last step: now reply with your answer as plain text and call no more tools"


async def test_the_follow_up_acknowledgement_tells_the_model_to_end_its_turn():
    from app.services.analytics_agent import presentation_tools as pt
    assert json.loads(await pt.run_presentation_tool("suggest_followups", {"items": ["Why?"]})) == {
        "shown": "followups", "items": 1, "next": FINISH}


def _repeat_after_followups(rounds: int, *, data_in_loop: bool = False) -> list[AIMessage]:
    # the live loop of 16 November: the answer beside state_findings, then the pair again and again
    window = {"group_by": ["user_name"], **plant.window()}
    msgs = [AIMessage(content="", tool_calls=[{"id": "d0", "name": "aggregate_releases", "args": window}])]
    for i in range(rounds):
        msgs.append(AIMessage(content="DBOBOC handled 3 of 4 releases (75%) and FNACHONLEO 1.",
                              tool_calls=[{"id": f"s{i}", "name": "state_findings",
                                           "args": {"findings": ["DBOBOC handled 3 of 4 releases"], "sources": ["aggregate_releases"]}}]))
        msgs.append(AIMessage(content="", tool_calls=[{"id": f"f{i}", "name": "suggest_followups", "args": {"items": ["Why?"]}}]))
        if data_in_loop:
            msgs.append(AIMessage(content="", tool_calls=[{"id": f"x{i}", "name": "aggregate_releases", "args": window}]))
    return msgs


async def test_a_run_that_only_repeats_drawing_calls_after_its_follow_ups_keeps_its_answer():
    await plant.plant_analytics(CC)
    result = await ask(analytics_runtime(CC, model=_scripted(*_repeat_after_followups(6))), "share of releases per picker")
    assert result["stop_reason"] == "end_turn"
    assert result["answer"].startswith("DBOBOC handled 3 of 4 releases (75%)")


async def test_a_run_that_reads_data_again_after_its_follow_ups_is_still_out_of_tool_calls():
    await plant.plant_analytics(CC)
    looping = _scripted(*_repeat_after_followups(4, data_in_loop=True))
    assert (await ask(analytics_runtime(CC, model=looping), "share of releases per picker"))["stop_reason"] == "max_iterations"
    # and a run that never offered follow-ups is out of tool calls as before
    assert (await ask(analytics_runtime(CC, model=_scripted(*_rounds(12))), "what is available?"))["stop_reason"] == "max_iterations"
