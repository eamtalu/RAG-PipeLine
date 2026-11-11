"""Chunk 170: a repeated question is answered from the data, never from the earlier turn.

On 11 November 2026 a person asked "Which products were short today, and by how many units?"
twice in one Teams conversation. The second time the model copied the first answer's lead-in and
freshness note ("current as of 14:09 UTC, about 73% provisional") without calling a tool, and the
card showed a sentence ending in a colon with no rows. The fabrication guard let it through because
it ignores dates, times, shares and small counts, which is right for a draft backed by a tool call
and wrong for a draft backed by nothing. A draft with no tool call at all is now held to the strict
rule: no date, time or share the instructions lack, or the draft is retried with the nudge and
then withheld. Small counts stay exempt so a description in words still passes.
"""

from langchain_core.messages import AIMessage

from app.agent.contracts import AgentResult
from app.agent.gateway import run_request
from app.agent.presentation import evidence as ev
from app.agent.profiles import load_profile
from app.agent.runtime.grounded import GroundedAgent
from app.agent.runtime.pipeline import looks_fabricated
from app.services.analytics_agent.namespaces import analytics_namespace
from tests.agent_helpers import analytics_runtime, request_for
from tests.test_analytics_agent_chunk124 import (  # noqa: F401 - autouse fixture
    CC,
    _plant,
    _scripted,
    _window,
    clean,
)

QUESTION = "Which products were short today, and by how many units?"
FIRST_ANSWER = (
    "On 2026-11-11, the following products were short, and by how many units:\n\n"
    "This data is across 20 pick-list releases with a shortfall. The figures are current as of "
    "14:09 UTC, but note that the most recent data is still provisional — about 73% of the "
    "data may adjust slightly as some handheld calls are not yet fully sealed into the system.\n\n"
    "From the data: 10 of 14 group(s), sorted by units short desc, 2026-11-10 to 2026-11-13; "
    "20 releases across the groups returned.\n\n"
    "| item number | ItemDescription | releases | units short |\n|---|---|---|---|\n"
    "| 104644 | POTATO WARE WASHED | 3 | 50 |\n| 101961 | FIG | 2 | 48 |"
)
# what the model wrote the second time: the lead-in and the note, nothing from a tool
ECHO = (
    "On 2026-11-11, the products that were short and the units short are:\n\n"
    "This data is across 20 pick-list releases with a shortfall. The figures are current as of "
    "14:09 UTC, but note that the most recent data is still provisional — about 73% of the "
    "data may adjust slightly as some handheld calls are not yet fully sealed into the system."
)
HISTORY = ({"role": "user", "content": QUESTION}, {"role": "assistant", "content": FIRST_ANSWER})


def _given() -> list[str]:
    profile = load_profile("analytics")
    return [profile.system_prompt, analytics_namespace().tool_text]


async def _ask(runtime: GroundedAgent, question: str, history=()) -> AgentResult:
    request = request_for(runtime.customer_code, question, profile=runtime.profile.name)
    return await run_request(request, runtime=runtime, prior_turns=tuple(history))


# ==================================================== 1. the strict rule, pure


def test_the_dates_times_and_shares_of_an_echo_are_its_stale_markers():
    assert ev.stale_markers(ECHO, []) == ["2026-11-11", "14:09", "73%"]


def test_a_moment_the_instructions_contain_is_not_stale():
    text = "Vans are usually ready well before the 11:30 the WMS prints, within 20 minutes."
    assert ev.stale_markers(text, ["loads before the 11:30 departure; 20 min of quiet"]) == []


def test_words_and_small_counts_carry_no_stale_marker():
    assert ev.stale_markers("12 tools cover the last 24 hours by default.", []) == []


def test_an_echo_with_no_tool_call_is_fabricated():
    fallback = looks_fabricated(ECHO, [], given=_given(), schema_tools=frozenset())
    assert fallback is not None and "no figures to give" in fallback


def test_a_plain_reply_with_no_tool_call_is_fine():
    assert looks_fabricated("You're welcome.", [], given=_given(), schema_tools=frozenset()) is None


# ==================================================== 2. the run


async def test_a_repeated_question_is_answered_from_the_data_not_from_the_earlier_turn():
    await _plant()
    model = _scripted(
        AIMessage(content=ECHO),
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {
            "group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="The products short today are:"),
    )
    result = await _ask(analytics_runtime(CC, model=model), QUESTION, HISTORY)
    assert result.stop_reason == "end_turn" and result.retries == 1 and result.iterations == 1
    assert result.tool_calls[0].tool == "aggregate_releases" and "14:09" not in result.answer
    posts = [(g["name"], g["verdict"]) for g in result.trace.guards if g["stage"] == "post"]
    assert posts == [("fabrication", "problem"), ("fabrication", "pass"), ("ungrounded", "pass")]
    # the nudge went with the second ask, after the stripped earlier turn
    assert len(model.seen) == 3 and model.seen[1][-1] == "HumanMessage"


async def test_a_second_echo_is_withheld_not_shown():
    await _plant()
    model = _scripted(AIMessage(content=ECHO), AIMessage(content=ECHO))
    result = await _ask(analytics_runtime(CC, model=model), QUESTION, HISTORY)
    assert result.stop_reason == "withheld" and result.retries == 1
    assert "no figures to give" in result.answer and "14:09" not in result.answer


async def test_a_reply_in_words_needs_no_tool_and_no_retry():
    model = _scripted(AIMessage(content="You're welcome. Ask me about pickers or shortfalls."))
    result = await _ask(analytics_runtime(CC, model=model), "thanks", HISTORY)
    assert result.stop_reason == "end_turn" and result.retries == 0 and len(model.seen) == 1
