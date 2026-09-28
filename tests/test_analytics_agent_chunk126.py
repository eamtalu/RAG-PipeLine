"""Chunk 126: what the first Bedrock run over Teams taught.

Qwen3 235B, handed the previous turn's answer (a table with 556 releases), answered "give me a round
up about today" from that history without calling a tool, and did the same for "what is available".
The guard withheld both, which was right, but a refusal is a poor second to a retry. Three changes:
the history no longer carries tables or "From the data" lines, a draft with figures and no data call
is retried once with a nudge, and an answer whose only digits are dates is not treated as figures.
"""

from langchain_core.messages import AIMessage, HumanMessage

from app.config.database import async_session
from app.services.analytics_agent import agent as agent_module
from app.services.analytics_agent.agent import AnalyticsAgent
from tests.test_analytics_agent_chunk124 import CC, _plant, _scripted, _window, clean  # noqa: F401 - autouse fixture

PRIOR = ("Here's the picking status for today (2026-09-28):\n\n"
         "From the data: 11 of 11 group(s), sorted by rows desc; 556 releases across the groups returned.\n\n"
         "| user name | releases |\n|---|---|\n| BCHAM | 175 |\n| PEVANS | 80 |\n")


# ==================================================== 1. history carries no figures to copy

def test_assistant_history_is_replayed_without_tables_and_data_lines():
    replayed = agent_module._history_as_messages([{"role": "user", "content": "picking status today"},
                                                  {"role": "assistant", "content": PRIOR}])
    assert isinstance(replayed[1], AIMessage)
    assert replayed[1].content == "Here's the picking status for today (2026-09-28):"
    assert "556" not in replayed[1].content and "BCHAM" not in replayed[1].content


def test_a_user_turn_and_an_empty_assistant_turn_are_kept_and_dropped_respectively():
    replayed = agent_module._history_as_messages([{"role": "user", "content": "hi"}, {"role": "assistant", "content": "|---|"}])
    assert [type(m) for m in replayed] == [HumanMessage]


# ==================================================== 2. a figure without a data call earns one retry

async def test_a_draft_with_figures_and_no_data_call_is_retried_once_with_a_nudge():
    await _plant()
    model = _scripted(
        AIMessage(content="Today there were 556 releases, BCHAM led with 175."),           # from history, no tool
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["user_name"], **_window()}}]),
        AIMessage(content="Across 4 releases, DBOBOC picked 3 of them."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("give me a round up about today",
                                                               history=[{"role": "assistant", "content": PRIOR}])
    assert result["stop_reason"] == "end_turn" and result["retries"] == 1
    assert result["answer"].startswith("Across 4 releases")
    # The retry starts over from the question plus the nudge; the discarded draft is not replayed.
    assert model.seen[1] == ["SystemMessage", "AIMessage", "HumanMessage"]


async def test_a_second_draft_without_a_data_call_is_withheld():
    await _plant()
    model = _scripted(AIMessage(content="556 releases today."), AIMessage(content="Still 556 releases today."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("round up today")
    assert result["stop_reason"] == "withheld" and result["retries"] == 1
    assert "no figures to give" in result["answer"]


async def test_the_nudge_names_what_went_wrong_and_what_to_do():
    await _plant()
    model = _scripted(AIMessage(content="556 releases today."), AIMessage(content="I cannot say without data."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("round up today")
    nudge = agent_module.RETRY_NUDGE
    assert "without calling a data tool" in nudge and "aggregate_releases" in nudge
    assert result["stop_reason"] == "end_turn" and result["retries"] == 1


# ==================================================== 3. digits that are dates are not figures

async def test_a_describe_only_answer_whose_digits_are_dates_is_not_withheld():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "d1", "name": "describe_releases", "args": {}}]),
        AIMessage(content="One row per release since 2026-09-01: expected, picked, shortfall, duration_s."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("what is available")
    assert result["stop_reason"] == "end_turn" and result["retries"] == 0
    assert result["answer"].startswith("One row per release")


async def test_an_ungrounded_draft_is_retried_with_the_figures_named_and_a_grounded_one_kept():
    await _plant()
    call = AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                              "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}])
    model = _scripted(call, AIMessage(content="Two items short; overall 2,000 units were short."),
                      call, AIMessage(content="Two items were short: 100230 by 10 units and 104568 by 1."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("which products were short?")
    assert result["stop_reason"] == "end_turn" and result["retries"] == 1 and result["withheld_figures"] == []
    assert result["answer"].startswith("Two items were short: 100230")
    assert "2,000" in agent_module.UNGROUNDED_NUDGE.format(figures="2,000")


async def test_a_description_with_small_counts_and_no_tool_call_is_not_treated_as_figures():
    await _plant()
    model = _scripted(AIMessage(content="One row per release; 12 tools cover the last 24 hours by default."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("what is available")
    assert result["stop_reason"] == "end_turn" and result["retries"] == 0


async def test_a_number_the_instructions_contain_is_not_an_invented_figure():
    """"filter duration_s>300" quotes the tool description; "556 releases" quotes nothing."""
    await _plant()
    model = _scripted(AIMessage(content="You can filter slow picks with duration_s>300 or shorts with shortfall<0."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("what is available")
    assert result["stop_reason"] == "end_turn" and result["retries"] == 0
    model = _scripted(AIMessage(content="556 releases today."), AIMessage(content="556 releases today."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("round up")
    assert result["stop_reason"] == "withheld"
