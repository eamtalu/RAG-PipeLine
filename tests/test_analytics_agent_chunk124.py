"""Chunk 124: the LangGraph analytics agent, provider-neutral, one caller, no MCP.

The four release tools read the settlement through `settle_reads`, the same functions the HTTP
endpoints call. The loop is exercised with a SCRIPTED model, so the suite never talks to a
provider: the fake returns a tool call, then an answer, and the test checks what was called, that
the tenant never appeared as an argument, and that a mistyped field came back to the model as a
readable problem rather than a crash.
"""

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from sqlalchemy import delete

from app.api.v1 import analytics as api
from app.api.v1 import analytics_agent as agent_api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.customer import Customer
from app.services.analytics_agent import tools as agent_tools
from app.services.analytics_agent import agent as agent_module
from app.services.analytics_agent.agent import AnalyticsAgent, tool_trace
from app.settings import settings

CC = "test_chunk124"
T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)

BODY = {
    "name": "pick_release", "description": "one row per release", "reads": ["ConfirmPickLine"], "key": ["attr:ReportingNumber"],
    "carry": ["delivery_number", "item_number", "attr:OrderLine", "user_name", "transaction_name"],
    "values": [
        {"name": "expected", "rule": "first", "field": "attr:ExpectedQuantity"},
        {"name": "picked", "rule": "sum", "field": "attr:QuantityPicked", "statuses": ["success"]},
        {"name": "calls", "rule": "count"},
        {"name": "refused", "rule": "count", "statuses": ["error"]},
        {"name": "shortfall", "rule": "difference", "left": "picked", "right": "expected"},
        {"name": "is_short", "rule": "flag", "left": "shortfall", "op": "<", "right_value": "0"},
    ],
}


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsSettledRow, AnalyticsSettlement, AnalyticsLookupValue, AnalyticsLookup,
                      AnalyticsFieldRegistry, AnalyticsFact):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="agent probe", timezone="Europe/London"))
        for f in ("ReportingNumber", "ExpectedQuantity", "QuantityPicked", "OrderLine"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="ConfirmPickLine", source="request", field=f, captured=True,
                                          seen_count=9))
        await db.commit()
    yield
    await _wipe()


def _fact(minutes, expected, picked, *, rep="540551", status="success", delivery="27907", item="104568",
          user="FNACHONLEO"):
    when = T0 + timedelta(minutes=minutes)
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.date(),
        transaction_name="JIT and Shorts Pick (Brighton)", method="ConfirmPickLine", status=status,
        quantity_classification="pick" if picked > 0 else "attempt",
        warehouse="BRI", delivery_number=delivery, item_number=item, lot_number="2609161191", user_name=user,
        attributes={"ReportingNumber": rep, "ExpectedQuantity": str(expected), "QuantityPicked": str(picked),
                    "OrderLine": "21"}, created_at=when)


async def _plant():
    """Release 540551 (nine calls, one accepted), a clean release, and two short ones on another item."""
    facts = [_fact(0, 10, 9)] + [_fact(7 + i, 1, 1, status="error") for i in range(8)]
    facts += [_fact(30, 4, 4, rep="A1", item="100606", user="DBOBOC"),
              _fact(40, 7, 4, rep="B1", delivery="25810", item="100230", user="DBOBOC"),
              _fact(50, 7, 0, rep="B2", delivery="25810", item="100230", user="DBOBOC")]
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await api.create_settlement(body=BODY, backfill=True, customer=CC, db=db)


def _window():
    return {"start": (T0 - timedelta(hours=1)).isoformat(), "end": (T0 + timedelta(hours=2)).isoformat()}


# ==================================================== 1. the release tools

async def test_describe_releases_lists_the_fields_a_model_may_use():
    await _plant()
    async with async_session() as db:
        out = await agent_tools.describe_releases(db, {}, CC)
    (s,) = out["settlements"]
    assert s["settlement"] == "pick_release" and s["rows"] == 4 and s["one_row_is"] == "one distinct ReportingNumber"
    assert {"is_short", "picked", "user_name", "OrderLine", "delivery_number"} <= set(s["fields"])
    assert "hour_start" in s["time_buckets"] and "distinct" in s["stat_kinds"]
    assert "never over handheld calls" in out["how_to_read"]


async def test_aggregate_releases_answers_top_shorted_items_in_one_call():
    """The question from the user's own example: the shorted products, most short first."""
    await _plant()
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {
            "group_by": ["item_number"], "where": ["shortfall<0"], **_window()}, CC)
    rows = {r["dimensions"][0]: r for r in out["rows"]}
    assert set(rows) == {"104568", "100230"}
    assert rows["100230"]["shortfall"] == "-10" and rows["100230"]["rows"] == 2 and rows["100230"]["is_short"] == "2"
    assert rows["104568"]["shortfall"] == "-1"
    assert out["grain"].startswith("one row = one pick-list release")


async def test_aggregate_releases_takes_stats_and_clamps_the_window():
    await _plant()
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {
            "group_by": ["user_name"], "stat": ["median:expected", "distinct:delivery_number"],
            "start": (T0 - timedelta(days=400)).isoformat(), "end": (T0 + timedelta(hours=2)).isoformat()}, CC)
    by = {r["dimensions"][0]: r for r in out["rows"]}
    assert by["DBOBOC"]["median_expected"] == "7" and by["DBOBOC"]["distinct_delivery_number"] == 2
    assert out["notes"] and "clamped" in out["notes"][0]


async def test_list_releases_sorts_and_filters_and_caps():
    await _plant()
    async with async_session() as db:
        out = await agent_tools.list_releases(db, {"where": ["picked==0"], "sort": "expected", "limit": 500, **_window()}, CC)
        longest = await agent_tools.list_releases(db, {"sort": "calls", "dir": "desc", "limit": 1, **_window()}, CC)
    assert out["total"] == 1 and out["rows"][0]["key"] == ["B2"] and out["limit"] == agent_tools.LIST_LIMIT
    assert longest["rows"][0]["key"] == ["540551"] and longest["rows"][0]["calls"] == 9


async def test_explain_release_lists_the_calls_and_never_sums_them():
    await _plant()
    async with async_session() as db:
        out = await agent_tools.explain_release(db, {"key": "540551"}, CC)
    assert len(out["calls"]) == 9 and sum(c["status"] == "error" for c in out["calls"]) == 8
    assert out["settled"]["values"]["picked"] == "9" and out["settled"]["values"]["expected"] == "10"


async def test_a_problem_is_returned_to_the_model_as_the_result():
    await _plant()
    async with async_session() as db:
        bad = json.loads(await agent_tools.run_release_tool("aggregate_releases", {"where": ["shortfal<0"]}, db, CC))
        unknown = json.loads(await agent_tools.run_release_tool("list_releases", {"settlement": "nope"}, db, CC))
        shape = json.loads(await agent_tools.run_release_tool("aggregate_releases", {"where": ["is_short"]}, db, CC))
    assert "problems" in bad and "shortfal" in bad["problems"][0] and "is_short" in bad["problems"][0]
    assert "no settlement called 'nope'" in unknown["error"]
    assert "is_short==1" in shape["problems"][0]


async def test_the_bound_tools_carry_no_tenant_argument():
    async with async_session() as db:
        tools = agent_tools.build_tools(db, CC)
    names = [t.name for t in tools]
    assert names[-4:] == ["describe_releases", "aggregate_releases", "list_releases", "explain_release"]
    assert "search_transactions" in names and "query_metric" in names
    for t in tools:
        assert "customer_code" not in t.args and "customer" not in t.args, t.name


# ==================================================== 2. the loop with a scripted model

class ScriptedModel(BaseChatModel):
    """A chat model that answers from a script and accepts tools, so the graph runs without a provider.
    LangChain's GenericFakeChatModel cannot bind tools, which `create_agent` requires."""

    script: list = []
    seen: list = []

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools, **kwargs):  # noqa: ARG002 - the script decides what is called
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.seen.append([type(m).__name__ for m in messages])
        if not self.script:
            raise AssertionError("the script ran out: the agent asked the model more often than expected")
        return ChatResult(generations=[ChatGeneration(message=self.script.pop(0))])


def _scripted(*messages: AIMessage) -> ScriptedModel:
    return ScriptedModel(script=list(messages), seen=[])


async def test_the_agent_calls_a_tool_then_answers_and_records_the_trace():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="Across 4 releases, two items were short: 100230 by 10 units, 104568 by 1."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("which products were short?")
    assert result["answer"].startswith("Across 4 releases")
    assert result["tool_calls"] == [{"tool": "aggregate_releases",
                                     "input": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]
    assert result["iterations"] == 1 and result["stop_reason"] == "end_turn"


async def test_a_mistyped_field_reaches_the_model_as_text_and_the_loop_goes_on():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"where": ["shortfal<0"]}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "aggregate_releases", "args": {"where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="Three releases were short."),
    )
    async with async_session() as db:
        agent = AnalyticsAgent(db, CC, model=model)
        result = await agent.ask("how many short?")
    assert result["answer"] == "Three releases were short." and result["iterations"] == 2


async def test_history_is_replayed_as_plain_text_only():
    await _plant()
    model = _scripted(AIMessage(content="Still 3."))
    async with async_session() as db:
        await AnalyticsAgent(db, CC, model=model).ask("and now?", history=[
            {"role": "user", "content": "how many short?"}, {"role": "assistant", "content": "Three."}])
    assert model.seen[0][:4] == ["SystemMessage", "HumanMessage", "AIMessage", "HumanMessage"]


def test_the_trace_pairs_each_call_with_a_preview_of_its_result():
    from langchain_core.messages import ToolMessage
    msgs = [AIMessage(content="", tool_calls=[{"id": "x", "name": "list_releases", "args": {"limit": 2}}]),
            ToolMessage(content='{"total": 4}', tool_call_id="x"), AIMessage(content="done")]
    assert tool_trace(msgs) == [{"tool": "list_releases", "input": {"limit": 2}, "result_preview": '{"total": 4}', "result": '{"total": 4}'}]


# ==================================================== 3. the endpoint and the Teams switch

async def test_the_endpoint_returns_the_agent_result_shape(monkeypatch):
    await _plant()
    monkeypatch.setattr(AnalyticsAgent, "ask", lambda self, q, history=None: _answer(q, history))
    async with async_session() as db:
        out = await agent_api.ask(agent_api.AskRequest(question="how many?", history=[
            agent_api.HistoryTurn(role="user", content="hi")]), customer=CC, db=db)
    assert out["answer"] == "how many? (1 earlier turn)" and out["tool_calls"] == []


async def _answer(question, history):
    return {"answer": f"{question} ({len(history or [])} earlier turn)", "tool_calls": [], "iterations": 0}


async def test_teams_uses_the_langgraph_agent_unless_told_otherwise(monkeypatch):
    from app import teams_consumer
    calls = []
    monkeypatch.setattr(AnalyticsAgent, "ask", lambda self, q, history=None: _record(calls, "langgraph", q))
    monkeypatch.setattr(teams_consumer.LogDebugAgent, "ask", lambda self, q, history=None: _record(calls, "claude", q))
    monkeypatch.setattr(settings, "teams_agent", "langgraph")
    await teams_consumer._run_agent(CC, "q1", [])
    monkeypatch.setattr(settings, "teams_agent", "claude")
    await teams_consumer._run_agent(CC, "q2", [])
    assert calls == [("langgraph", "q1"), ("claude", "q2")]


async def _record(calls, which, q):
    calls.append((which, q))
    return {"answer": "", "tool_calls": []}


# ==================================================== 4. the model is one setting

def test_an_ollama_model_gets_the_base_url_thinking_off_and_a_wide_context(monkeypatch):
    seen = {}
    monkeypatch.setattr(agent_module, "init_chat_model", lambda name, **kw: seen.update(name=name, **kw) or object())
    monkeypatch.setattr(settings, "ollama_base_url", "http://127.0.0.1:11434")
    agent_module.make_model("ollama:qwen3:8b")
    assert seen == {"name": "ollama:qwen3:8b", "base_url": "http://127.0.0.1:11434", "reasoning": False, "num_ctx": 16384,
                    "temperature": 0}
    seen.clear()
    agent_module.make_model("anthropic:claude-sonnet-5")
    assert seen == {"name": "anthropic:claude-sonnet-5"}


# ==================================================== 5. what the first live run taught

def test_stats_a_model_invents_for_sums_and_counts_are_dropped_with_a_note():
    kept, notes = agent_tools._stats_asked(["count:releases", "sum:picked", "median:duration_s", "distinct:user_name", "count"])
    assert kept == ["median:duration_s", "distinct:user_name"]
    assert len(notes) == 3 and "always returned" in notes[0]


async def test_a_sum_stat_no_longer_refuses_the_whole_call():
    await _plant()
    async with async_session() as db:
        out = json.loads(await agent_tools.run_release_tool(
            "aggregate_releases", {"group_by": ["user_name"], "stat": ["sum:picked", "median:expected"], **_window()}, db, CC))
    assert "problems" not in out and out["stats"] == ["median_expected"]
    assert any("'sum:picked' dropped" in n for n in out["notes"])


async def test_running_out_of_tool_rounds_ends_with_a_plain_sentence_not_an_error(monkeypatch):
    await _plant()
    monkeypatch.setattr(settings, "analytics_agent_max_iterations", 2)
    call = AIMessage(content="", tool_calls=[{"id": "c", "name": "describe_releases", "args": {}}])
    model = _scripted(*[AIMessage(content="", tool_calls=[{"id": f"c{i}", "name": "describe_releases", "args": {}}]) for i in range(6)])
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("loop forever")
    assert result["stop_reason"] == "max_iterations" and result["iterations"] >= 2
    assert result["answer"].startswith("I could not settle on an answer")
    del call


async def test_top_shorted_items_come_back_in_shortfall_order_with_the_grain_counted():
    await _plant()
    async with async_session() as db:
        out = await agent_tools.aggregate_releases(db, {
            "group_by": ["item_number"], "where": ["shortfall<0"], "sort": "shortfall", "dir": "asc", "limit": 1, **_window()}, CC)
    assert [r["dimensions"][0] for r in out["rows"]] == ["100230"] and out["sort"] == {"by": "shortfall", "dir": "asc"}
    assert out["total_rows"] == 2 and out["groups"] == 1 and out["truncated"] is True


async def test_an_unknown_sort_is_a_readable_problem():
    await _plant()
    async with async_session() as db:
        out = json.loads(await agent_tools.run_release_tool("aggregate_releases", {"sort": "speed", **_window()}, db, CC))
    assert "cannot be ordered by 'speed'" in out["problems"][0]


# ==================================================== 6. an invented answer is withheld

async def test_figures_with_no_successful_data_call_are_withheld(monkeypatch):
    """The first live Teams run: refused on a field that does not exist, the model answered with
    Customer A to J and round numbers, all invented. That answer never reaches a person."""
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "describe_releases", "args": {}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "aggregate_releases", "args": {"group_by": ["customer_number"]}}]),
        AIMessage(content="Top customers: Customer A -1,200 units, Customer B -950 units."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("top customers by units short")
    assert result["stop_reason"] == "withheld"
    assert result["answer"].startswith("I could not get the data") and "customer_number" in result["answer"]
    assert "Customer A" not in result["answer"]


async def test_a_real_answer_after_a_corrected_call_is_kept():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"where": ["shortfal<0"]}}]),
        AIMessage(content="", tool_calls=[{"id": "c2", "name": "aggregate_releases", "args": {"where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="Across 3 releases, 11 units were short."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("how many units short?")
    assert result["stop_reason"] == "end_turn" and result["answer"] == "Across 3 releases, 11 units were short."


async def test_a_plain_sentence_without_figures_is_never_withheld():
    await _plant()
    model = _scripted(AIMessage(content="", tool_calls=[{"id": "c1", "name": "describe_releases", "args": {}}]),
                      AIMessage(content="I can answer questions about releases, pickers, customers and items."))
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("what can you do?")
    assert result["stop_reason"] == "end_turn"


# ==================================================== 7. figures checked, tables from the rows

async def test_a_figure_no_tool_returned_is_withheld_by_name():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases", "args": {"group_by": ["item_number"], "where": ["shortfall<0"], **_window()}}]),
        AIMessage(content="Two items were short, 100230 by 10 units and 104568 by 1; overall 2,000 units were short."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("which products were short?")
    assert result["stop_reason"] == "withheld" and result["withheld_figures"] == ["2,000"]
    assert result["answer"].startswith("I could not verify these figures") and "2,000" in result["answer"]


async def test_a_top_n_answer_carries_the_table_from_the_rows_not_the_models_own():
    await _plant()
    model = _scripted(
        AIMessage(content="", tool_calls=[{"id": "c1", "name": "aggregate_releases",
                                           "args": {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "shortfall", "dir": "asc", **_window()}}]),
        AIMessage(content="The most short items:\n\n| item | short |\n|---|---|\n| 104568 | 1 |\n| 100230 | 10 |\n\nAcross 3 releases."),
    )
    async with async_session() as db:
        result = await AnalyticsAgent(db, CC, model=model).ask("top 2 shorted items")
    assert result["stop_reason"] == "end_turn"
    assert "| item | short |" not in result["answer"]             # the model's table, in the wrong order, is gone
    assert result["evidence"].startswith("From the data: 2 of 2 group(s), sorted by shortfall asc")
    lines = result["answer"].splitlines()
    assert lines[-2] == "| 100230 | 2 | -10 |" and lines[-1] == "| 104568 | 1 | -1 |"
    assert result["answer"].startswith("The most short items:")


async def test_units_short_is_a_sort_the_model_can_name_and_shortfall_desc_on_short_rows_means_the_same():
    """The first Web Chat run asked for shortfall descending over the short releases and got the ten
    customers short by one unit as the 'top 10'. Both spellings now mean biggest shortfall first."""
    await _plant()
    async with async_session() as db:
        named = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], "where": ["shortfall<0"], "sort": "units_short", **_window()}, CC)
        flipped = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], "where": ["is_short==1"], "sort": "shortfall", "dir": "desc", **_window()}, CC)
        plain = await agent_tools.aggregate_releases(db, {"group_by": ["item_number"], "sort": "shortfall", "dir": "desc", **_window()}, CC)
    assert [r["dimensions"][0] for r in named["rows"]] == ["100230", "104568"] and named["sort"] == {"by": "units_short", "dir": "desc"}
    assert [r["dimensions"][0] for r in flipped["rows"]] == ["100230", "104568"] and "biggest shortfall first" in flipped["notes"][0]
    assert plain["rows"][0]["dimensions"][0] == "100606" and plain["sort"] == {"by": "shortfall", "dir": "desc"}
