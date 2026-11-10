"""Step 11 of docs/esmart-agent-execution-plan.md: one frozen context, built once, by the gateway.

Three channels used to run the agent each their own way, and a run read settings as it went. Now
a channel builds an `AgentRequest`, the gateway snapshots settings once, builds the frozen
`ExecutionContext` and hands both to the runtime; nothing inside a run reads settings again. The
answers a person sees are unchanged: `AgentResult.to_legacy_dict()` returns today's keys.
"""

import dataclasses
import logging
import sys
from datetime import datetime, timedelta, timezone

import pytest
from langchain_core.messages import AIMessage

from app import log_context
from app.agent import contracts
from app.agent.contracts import (
    AgentRequest,
    AgentResult,
    ExecutionContext,
    ModelSettings,
    Principal,
    RunBudgets,
    RunSettings,
    ToolCall,
)
from app.agent.gateway import build_context, run_request, snapshot_settings
from app.settings import settings

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
MODEL = ModelSettings(
    name="ollama:qwen3:8b",
    ollama_base_url="http://127.0.0.1:11434",
    think=False,
    context_tokens=16384,
    bedrock_region="eu-west-2",
    max_tokens=4000,
)
RUN = RunSettings(
    model=MODEL,
    budgets=RunBudgets(max_iterations=10, run_timeout_seconds=120.0),
    public_base_url="http://eye",
)


def _request(**over) -> AgentRequest:
    base = dict(
        request_id="req-1",
        tenant="acme",
        principal=Principal(tenant="acme", object_id="u-1"),
        profile="analytics",
        text="how many?",
        conversation_id="conv-1",
    )
    base.update(over)
    return AgentRequest(**base)


# ==================================================== 1. the contracts


def test_the_contracts_are_frozen():
    request = _request()
    with pytest.raises(dataclasses.FrozenInstanceError):
        request.text = "changed"
    context = build_context(request, RUN, now=NOW)
    with pytest.raises(dataclasses.FrozenInstanceError):
        context.tenant = "other"


@pytest.mark.parametrize("field", ["text", "tenant", "request_id"])
def test_a_request_with_a_blank_field_is_refused(field):
    with pytest.raises(ValueError, match=field):
        _request(**{field: "  "})


def test_the_legacy_dict_has_exactly_the_keys_the_channels_read_today():
    result = AgentResult(
        answer="Two.",
        stop_reason="end_turn",
        model="m",
        tool_calls=(ToolCall("describe_releases", {}),),
        iterations=1,
        evidence="| a |",
        evidence_data={"title": "t"},
        withheld_figures=("9",),
        request_id="req-1",
        trace_id="req-1",
    )
    legacy = result.to_legacy_dict()
    assert list(legacy) == [
        "answer",
        "stop_reason",
        "tool_calls",
        "iterations",
        "retries",
        "model",
        "evidence",
        "evidence_data",
        "withheld_figures",
    ]
    assert legacy["tool_calls"] == [{"tool": "describe_releases", "input": {}}]
    assert legacy["withheld_figures"] == ["9"] and "request_id" not in legacy
    declined = AgentResult(answer="No.", stop_reason="declined", model="m", declined="off_topic")
    assert declined.to_legacy_dict()["declined"] == "off_topic"


def test_status_follows_the_stop_reason():
    assert AgentResult(answer="", stop_reason="end_turn", model="m").status == "ok"
    assert AgentResult(answer="", stop_reason="withheld", model="m").status == "ok"
    assert (
        AgentResult(answer="", stop_reason="declined", model="m", declined="x").status == "declined"
    )
    assert AgentResult(answer="", stop_reason="timeout", model="m").status == "timeout"


def test_seconds_left_never_goes_below_zero():
    context = build_context(_request(), RUN, now=NOW)
    assert context.seconds_left(now=NOW) == 120.0
    assert context.seconds_left(now=NOW + timedelta(seconds=500)) == 0.0


# ==================================================== 2. the context builder


def test_snapshot_settings_mirrors_the_settings(monkeypatch):
    monkeypatch.setattr(settings, "analytics_agent_model", "bedrock_converse:x")
    monkeypatch.setattr(settings, "analytics_agent_max_iterations", 7)
    monkeypatch.setattr(settings, "analytics_agent_run_timeout_seconds", 42.0)
    monkeypatch.setattr(settings, "app_public_base_url", "http://eye/")
    snap = snapshot_settings()
    assert snap.model.name == "bedrock_converse:x" and snap.budgets.max_iterations == 7
    assert snap.budgets.run_timeout_seconds == 42.0 and snap.public_base_url == "http://eye"


def test_the_context_carries_the_request_and_a_deadline_from_the_budget():
    context = build_context(_request(), RUN, now=NOW)
    assert (context.request_id, context.trace_id, context.tenant) == ("req-1", "req-1", "acme")
    assert context.principal.object_id == "u-1" and context.profile == "analytics"
    assert context.conversation_id == "conv-1" and context.conversation_type == "personal"
    assert context.deadline_at == NOW + timedelta(seconds=120)
    assert context.model is MODEL and context.public_base_url == "http://eye"
    shorter = build_context(_request(deadline_seconds=5), RUN, now=NOW)
    assert shorter.deadline_at == NOW + timedelta(seconds=5)


def test_the_default_deadline_sits_below_the_visibility_window():
    assert snapshot_settings().budgets.run_timeout_seconds < settings.teams_sqs_visibility_seconds


# ==================================================== 3. the gateway run


class ScriptedRuntime:
    """A runtime twin: records what it was handed and answers from a script."""

    def __init__(self, answer="ok", error: Exception | None = None):
        self.answer, self.error = answer, error
        self.seen: list[tuple] = []

    async def run(
        self, context: ExecutionContext, request: AgentRequest, prior_turns=()
    ) -> AgentResult:
        self.seen.append((context, request, prior_turns, log_context.current()))
        if self.error:
            raise self.error
        return AgentResult(
            answer=self.answer,
            stop_reason="end_turn",
            model=context.model.name,
            request_id=context.request_id,
            trace_id=context.trace_id,
        )


async def test_the_gateway_builds_one_context_and_binds_the_log_context():
    runtime = ScriptedRuntime()
    turns = ({"role": "user", "content": "earlier"},)
    result = await run_request(_request(), runtime=runtime, prior_turns=turns, run_settings=RUN)
    [(context, request, seen_turns, bound)] = runtime.seen
    assert isinstance(context, ExecutionContext) and request.text == "how many?"
    assert (context.tenant, context.principal.object_id, context.profile) == (
        "acme",
        "u-1",
        "analytics",
    )
    assert seen_turns is turns
    assert bound == {"request_id": "req-1", "tenant": "acme"}
    assert result.answer == "ok" and result.request_id == "req-1"


async def test_a_runtime_failure_propagates_to_the_channel():
    runtime = ScriptedRuntime(error=RuntimeError("provider down"))
    with pytest.raises(RuntimeError, match="provider down"):
        await run_request(_request(), runtime=runtime, run_settings=RUN)


async def test_the_gateway_snapshots_settings_when_the_channel_did_not(monkeypatch):
    monkeypatch.setattr(settings, "analytics_agent_model", "ollama:tiny")
    runtime = ScriptedRuntime()
    result = await run_request(_request(), runtime=runtime)
    assert result.model == "ollama:tiny"


# ==================================================== 4. the runtime under a context

from app.agent.runtime.guards import TopicGate  # noqa: E402
from app.services.analytics_agent import agent as analytics_module  # noqa: E402
from app.services.analytics_agent.agent import AnalyticsAgent  # noqa: E402
from tests.test_analytics_agent_chunk124 import (
    CC,
    ScriptedModel,
    _plant,
    _scripted,
    clean,
)  # noqa: E402,F401


def _describe(call_id: str = "c1") -> AIMessage:
    return AIMessage(
        content="", tool_calls=[{"id": call_id, "name": "describe_releases", "args": {}}]
    )


async def test_a_runtime_built_for_one_tenant_refuses_another_tenants_context():
    agent = AnalyticsAgent("other", model=_scripted())
    context = build_context(_request(tenant=CC), RUN, now=NOW)
    with pytest.raises(ValueError, match="tenant"):
        await agent.run(context, _request(tenant=CC))


async def test_run_and_the_transitional_ask_give_the_same_legacy_dict():
    await _plant()
    script = [_describe(), AIMessage(content="There are releases.")]
    via_run = await run_request(
        _request(tenant=CC), runtime=AnalyticsAgent(CC, model=_scripted(*script))
    )
    via_ask = await AnalyticsAgent(CC, model=_scripted(*script)).ask("how many?")
    assert via_run.to_legacy_dict() == via_ask
    assert via_run.request_id == "req-1" and via_run.stop_reason == "end_turn"


def test_resolve_model_gives_a_gate_only_when_no_model_was_injected(monkeypatch):
    context = build_context(_request(tenant=CC), RUN, now=NOW)
    model, gate, name = AnalyticsAgent(CC, model=_scripted()).resolve_model(context)
    assert gate is None and name == "ollama:qwen3:8b"
    monkeypatch.setattr(
        analytics_module, "init_chat_model", lambda name, **kw: ScriptedModel(script=[], seen=[])
    )
    model, gate, name = AnalyticsAgent(CC).resolve_model(context)
    assert isinstance(gate, TopicGate)


# ==================================================== 5. no settings are read inside a run


class _ForbiddenSettings:
    def __getattr__(self, name):
        raise AssertionError(f"settings.{name} was read inside a run")


def _poison(monkeypatch) -> None:
    """Every loaded module the agent owns loses its settings, except the gateway's one reader."""
    for name, module in list(sys.modules.items()):
        if name == "app.agent.gateway.context":
            continue
        if name.startswith(
            ("app.agent.", "app.services.analytics_agent", "app.services.logspace_agent")
        ) and hasattr(module, "settings"):
            monkeypatch.setattr(module, "settings", _ForbiddenSettings())


async def test_the_analytics_runtime_reads_no_settings_inside_a_run(monkeypatch):
    await _plant()
    run_settings = snapshot_settings()
    _poison(monkeypatch)
    script = [_describe(), AIMessage(content="There are releases.")]
    result = await run_request(
        _request(tenant=CC),
        runtime=AnalyticsAgent(CC, model=_scripted(*script)),
        run_settings=run_settings,
    )
    assert result.stop_reason == "end_turn" and len(result.tool_calls) == 1


# ==================================================== 5b/5c. the logspace runtime and the adapters

from sqlalchemy import text  # noqa: E402

from app import teams_consumer  # noqa: E402
from app.api.v1 import analytics_agent as agent_api  # noqa: E402
from app.api.v1 import logs as logs_api  # noqa: E402
from app.config.database import async_session  # noqa: E402
from app.services.logspace_agent import agent as logspace_module  # noqa: E402
from app.services.logspace_agent.agent import LogspaceAgent  # noqa: E402
from app.services.teams.contracts import QuestionJob  # noqa: E402
from tests.test_logspace_agent_chunk132 import SCOPE, TODAY, TZ, cc  # noqa: E402,F401


def _trace_d1() -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {"id": "c1", "name": "trace", "args": {"key": "delivery_number", "value": "D1"}}
        ],
    )


async def test_the_logspace_runtime_reads_no_settings_inside_a_run(monkeypatch, cc):
    run_settings = snapshot_settings()
    _poison(monkeypatch)
    agent = LogspaceAgent(
        cc,
        scope=SCOPE,
        tz_name=TZ,
        today=TODAY,
        model=_scripted(_trace_d1(), AIMessage(content="D1 was picked.")),
    )
    result = await run_request(
        _request(tenant=cc, profile="logspace"), runtime=agent, run_settings=run_settings
    )
    assert result.stop_reason == "end_turn" and [t.tool for t in result.tool_calls] == ["trace"]


def _job(**over) -> QuestionJob:
    base = dict(
        job_id="job-7",
        tenant_id="t-1",
        customer_code=CC,
        conversation_id="conv-7",
        sender_object_id="u-7",
        sender_name="Jo",
        question="how many?",
        source="tab",
    )
    base.update(over)
    return QuestionJob(**base)


async def test_the_consumer_adapter_builds_the_request_from_the_job(monkeypatch):
    seen = {}

    async def spy(request, *, runtime, prior_turns=(), run_settings=None):
        seen.update(request=request, runtime=runtime, prior_turns=prior_turns)
        return AgentResult(answer="ok", stop_reason="end_turn", model="m")

    monkeypatch.setattr(teams_consumer, "run_request", spy)
    out = await teams_consumer._run_agent(_job(), [{"role": "user", "content": "earlier"}])
    request = seen["request"]
    assert (request.request_id, request.tenant, request.profile) == ("job-7", CC, "analytics")
    assert request.principal == Principal(tenant=CC, object_id="u-7", display_name="Jo")
    assert request.conversation_id == "conv-7" and request.conversation_type == "personal"
    assert isinstance(seen["runtime"], AnalyticsAgent) and seen["prior_turns"] == (
        {"role": "user", "content": "earlier"},
    )
    assert out == {
        "answer": "ok",
        "stop_reason": "end_turn",
        "tool_calls": [],
        "iterations": 0,
        "retries": 0,
        "model": "m",
        "evidence": None,
        "evidence_data": None,
        "withheld_figures": [],
    }


async def test_the_consumer_adapter_reads_no_settings_inside_the_run(monkeypatch):
    await _plant()
    # no model is injected, so the real gate runs first and answers from the script too
    script = [AIMessage(content="in_scope"), _describe(), AIMessage(content="There are releases.")]
    monkeypatch.setattr(
        AnalyticsAgent, "make_model", lambda self, model_settings: _scripted(*script)
    )
    monkeypatch.setattr(settings, "teams_agent", "langgraph")
    _poison(monkeypatch)
    out = await teams_consumer._run_agent(_job(), [])
    assert out["stop_reason"] == "end_turn" and len(out["tool_calls"]) == 1


async def test_the_web_adapters_build_a_service_principal_with_the_bound_request_id(
    monkeypatch, cc
):
    seen = []

    async def spy(request, *, runtime, prior_turns=(), run_settings=None):
        seen.append(request)
        return AgentResult(answer="ok", stop_reason="end_turn", model="m")

    monkeypatch.setattr(agent_api, "run_request", spy)
    monkeypatch.setattr(logs_api, "run_request", spy)
    monkeypatch.setattr(logs_api, "local_today", lambda tz: TODAY)
    with log_context.bind(request_id="web-req-1", tenant=CC):
        async with async_session() as db:
            out = await agent_api.ask(
                agent_api.AskRequest(
                    question="how many?", history=[agent_api.HistoryTurn(role="user", content="hi")]
                ),
                customer=CC,
                db=db,
            )
            out2 = await logs_api.debug_ask(
                logs_api.DebugAskRequest(question="D1?"),
                customer=cc,
                db=db,
                pending={"pending": False},
            )
    analytics, logspace = seen
    assert analytics.request_id == "web-req-1" and analytics.tenant == CC
    assert analytics.principal.kind == "service" and analytics.principal.object_id.startswith(
        "service:"
    )
    assert analytics.profile == "analytics" and analytics.conversation_id is None
    assert logspace.profile == "logspace" and logspace.tenant == cc
    assert (
        out["answer"] == "ok"
        and out2["answer"] == "ok"
        and out2["pending_regroup"] == {"pending": False}
    )
    assert out2["refs"] == []


async def test_the_web_adapters_read_no_settings_inside_the_run(monkeypatch, cc):
    await _plant()
    monkeypatch.setattr(
        analytics_module,
        "init_chat_model",
        lambda name, **kw: _scripted(
            AIMessage(content="in_scope"), _describe(), AIMessage(content="Done.")
        ),
    )
    monkeypatch.setattr(
        logspace_module,
        "init_chat_model",
        lambda name, **kw: _scripted(
            AIMessage(content="in_scope"), _trace_d1(), AIMessage(content="Picked.")
        ),
    )
    monkeypatch.setattr(logs_api, "local_today", lambda tz: TODAY)
    _poison(monkeypatch)
    async with async_session() as db:
        out = await agent_api.ask(
            agent_api.AskRequest(question="what is there?"), customer=CC, db=db
        )
        out2 = await logs_api.debug_ask(
            logs_api.DebugAskRequest(question="has D1 been picked?"),
            customer=cc,
            db=db,
            pending={"pending": False},
        )
    assert out["answer"] == "Done." and out2["answer"].startswith("Picked.")
