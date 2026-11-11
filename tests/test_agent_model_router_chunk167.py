"""Step 17 of docs/esmart-agent-execution-plan.md: the model router; one rerun on fallback.

One setting named one model and a provider failure reached the channel as a 503 or a "service is
busy" card. Now the profile names what it needs of a model, the settings name the candidates (a
primary, fallbacks, an optional cheap model for the gate), the router pins the first candidate
that can serve the needs, and when the pinned provider fails mid-run the run is tried once more
from the start on the next candidate, inside the same deadline. Answers do not change.
"""

import dataclasses
import logging

import pytest
from langchain_core.messages import AIMessage

from app.agent.contracts import ModelSettings
from app.agent.gateway import run_request, snapshot_settings
from app.agent.profiles import load_profile, read_profile
from app.agent.runtime import grounded
from app.agent.runtime.grounded import GroundedAgent
from app.agent.router.policy import ModelNeeds, ModelPolicy
from app.agent.router.providers import adapter_for
from app.agent.router.router import ModelRouter
from tests.agent_helpers import analytics_runtime, request_for
from tests.test_agent_profiles_chunk162 import _write
from tests.test_agent_run_deadline_chunk155 import HangingModel
from tests.test_analytics_agent_chunk124 import (  # noqa: F401 - autouse fixture
    CC,
    ScriptedModel,
    clean,
)

PRIMARY, FALLBACK, CHEAP = "ollama:primary", "bedrock_converse:fb", "ollama:cheap"
NEEDS = ModelNeeds(tools=True, context_tokens=16000)
POLICY = ModelPolicy(needs=NEEDS, gate="cheap")


def _model_settings(name=PRIMARY, *, fallbacks=(), gate_model="", context_tokens=16384):
    base = snapshot_settings().model
    return dataclasses.replace(
        base,
        name=name,
        fallback_models=tuple(fallbacks),
        gate_model=gate_model,
        context_tokens=context_tokens,
    )


def _run_settings(**over):
    rs = snapshot_settings()
    return dataclasses.replace(rs, model=_model_settings(**over))


class FailingModel(ScriptedModel):
    """A provider that raises on its n-th call, then (or otherwise) answers from the script."""

    fail_at: int = 1
    calls: int = 0

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("ServiceUnavailableException: the provider returned 503")
        return super()._generate(messages, stop, run_manager, **kwargs)


def _call(name: str, args: dict | None = None, call_id: str = "c1") -> AIMessage:
    return AIMessage(content="", tool_calls=[{"id": call_id, "name": name, "args": args or {}}])


def _factory(monkeypatch, models: dict):
    """`init_chat_model` answering from a table of model name to instance, recording the names."""
    asked: list[str] = []

    def build(name, **kwargs):
        asked.append(name)
        return models[name]

    monkeypatch.setattr(grounded, "init_chat_model", build)
    return asked


# ==================================================== 1. the profile names its needs


def test_both_profiles_name_their_needs():
    for name in ("analytics", "logspace"):
        assert load_profile(name).model == POLICY


@pytest.mark.parametrize(
    "value",
    [
        None,
        {"tools": "yes", "context_tokens": 16000, "gate": "cheap"},
        {"tools": True, "context_tokens": 0, "gate": "cheap"},
        {"tools": True, "context_tokens": 16000, "gate": "fast"},
        {"tools": True, "context_tokens": 16000},
        {"tools": True, "context_tokens": 16000, "gate": "cheap", "provider": "ollama"},
    ],
)
def test_the_loader_refuses_a_malformed_model_policy(tmp_path, value):
    with pytest.raises(ValueError, match="model"):
        read_profile(_write(tmp_path, "custom", model=value))


# ==================================================== 2. the adapters


def test_the_adapters_build_todays_arguments_and_say_what_they_can_do():
    settings = _model_settings()
    ollama = adapter_for("ollama:qwen3:8b")
    assert ollama.name == "ollama" and ollama.calls_tools
    assert ollama.arguments(settings) == {
        "base_url": settings.ollama_base_url,
        "reasoning": settings.think,
        "num_ctx": settings.context_tokens,
        "temperature": 0,
    }
    assert ollama.context_tokens(settings) == settings.context_tokens
    bedrock = adapter_for("bedrock_converse:qwen.qwen3-235b-a22b-2507-v1:0")
    assert bedrock.arguments(settings) == {
        "region_name": settings.bedrock_region,
        "temperature": 0,
        "max_tokens": settings.max_tokens,
    }
    assert bedrock.context_tokens(settings) >= 100_000 and bedrock.calls_tools
    assert adapter_for("bedrock:qwen.qwen3-32b-v1:0").name == "bedrock"
    assert adapter_for("anthropic:claude-sonnet-5").arguments(settings) == {}
    assert adapter_for("openai:gpt-5").calls_tools
    assert adapter_for("nope:x") is None and adapter_for("no-prefix") is None


# ==================================================== 3. the pick


def test_the_primary_is_pinned_with_the_fallbacks_behind_it():
    choice = ModelRouter().pick(POLICY, _model_settings(fallbacks=(FALLBACK, "anthropic:x")))
    assert (choice.model, choice.fallbacks, choice.gate_model) == (
        PRIMARY,
        (FALLBACK, "anthropic:x"),
        PRIMARY,
    )


def test_an_unusable_candidate_is_skipped_with_a_line(caplog):
    with caplog.at_level(logging.WARNING):
        choice = ModelRouter().pick(POLICY, _model_settings("nope:x", fallbacks=(FALLBACK,)))
    assert choice.model == FALLBACK and choice.fallbacks == ()
    assert any("agent.router.skipped" in r.getMessage() and "nope:x" in r.getMessage()
               for r in caplog.records)
    narrow = _model_settings(PRIMARY, fallbacks=(FALLBACK,), context_tokens=8000)
    assert ModelRouter().pick(POLICY, narrow).model == FALLBACK  # Ollama's num_ctx is too small


def test_no_usable_candidate_is_a_named_error():
    with pytest.raises(ValueError, match="16000"):
        ModelRouter().pick(POLICY, _model_settings("nope:x"))


def test_the_gate_model_is_the_cheap_one_only_when_configured_and_asked_for():
    settings = _model_settings(gate_model=CHEAP)
    assert ModelRouter().pick(POLICY, settings).gate_model == CHEAP
    primary_only = ModelPolicy(needs=NEEDS, gate="primary")
    assert ModelRouter().pick(primary_only, settings).gate_model == PRIMARY
    assert ModelRouter().pick(POLICY, _model_settings()).gate_model == PRIMARY


# ==================================================== 4. fake providers, through the gateway


async def test_the_fallback_answers_when_the_primary_fails(monkeypatch, caplog):
    primary = FailingModel(script=[], seen=[], fail_at=1)
    fallback = ScriptedModel(script=[AIMessage(content="in_scope"), AIMessage(content="Fine.")],
                             seen=[])
    asked = _factory(monkeypatch, {PRIMARY: primary, FALLBACK: fallback})
    with caplog.at_level(logging.WARNING):
        result = await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(fallbacks=(FALLBACK,)),
        )
    assert result.answer == "Fine." and result.model == FALLBACK
    assert asked == [PRIMARY, FALLBACK]
    assert any("agent.fallback" in r.getMessage() and FALLBACK in r.getMessage()
               for r in caplog.records)


async def test_a_failure_after_a_tool_call_reruns_from_the_start(monkeypatch):
    primary = FailingModel(
        script=[AIMessage(content="in_scope"), _call("describe_releases")], seen=[], fail_at=3
    )
    fallback = ScriptedModel(
        script=[AIMessage(content="in_scope"), _call("describe_releases"),
                AIMessage(content="There are releases.")],
        seen=[],
    )
    _factory(monkeypatch, {PRIMARY: primary, FALLBACK: fallback})
    result = await run_request(
        request_for(CC, "what is there?"),
        runtime=analytics_runtime(CC),
        run_settings=_run_settings(fallbacks=(FALLBACK,)),
    )
    assert result.answer == "There are releases." and result.model == FALLBACK
    assert [t.tool for t in result.tool_calls] == ["describe_releases"]
    assert [c["name"] for c in result.trace.tool_calls] == ["describe_releases"] * 2
    # every attempt counts (step 19): the primary's gate, tool call and failed call, then the
    # fallback's gate, tool call and answer
    assert result.usage["model_calls"] == 6


async def test_both_candidates_failing_propagates_the_second_error(monkeypatch):
    models = {PRIMARY: FailingModel(script=[], seen=[]), FALLBACK: FailingModel(script=[], seen=[])}
    _factory(monkeypatch, models)
    with pytest.raises(RuntimeError, match="503"):
        await run_request(
            request_for(CC, "hi"),
            runtime=analytics_runtime(CC),
            run_settings=_run_settings(fallbacks=(FALLBACK,)),
        )
    assert models[PRIMARY].calls == 1 and models[FALLBACK].calls == 1


async def test_without_a_fallback_the_first_error_propagates(monkeypatch):
    models = {PRIMARY: FailingModel(script=[], seen=[])}
    _factory(monkeypatch, models)
    with pytest.raises(RuntimeError, match="503"):
        await run_request(
            request_for(CC, "hi"), runtime=analytics_runtime(CC), run_settings=_run_settings()
        )


async def test_a_hanging_primary_gives_the_timeout_answer_and_no_fallback_is_tried(monkeypatch):
    asked = _factory(
        monkeypatch,
        {PRIMARY: HangingModel(script=[], seen=[]), FALLBACK: ScriptedModel(script=[], seen=[])},
    )
    result = await run_request(
        request_for(CC, "hi", deadline_seconds=0.2),
        runtime=analytics_runtime(CC),
        run_settings=_run_settings(fallbacks=(FALLBACK,)),
    )
    assert result.stop_reason == "timeout" and result.answer == GroundedAgent.TIMED_OUT
    assert asked == [PRIMARY]


# ==================================================== 5. the gate model


async def test_the_gate_runs_on_the_cheap_model_and_the_loop_on_the_primary(monkeypatch):
    cheap = ScriptedModel(script=[AIMessage(content="in_scope")], seen=[])
    primary = ScriptedModel(script=[AIMessage(content="Fine.")], seen=[])
    asked = _factory(monkeypatch, {PRIMARY: primary, CHEAP: cheap})
    result = await run_request(
        request_for(CC, "hi"),
        runtime=analytics_runtime(CC),
        run_settings=_run_settings(gate_model=CHEAP),
    )
    assert result.answer == "Fine." and result.model == PRIMARY
    assert sorted(asked) == [CHEAP, PRIMARY] and len(cheap.seen) == 1 and len(primary.seen) == 1


async def test_without_a_gate_model_the_gate_and_the_loop_share_the_primary(monkeypatch):
    primary = ScriptedModel(
        script=[AIMessage(content="in_scope"), AIMessage(content="Fine.")], seen=[]
    )
    asked = _factory(monkeypatch, {PRIMARY: primary})
    result = await run_request(
        request_for(CC, "hi"), runtime=analytics_runtime(CC), run_settings=_run_settings()
    )
    assert result.answer == "Fine." and asked == [PRIMARY] and len(primary.seen) == 2


def test_the_snapshot_carries_the_candidates(monkeypatch):
    from app.settings import settings

    monkeypatch.setattr(settings, "analytics_agent_fallback_models", " bedrock_converse:a, ,x:b ")
    monkeypatch.setattr(settings, "analytics_agent_gate_model", " ollama:cheap ")
    model = snapshot_settings().model
    assert model.fallback_models == ("bedrock_converse:a", "x:b") and model.gate_model == CHEAP
    assert ModelSettings("m", "", False, 1, "", 1).fallback_models == ()
