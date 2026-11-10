"""Step 15 of docs/esmart-agent-execution-plan.md: the golden questions, one test per case.

`tests/golden/<profile>.yaml` lists what each agent is known to do, as data a non-programmer can
read: the question, the scripted model's turns, and what the run must give. Every case runs here
through the gateway on the planted dataset with the scripted model, so a change to the runtime,
the guards, the tools or the presentation that alters an answer fails one named case. The same
questions run nightly against the real model with `scripts/golden_questions.py`.
"""

import pytest
from langchain_core.messages import AIMessage

from app.agent.contracts import AgentResult, ToolCall
from app.agent.gateway import run_request
from app.agent.profiles import load_profile
from app.agent.runtime.grounded import GroundedAgent
from app.agent.runtime.guards import TopicGate
from tests.agent_helpers import request_for, tool_registry
from tests.golden import plant
from tests.golden.harness import PROFILES, Case, load_cases, mismatches, scripted_replies
from tests.test_analytics_agent_chunk124 import ScriptedModel

CASES = [case for profile in PROFILES for case in load_cases(profile)]
IDS = [f"{case.profile}:{case.name}" for case in CASES]
TENANT = {"analytics": "test_golden_a", "logspace": "test_golden_l"}


async def _plant(profile: str) -> str:
    cc = TENANT[profile]
    if profile == "analytics":
        await plant.plant_analytics(cc)
    else:
        await plant.plant_logspace(cc)
    return cc


async def _wipe(profile: str, cc: str) -> None:
    if profile == "analytics":
        await plant.wipe_analytics(cc)
    else:
        await plant.wipe_logspace(cc)


async def run_case(case: Case, cc: str) -> AgentResult:
    """One case through the gateway with its scripted model, on the tenant's planted data."""
    model = ScriptedModel(script=scripted_replies(case), seen=[])
    profile = load_profile(case.profile)
    runtime = GroundedAgent(
        cc,
        profile=profile,
        registry=tool_registry(),
        model=model,
        gate=TopicGate(model, profile.domain) if case.gate else None,
    )
    request = request_for(cc, case.question, profile=case.profile)
    return await run_request(request, runtime=runtime, prior_turns=case.history)


# ==================================================== 1. the files themselves


def test_every_case_has_a_unique_name_and_expects_tools_its_profile_offers():
    registry = tool_registry()
    for profile in PROFILES:
        cases = load_cases(profile)
        names = [c.name for c in cases]
        assert len(set(names)) == len(names), f"{profile}: repeated case names"
        offered = {s.name for s in registry.offered(load_profile(profile).namespaces)}
        for case in cases:
            unknown = set(case.expect["tools"]) - offered
            assert not unknown, f"{profile}:{case.name} expects tools not offered: {unknown}"
            for reply in case.replies:
                if "tool" in reply:
                    assert reply["tool"] in offered, f"{profile}:{case.name} scripts {reply}"


def test_both_profiles_have_cases():
    assert all(load_cases(profile) for profile in PROFILES)


# ==================================================== 2. every case, scripted


@pytest.mark.parametrize("case", CASES, ids=IDS)
async def test_golden_case(case: Case):
    cc = await _plant(case.profile)
    try:
        result = await run_case(case, cc)
        assert mismatches(case, result, scripted=True) == [], result.answer
    finally:
        await _wipe(case.profile, cc)


# ==================================================== 3. the real-model checks are the subset


def test_the_real_model_checks_ignore_the_models_exact_words():
    [case] = [c for c in load_cases("analytics") if c.name == "which_products_were_short"]
    worded_differently = AgentResult(
        answer="Two products were short that morning.",
        stop_reason="end_turn",
        model="real",
        tool_calls=(ToolCall("describe_releases", {}), ToolCall("aggregate_releases", {})),
        iterations=2,
        retries=1,
    )
    assert mismatches(case, worded_differently, scripted=False) == []
    assert mismatches(case, worded_differently, scripted=True) != []
    declined = AgentResult(answer="no", stop_reason="declined", model="real", declined="off_topic")
    assert mismatches(case, declined, scripted=False) == [
        "stop_reason: expected 'end_turn', got 'declined'",
        "declined: expected None, got 'off_topic'",
        "tools: ['aggregate_releases'] were not called; called []",
    ]


def test_scripted_replies_become_tool_calls_and_texts():
    [case] = [c for c in load_cases("logspace") if c.name == "an_invented_figure_is_withheld"]
    replies = scripted_replies(case)
    assert [type(m) for m in replies] == [AIMessage] * 4
    assert replies[0].tool_calls[0]["name"] == "trace" and replies[1].content.startswith("Delivery")
