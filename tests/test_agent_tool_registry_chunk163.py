"""Step 13 of docs/esmart-agent-execution-plan.md: one registry of tools, selected by namespace.

A profile used to name a tool set builder and each channel paired the two by hand. Now the
registry holds every spec under a namespace, a profile lists its namespaces, and the offered
set is built per run from that list alone. The tools, their order, their schemas and the text
the fabrication guard is given are byte for byte what they were.
"""

import asyncio
import importlib.util
import json
import logging

import pytest
import yaml
from langchain_core.tools import BaseTool

from app.agent.capabilities.registry import ToolRegistry
from app.agent.capabilities.spec import Namespace, ToolSpec
from app.agent.profiles import load_profile, read_profile
from app.agent.runtime.grounded import GroundedAgent
from app.services.analytics_agent import at_risk_tools
from app.services.analytics_agent.action_tools import ACTION_TOOLS
from app.services.analytics_agent.presentation_tools import PRESENTATION_TOOLS
from app.services.analytics_agent.tools import RECIPES, RELEASE_TOOLS
from app.services.log_feed.scope import describe, scope_from
from app.services.logspace_agent.tools import LOGSPACE_TOOLS
from tests.agent_helpers import TODAY, context_for, request_for, tool_registry
from tests.test_agent_profiles_chunk162 import PROFILES, _write
from tests.test_analytics_agent_chunk124 import CC

ANALYTICS_17 = [
    "search_transactions",
    "count_transactions",
    "find_errors",
    "get_transaction",
    "search_entries",
    "list_metrics",
    "query_metric",
    "explain_freshness",
    "describe_releases",
    "aggregate_releases",
    "list_releases",
    "explain_release",
    "describe_at_risk",
    "at_risk_history",
    "at_risk_board",
    "at_risk_vans",
    "explain_at_risk_delivery",
]
LOGSPACE_6 = [
    "overview",
    "find_transactions",
    "trace",
    "aggregate",
    "get_transaction",
    "search_entries",
]


class SpyRunner:
    """Records every call it gets and answers from a script or a delay."""

    def __init__(self, answer='{"ok": true}', delay: float = 0.0):
        self.answer, self.delay = answer, delay
        self.calls: list[tuple[str, str, dict]] = []

    async def __call__(self, context, request, name, args):
        self.calls.append((context.tenant, name, dict(args)))
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.answer


def _spy_namespace(runner, *, timeout: float | None = None) -> Namespace:
    spec = ToolSpec(
        namespace="t",
        name="spy",
        description="a spy",
        input_schema={"type": "object", "properties": {}},
        timeout_seconds=timeout,
    )
    return Namespace(name="t", specs=(spec,), runner=runner)


# ==================================================== 1. the allow-list


def test_a_profile_only_gets_its_namespaces_tools_in_the_old_order():
    registry = tool_registry()
    # the analytics profile gained the presentation namespace in step 5 (chunk 171); its three
    # tools come after the seventeen data tools, which keep their old order
    analytics_20 = ANALYTICS_17 + [t["name"] for t in PRESENTATION_TOOLS] + [t["name"] for t in ACTION_TOOLS]
    assert [s.name for s in registry.offered(load_profile("analytics").namespaces)] == analytics_20
    assert [s.name for s in registry.offered(load_profile("logspace").namespaces)] == LOGSPACE_6
    bound = registry.bind(context_for(CC), request_for(CC), load_profile("analytics").namespaces)
    assert [t.name for t in bound.tools] == analytics_20 and all(
        isinstance(t, BaseTool) for t in bound.tools
    )
    bound = registry.bind(
        context_for("x", profile="logspace"),
        request_for("x", profile="logspace"),
        load_profile("logspace").namespaces,
    )
    assert [t.name for t in bound.tools] == LOGSPACE_6


def test_an_unknown_namespace_fails_at_construction():
    with pytest.raises(ValueError, match="namespace"):
        GroundedAgent(CC, profile=load_profile("analytics"), registry=ToolRegistry())


def test_two_namespaces_with_the_same_tool_name_cannot_be_selected_together():
    with pytest.raises(ValueError, match="get_transaction"):
        tool_registry().offered(("logs", "logspace"))


def test_schema_tools_must_be_offered(tmp_path):
    profile = read_profile(_write(tmp_path, "custom", schema_tools=["nope"]))
    with pytest.raises(ValueError, match="nope"):
        GroundedAgent(CC, profile=profile, registry=tool_registry())


def test_the_registry_refuses_a_duplicate_namespace():
    registry = ToolRegistry([_spy_namespace(SpyRunner())])
    with pytest.raises(ValueError, match="namespace"):
        registry.register(_spy_namespace(SpyRunner()))
    spec = ToolSpec(namespace="t", name="spy", description="a", input_schema={"type": "object"})
    with pytest.raises(ValueError, match="spy"):
        Namespace(name="t", specs=(spec, spec), runner=SpyRunner())


# ==================================================== 2. the tenant comes from the context


async def test_a_tool_call_runs_for_the_context_tenant_never_an_argument():
    runner = SpyRunner()
    registry = ToolRegistry([_spy_namespace(runner)])
    [tool] = registry.bind(context_for("acme"), request_for("acme"), ("t",)).tools
    assert await tool.ainvoke({}) == '{"ok": true}'
    assert runner.calls == [("acme", "spy", {})]
    real = tool_registry()
    for namespaces, cc, profile in (
        (("logs", "analytics"), CC, "analytics"),
        (("logspace",), "x", "logspace"),
    ):
        bound = real.bind(
            context_for(cc, profile=profile), request_for(cc, profile=profile), namespaces
        )
        for t in bound.tools:
            assert "customer_code" not in t.args and "customer" not in t.args, t.name


# ==================================================== 3. what the model is given is unchanged


def test_the_tool_text_per_profile_is_byte_for_byte_what_it_was():
    registry = tool_registry()
    analytics = json.dumps(
        [RELEASE_TOOLS, RECIPES, at_risk_tools.AT_RISK_TOOLS, at_risk_tools.RECIPES], default=str
    )
    assert registry.tool_text(("logs", "analytics")) == analytics
    assert registry.tool_text(("logspace",)) == json.dumps(LOGSPACE_TOOLS)
    assert "at_risk_history" in analytics and "search_transactions" not in analytics


def test_the_logspace_scope_description_is_unchanged():
    registry = tool_registry()
    filters = {"date": "2026-09-29", "user": "PEVANS", "orderNumber": "O1", "limit": 300}
    bound = registry.bind(
        context_for("x", profile="logspace"),
        request_for("x", profile="logspace", scope=filters),
        ("logspace",),
    )
    assert bound.scope_description == describe(scope_from(filters, TODAY), today=TODAY)
    with pytest.raises(ValueError, match="tz_name"):
        registry.bind(
            context_for("x", profile="logspace", tz_name=None),
            request_for("x", profile="logspace"),
            ("logspace",),
        )


# ==================================================== 4. the executor around every call


async def test_every_tool_call_writes_one_log_line(caplog):
    runner = SpyRunner()
    [tool] = (
        ToolRegistry([_spy_namespace(runner)])
        .bind(context_for("acme"), request_for("acme"), ("t",))
        .tools
    )
    with caplog.at_level(logging.INFO, logger="app.agent.capabilities.registry"):
        await tool.ainvoke({})
        runner.answer = '{"error": "no"}'
        await tool.ainvoke({})
    lines = [r.message for r in caplog.records if r.message.startswith("agent.tool")]
    assert len(lines) == 2 and "ns=t name=spy ms=" in lines[0]
    assert "ok=True" in lines[0] and "ok=False" in lines[1]


async def test_a_spec_timeout_turns_a_hanging_runner_into_a_tool_error(caplog):
    runner = SpyRunner(delay=1.0)
    registry = ToolRegistry([_spy_namespace(runner, timeout=0.05)])
    [tool] = registry.bind(context_for("acme"), request_for("acme"), ("t",)).tools
    with caplog.at_level(logging.INFO, logger="app.agent.capabilities.registry"):
        result = await asyncio.wait_for(tool.ainvoke({}), timeout=0.8)
    assert result.startswith('{"error"') and "spy" in result
    assert any("ok=False" in r.message for r in caplog.records)


# ==================================================== 5. the profile key


@pytest.mark.parametrize("value", [[], "logs", ["logs", "logs"]])
def test_the_loader_wants_a_non_empty_list_of_unique_namespaces(tmp_path, value):
    with pytest.raises(ValueError, match="namespaces"):
        read_profile(_write(tmp_path, "custom", namespaces=value))


def test_the_profiles_list_their_namespaces():
    assert load_profile("analytics").namespaces == ("logs", "analytics", "presentation", "actions")
    assert load_profile("logspace").namespaces == ("logspace",)
    for name in ("analytics", "logspace"):
        raw = yaml.safe_load((PROFILES / f"{name}.yaml").read_text())
        assert "tool_set" not in raw and "namespaces" in raw


def test_the_tool_set_modules_are_gone():
    for module in (
        "app.agent.runtime.tool_set",
        "app.services.analytics_agent.tool_set",
        "app.services.logspace_agent.tool_set",
    ):
        assert importlib.util.find_spec(module) is None, module
