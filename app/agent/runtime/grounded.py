"""The grounded tool-loop every profile runs on (chunk 132; the logic came from chunk 124-126).

A LangGraph loop (model, tool calls, tool results, repeat) with the guards that keep an answer to
the data, run as the pipeline the profile lists (app/agent/runtime/pipeline.py): before the model,
manipulation is declined in code and the topic gate declines what is not about the agent's data;
after it, a draft whose figures no data call produced, or that no tool returned, is retried once
with a nudge and then withheld; a draft that called no tool at all may carry no date, time or
share the instructions lack, because that is how a repeated question gets the earlier turn echoed
back (11 November 2026). History is replayed without its figures; the
table is drawn from the rows; the answer and every tool result are redacted whatever the profile lists. Every run
collects a trace (app/agent/runtime/trace.py) that the gateway stores.

An agent is a `Profile` (app/agent/profiles, the prompts and names as data) on the tool
registry (app/agent/capabilities, the profile's namespaces bound per run); `GroundedAgent`
holds the two and nothing else varies (steps 12 and 13). The analytics and logspace agents
are the two profiles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import replace
from datetime import date
from typing import Any, Sequence

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.tools import BaseTool
from langgraph.errors import GraphRecursionError

from app.agent.contracts import (AgentRequest, AgentResult, ExecutionContext, ModelSettings,
                                 ToolCall)
from app.agent.presentation import presentation_for
from app.agent.profiles import Profile
from app.agent.profiles.version import code_version
from app.agent.capabilities.registry import ToolRegistry
from app.agent.capabilities.spec import ToolSet
from app.agent.runtime.guards import TopicGate
from app.agent.runtime.pipeline import (  # noqa: F401 - looks_fabricated kept importable here
    MANIPULATION_PREFIX,
    Draft,
    build_pipeline,
    looks_fabricated,
    unknown_guards,
)
from app.agent.runtime.trace import RunTrace
from app.agent.router.providers import adapter_for
from app.agent.router.router import ModelRouter
from app.agent.presentation import evidence
from app.services.notifications.redact import redact_secrets

logger = logging.getLogger(__name__)

_DATA_LINE = re.compile(r"^From the data:.*$", re.M)


def build_model(model: ModelSettings, factory) -> BaseChatModel:
    """The chat model the settings name ("ollama:…", "bedrock_converse:…", "anthropic:…").

    (ModelSettings(name="ollama:qwen3:8b", ...)) -> factory("ollama:qwen3:8b", base_url=..., ...)
    """
    # The provider's arguments live with its adapter (app/agent/router/providers.py); a prefix no
    # adapter knows is built bare, as it always was, though the router never pins one.
    adapter = adapter_for(model.name)
    kwargs: dict[str, Any] = adapter.arguments(model) if adapter is not None else {}
    return factory(model.name, **kwargs)


def make_model(model_settings: ModelSettings) -> BaseChatModel:
    """The chat model the context names, whichever provider the string names (tests patch this)."""
    return build_model(model_settings, init_chat_model)


def frame_question(profile: Profile, question: str, *, today: date,
                   scope_description: str | None) -> str:
    """The question as the model sees it: the profile's frame around it.

    (analytics, "how many?", today=2026-09-30) -> "(Today is 2026-09-30.)\n\nhow many?"
    """
    return profile.question_frame.format(today=today.isoformat(),
                                         scope_description=scope_description or "",
                                         question=question)


def history_as_messages(history: list[dict] | None) -> list[BaseMessage]:
    """Prior turns as messages. An assistant turn is replayed without its table, ranked list or
    "From the data" line: those figures are stale, and a model handed them answers the next question
    from them instead of calling a tool (the first Bedrock run did exactly that)."""
    out: list[BaseMessage] = []
    for turn in history or []:
        content = str(turn.get("content") or "").strip()
        if turn.get("role") == "assistant":
            content = evidence.strip_tables(_DATA_LINE.sub("", content)).strip()
        if not content:
            continue
        out.append(AIMessage(content=content) if turn.get("role") == "assistant" else HumanMessage(content=content))
    return out


def tool_trace(messages: list[BaseMessage]) -> list[dict]:
    """Every tool call the model made, in order, with the first line of what came back."""
    results = {m.tool_call_id: m for m in messages if isinstance(m, ToolMessage)}
    trace = []
    for m in messages:
        if isinstance(m, AIMessage):
            for call in m.tool_calls or []:
                result = results.get(call.get("id"))
                text = str(result.content) if result is not None else ""
                trace.append({"tool": call["name"], "input": call.get("args") or {},
                              "result_preview": text[:200], "result": text})
    return trace


def text_of(message: AIMessage) -> str:
    content = message.content
    if isinstance(content, str):
        return content.strip()
    parts = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return "".join(parts).strip()


def short(value: Any, n: int = 200) -> str:
    text = json.dumps(value, default=str)
    return text if len(text) <= n else text[:n] + "…"


class GroundedAgent:
    """One question in, one checked answer out: a profile's prompts on a tool set's tools."""

    MANIPULATION_PREFIX = MANIPULATION_PREFIX
    #: the answer when the run reaches its deadline; the slot is freed and the person can retry
    TIMED_OUT = ("That took too long to answer, so I stopped. Ask again more narrowly, for example "
                 "a shorter time window or one delivery, picker or item.")

    def __init__(self, customer_code: str, *, profile: Profile, registry: ToolRegistry,
                 model: BaseChatModel | None = None, gate: TopicGate | None = None,
                 router: ModelRouter | None = None):
        # No database session and no settings live here: a run gets everything from the context
        # the gateway builds (step 11); the model and gate arguments are overrides for tests.
        # Step 1: Resolve the profile's namespaces now, so that an unknown namespace or a name
        # clash fails where the runtime is built, not at the first question.
        offered = {spec.name for spec in registry.offered(profile.namespaces)}
        missing = profile.schema_tools - offered
        if missing:
            raise ValueError(f"profile {profile.name!r} names schema tools that are not offered: "
                             f"{sorted(missing)}")
        # Step 2: Resolve the guard names too, so that a profile typo cannot run with one guard
        # fewer; the pipeline itself is built per run because the topic gate needs the model.
        bad = unknown_guards(profile.guards)
        if bad:
            raise ValueError(f"profile {profile.name!r} names guards that do not exist: {bad}")
        self.customer_code = customer_code  # every tool is hard-scoped to this tenant
        self.profile = profile
        self.registry = registry
        self.presentation = presentation_for(profile.presentation)
        self._model_override = model
        self._gate_override = gate
        self.router = router or ModelRouter()  # stateless today; breakers make it shared later

    def resolve_model(self, context: ExecutionContext
                      ) -> tuple[BaseChatModel, TopicGate | None, str]:
        """The pinned model, the gate and the model's name, from the router or the overrides.

        (context, no overrides) -> (the pinned candidate built, TopicGate on it, its name) ;
        (context, injected scripted model) -> (that model, None, its model_name or the context's)
        """
        # In real use the gate always runs. A test that injects a scripted model counts its calls,
        # so it gets the gate only when it passes one. The run itself goes through `_attempts`,
        # which also builds the cheap gate model and reruns on a fallback.
        injected = self._model_override is not None
        if injected:
            model, name = self._model_override, context.model.name
        else:
            choice = self.router.pick(self.profile.model, context.model, context.resilience)
            model, name = make_model(replace(context.model, name=choice.model)), choice.model
        gate = self._gate_override if self._gate_override is not None else (
            None if injected else TopicGate(model, self.profile.domain))
        model_name = getattr(model, "model_name", None) or name
        return model, gate, model_name

    # ---- the run -----------------------------------------------------------------------------------
    async def run(self, context: ExecutionContext, request: AgentRequest,
                  prior_turns: Sequence[dict] = (), *,
                  trace: RunTrace | None = None) -> AgentResult:
        """Answer one request under its context: the text and the tool calls made, as a result.

        (context, request) -> AgentResult(stop_reason="end_turn", ...) ; a provider that never
        answers -> AgentResult(answer=TIMED_OUT, stop_reason="timeout") ; another tenant's context
        -> ValueError
        """
        # Step 1: Refuse a context for another tenant, because every tool of this runtime is
        # hard-scoped to its own customer and a mismatch would answer from the wrong data.
        if context.tenant != self.customer_code:
            raise ValueError(f"runtime for tenant {self.customer_code!r} given a context for "
                             f"{context.tenant!r}")
        # Step 2: The trace is the gateway's when it gave one (so a run that raised still has
        # its calls there), else this run's own; it lives outside the deadline, so a run that
        # times out keeps what it collected. `current` names the model of the attempt in
        # progress for the timeout answer.
        trace = trace if trace is not None else RunTrace()
        current = {"model": context.model.name}
        # Step 3: The deadline wraps the gate and the loop together, so that a provider that hangs
        # can never hold a consumer slot or a request past the queue's visibility window.
        try:
            return await asyncio.wait_for(
                self._attempts(context, request, prior_turns, trace=trace, current=current),
                timeout=context.seconds_left())
        except asyncio.TimeoutError:
            logger.warning("agent run timed out after %.0fs for %s: %s",
                           context.budgets.run_timeout_seconds, self.customer_code,
                           request.text[:200])
            return self._timed_out(context, request, current["model"], trace)

    async def _attempts(self, context: ExecutionContext, request: AgentRequest,
                        prior_turns: Sequence[dict], *, trace: RunTrace,
                        current: dict) -> AgentResult:
        """The run on the pinned model, then once more on the next fallback when its provider fails.

        (primary answers) -> its result ; (primary raises, fallback answers) -> the fallback's
        result after one agent.fallback line ; (both raise) -> the second error
        """
        # Step 1: An injected model (tests) bypasses the router: one attempt, as always.
        if self._model_override is not None:
            raw, gate, name = self.resolve_model(context)
            current["model"] = name
            # the injected model goes through a "test" guard too, so accounting is one path
            model = self._guarded(raw, "test:injected", context, trace)
            return await self._run(context, request, prior_turns, model=model, gate=gate,
                                   model_name=name, trace=trace)
        # Step 2: Pin the first candidate that serves the profile; the gate's model is built once
        # and shared by every attempt, so a fallback does not change who screens the question.
        choice = self.router.pick(self.profile.model, context.model, context.resilience)
        candidates = (choice.model, *choice.fallbacks)
        gate_model = (None if choice.gate_model == choice.model
                      else self._guarded(make_model(replace(context.model, name=choice.gate_model)),
                                         choice.gate_model, context, trace))
        for i, name in enumerate(candidates):
            built = make_model(replace(context.model, name=name))
            model_name = getattr(built, "model_name", None) or name
            # every call of the run, the gate's and the loop's, goes through the provider's guard
            model = self._guarded(built, name, context, trace)
            gate = (self._gate_override if self._gate_override is not None
                    else TopicGate(gate_model or model, self.profile.domain))
            current["model"] = name
            try:
                return await self._run(context, request, prior_turns, model=model, gate=gate,
                                       model_name=model_name, trace=trace)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                raise
            except Exception as exc:  # noqa: BLE001 - provider failed; the next one gets one try
                # Step 3: Rerun from the start on the next fallback, never mid-run, because
                # switching providers inside a run would mix tool-call ids; only with time left.
                if i == len(candidates) - 1 or context.seconds_left() <= 0:
                    raise
                logger.warning("agent.fallback from=%s to=%s error=%s: %s", name,
                               candidates[i + 1], type(exc).__name__, str(exc)[:200])
        raise AssertionError("unreachable: every candidate returned or raised")

    def _guarded(self, model: BaseChatModel, name: str, context: ExecutionContext,
                 trace: RunTrace) -> BaseChatModel:
        return self.router.guarded(model, name, resilience=context.resilience,
                                   timeout_seconds=context.budgets.call_timeout_seconds,
                                   on_call=trace.record_model_call)

    async def _run(self, context: ExecutionContext, request: AgentRequest,
                   prior_turns: Sequence[dict], *, model: BaseChatModel, gate: TopicGate | None,
                   model_name: str, trace: RunTrace) -> AgentResult:
        """The run itself, with no deadline of its own.

        `prior_turns` are the conversation's earlier turns, oldest first, as
        {"role": "user"|"assistant", "content": str}. Only plain text is replayed, without figures.
        A draft whose figures no data call produced is discarded and the question asked once more
        with a nudge; a second such draft is withheld."""
        question = request.text
        history = list(prior_turns)
        # Step 1: Run the profile's pre-guards in its order, because the first decline wins and
        # a declined question must never reach the model or open a session.
        pipeline = build_pipeline(self.profile.guards, profile=self.profile, gate=gate)
        decline = await pipeline.before(question, history, trace)
        if decline is not None:
            logger.log(logging.WARNING if decline.reason == "manipulation" else logging.INFO,
                       "agent declined (%s) for %s: %s", decline.reason, self.customer_code,
                       question[:200])
            return self._declined(context, request, model_name, decline.reason, decline.answer,
                                  trace)

        # Step 2: Bind the tools after the pre-guards, recording every call on the trace.
        tools: ToolSet = self.registry.bind(context, request, self.profile.namespaces, trace=trace)
        graph = create_agent(model, list(tools.tools), system_prompt=self.profile.system_prompt)
        prior = history_as_messages(history)
        asked = frame_question(self.profile, question, today=context.today,
                               scope_description=tools.scope_description)
        max_iterations = context.budgets.max_iterations
        retries = 0
        draft = await self._draft(graph, prior + [HumanMessage(content=asked)], question,
                                  max_iterations=max_iterations, tools=tools, trace=trace,
                                  pipeline=pipeline)
        if draft["problem"]:
            retries = 1
            nudge = (self.profile.ungrounded_nudge.format(figures=", ".join(draft["ungrounded"]))
                     if draft["problem"] == "ungrounded" else self.profile.retry_nudge)
            # the nudge follows the question after a blank line, as the old constants carried it
            draft = await self._draft(graph, prior + [HumanMessage(content=asked + "\n\n" + nudge)],
                                      question, max_iterations=max_iterations, tools=tools,
                                      trace=trace, pipeline=pipeline)
        answer, stop_reason, calls = draft["answer"], draft["stop_reason"], draft["trace"]
        withheld_figures: list[str] = []
        table: str | None = None
        data: dict | None = None
        if draft["problem"] == "fabricated":
            logger.warning("agent answer withheld for %s: figures with no successful data call: %s",
                           self.customer_code, answer[:200])
            answer, stop_reason = draft["fallback"], "withheld"
        elif draft["problem"] == "ungrounded":
            withheld_figures = draft["ungrounded"]
            logger.warning("agent answer withheld for %s: figures not in any tool result %s: %s",
                           self.customer_code, withheld_figures, answer[:200])
            answer = ("I could not verify these figures against the data, so I am not giving them: "
                      + ", ".join(withheld_figures) + ". Ask again more narrowly, or ask me to show the rows.")
            stop_reason = "withheld"
        if draft["problem"] != "fabricated":
            table = self.presentation.render(calls)
            if table:
                answer = (self.presentation.strip(answer) + "\n\n" + table).strip()
                data = self.presentation.structured(calls, context.public_base_url)
        result = AgentResult(
            answer=redact_secrets(answer), stop_reason=stop_reason, model=model_name,
            tool_calls=tuple(ToolCall(t["tool"], t["input"]) for t in calls),
            iterations=len(calls), retries=retries, evidence=table, evidence_data=data,
            withheld_figures=tuple(withheld_figures), usage=trace.usage,
            request_id=context.request_id, trace_id=context.trace_id,
            profile_version=self.profile.version, code_version=code_version())
        # Step 3: Finish the trace from the result, so the stored record and the answer agree.
        return replace(result, trace=trace.finish(context, request, result))

    def _declined(self, context: ExecutionContext, request: AgentRequest, model_name: str,
                  why: str, answer: str, trace: RunTrace) -> AgentResult:
        result = AgentResult(answer=answer, stop_reason="declined", model=model_name, declined=why,
                             request_id=context.request_id, trace_id=context.trace_id,
                             profile_version=self.profile.version, code_version=code_version())
        return replace(result, trace=trace.finish(context, request, result))

    def _timed_out(self, context: ExecutionContext, request: AgentRequest, model_name: str,
                   trace: RunTrace) -> AgentResult:
        result = AgentResult(answer=self.TIMED_OUT, stop_reason="timeout", model=model_name,
                             usage=trace.usage, request_id=context.request_id,
                             trace_id=context.trace_id, profile_version=self.profile.version,
                             code_version=code_version())
        return replace(result, trace=trace.finish(context, request, result))

    async def _draft(self, graph, messages: list[BaseMessage], question: str, *,
                     max_iterations: int, tools: ToolSet, trace: RunTrace, pipeline) -> dict:
        """One run of the tool loop, checked: the answer, its trace, and what is wrong with it
        ("fabricated" = figures with no successful data call, "ungrounded" = a figure no tool
        returned, None = fine)."""
        # recursion_limit counts graph steps; a tool round is two (model, tools), plus the final answer.
        seen: list[BaseMessage] = list(messages)
        stop_reason = "end_turn"
        try:
            # Stream by graph step so a run that hits the limit still yields every message before it.
            async for step in graph.astream({"messages": messages}, stream_mode="values",
                                            config={"recursion_limit": max_iterations * 2 + 1}):
                seen = step["messages"]
        except GraphRecursionError:
            stop_reason = "max_iterations"
        final = next((m for m in reversed(seen) if isinstance(m, AIMessage) and not m.tool_calls), None)
        answer = text_of(final) if final is not None else ""
        if stop_reason == "max_iterations" and not answer:
            answer = ("I could not settle on an answer within the allowed number of tool calls. "
                      "Ask a narrower question, or name the field or window you want.")
        calls = tool_trace(seen)
        for t in calls:
            logger.info("agent tool %s %s -> %s", t["tool"], short(t["input"]), t["result_preview"][:160])
        # The profile's post-guards, in its order: a fabricated draft (chunk 124) or a figure no
        # tool returned (chunk 125) is a problem the caller answers with one nudge.
        problem = pipeline.after(
            Draft(answer=answer, trace=tuple(calls), question=question,
                  given=(self.profile.system_prompt, tools.tool_text),
                  schema_tools=self.profile.schema_tools),
            trace)
        return {"answer": answer, "stop_reason": stop_reason, "trace": calls,
                "problem": problem.kind if problem else None,
                "fallback": problem.fallback if problem else None,
                "ungrounded": list(problem.figures) if problem else []}
