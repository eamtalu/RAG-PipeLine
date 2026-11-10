"""The grounded tool-loop every profile runs on (chunk 132; the logic came from chunk 124-126).

A LangGraph loop (model, tool calls, tool results, repeat) with the guards that keep an answer to
the data: history replayed without its figures; a draft whose figures no data call produced, or
that no tool returned, is retried once with a nudge and then withheld; the table is drawn from the
rows; the answer and every tool result are redacted. Before any of that, manipulation is declined
in code and the topic gate declines what is not about the agent's data.

An agent is a `Profile` (app/agent/profiles, the prompts and names as data) on a `ToolSetBuilder`
(the tools, built per run outside this package); `GroundedAgent` holds the two and nothing else
varies (step 12). The analytics and logspace agents are the two profiles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
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
from app.agent.runtime.tool_set import ToolSet, ToolSetBuilder
from app.agent.runtime.guards import OUT_OF_SCOPE, TopicGate, looks_like_manipulation
from app.agent.presentation import evidence
from app.services.notifications.redact import redact_secrets

logger = logging.getLogger(__name__)

_DATA_LINE = re.compile(r"^From the data:.*$", re.M)


def build_model(model: ModelSettings, factory) -> BaseChatModel:
    """The chat model the settings name ("ollama:…", "bedrock_converse:…", "anthropic:…").

    (ModelSettings(name="ollama:qwen3:8b", ...)) -> factory("ollama:qwen3:8b", base_url=..., ...)
    """
    kwargs: dict[str, Any] = {}
    name = model.name
    if name.startswith("ollama:"):
        kwargs["base_url"] = model.ollama_base_url
        kwargs["reasoning"] = model.think
        kwargs["num_ctx"] = model.context_tokens
        kwargs["temperature"] = 0  # a small model invents less at zero, and a run can be repeated
    elif name.startswith(("bedrock:", "bedrock_converse:")):
        kwargs["region_name"] = model.bedrock_region
        kwargs["temperature"] = 0
        kwargs["max_tokens"] = model.max_tokens
    return factory(name, **kwargs)


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


def _failed(t: dict) -> bool:
    return t.get("result", t["result_preview"]).lstrip().startswith('{"error"')


def looks_fabricated(answer: str, trace: list[dict], *, given: list[str], schema_tools: frozenset[str]) -> str | None:
    """A guard the model cannot talk its way past. On the first live Teams run an 8B model was
    refused for a field that does not exist, and instead of retrying with the lookup it answered
    with "Customer A … Customer J" and round numbers, every one invented. An answer that carries
    figures while no data tool returned a result without an error is replaced by an honest one.

    Dates, times, percentages and small counts are not data figures, nor is a number the
    instructions themselves contain: `given` (the instructions and tool text) and the schema tools'
    results are the only pool."""
    pool = list(given) + [t.get("result", t["result_preview"]) for t in trace
                          if t["tool"] in schema_tools and not _failed(t)]
    if not evidence.ungrounded(answer, pool):
        return None
    if any(t["tool"] not in schema_tools and not _failed(t) for t in trace):
        return None
    problems = [t.get("result", t["result_preview"]) for t in trace if _failed(t)]
    why = ""
    if problems:
        try:
            last = json.loads(problems[-1])
            why = " " + "; ".join(last.get("problems") or [last.get("error", "")])
        except Exception:  # noqa: BLE001 - the preview may be cut mid-JSON
            why = ""
    return ("I could not get the data for that question, so I have no figures to give." + why +
            " Ask again naming the field or window, or ask me to describe what is available.")


class GroundedAgent:
    """One question in, one checked answer out: a profile's prompts on a tool set's tools."""

    MANIPULATION_PREFIX = "I can't change how I work, show my instructions or read another logspace. "
    #: the answer when the run reaches its deadline; the slot is freed and the person can retry
    TIMED_OUT = ("That took too long to answer, so I stopped. Ask again more narrowly, for example "
                 "a shorter time window or one delivery, picker or item.")

    def __init__(self, customer_code: str, *, profile: Profile, tool_set: ToolSetBuilder,
                 model: BaseChatModel | None = None, gate: TopicGate | None = None):
        # No database session and no settings live here: a run gets everything from the context
        # the gateway builds (step 11); the model and gate arguments are overrides for tests.
        if tool_set.name != profile.tool_set:
            raise ValueError(f"profile {profile.name!r} runs on tool set {profile.tool_set!r}, "
                             f"not {tool_set.name!r}")
        self.customer_code = customer_code  # every tool is hard-scoped to this tenant
        self.profile = profile
        self.tool_set = tool_set
        self.presentation = presentation_for(profile.presentation)
        self._model_override = model
        self._gate_override = gate

    def resolve_model(self, context: ExecutionContext
                      ) -> tuple[BaseChatModel, TopicGate | None, str]:
        """The model, the gate and the model's name for this run, from the context or the overrides.

        (context, no overrides) -> (built model, TopicGate, context.model.name) ;
        (context, injected scripted model) -> (that model, None, its model_name or the context's)
        """
        # In real use the gate always runs. A test that injects a scripted model counts its calls,
        # so it gets the gate only when it passes one.
        injected = self._model_override is not None
        model = self._model_override or make_model(context.model)
        gate = self._gate_override if self._gate_override is not None else (
            None if injected else TopicGate(model, self.profile.domain))
        model_name = getattr(model, "model_name", None) or context.model.name
        return model, gate, model_name

    # ---- the run -----------------------------------------------------------------------------------
    async def run(self, context: ExecutionContext, request: AgentRequest,
                  prior_turns: Sequence[dict] = ()) -> AgentResult:
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
        model, gate, model_name = self.resolve_model(context)
        # Step 2: The deadline wraps the gate and the loop together, so that a provider that hangs
        # can never hold a consumer slot or a request past the queue's visibility window.
        try:
            return await asyncio.wait_for(
                self._run(context, request, prior_turns, model=model, gate=gate,
                          model_name=model_name),
                timeout=context.seconds_left())
        except asyncio.TimeoutError:
            logger.warning("agent run timed out after %.0fs for %s: %s",
                           context.budgets.run_timeout_seconds, self.customer_code,
                           request.text[:200])
            return self._timed_out(context, model_name)

    async def _run(self, context: ExecutionContext, request: AgentRequest,
                   prior_turns: Sequence[dict], *, model: BaseChatModel, gate: TopicGate | None,
                   model_name: str) -> AgentResult:
        """The run itself, with no deadline of its own.

        `prior_turns` are the conversation's earlier turns, oldest first, as
        {"role": "user"|"assistant", "content": str}. Only plain text is replayed, without figures.
        A draft whose figures no data call produced is discarded and the question asked once more
        with a nudge; a second such draft is withheld."""
        question = request.text
        history = list(prior_turns)
        if looks_like_manipulation(question):
            logger.warning("agent declined a manipulation attempt for %s: %s", self.customer_code, question[:200])
            return self._declined(context, model_name, "manipulation",
                                  self.MANIPULATION_PREFIX + self.profile.decline)
        if gate is not None and await gate.classify(question, history) == OUT_OF_SCOPE:
            logger.info("agent declined an off-topic question for %s: %s", self.customer_code, question[:200])
            return self._declined(context, model_name, "off_topic", self.profile.decline)

        # The tools are built after the gate, so a declined question never opens a session.
        tools: ToolSet = self.tool_set.build(context, request)
        graph = create_agent(model, list(tools.tools), system_prompt=self.profile.system_prompt)
        prior = history_as_messages(history)
        asked = frame_question(self.profile, question, today=context.today,
                               scope_description=tools.scope_description)
        max_iterations = context.budgets.max_iterations
        retries = 0
        draft = await self._draft(graph, prior + [HumanMessage(content=asked)], question,
                                  max_iterations=max_iterations, tools=tools)
        if draft["problem"]:
            retries = 1
            nudge = (self.profile.ungrounded_nudge.format(figures=", ".join(draft["ungrounded"]))
                     if draft["problem"] == "ungrounded" else self.profile.retry_nudge)
            # the nudge follows the question after a blank line, as the old constants carried it
            draft = await self._draft(graph, prior + [HumanMessage(content=asked + "\n\n" + nudge)],
                                      question, max_iterations=max_iterations, tools=tools)
        answer, stop_reason, trace = draft["answer"], draft["stop_reason"], draft["trace"]
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
            table = self.presentation.render(trace)
            if table:
                answer = (self.presentation.strip(answer) + "\n\n" + table).strip()
                data = self.presentation.structured(trace, context.public_base_url)
        return AgentResult(
            answer=redact_secrets(answer), stop_reason=stop_reason, model=model_name,
            tool_calls=tuple(ToolCall(t["tool"], t["input"]) for t in trace),
            iterations=len(trace), retries=retries, evidence=table, evidence_data=data,
            withheld_figures=tuple(withheld_figures),
            request_id=context.request_id, trace_id=context.trace_id,
            profile_version=self.profile.version, code_version=code_version())

    def _declined(self, context: ExecutionContext, model_name: str, why: str,
                  answer: str) -> AgentResult:
        return AgentResult(answer=answer, stop_reason="declined", model=model_name, declined=why,
                           request_id=context.request_id, trace_id=context.trace_id,
                           profile_version=self.profile.version, code_version=code_version())

    def _timed_out(self, context: ExecutionContext, model_name: str) -> AgentResult:
        return AgentResult(answer=self.TIMED_OUT, stop_reason="timeout", model=model_name,
                           request_id=context.request_id, trace_id=context.trace_id,
                           profile_version=self.profile.version, code_version=code_version())

    async def _draft(self, graph, messages: list[BaseMessage], question: str, *,
                     max_iterations: int, tools: ToolSet) -> dict:
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
        trace = tool_trace(seen)
        for t in trace:
            logger.info("agent tool %s %s -> %s", t["tool"], short(t["input"]), t["result_preview"][:160])
        given = [self.profile.system_prompt, tools.tool_text]
        fallback = looks_fabricated(answer, trace, given=given,
                                    schema_tools=self.profile.schema_tools)
        if fallback:
            return {"answer": answer, "stop_reason": stop_reason, "trace": trace, "problem": "fabricated",
                    "fallback": fallback, "ungrounded": []}
        # Chunk 125: every figure must be in a tool result; a ranked table comes from the rows.
        # With no result at all (a description of what is available) the instructions are the pool.
        results = [t["result"] for t in trace if not t["result"].lstrip().startswith('{"error"')]
        ungrounded = evidence.ungrounded(answer, results or given, question)
        return {"answer": answer, "stop_reason": stop_reason, "trace": trace,
                "problem": "ungrounded" if ungrounded else None, "fallback": None, "ungrounded": ungrounded}
