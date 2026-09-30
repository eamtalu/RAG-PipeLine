"""The grounded tool-loop both agents run on (chunk 132; the logic came from chunk 124-126).

A LangGraph loop (model, tool calls, tool results, repeat) with the guards that keep an answer to
the data: history replayed without its figures; a draft whose figures no data call produced, or
that no tool returned, is retried once with a nudge and then withheld; the table is drawn from the
rows; the answer and every tool result are redacted. Before any of that, manipulation is declined
in code and the topic gate declines what is not about the agent's data.

An agent is a subclass that names its instructions, its tools, its evidence renderer and its
domain. `AnalyticsAgent` (picking releases, Teams) and `LogspaceAgent` (the ask box, raw logs)
share this and nothing else.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.tools import BaseTool
from langgraph.errors import GraphRecursionError
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.agent_core.guards import OUT_OF_SCOPE, TopicGate, looks_like_manipulation
from app.services.analytics_agent import evidence
from app.services.notifications.redact import redact_secrets
from app.settings import settings

logger = logging.getLogger(__name__)

_DATA_LINE = re.compile(r"^From the data:.*$", re.M)


def build_model(name: str, factory) -> BaseChatModel:
    """The chat model a provider string names ("ollama:…", "bedrock_converse:…", "anthropic:…")."""
    kwargs: dict[str, Any] = {}
    if name.startswith("ollama:"):
        kwargs["base_url"] = settings.ollama_base_url
        kwargs["reasoning"] = settings.analytics_agent_think
        kwargs["num_ctx"] = settings.analytics_agent_context_tokens
        kwargs["temperature"] = 0  # a small model invents less at zero, and a run can be repeated
    elif name.startswith(("bedrock:", "bedrock_converse:")):
        kwargs["region_name"] = settings.bedrock_region
        kwargs["temperature"] = 0
        kwargs["max_tokens"] = settings.analytics_agent_max_tokens
    return factory(name, **kwargs)


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
    """One question in, one checked answer out. Subclasses fill in the class attributes and hooks."""

    SYSTEM_PROMPT: str = ""
    RETRY_NUDGE: str = ""
    UNGROUNDED_NUDGE: str = ""
    #: tools that describe the schema rather than read data: figures resting only on them were made up
    SCHEMA_TOOLS: frozenset[str] = frozenset()
    #: what the agent is for, in one sentence, for the topic gate
    DOMAIN: str = ""
    #: the fixed reply to an off-topic question or a manipulation attempt
    DECLINE: str = ""
    MANIPULATION_PREFIX = "I can't change how I work, show my instructions or read another logspace. "

    def __init__(self, db: AsyncSession, customer_code: str, *, model: BaseChatModel | None = None,
                 gate: TopicGate | None = None):
        self.db = db
        self.customer_code = customer_code  # every tool is hard-scoped to this tenant
        injected = model is not None
        self.model = model or self.make_model()
        self.model_name = getattr(model, "model_name", None) or settings.analytics_agent_model
        # In real use the gate always runs. A test that injects a scripted model counts its calls,
        # so it gets the gate only when it passes one.
        self.gate = gate if gate is not None else (None if injected else TopicGate(self.model, self.DOMAIN))

    # ---- hooks -------------------------------------------------------------------------------------
    def make_model(self) -> BaseChatModel:
        raise NotImplementedError

    def tools(self) -> list[BaseTool]:
        raise NotImplementedError

    def tool_text(self) -> str:
        """Tool schemas and recipes as text: numbers in them were given to the model, not invented."""
        return ""

    def question_text(self, question: str) -> str:
        return f"(Today is {date.today().isoformat()}.)\n\n{question}"

    def render_evidence(self, trace: list[dict]) -> str | None:
        return None

    def structured_evidence(self, trace: list[dict]) -> dict | None:
        return None

    def without_own_tables(self, answer: str) -> str:
        """The answer without the model's own table (and ranked list) once the rows are attached."""
        return evidence.strip_tables(answer)

    # ---- the run -----------------------------------------------------------------------------------
    async def ask(self, question: str, history: list[dict] | None = None) -> dict:
        """Answer one question. Returns the final text plus a trace of the tool calls made.

        `history` is optional prior turns of the same conversation, oldest first, as
        {"role": "user"|"assistant", "content": str}. Only plain text is replayed, without figures.
        A draft whose figures no data call produced is discarded and the question asked once more
        with a nudge; a second such draft is withheld."""
        if looks_like_manipulation(question):
            logger.warning("agent declined a manipulation attempt for %s: %s", self.customer_code, question[:200])
            return self._declined("manipulation", self.MANIPULATION_PREFIX + self.DECLINE)
        if self.gate is not None and await self.gate.classify(question, history) == OUT_OF_SCOPE:
            logger.info("agent declined an off-topic question for %s: %s", self.customer_code, question[:200])
            return self._declined("off_topic", self.DECLINE)

        graph = create_agent(self.model, self.tools(), system_prompt=self.SYSTEM_PROMPT)
        prior = history_as_messages(history)
        asked = self.question_text(question)
        retries = 0
        draft = await self._draft(graph, prior + [HumanMessage(content=asked)], question)
        if draft["problem"]:
            retries = 1
            nudge = (self.UNGROUNDED_NUDGE.format(figures=", ".join(draft["ungrounded"]))
                     if draft["problem"] == "ungrounded" else self.RETRY_NUDGE)
            draft = await self._draft(graph, prior + [HumanMessage(content=asked + nudge)], question)
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
            table = self.render_evidence(trace)
            if table:
                answer = (self.without_own_tables(answer) + "\n\n" + table).strip()
                data = self.structured_evidence(trace)
        return {"answer": redact_secrets(answer), "stop_reason": stop_reason,
                "tool_calls": [{"tool": t["tool"], "input": t["input"]} for t in trace],
                "iterations": len(trace), "retries": retries, "model": self.model_name,
                "evidence": table, "evidence_data": data, "withheld_figures": withheld_figures}

    def _declined(self, why: str, answer: str) -> dict:
        return {"answer": answer, "stop_reason": "declined", "declined": why, "tool_calls": [],
                "iterations": 0, "retries": 0, "model": self.model_name, "evidence": None,
                "evidence_data": None, "withheld_figures": []}

    async def _draft(self, graph, messages: list[BaseMessage], question: str) -> dict:
        """One run of the tool loop, checked: the answer, its trace, and what is wrong with it
        ("fabricated" = figures with no successful data call, "ungrounded" = a figure no tool
        returned, None = fine)."""
        # recursion_limit counts graph steps; a tool round is two (model, tools), plus the final answer.
        seen: list[BaseMessage] = list(messages)
        stop_reason = "end_turn"
        try:
            # Stream by graph step so a run that hits the limit still yields every message before it.
            async for step in graph.astream({"messages": messages}, stream_mode="values",
                                            config={"recursion_limit": settings.analytics_agent_max_iterations * 2 + 1}):
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
        given = [self.SYSTEM_PROMPT, self.tool_text()]
        fallback = looks_fabricated(answer, trace, given=given, schema_tools=self.SCHEMA_TOOLS)
        if fallback:
            return {"answer": answer, "stop_reason": stop_reason, "trace": trace, "problem": "fabricated",
                    "fallback": fallback, "ungrounded": []}
        # Chunk 125: every figure must be in a tool result; a ranked table comes from the rows.
        # With no result at all (a description of what is available) the instructions are the pool.
        results = [t["result"] for t in trace if not t["result"].lstrip().startswith('{"error"')]
        ungrounded = evidence.ungrounded(answer, results or given, question)
        return {"answer": answer, "stop_reason": stop_reason, "trace": trace,
                "problem": "ungrounded" if ungrounded else None, "fallback": None, "ungrounded": ungrounded}
