"""The analytics agent on LangGraph. Chunk 124.

Provider-neutral: `settings.analytics_agent_model` is one string, "ollama:qwen3:8b" on a laptop
today, "anthropic:claude-sonnet-5" or "bedrock:…" later, and nothing else changes. The loop is
LangChain's `create_agent` (a LangGraph graph: model, tool calls, tool results, repeat until the
model answers), bounded by `analytics_agent_max_iterations`.

It answers the same questions as the Claude debugging agent, with the same signature
`ask(question, history)` and the same result shape, so the Teams consumer and the web endpoint
use either through one setting. History is plain text turns only; the model re-reads the data for
every question, so a follow-up never quotes a stale tool result. No checkpointer in this chunk.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langgraph.errors import GraphRecursionError
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_agent.tools import build_tools
from app.settings import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are the analytics assistant for a warehouse picking operation run on an Infor M3 WMS.
You answer questions by calling tools that read this logspace's data. You never guess a number.

Two kinds of data, two kinds of tool:
- Pick-list RELEASES (settled rows, one row per release): describe_releases, aggregate_releases,
  list_releases, explain_release. Use these for anything about picking: lines, deliveries, pickers,
  customers, items, shortfalls, zero-picks, over-picks, refusals, how long a line took.
- Raw LOG transactions and metrics: search_transactions, count_transactions, find_errors,
  get_transaction, search_entries, list_metrics, query_metric, explain_freshness. Use these for
  why one API call failed, what a transaction did, or a registered metric.

Rules for release answers:
- Call describe_releases once before your first aggregate_releases or list_releases, so every field
  name is real. If a tool returns "problems", fix exactly what it names and call once more. If the
  same call fails twice, stop retrying: answer with what you have and say what you could not get.
- aggregate_releases always returns rows and the SUM of every settled value per group. Never ask for
  sum:… or count:… as a stat; use stat only for median, p95, mean, min, max or distinct.
- One row = one release. Every figure you quote is over releases, never over handheld calls. Say the
  grain in every answer: "across 1,375 releases today".
- Zero-pick (picked == 0, a stock-out) and partial (short but picked > 0) are different things. Never
  add them into one "short" number; report them apart. A ratio is computed from numbers returned by
  ONE call (for a rate, make one call for the numerator filter and read the denominator from the
  unfiltered call in the same window).
- "Top N shorted products" means group_by ["item_number", "lookup:item description.ItemDescription"],
  where ["shortfall<0"], sort "shortfall", dir "asc" (most negative first), limit N; units short is
  -shortfall. Always pass sort for any "top", "most", "biggest", "longest" question; the groups come
  back in that order, keep it. Use total_rows from the result as the grain, never add groups up.
  "This week" is the last 7 days unless the person says otherwise. Default window is the last 24
  hours; say the window you used. For "today" or "yesterday" do not compute start and end: filter
  with where ["business_date==YYYY-MM-DD"] using the date line at the top of the question (yesterday
  is that date minus one day) and leave start and end out.
- Speed (duration_s) depends on the transaction: milk lines run about 16 s, freezer about 130 s.
  Compare pickers within a transaction, never across, and say so.
- NEVER invent a name, a number or a row. Every figure in your answer must come from a tool result
  in this conversation. If the data call failed and you cannot fix it, say what failed and stop;
  a short honest "I could not get that" is the right answer. Customer name and number are lookups:
  group_by ["lookup:delivery.customer_name"] (or customer_number); they are not fields.
- Answer in plain sentences, short, with the numbers, then a compact table if there are several rows.
  Cite the release key (reporting number) when you explain one release.
"""


def make_model(model_name: str | None = None) -> BaseChatModel:
    """The chat model for `settings.analytics_agent_model`, whichever provider the string names."""
    name = model_name or settings.analytics_agent_model
    kwargs: dict[str, Any] = {}
    if name.startswith("ollama:"):
        kwargs["base_url"] = settings.ollama_base_url
        kwargs["reasoning"] = settings.analytics_agent_think
        kwargs["num_ctx"] = settings.analytics_agent_context_tokens
        kwargs["temperature"] = 0  # a small model invents less at zero, and a run can be repeated
    return init_chat_model(name, **kwargs)


def _history_as_messages(history: list[dict] | None) -> list[BaseMessage]:
    out: list[BaseMessage] = []
    for turn in history or []:
        content = str(turn.get("content") or "").strip()
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


class AnalyticsAgent:
    """Runs the tool loop for one question against a request-scoped session and tenant."""

    def __init__(self, db: AsyncSession, customer_code: str, *, model: BaseChatModel | None = None):
        self.db = db
        self.customer_code = customer_code  # every tool is hard-scoped to this tenant
        self.model = model or make_model()
        self.model_name = getattr(model, "model_name", None) or settings.analytics_agent_model

    async def ask(self, question: str, history: list[dict] | None = None) -> dict:
        """Answer one question. Returns the final text plus a trace of the tool calls made.

        `history` is optional prior turns of the same conversation, oldest first, as
        {"role": "user"|"assistant", "content": str}. Only plain text is replayed."""
        graph = create_agent(self.model, build_tools(self.db, self.customer_code), system_prompt=SYSTEM_PROMPT)
        today = date.today().isoformat()
        messages = _history_as_messages(history) + [HumanMessage(content=f"(Today is {today}.)\n\n{question}")]
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
        answer = _text(final) if final is not None else ""
        if stop_reason == "max_iterations" and not answer:
            answer = ("I could not settle on an answer within the allowed number of tool calls. "
                      "Ask a narrower question, or name the field or window you want.")
        trace = tool_trace(seen)
        for t in trace:
            logger.info("agent tool %s %s -> %s", t["tool"], _short(t["input"]), t["result_preview"][:160])
        fabricated = _looks_fabricated(answer, trace)
        if fabricated:
            logger.warning("agent answer withheld for %s: figures with no successful data call: %s",
                           self.customer_code, answer[:200])
            answer = fabricated
            stop_reason = "withheld"
        return {"answer": answer, "stop_reason": stop_reason,
                "tool_calls": [{"tool": t["tool"], "input": t["input"]} for t in trace],
                "iterations": len(trace), "model": self.model_name}


#: Tools that describe the schema rather than read data. A numeric answer resting only on these
#: was made up.
SCHEMA_TOOLS = frozenset({"describe_releases", "list_metrics", "explain_freshness"})


def _looks_fabricated(answer: str, trace: list[dict]) -> str | None:
    """A guard the model cannot talk its way past. On the first live Teams run an 8B model was
    refused for a field that does not exist, and instead of retrying with the lookup it answered
    with "Customer A … Customer J" and round numbers, every one invented. An answer that carries
    figures while no data tool returned a result without an error is replaced by an honest one."""
    if not any(ch.isdigit() for ch in answer):
        return None
    failed = lambda t: t.get("result", t["result_preview"]).lstrip().startswith('{"error"')  # noqa: E731
    if any(t["tool"] not in SCHEMA_TOOLS and not failed(t) for t in trace):
        return None
    problems = [t.get("result", t["result_preview"]) for t in trace if failed(t)]
    why = ""
    if problems:
        try:
            import json
            last = json.loads(problems[-1])
            why = " " + "; ".join(last.get("problems") or [last.get("error", "")])
        except Exception:  # noqa: BLE001 - the preview may be cut mid-JSON
            why = ""
    return ("I could not get the data for that question, so I have no figures to give." + why +
            " Ask again naming the field or window, or ask me to describe what is available.")


def _short(value: Any, n: int = 200) -> str:
    import json
    text = json.dumps(value, default=str)
    return text if len(text) <= n else text[:n] + "…"


def _text(message: AIMessage) -> str:
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
