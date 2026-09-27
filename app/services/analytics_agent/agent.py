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
  name is real. If a tool returns "problems", fix the spelling and call again; do not apologise.
- One row = one release. Every figure you quote is over releases, never over handheld calls. Say the
  grain in every answer: "across 1,375 releases today".
- Zero-pick (picked == 0, a stock-out) and partial (short but picked > 0) are different things. Never
  add them into one "short" number; report them apart. A ratio is computed from numbers returned by
  ONE call (for a rate, make one call for the numerator filter and read the denominator from the
  unfiltered call in the same window).
- "Top N shorted products" means group_by item_number with the item description lookup, where
  shortfall<0, sorted by the sum of shortfall ascending (most negative first); units short is
  -shortfall. "This week" is the last 7 days unless the person says otherwise. Default window is
  the last 24 hours; say the window you used.
- Speed (duration_s) depends on the transaction: milk lines run about 16 s, freezer about 130 s.
  Compare pickers within a transaction, never across, and say so.
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
                              "result_preview": text[:200]})
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
        state = await graph.ainvoke({"messages": messages},
                                    config={"recursion_limit": settings.analytics_agent_max_iterations * 2 + 1})
        out: list[BaseMessage] = state["messages"]
        final = next((m for m in reversed(out) if isinstance(m, AIMessage) and not m.tool_calls), None)
        answer = _text(final) if final is not None else ""
        trace = tool_trace(out)
        return {"answer": answer, "stop_reason": "end_turn" if answer else "max_iterations",
                "tool_calls": [{"tool": t["tool"], "input": t["input"]} for t in trace],
                "iterations": len(trace), "model": self.model_name}


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
