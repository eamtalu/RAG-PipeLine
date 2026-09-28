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

import json
import logging
import re
from datetime import date
from typing import Any

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langgraph.errors import GraphRecursionError
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_agent import evidence
from app.services.analytics_agent.tools import RECIPES, RELEASE_TOOLS, build_tools
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
  name is real. Its "recipes" are exact calls for common questions: copy the matching one. A trend
  over days is group_by ["day"]; "number of deliveries" is the deliveries count every aggregate
  returns. Do not use list_metrics or query_metric for picking questions. If a tool returns "problems", fix exactly what it names and call once more. If the
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
  where ["shortfall<0"], sort "units_short", limit N. "Top N customers by units short" is the same
  with group_by ["lookup:delivery.customer_name"]. Always pass sort for any "top", "most",
  "biggest", "longest" question; the groups come back in that order, keep it. The table in your
  answer is drawn from the rows for you: write one or two sentences and do not repeat the rows as a
  list. Use total_rows from the result as the grain, never add groups up.
  "This week" is the last 7 days unless the person says otherwise. Default window is the last 24
  hours; say the window you used. For "today", "yesterday" or one named date pass day: "today",
  day: "yesterday" or day: "YYYY-MM-DD" and leave start and end out; the tool resolves the day on
  the warehouse's clock and tells you which date it used.
- Speed (duration_s) depends on the transaction: milk lines run about 16 s, freezer about 130 s.
  Compare pickers within a transaction, never across, and say so.
- NEVER invent a name, a number or a row. Every figure in your answer must come from a tool result
  in this conversation. If the data call failed and you cannot fix it, say what failed and stop;
  a short honest "I could not get that" is the right answer. Customer name and number are lookups:
  group_by ["lookup:delivery.customer_name"] (or customer_number); they are not fields.
- Answer in plain sentences, short, with the numbers, then a compact table if there are several rows.
  Cite the release key (reporting number) when you explain one release.
- Never say something is absent ("no zero-picks", "no errors", "nobody was short") unless a call
  filtered for exactly that returned zero rows in this turn. Zero-picks are where ["picked==0"].
- Answer the latest question only. Figures in earlier turns are stale: an answer with figures needs
  a data call in THIS turn, even for "again", "a round up" or "summarise". "What is available" is
  answered from describe_releases in words, without figures.
"""

#: Appended to the question for the one retry a draft earns when it carries figures that no data
#: call in this turn produced (copied from an earlier turn, or invented).
RETRY_NUDGE = ("\n\n(Your draft answered with figures without calling a data tool in this turn, so it was "
               "discarded. Figures from earlier turns are stale. Call describe_releases, then "
               "aggregate_releases or list_releases, and answer only from what they return. If the "
               "question needs no data, answer without any figure. If it asks what is available or "
               "what you can do, describe the data in words from describe_releases, with no figure.)")

#: The retry for a draft whose figures no tool returned: the model did arithmetic across calls.
UNGROUNDED_NUDGE = ("\n\n(Your draft was discarded: these figures appear in no tool result: {figures}. "
                    "Never add, subtract or total numbers yourself. Quote only numbers exactly as a tool "
                    "returned them, or ask the tool for the total with one unfiltered call.)")


def make_model(model_name: str | None = None) -> BaseChatModel:
    """The chat model for `settings.analytics_agent_model`, whichever provider the string names."""
    name = model_name or settings.analytics_agent_model
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
    return init_chat_model(name, **kwargs)


_DATA_LINE = re.compile(r"^From the data:.*$", re.M)


def _history_as_messages(history: list[dict] | None) -> list[BaseMessage]:
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
        {"role": "user"|"assistant", "content": str}. Only plain text is replayed, without figures.
        A draft whose figures no data call produced is discarded and the question asked once more
        with a nudge; a second such draft is withheld."""
        graph = create_agent(self.model, build_tools(self.db, self.customer_code), system_prompt=SYSTEM_PROMPT)
        prior = _history_as_messages(history)
        asked = f"(Today is {date.today().isoformat()}.)\n\n{question}"
        retries = 0
        draft = await self._draft(graph, prior + [HumanMessage(content=asked)], question)
        if draft["problem"]:
            retries = 1
            nudge = (UNGROUNDED_NUDGE.format(figures=", ".join(draft["ungrounded"]))
                     if draft["problem"] == "ungrounded" else RETRY_NUDGE)
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
            table = evidence.render(trace)
            if table:
                answer = (evidence.strip_tables(answer) + "\n\n" + table).strip()
                data = evidence.structured(trace, link=_page_link())
        return {"answer": answer, "stop_reason": stop_reason,
                "tool_calls": [{"tool": t["tool"], "input": t["input"]} for t in trace],
                "iterations": len(trace), "retries": retries, "model": self.model_name,
                "evidence": table, "evidence_data": data, "withheld_figures": withheld_figures}

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
        answer = _text(final) if final is not None else ""
        if stop_reason == "max_iterations" and not answer:
            answer = ("I could not settle on an answer within the allowed number of tool calls. "
                      "Ask a narrower question, or name the field or window you want.")
        trace = tool_trace(seen)
        for t in trace:
            logger.info("agent tool %s %s -> %s", t["tool"], _short(t["input"]), t["result_preview"][:160])
        fallback = _looks_fabricated(answer, trace)
        if fallback:
            return {"answer": answer, "stop_reason": stop_reason, "trace": trace, "problem": "fabricated",
                    "fallback": fallback, "ungrounded": []}
        # Chunk 125: every figure must be in a tool result; a ranked table comes from the rows.
        # With no result at all (a description of what is available) the instructions are the pool.
        results = [t["result"] for t in trace if not t["result"].lstrip().startswith('{"error"')]
        ungrounded = evidence.ungrounded(answer, results or [SYSTEM_PROMPT, _TOOL_TEXT], question)
        return {"answer": answer, "stop_reason": stop_reason, "trace": trace,
                "problem": "ungrounded" if ungrounded else None, "fallback": None, "ungrounded": ungrounded}


def _page_link() -> str | None:
    """The pick releases page, when the app has a public address; a card can then open it."""
    base = (settings.app_public_base_url or "").rstrip("/")
    return f"{base}/matrix/releases" if base else None


#: Tools that describe the schema rather than read data. A numeric answer resting only on these
#: was made up.
SCHEMA_TOOLS = frozenset({"describe_releases", "list_metrics", "explain_freshness"})

#: The tool schemas and recipes as text: numbers in them were given to the model, not invented by it.
_TOOL_TEXT = json.dumps([RELEASE_TOOLS, RECIPES], default=str)


def _looks_fabricated(answer: str, trace: list[dict]) -> str | None:
    """A guard the model cannot talk its way past. On the first live Teams run an 8B model was
    refused for a field that does not exist, and instead of retrying with the lookup it answered
    with "Customer A … Customer J" and round numbers, every one invented. An answer that carries
    figures while no data tool returned a result without an error is replaced by an honest one."""
    # Dates, times, percentages and small counts ("one of 12 fields", "the last 24 hours") are not
    # data figures, nor is a number the instructions themselves contain ("duration_s>300", "about
    # 130 s"): a description of what is available quotes those. The same rule the grounding check
    # applies, with the instructions and the schema tools' results as the only pool.
    failed = lambda t: t.get("result", t["result_preview"]).lstrip().startswith('{"error"')  # noqa: E731
    given = [SYSTEM_PROMPT, _TOOL_TEXT] + [t.get("result", t["result_preview"]) for t in trace
                                          if t["tool"] in SCHEMA_TOOLS and not failed(t)]
    if not evidence.ungrounded(answer, given):
        return None
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
