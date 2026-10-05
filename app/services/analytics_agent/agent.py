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

from langchain.chat_models import init_chat_model
from langchain_core.language_models import BaseChatModel
from langchain_core.tools import BaseTool

from app.services.agent_core import grounded
from app.services.agent_core.grounded import GroundedAgent
from app.services.analytics_agent import evidence
from app.services.analytics_agent import at_risk_tools
from app.services.analytics_agent.tools import RECIPES, RELEASE_TOOLS, build_tools
from app.settings import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are the analytics assistant for a warehouse picking operation run on an Infor M3 WMS.
You answer questions by calling tools that read this logspace's data. You never guess a number.

Three kinds of data, three kinds of tool:
- Pick-list RELEASES (settled rows, one row per release): describe_releases, aggregate_releases,
  list_releases, explain_release. Use these for anything about picking: lines, deliveries, pickers,
  customers, items, shortfalls, zero-picks, over-picks, refusals, how long a line took.
- DELIVERIES AT RISK and the VANS: describe_at_risk, at_risk_history, at_risk_board, at_risk_vans,
  explain_at_risk_delivery. Use these for anything about deliveries missing their van, being at
  risk, left behind, holding the van, vans running late, when a route's van is usually ready, or
  what happened to one delivery. Never answer these from the release tools.
- Raw LOG transactions and metrics: search_transactions, count_transactions, find_errors,
  get_transaction, search_entries, list_metrics, query_metric, explain_freshness. Use these for
  why one API call failed, what a transaction did, or a registered metric.

Rules for deliveries at risk:
- The clock is the VAN, not the WMS departure time: each route's van is usually ready at a learned
  time of day, hours before the 11:30 the WMS prints. Say times as the tools give them.
- Call describe_at_risk once before the first at_risk tool in a conversation; copy the matching
  recipe. Three words for a closed delivery: missed (the van went without it), held the van (on the
  van only after the van's usual time plus the allowance), fine. Three live tiers: Watch, At risk,
  Left behind. Keep the words; do not translate them into "late" or "delayed".
- Counts come from `counts` or `summary` in the result; quote them, never add rows up. Rows marked
  reconstructed were written from the logs after the fact; say so only if asked how the data was made.
- "This week" is the last 7 days, "this month" the first of the month to today; pass dates as
  YYYY-MM-DD, or day: "today" / "yesterday"; the tool resolves them on the warehouse's clock.

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


#: What this agent is for, for the topic gate, and its reply to anything else.
DOMAIN = ("questions about this warehouse's picking operation and its data: pick-list releases, lines, deliveries, "
          "pickers, customers, items, shortfalls, zero-picks, over-picks, refusals, timings, deliveries at risk of "
          "missing their van (missed, held the van, fine; watch, at risk, left behind), the vans and when each route's "
          "van is usually ready, and the handheld/M3 log transactions of this logspace (what a call did, why it failed).")
DECLINE = ("I can only help with this warehouse's picking, delivery and log data: for example lines, deliveries, pickers, "
           "shortfalls and zero-picks, which deliveries missed their van or held it, or why a handheld call failed. "
           "Try \"top 5 shorted items today\" or \"which deliveries were missed this week\".")


def make_model(model_name: str | None = None) -> BaseChatModel:
    """The chat model for `settings.analytics_agent_model`, whichever provider the string names."""
    return grounded.build_model(model_name or settings.analytics_agent_model, init_chat_model)


# The shared loop's pieces, under the names this module has always had.
_history_as_messages = grounded.history_as_messages
tool_trace = grounded.tool_trace
_text = grounded.text_of
_short = grounded.short

#: Tools that describe the schema rather than read data. A numeric answer resting only on these
#: was made up.
SCHEMA_TOOLS = frozenset({"describe_releases", "list_metrics", "explain_freshness", "describe_at_risk"})

#: The tool schemas and recipes as text: numbers in them were given to the model, not invented by it.
_TOOL_TEXT = json.dumps([RELEASE_TOOLS, RECIPES, at_risk_tools.AT_RISK_TOOLS, at_risk_tools.RECIPES], default=str)


def _looks_fabricated(answer: str, trace: list[dict]) -> str | None:
    return grounded.looks_fabricated(answer, trace, given=[SYSTEM_PROMPT, _TOOL_TEXT], schema_tools=SCHEMA_TOOLS)


def _page_link() -> str | None:
    """The pick releases page, when the app has a public address; a card can then open it."""
    base = (settings.app_public_base_url or "").rstrip("/")
    return f"{base}/matrix/releases" if base else None


def _at_risk_link() -> str | None:
    base = (settings.app_public_base_url or "").rstrip("/")
    return f"{base}/matrix/at-risk" if base else None


class AnalyticsAgent(GroundedAgent):
    """Picking releases and the logspace's log calls, for Teams and `/analytics/agent/ask`."""

    SYSTEM_PROMPT = SYSTEM_PROMPT
    RETRY_NUDGE = RETRY_NUDGE
    UNGROUNDED_NUDGE = UNGROUNDED_NUDGE
    SCHEMA_TOOLS = SCHEMA_TOOLS
    DOMAIN = DOMAIN
    DECLINE = DECLINE

    def make_model(self) -> BaseChatModel:
        return make_model()

    def tools(self) -> list[BaseTool]:
        return build_tools(self.db, self.customer_code)

    def tool_text(self) -> str:
        return _TOOL_TEXT

    def render_evidence(self, trace: list[dict]) -> str | None:
        return evidence.render(trace)

    def structured_evidence(self, trace: list[dict]) -> dict | None:
        return evidence.structured(trace, link=_page_link(), at_risk_link=_at_risk_link())
