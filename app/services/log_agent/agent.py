"""The Claude tool-use debugging agent.

A manual async agentic loop: Claude is given the read-only tools in tools.py and a system
prompt describing the M3 WMS log model, then it picks tools, we run them against the DB,
feed results back, and repeat until it produces a final answer. A manual loop (rather than the
SDK tool runner) keeps the AsyncSession lifecycle explicit — every tool call runs against the
request-scoped session injected by FastAPI.

Model + thinking follow the claude-api skill defaults: claude-opus-4-8 with adaptive thinking.
"""

from datetime import date

import anthropic
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.api.deps import get_current_customer
from app.settings import settings
from app.services.log_agent.tools import TOOLS, execute_tool

_SYSTEM_PROMPT = """You are a debugging assistant for an Infor M3 WMS (.NET) warehouse system. \
You answer engineers' questions by querying a relational store derived from the server logs.

The data model:
- A `log_transaction` is one API request/response cycle (bracketed by REQUEST -> RESPONSE), \
named by its MethodName, keyed by a ReqID. It promotes groupable WMS dimensions (user, company, \
warehouse, division, device, item/delivery/order numbers, method, status, timing) to queryable \
columns.
- Its `status` is one of: success (clean), soft (M3 returned not-found / needs-a-value but the \
app coped, e.g. "Location does not exist"), error (a real ERROR-level failure, e.g. a printer \
error), or incomplete (REQUEST seen but RESPONSE not yet ingested).
- Each transaction has an ordered timeline of `log_entry` rows: the REQUEST, internal M3 MI \
calls (mi_program like MMS200MI + mi_transaction) and their results, SQL, errors, and the RESPONSE.

How to work:
- Use the tools to gather evidence before answering. Start broad (search/count/find_errors) to \
locate the relevant transaction(s), then call get_transaction to read the timeline and explain \
what actually happened.
- For "how many" questions use count_transactions. For failure triage use find_errors. For a \
question about a specific message/MI program/SQL use search_entries.
- Distinguish soft results from real errors — don't report a soft "location does not exist" as a \
system failure unless that's what the user is asking about.
- Always ground your answer in the data you retrieved and CITE the transaction id(s) (and ReqID \
where useful) you based it on. If the data doesn't contain the answer, say so plainly rather than \
guessing. Be concise and concrete.

Analytics (aggregate questions: how many, how much, per warehouse, per day, trends):
- Use list_metrics to see the registered metrics and their meaning, then query_metric. Do not add \
up transactions yourself when a metric answers the question; the metric is what the dashboard shows.
- Every analytics answer is TWO-TIER. Buckets before the analytics watermark come from settled \
rollups; `live_spans` were folded from the facts on the fly and are PROVISIONAL, because some of \
their transactions may still be open. Call explain_freshness and say how far behind analytics is \
and whether the tail is provisional whenever you quote a figure. A `distinct` measure is an \
estimate (about 1.6 percent) and must be described as one.
- Metrics with source "record" describe what M3 ANSWERED during a call, not what the warehouse \
consumed: `units-observed-by-item` sums the stock quantities M3 reported per item, which is a \
snapshot per response, not units picked. Transaction-source metrics such as `consumption` are the \
ones about movement."""


# Prompt caching: the system prompt and the tool definitions are identical on every call, and a
# question makes up to a dozen calls. Marking them cacheable means only the growing message list is
# billed at full input price after the first call. The marker goes on the LAST tool because the
# cache prefix runs tools -> system -> messages, so one breakpoint after the tools covers both.
_CACHED_SYSTEM: list[dict] = [
    {"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}},
]
_CACHED_TOOLS: list[dict] = [*TOOLS[:-1], {**TOOLS[-1], "cache_control": {"type": "ephemeral"}}]


def _history_as_messages(history: list[dict] | None) -> list[dict]:
    """Validate prior turns into alternating text messages. Drops anything malformed rather than
    letting one bad row make the whole question fail; the API rejects two consecutive same-role
    turns, so a same-role run is collapsed onto the earlier one."""
    out: list[dict] = []
    for turn in history or []:
        role = turn.get("role")
        content = (turn.get("content") or "").strip()
        if role not in ("user", "assistant") or not content:
            continue
        if out and out[-1]["role"] == role:
            out[-1]["content"] += "\n\n" + content
            continue
        out.append({"role": role, "content": content})
    # a history must start with the user and end with the assistant to precede the new question
    if out and out[0]["role"] != "user":
        out.pop(0)
    if out and out[-1]["role"] != "assistant":
        out.pop()
    return out


class LogDebugAgent:
    """Runs the tool-use loop for a single question against a request-scoped DB session."""

    def __init__(self, db: AsyncSession, customer_code: str):
        self.db = db
        self.customer_code = customer_code  # every tool call is hard-scoped to this tenant
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key or None,
                                               max_retries=settings.log_agent_max_retries)

    async def ask(self, question: str, history: list[dict] | None = None) -> dict:
        """Answer one question. Returns the final text plus a trace of tool calls made.

        `history` is optional prior turns of the same conversation, oldest first, as
        {"role": "user"|"assistant", "content": str}. The web endpoint passes none (one question, one
        answer, as before); the Teams consumer passes the last few so follow-ups carry context. Only
        plain text is replayed, never earlier tool calls, so the model re-reads the data fresh.
        """
        if not settings.anthropic_api_key:
            raise RuntimeError(
                "anthropic_api_key is not configured — set it in .env to use the debugging agent."
            )

        today = date.today().isoformat()
        messages: list[dict] = _history_as_messages(history) + [
            {"role": "user", "content": f"(Today is {today}.)\n\n{question}"}
        ]
        tool_calls: list[dict] = []

        for _ in range(settings.log_agent_max_iterations):
            response = await self.client.messages.create(
                model=settings.log_agent_model,
                max_tokens=settings.log_agent_max_tokens,
                thinking={"type": "adaptive"},
                system=_CACHED_SYSTEM,
                tools=_CACHED_TOOLS,
                messages=messages,
            )

            if response.stop_reason != "tool_use":
                answer = "".join(b.text for b in response.content if b.type == "text").strip()
                return {
                    "answer": answer,
                    "stop_reason": response.stop_reason,
                    "tool_calls": tool_calls,
                    "iterations": len(tool_calls),
                }

            # Preserve the assistant turn (thinking + tool_use blocks) verbatim, then run the tools.
            messages.append({"role": "assistant", "content": response.content})

            results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                result_json = await execute_tool(block.name, block.input, self.db, self.customer_code)
                tool_calls.append({"tool": block.name, "input": block.input})
                results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_json,
                })
            messages.append({"role": "user", "content": results})

        # Hit the iteration cap without a final answer — make one last call with tools off.
        final = await self.client.messages.create(
            model=settings.log_agent_model,
            max_tokens=settings.log_agent_max_tokens,
            system=_CACHED_SYSTEM,
            messages=messages + [{
                "role": "user",
                "content": "Stop investigating and answer now using what you already found. "
                           "Cite the transaction ids you relied on.",
            }],
        )
        answer = "".join(b.text for b in final.content if b.type == "text").strip()
        return {
            "answer": answer,
            "stop_reason": "max_iterations",
            "tool_calls": tool_calls,
            "iterations": len(tool_calls),
        }


def get_log_debug_agent(db: AsyncSession = Depends(get_session),
                        customer: str = Depends(get_current_customer)) -> LogDebugAgent:
    """FastAPI dependency — one agent bound to the request's DB session and tenant."""
    return LogDebugAgent(db, customer)
