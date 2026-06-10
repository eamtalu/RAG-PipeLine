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
guessing. Be concise and concrete."""


class LogDebugAgent:
    """Runs the tool-use loop for a single question against a request-scoped DB session."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key or None)

    async def ask(self, question: str) -> dict:
        """Answer one question. Returns the final text plus a trace of tool calls made."""
        if not settings.anthropic_api_key:
            raise RuntimeError(
                "anthropic_api_key is not configured — set it in .env to use the debugging agent."
            )

        today = date.today().isoformat()
        messages: list[dict] = [
            {"role": "user", "content": f"(Today is {today}.)\n\n{question}"}
        ]
        tool_calls: list[dict] = []

        for _ in range(settings.log_agent_max_iterations):
            response = await self.client.messages.create(
                model=settings.log_agent_model,
                max_tokens=settings.log_agent_max_tokens,
                thinking={"type": "adaptive"},
                system=_SYSTEM_PROMPT,
                tools=TOOLS,
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
                result_json = await execute_tool(block.name, block.input, self.db)
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
            system=_SYSTEM_PROMPT,
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


def get_log_debug_agent(db: AsyncSession = Depends(get_session)) -> LogDebugAgent:
    """FastAPI dependency — one agent bound to the request's DB session."""
    return LogDebugAgent(db)
