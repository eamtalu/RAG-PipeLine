"""What the gateway needs from the outside, as roles: the runtime and the trace store."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence

from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext


class AgentRuntime(Protocol):
    async def run(
        self, context: ExecutionContext, request: AgentRequest, prior_turns: Sequence[dict] = ()
    ) -> AgentResult:
        """Answer the request under the context; prior turns are the channel's, oldest first."""
        ...


class TraceStore(Protocol):
    async def save(self, row: Mapping[str, Any]) -> None:
        """Keep one run's record (`TraceRecord.as_row()`); a repeated trace id is a no-op."""
        ...
