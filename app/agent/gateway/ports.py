"""What the gateway needs from the outside, as roles; the runtime is the first."""

from __future__ import annotations

from typing import Protocol, Sequence

from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext


class AgentRuntime(Protocol):
    async def run(
        self, context: ExecutionContext, request: AgentRequest, prior_turns: Sequence[dict] = ()
    ) -> AgentResult:
        """Answer the request under the context; prior turns are the channel's, oldest first."""
        ...
