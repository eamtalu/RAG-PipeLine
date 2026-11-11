"""What the gateway needs from the outside, as roles: the runtime and the three stores."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence

from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext


class AgentRuntime(Protocol):
    async def run(
        self,
        context: ExecutionContext,
        request: AgentRequest,
        prior_turns: Sequence[dict] = (),
        *,
        trace: Any = None,
    ) -> AgentResult:
        """Answer the request under the context; prior turns are the channel's, oldest first."""
        ...


class TraceStore(Protocol):
    async def save(self, row: Mapping[str, Any]) -> None:
        """Keep one run's record (`TraceRecord.as_row()`); a repeated trace id is a no-op."""
        ...


class Admission(Protocol):
    async def admit(
        self, *, tenant: str, user_id: str, trace_id: str, deadline_at: datetime, now: datetime
    ) -> str | None:
        """None when the run may start, else the limit it would exceed; admitted runs count."""
        ...

    async def release(self, *, tenant: str, trace_id: str) -> None:
        """Free the run's in-flight slot; a run never admitted is a no-op."""
        ...


class UsageStore(Protocol):
    async def add(self, rows: Sequence[Mapping[str, Any]]) -> None:
        """Add each row's counters into its (tenant, day, provider, model) row, creating it."""
        ...
