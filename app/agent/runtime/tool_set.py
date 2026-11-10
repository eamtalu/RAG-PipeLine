"""What the loop needs from a profile's tools, and the role that builds them per run.

The tools themselves read the database, so their builders live outside this package
(app/services/<x>_agent/tool_set.py) and are handed to the runtime by the channel. Step 13's
registry replaces this seam with namespaces a profile selects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from langchain_core.tools import BaseTool

from app.agent.contracts import AgentRequest, ExecutionContext


@dataclass(frozen=True, slots=True)
class ToolSet:
    """The bound tools for one run, their schemas as text, and what the records are."""

    tools: tuple[BaseTool, ...]
    tool_text: (
        str  # schemas and recipes as text: numbers in them were given to the model, not invented
    )
    scope_description: str | None = None


class ToolSetBuilder(Protocol):
    name: str  # must equal the profile's tool_set

    def build(self, context: ExecutionContext, request: AgentRequest) -> ToolSet:
        """The tools for this tenant and request; a session is opened per call, never held."""
        ...
