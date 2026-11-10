"""A tool as data, and a namespace as a family of tools with one runner."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Mapping

from langchain_core.tools import BaseTool

from app.agent.contracts import AgentRequest, ExecutionContext

#: Runs one tool call: the tenant is joined from the context inside, never taken from the args.
ToolRunner = Callable[[ExecutionContext, AgentRequest, str, Mapping[str, Any]], Awaitable[str]]
#: Says what the records a namespace reads are, for the question frame; may refuse the context.
Describer = Callable[[ExecutionContext, AgentRequest], "str | None"]

_NAMESPACE = re.compile(r"^[a-z][a-z0-9_.]*$")


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """One tool: its model-visible name and schema, plus what the executor needs to know."""

    namespace: str
    name: str  # the name the model sees; providers accept [a-zA-Z0-9_-]{1,64}
    description: str
    input_schema: Mapping[str, Any] = field(hash=False)
    side_effects: bool = False
    cost_class: Literal["read", "compute", "write"] = "read"
    timeout_seconds: float | None = None
    sensitive_args: bool = False

    @classmethod
    def from_dict(cls, namespace: str, raw: Mapping[str, Any]) -> ToolSpec:
        """The spec of a tool module's dict, unchanged.

        >>> ToolSpec.from_dict("t", {"name": "a", "description": "d", "input_schema": {}}).name
        'a'
        """
        return cls(
            namespace=namespace,
            name=raw["name"],
            description=raw["description"],
            input_schema=raw["input_schema"],
        )

    def as_dict(self) -> dict:
        """The three keys `wrap_tool` reads, the shape the tool modules have always used."""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
        }


@dataclass(frozen=True, slots=True)
class Namespace:
    """A family of tools that share one runner, plus the exact text the model is given for them."""

    name: str
    specs: tuple[ToolSpec, ...]
    runner: ToolRunner
    tool_text: str = ""
    describe: Describer | None = None

    def __post_init__(self) -> None:
        if not _NAMESPACE.match(self.name):
            raise ValueError(f"namespace name {self.name!r} must match {_NAMESPACE.pattern}")
        seen: set[str] = set()
        for spec in self.specs:
            if spec.namespace != self.name:
                raise ValueError(
                    f"tool {spec.name!r} belongs to {spec.namespace!r}, not {self.name!r}"
                )
            if spec.name in seen:
                raise ValueError(f"namespace {self.name!r} lists the tool {spec.name!r} twice")
            seen.add(spec.name)


@dataclass(frozen=True, slots=True)
class ToolSet:
    """The bound tools for one run, their schemas as text, and what the records are."""

    tools: tuple[BaseTool, ...]
    tool_text: (
        str  # schemas and recipes as text: numbers in them were given to the model, not invented
    )
    scope_description: str | None = None
