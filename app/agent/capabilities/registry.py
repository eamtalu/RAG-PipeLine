"""The registry: every spec under its namespace, and the offered set built per run."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Iterable, Mapping, Sequence

from app.agent.capabilities.spec import Namespace, ToolSet, ToolSpec
from app.agent.contracts import AgentRequest, ExecutionContext
from app.agent.runtime.guards import wrap_tool
from app.agent.runtime.trace import RunTrace

logger = logging.getLogger(__name__)


class ToolRegistry:
    """Holds every namespace; a profile reaches only the tools of the namespaces it lists."""

    def __init__(self, namespaces: Iterable[Namespace] = ()):
        self._namespaces: dict[str, Namespace] = {}
        for namespace in namespaces:
            self.register(namespace)

    def register(self, namespace: Namespace) -> None:
        """(a new name) -> registered ; (a name already registered) -> ValueError"""
        if namespace.name in self._namespaces:
            raise ValueError(f"namespace {namespace.name!r} is already registered")
        self._namespaces[namespace.name] = namespace

    def namespaces(self) -> tuple[str, ...]:
        return tuple(self._namespaces)

    def offered(self, selected: Sequence[str]) -> tuple[ToolSpec, ...]:
        """The specs a profile may use: its namespaces in its order, each namespace in spec order.

        (("logs", "analytics")) -> 17 specs ; (("nope",)) -> ValueError ;
        (("logs", "logspace")) -> ValueError, both expose get_transaction
        """
        # Step 1: Refuse what the profile cannot mean, so that a typo in a profile fails when the
        # runtime is built rather than as an empty tool list at the first question.
        unknown = [n for n in selected if n not in self._namespaces]
        if unknown:
            raise ValueError(
                f"unknown namespace(s) {unknown}; the namespaces are {list(self._namespaces)}"
            )
        if len(set(selected)) != len(selected):
            raise ValueError(f"a namespace is selected twice in {list(selected)}")
        # Step 2: Refuse two namespaces that expose one model-visible name, because the model
        # would call one of them and the runner could not know which.
        owner: dict[str, str] = {}
        specs: list[ToolSpec] = []
        for name in selected:
            for spec in self._namespaces[name].specs:
                if spec.name in owner:
                    raise ValueError(
                        f"tool {spec.name!r} is exposed by both {owner[spec.name]!r} and "
                        f"{name!r}; a profile cannot select both"
                    )
                owner[spec.name] = name
                specs.append(spec)
        return tuple(specs)

    def tool_text(self, selected: Sequence[str]) -> str:
        """The text the model is given for these namespaces: the non-empty texts, one per line.

        (("logs", "analytics")) -> the analytics text alone, logs has none
        """
        self.offered(selected)
        return "\n".join(
            self._namespaces[n].tool_text for n in selected if self._namespaces[n].tool_text
        )

    def bind(
        self,
        context: ExecutionContext,
        request: AgentRequest,
        selected: Sequence[str],
        *,
        trace: RunTrace | None = None,
    ) -> ToolSet:
        """The tools for one run, each wrapped with the executor, plus the text and the description.

        (context for acme, request, ("logs", "analytics")) -> ToolSet(17 tools, the text, None) ;
        with trace= -> every call is recorded on it
        """
        specs = self.offered(selected)
        # The first namespace that can describe its records does, before any model call, so a
        # refusal (a logspace request without a timezone) happens here and not mid-run.
        description = None
        for name in selected:
            namespace = self._namespaces[name]
            if namespace.describe is not None:
                description = namespace.describe(context, request)
                break
        tools = tuple(
            wrap_tool(
                spec.as_dict(),
                self._executor(self._namespaces[spec.namespace], spec, context, request, trace),
            )
            for spec in specs
        )
        return ToolSet(
            tools=tools, tool_text=self.tool_text(selected), scope_description=description
        )

    @staticmethod
    def _executor(
        namespace: Namespace,
        spec: ToolSpec,
        context: ExecutionContext,
        request: AgentRequest,
        trace: RunTrace | None,
    ):
        """The runner `wrap_tool` calls: tenant from the context, the spec's cap, one log line,
        one entry on the run's trace."""

        async def run(name: str, args: Mapping[str, Any]) -> str:
            started = time.monotonic()
            ok = False
            result = ""
            try:
                # Step 1: Join the tenant from the context, never from the arguments, so that
                # which tenant a tool reads for is never a model decision.
                call = namespace.runner(context, request, name, args)
                # Step 2: Cap the call when the spec says so, because a tool that hangs must
                # cost one failed call, not the whole run's deadline.
                if spec.timeout_seconds is not None:
                    try:
                        result = await asyncio.wait_for(call, timeout=spec.timeout_seconds)
                    except asyncio.TimeoutError:
                        result = json.dumps(
                            {
                                "error": f"{name} did not finish within "
                                f"{spec.timeout_seconds:g}s and was stopped"
                            }
                        )
                else:
                    result = await call
                ok = not result.lstrip().startswith('{"error"')
                return result
            except Exception as exc:  # noqa: BLE001 - recorded, then left to the loop
                result = json.dumps({"error": f"{type(exc).__name__}: {exc}"})
                raise
            finally:
                # Step 3: One line per call with its timing, so a slow or failing tool is visible
                # per tenant and per request without reading the whole trace.
                ms = int((time.monotonic() - started) * 1000)
                logger.info("agent.tool ns=%s name=%s ms=%d ok=%s", namespace.name, name, ms, ok)
                # Step 4: Record the raw call on the run's trace, because the record is redacted
                # and capped once, when the run finishes, not here.
                if trace is not None:
                    trace.record_tool(
                        namespace=namespace.name,
                        name=name,
                        args=args,
                        result=result,
                        ms=ms,
                        ok=ok,
                        sensitive_args=spec.sensitive_args,
                    )

        return run
