"""What crosses the agent's boundary: who asks, what, what the run was given, what came back.

Frozen dataclasses, no web or database types, so a channel (the Teams consumer, a web route) and
the runtime agree on one shape (docs/esmart-agent-architecture.md, section 3). Freezing is
shallow: `AgentResult.evidence_data` and `ToolCall.input` are the plain dicts the renderers
build, kept as they are so the channels' payloads do not change.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Literal, Mapping

ConversationType = Literal["personal", "group", "channel"]
StopReason = Literal["end_turn", "max_iterations", "withheld", "declined", "timeout"]
Status = Literal["ok", "declined", "timeout"]
PrincipalKind = Literal["user", "service"]


@dataclass(frozen=True, slots=True)
class Principal:
    """The person or system behind a request, by Entra object id; roles come later."""

    tenant: str
    object_id: str
    display_name: str | None = None
    roles: tuple[str, ...] = ()
    kind: PrincipalKind = "user"

    @classmethod
    def for_service(cls, tenant: str, caller_name: str) -> Principal:
        """A system caller named by its service token, until the web has signed-in people.

        >>> Principal.for_service("acme", "web").object_id
        'service:web'
        """
        return cls(tenant=tenant, object_id=f"service:{caller_name}", kind="service")



@dataclass(frozen=True, slots=True)
class AgentRequest:
    """One question as a channel hands it over; it never carries history, the channel does.

    `scope` is the raw feed filter dict of a logspace question (shallow-frozen like evidence);
    `tz_name` and `today` are the channel's view of the day the question is about.
    """

    request_id: str
    tenant: str
    principal: Principal
    profile: str
    text: str
    conversation_id: str | None
    conversation_type: ConversationType = "personal"
    #: The site the page looked at when asking (step 8); the action tools pre-fill it.
    site_id: str | None = None
    action: str | None = None
    deadline_seconds: float | None = None
    scope: Mapping[str, Any] | None = None
    tz_name: str | None = None
    today: date | None = None

    def __post_init__(self) -> None:
        # An adapter bug must fail here, before any model is paid for, not as an empty question.
        for name in ("request_id", "tenant", "text"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"AgentRequest.{name} must not be blank")


@dataclass(frozen=True, slots=True)
class ModelSettings:
    """What building the chat model needs, snapshotted from settings before the run."""

    name: str
    ollama_base_url: str
    think: bool
    context_tokens: int
    bedrock_region: str
    max_tokens: int
    fallback_models: tuple[str, ...] = ()  # tried in order when the pinned provider fails
    gate_model: str = ""  # a cheaper model for the topic gate; empty = the pinned model


@dataclass(frozen=True, slots=True)
class RunBudgets:
    max_iterations: int
    run_timeout_seconds: float
    call_timeout_seconds: float = 60.0  # one model call; the run deadline covers them all


@dataclass(frozen=True, slots=True)
class ResilienceSettings:
    """The numbers of a provider's guard: calls in flight, failures to open, cool-down."""

    bulkhead: int = 8
    breaker_failures: int = 3
    breaker_cooldown_seconds: float = 30.0


@dataclass(frozen=True, slots=True)
class ModelPrice:
    """USD per million tokens; the cached input price defaults to the input price."""

    input_per_million: float
    output_per_million: float
    cached_input_per_million: float | None = None


@dataclass(frozen=True, slots=True)
class RunSettings:
    """The gateway's snapshot of every setting a run needs; the only place they are read."""

    model: ModelSettings
    budgets: RunBudgets
    public_base_url: str
    resilience: ResilienceSettings = ResilienceSettings()
    prices: Mapping[str, ModelPrice] = field(default_factory=dict)  # by model string


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Everything a run may know, fixed before it starts; the model never sees it."""

    request_id: str
    trace_id: str
    tenant: str
    principal: Principal
    profile: str
    conversation_id: str | None
    conversation_type: ConversationType
    started_at: datetime
    deadline_at: datetime
    model: ModelSettings
    budgets: RunBudgets
    public_base_url: str
    tz_name: str | None = None
    today: date = date.today()  # the builder always sets it; the default keeps the field optional
    resilience: ResilienceSettings = ResilienceSettings()

    def seconds_left(self, now: datetime | None = None) -> float:
        """How long the run may still take, never negative.

        (deadline 120 s ahead) -> 120.0 ; (deadline passed) -> 0.0
        """
        now = now or datetime.now(timezone.utc)
        return max(0.0, (self.deadline_at - now).total_seconds())


@dataclass(frozen=True, slots=True)
class ToolCall:
    tool: str
    input: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TraceRecord:
    """One run as stored: redacted and capped by `RunTrace.finish`, nothing else builds one."""

    trace_id: str
    request_id: str
    tenant: str
    principal_id: str
    principal_kind: str
    profile: str
    profile_version: str
    code_version: str
    model: str
    conversation_id: str | None
    conversation_type: str
    status: str  # ok | declined | timeout | error
    stop_reason: str
    declined: str | None
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    iterations: int
    retries: int
    question: str
    answer: str
    guards: tuple[Mapping[str, Any], ...]
    tool_calls: tuple[Mapping[str, Any], ...]
    withheld_figures: tuple[str, ...]
    usage: Mapping[str, int] | None
    error: str | None

    def as_row(self) -> dict[str, Any]:
        """The row a store writes: every field by name, the tuples as lists.

        (a record with two guards) -> {"trace_id": ..., "guards": [{...}, {...}], ...}
        """
        row = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        row["guards"] = [dict(g) for g in self.guards]
        row["tool_calls"] = [dict(t) for t in self.tool_calls]
        row["withheld_figures"] = list(self.withheld_figures)
        row["usage"] = dict(self.usage) if self.usage is not None else None
        return row


@dataclass(frozen=True, slots=True)
class AgentResult:
    """What a run produced; `to_legacy_dict` is the shape the channels have always read."""

    answer: str
    stop_reason: StopReason
    model: str
    tool_calls: tuple[ToolCall, ...] = ()
    iterations: int = 0
    retries: int = 0
    evidence: str | None = None
    evidence_data: Mapping[str, Any] | None = None
    withheld_figures: tuple[str, ...] = ()
    #: The typed blocks the presentation calls asked for (step 5), as wire-contract dicts.
    blocks: tuple[Mapping[str, Any], ...] = ()
    declined: str | None = None
    request_id: str = ""
    trace_id: str = ""
    citations: tuple = ()
    usage: Mapping[str, int] | None = None
    profile_version: str = ""  # hash of the profile file the run used
    code_version: str = ""  # short git hash of the running code
    trace: TraceRecord | None = None  # what the run did, redacted; the gateway stores it

    @property
    def status(self) -> Status:
        """Ok, declined or timeout: what a channel needs to know without the stop reason.

        >>> AgentResult(answer="", stop_reason="withheld", model="m").status
        'ok'
        """
        if self.stop_reason in ("declined", "timeout"):
            return self.stop_reason  # type: ignore[return-value]
        return "ok"

    def to_legacy_dict(self) -> dict:
        """Today's result dict, key for key, so the Teams card and the web JSON do not change.

        >>> list(AgentResult(answer="x", stop_reason="end_turn", model="m").to_legacy_dict())
        ['answer', 'stop_reason', 'tool_calls', 'iterations', 'retries', 'model', 'evidence', \
'evidence_data', 'withheld_figures', 'blocks']
        """
        out: dict[str, Any] = {
            "answer": self.answer,
            "stop_reason": self.stop_reason,
            "tool_calls": [{"tool": t.tool, "input": t.input} for t in self.tool_calls],
            "iterations": self.iterations,
            "retries": self.retries,
            "model": self.model,
            "evidence": self.evidence,
            "evidence_data": self.evidence_data,
            "withheld_figures": list(self.withheld_figures),
            "blocks": [dict(b) for b in self.blocks],
        }
        if self.declined is not None:
            out["declined"] = self.declined
        return out
