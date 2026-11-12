"""What one run did, collected as it runs and finished into the record that is stored (step 14).

`RunTrace` is the mutable collector a run owns: every tool call as the executor ran it, every
guard verdict, every model call's usage. `finish` turns it into the frozen `TraceRecord`, and
that is the one redaction step before persistence: secrets replaced in the question, the answer,
the arguments and the results; a result capped; the arguments of a sensitive tool replaced by a
hash. Finishing twice gives an equal record, and redacting a finished record changes nothing.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from app.agent.contracts import AgentRequest, AgentResult, ExecutionContext, TraceRecord
from app.agent.profiles.version import code_version
from app.services.notifications.redact import redact_secrets, redact_value

RESULT_CAP = 2000
CAP_MARKER = "...[cut]"


@dataclass
class RunTrace:
    """The collector for one run; the runtime, the executor and the pipeline write into it."""

    tool_calls: list[dict] = field(default_factory=list)
    guards: list[dict] = field(default_factory=list)
    model_calls_made: list[dict] = field(default_factory=list)  # one per call, by the guard
    finished_at: datetime | None = None  # fixed by the first finish, so a repeat is equal
    #: Told of every event as it is recorded ("tool", "model_call", "guard" with a small dict),
    #: for a channel that shows progress (step 6); never affects the record, never raises out.
    listener: Callable[[str, dict], None] | None = field(default=None, compare=False, repr=False)

    def _emit(self, kind: str, data: dict) -> None:
        if self.listener is None:
            return
        try:
            self.listener(kind, data)
        except Exception:  # noqa: BLE001 - a progress listener must never break the run
            pass

    def record_tool(
        self,
        *,
        namespace: str,
        name: str,
        args: Mapping[str, Any],
        result: str,
        ms: int,
        ok: bool,
        sensitive_args: bool = False,
    ) -> None:
        self.tool_calls.append(
            {
                "namespace": namespace,
                "name": name,
                "args": dict(args),
                "result": result,
                "ms": int(ms),
                "ok": bool(ok),
                "sensitive_args": sensitive_args,
            }
        )
        self._emit("tool", {"namespace": namespace, "name": name, "ok": bool(ok)})

    def record_guard(
        self, *, name: str, stage: str, verdict: str, ms: int, detail: str | None = None
    ) -> None:
        self.guards.append(
            {"name": name, "stage": stage, "verdict": verdict, "detail": detail, "ms": int(ms)}
        )
        self._emit("guard", {"name": name, "stage": stage, "verdict": verdict})

    def record_model_call(
        self,
        *,
        provider: str,
        model: str,
        ms: int,
        ok: bool,
        usage: Mapping[str, Any] | None,
    ) -> None:
        """One provider call as the guard ran it, with the tokens the provider reported.

        (provider="ollama", model="ollama:x", ms=120, ok=True, usage={"input_tokens": 10,
        "output_tokens": 3, "input_token_details": {"cache_read": 4}}) -> one record
        """
        usage = usage or {}
        details = usage.get("input_token_details") or {}
        self.model_calls_made.append(
            {
                "provider": provider,
                "model": model,
                "ms": int(ms),
                "ok": bool(ok),
                "input_tokens": int(usage.get("input_tokens") or 0),
                "output_tokens": int(usage.get("output_tokens") or 0),
                "cached_tokens": int(details.get("cache_read") or 0),
            }
        )
        self._emit("model_call", {"provider": provider, "model": model, "ok": bool(ok)})

    @property
    def usage(self) -> dict[str, Any] | None:
        """The run's model usage summed, with one row per provider and model, or None.

        >>> RunTrace().usage is None
        True
        """
        if not self.model_calls_made:
            return None
        by_model: dict[tuple[str, str], dict[str, Any]] = {}
        for call in self.model_calls_made:
            row = by_model.setdefault(
                (call["provider"], call["model"]),
                {
                    "provider": call["provider"],
                    "model": call["model"],
                    "calls": 0,
                    "failed_calls": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cached_tokens": 0,
                    "latency_ms": 0,
                },
            )
            row["calls"] += 1
            row["failed_calls"] += 0 if call["ok"] else 1
            for key in ("input_tokens", "output_tokens", "cached_tokens"):
                row[key] += call[key]
            row["latency_ms"] += call["ms"]
        rows = list(by_model.values())
        return {
            "model_calls": len(self.model_calls_made),
            "input_tokens": sum(r["input_tokens"] for r in rows),
            "output_tokens": sum(r["output_tokens"] for r in rows),
            "cached_tokens": sum(r["cached_tokens"] for r in rows),
            "by_model": rows,
        }

    def finish(
        self,
        context: ExecutionContext,
        request: AgentRequest,
        result: AgentResult,
        *,
        finished_at: datetime | None = None,
    ) -> TraceRecord:
        """The stored record of this run: redacted and capped, from the context and the result.

        (context, request, result with 1 tool call) -> TraceRecord(status="ok", 1 tool call) ;
        called twice -> two equal records
        """
        # The run finishes once: the first call fixes the moment, so a second record is equal.
        self.finished_at = self.finished_at or finished_at or datetime.now(timezone.utc)
        return _record(
            context,
            request,
            status=result.status,
            stop_reason=result.stop_reason,
            declined=result.declined,
            model=result.model,
            profile_version=result.profile_version,
            code_version=result.code_version,
            iterations=result.iterations,
            retries=result.retries,
            answer=result.answer,
            withheld_figures=result.withheld_figures,
            guards=self.guards,
            tool_calls=self.tool_calls,
            usage=self.usage,
            error=None,
            finished_at=self.finished_at,
        )


def error_record(
    context: ExecutionContext,
    request: AgentRequest,
    error: BaseException,
    trace: RunTrace | None = None,
) -> TraceRecord:
    """The record of a run that raised before it could answer, with what it did until then.

    (context, request, RuntimeError("down")) -> TraceRecord(status="error",
    error="RuntimeError: down") ; with a trace -> its tool calls, verdicts and usage kept
    """
    trace = trace or RunTrace()
    return _record(
        context,
        request,
        status="error",
        stop_reason="error",
        declined=None,
        model=context.model.name,
        profile_version="",
        code_version=code_version(),
        iterations=len(trace.tool_calls),
        retries=0,
        answer="",
        withheld_figures=(),
        guards=trace.guards,
        tool_calls=trace.tool_calls,
        usage=trace.usage,
        error=f"{type(error).__name__}: {str(error)[:500]}",
        finished_at=None,
    )


def _record(
    context: ExecutionContext,
    request: AgentRequest,
    *,
    status: str,
    stop_reason: str,
    declined: str | None,
    model: str,
    profile_version: str,
    code_version: str,
    iterations: int,
    retries: int,
    answer: str,
    withheld_figures: tuple[str, ...],
    guards: list[dict],
    tool_calls: list[dict],
    usage: dict[str, int] | None,
    error: str | None,
    finished_at: datetime | None,
) -> TraceRecord:
    # Step 1: Fix the end of the run now, so that the duration covers everything up to the
    # record and two records of one run differ only by the clock.
    finished = finished_at or datetime.now(timezone.utc)
    duration_ms = max(0, int((finished - context.started_at).total_seconds() * 1000))
    # Step 2: Redact and cap here and nowhere else, because this is the one path to the store
    # and a secret that reaches a row is a secret in every backup of it.
    return TraceRecord(
        trace_id=context.trace_id,
        request_id=context.request_id,
        tenant=context.tenant,
        principal_id=context.principal.object_id,
        principal_kind=context.principal.kind,
        profile=context.profile,
        profile_version=profile_version,
        code_version=code_version,
        model=model,
        conversation_id=context.conversation_id,
        conversation_type=context.conversation_type,
        status=status,
        stop_reason=stop_reason,
        declined=declined,
        started_at=context.started_at,
        finished_at=finished,
        duration_ms=duration_ms,
        iterations=iterations,
        retries=retries,
        question=redact_secrets(request.text) or "",
        answer=redact_secrets(answer) or "",
        guards=tuple(dict(g) for g in guards),
        tool_calls=tuple(_stored_call(c) for c in tool_calls),
        withheld_figures=tuple(withheld_figures),
        usage=dict(usage) if usage is not None else None,
        error=error,
    )


def _stored_call(call: Mapping[str, Any]) -> dict[str, Any]:
    """One tool call as stored: args hashed when sensitive, result redacted and capped.

    >>> _stored_call({"namespace": "t", "name": "spy", "args": {"q": 1}, "result": "r",
    ...               "ms": 2, "ok": True, "sensitive_args": False})["args"]
    {'q': 1}
    """
    if call["sensitive_args"]:
        raw = json.dumps(dict(call["args"]), sort_keys=True, default=str).encode()
        args: dict[str, Any] = {"sha256": hashlib.sha256(raw).hexdigest()}
    else:
        args = redact_value(dict(call["args"]))
    result = redact_secrets(call["result"]) or ""
    if len(result) > RESULT_CAP:
        result = result[:RESULT_CAP] + CAP_MARKER
    return {
        "namespace": call["namespace"],
        "name": call["name"],
        "args": args,
        "result": result,
        "ms": call["ms"],
        "ok": call["ok"],
    }
