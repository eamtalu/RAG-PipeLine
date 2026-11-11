# The guard pipeline and the stored trace

Since 10 November 2026 (step 14 of `docs/esmart-agent-execution-plan.md`) the four guards run as a pipeline the profile lists by name, and every run leaves a redacted record of what it did in `agent_traces`.
Nothing a person sees changed: both profiles list today's four guards in today's order, and the answers, declines and the legacy result dict are byte for byte what they were.

## The pipeline (`app/agent/runtime/pipeline.py`)

| stage | guard | what it does | verdict |
| --- | --- | --- | --- |
| pre | `manipulation` | declines, in code, an attempt to change the rules, see the prompt or reach another tenant (`looks_like_manipulation`) | `Decline("manipulation", prefix + decline)` |
| pre | `topic_gate` | one short model call: declines a question that is not about the agent's data (`TopicGate`) | `Decline("off_topic", decline)` |
| post | `fabrication` | figures with no successful data call behind them are replaced by an honest answer (`looks_fabricated`); a draft that called no tool at all may carry no date, time or share the instructions lack (`evidence.stale_markers`, chunk 170) | `Problem("fabricated", fallback=...)` |
| post | `ungrounded` | every figure in the draft must be in a tool result, or in the instructions when no tool returned anything | `Problem("ungrounded", figures=...)` |

A pre-guard looks at the question and the earlier turns before any model call; a post-guard looks at a `Draft` (the answer, the tool trace behind it, the question, the given text, the schema tools).
The profile's list is the run order, and the first decline or problem of a stage wins: the later guards of that stage do not run.
The runtime answers a problem on the first draft with one nudged redraft and a problem on the second with a withheld answer, exactly as before; the pipeline decides and never loops.
A test model injected without a gate makes `topic_gate` a skipped guard, recorded as such, as the runtime always did.

Secret redaction is not a guard a profile can list.
`redact_secrets` runs on every tool result before the model reads it and on every answer whatever the profile says, because a profile must not be able to switch a safety invariant off.

## The profile key

`guards: [manipulation, topic_gate, fabrication, ungrounded]` in both `analytics.yaml` and `logspace.yaml`.
The loader wants a non-empty list of unique names; a name no guard has fails when `GroundedAgent` is built, the same rule as namespaces.
A profile that lists fewer guards gets fewer: a profile with only `manipulation` lets an unbacked figure through, which a test holds so the list really governs.

## The trace (`app/agent/runtime/trace.py`)

`RunTrace` is the collector one run owns, created in `GroundedAgent.run` before the deadline wrapper so a timed-out run keeps what it collected.
The registry's executor records every tool call on it (namespace, name, arguments, raw result, ms, ok); the pipeline records every verdict (name, stage, verdict, detail, ms); the loop records every model call with the tokens the provider reported.
`finish` turns it into the frozen `TraceRecord` (`app/agent/contracts.py`) once, and that is the redaction step: secrets replaced in the question, the answer, the arguments and the results; a result capped at 2000 characters with `...[cut]`; the arguments of a `sensitive_args` tool replaced by `{"sha256": ...}`.
Finishing twice gives an equal record (the first call fixes the end time), and redacting a finished record changes nothing.
`AgentResult.trace` carries the record on every path, ok, declined and timed out; `AgentResult.usage` carries the loop's `model_calls`, `input_tokens` and `output_tokens`.
Neither is in the legacy dict the channels return.

The topic gate's own model call is not in `usage`; it is one short call per run, visible on the `topic_gate` verdict's `ms`.

## The store

The gateway stores through the `TraceStore` port (`app/agent/gateway/ports.py`): `save(row)` takes the plain dict of `TraceRecord.as_row()`, so `PostgresTraceStore` (`app/services/agent_traces/store.py`) imports nothing from the agent package and the layout rule holds.
It writes each row in its own short session and lets the unique `trace_id` absorb a repeat.
`agent_traces` (migration `a7c3e9d41b02`) carries `customer_code` as the soft tenant key like `agent_runs`, indexed with `started_at` and with `request_id`; a tenant purge deletes its rows.
The trace id is fresh per run, so a request retried after a stale claim keeps both runs under one `request_id`.

Who passes the store: `app/teams_consumer.py` builds one at module level beside its registry; `app/api/deps.py` has a cached `trace_store()` for both ask endpoints.
A store that fails is logged as `agent.trace_store_failed` and never changes the answer; a runtime that raises leaves an error row (`status=error`, `error="Type: message"`) and the error reaches the channel as before.

## What later steps add

Retention sweeps of old traces, per-model-call timing (a callback handler), confirmation before a side-effecting tool, roles on the principal.
