# The agent gateway and its contracts

Since 10 November 2026 (step 11 of `docs/esmart-agent-execution-plan.md`) every run of the agent goes through one entry, `app.agent.gateway.run_request`, under one frozen `ExecutionContext` that the gateway builds and nothing else may build.
The three channels (the Teams consumer, `POST /analytics/agent/ask`, `POST /logs/debug/ask`) are adapters: they turn their input into an `AgentRequest` and the `AgentResult` back into the shape they always returned.
Nothing a person sees changed in this step.

## The contracts (`app/agent/contracts.py`)

| type | fields | notes |
| --- | --- | --- |
| `Principal` | `tenant`, `object_id`, `display_name`, `roles`, `kind` | the person by Entra object id, or a service caller (`service:<token name>`); roles arrive with the roles table |
| `AgentRequest` | `request_id`, `tenant`, `principal`, `profile`, `text`, `conversation_id`, `conversation_type`, `action`, `deadline_seconds`, `scope`, `tz_name`, `today` | no history field: the channel passes earlier turns separately, so a client can never put turns on the request; `scope` is the raw feed filter dict of a logspace question |
| `RunSettings` | `model` (`ModelSettings`), `budgets` (`RunBudgets`), `public_base_url` | the gateway's snapshot of every setting a run needs |
| `ExecutionContext` | the request's identity plus `started_at`, `deadline_at`, `model`, `budgets`, `public_base_url`, `trace_id`, `tz_name`, `today` | frozen, built once by `build_context`; the model never sees it; `today` is the server date unless the channel said which day the question is about |
| `AgentResult` | `answer`, `stop_reason`, `model`, `tool_calls`, `iterations`, `retries`, `evidence`, `evidence_data`, `withheld_figures`, `declined`, `request_id`, `trace_id`, `citations`, `usage`, `profile_version`, `code_version`, `trace` | `to_legacy_dict()` returns today's keys exactly; `status` is ok, declined or timeout; `usage` is the loop's model calls and tokens; `trace` is the run's `TraceRecord` |
| `TraceRecord` | the run's identity, versions, status, timing, question, answer, `guards`, `tool_calls`, `withheld_figures`, `usage`, `error` | built once per run by `RunTrace.finish`, redacted and capped; `as_row()` is what the store receives (see `docs/agent-guards-and-traces.md`) |

## Who builds the context

`app/agent/gateway/context.py` is the only module under `app/agent` that reads settings (a layout test holds it to that).
`snapshot_settings()` reads the model name, the Ollama and Bedrock details, the token and iteration budgets, the run deadline and the public base url once; `build_context` turns a request and that snapshot into the context with an absolute deadline.
`run_request` snapshots when the channel did not, builds the context, binds the request id and tenant into the log context, runs the runtime, writes one `agent.run` line (request id, trace id, both versions, status, tokens, seconds) and hands the run's redacted trace row to the `TraceStore` the channel passed, if any.
The trace id is fresh per run (`uuid4().hex`), not the request id, because a request retried after a stale claim must keep both runs; the row carries the request id beside it.
A store that fails is logged as `agent.trace_store_failed` and never changes the answer; a runtime that raises leaves an error row and the error reaches the channel as before.
Before the runtime runs, `run_request` admits the run through the `Admission` port when the channel passed one (`docs/agent-quotas.md`): a refusal is a declined result with a plain sentence and a trace row, and an admitted run releases its slot in a `finally`.

The runtime (`GroundedAgent.run`) refuses a context for another tenant, resolves the model and the topic gate from the context, keeps the deadline with `asyncio.wait_for` from `context.seconds_left()`, and returns an `AgentResult`.
A test replaces the settings object in every agent module with one that raises on any read and runs a question through both agents and all three adapters: nothing inside a run reads settings.

## The adapters

| channel | request id | principal | profile | earlier turns |
| --- | --- | --- | --- | --- |
| Teams consumer (`app/teams_consumer.py`) | the edge's job id | the sender's Entra object id and display name from the job | `analytics` | the conversation store, loaded by the consumer |
| `POST /analytics/agent/ask` | the bound `X-Request-ID` | the service token's name (`service:web`) until OIDC | `analytics` | the client's `history` field (transitional) |
| `POST /logs/debug/ask` | the bound `X-Request-ID` | the service token's name | `logspace`, with the feed's filters as `scope` and the tenant's `tz_name` and `today` | the client's `history` field (transitional) |

## Transitional, by design

- The web sends its own earlier turns because it has no conversation store and no sign-in yet; the field is documented as transitional and goes when the web gets a store.
- Each channel names only the profile (`GroundedAgent(customer, profile=load_profile(...), registry=...)`) over the registry and the trace store it composed at start (`app/teams_consumer.py` at module level, `app/api/deps.py` `tool_registry()` and `trace_store()`); the profile lists its namespaces and its guards. See `docs/agent-profiles.md`, `docs/agent-tool-registry.md` and `docs/agent-guards-and-traces.md`.

## What the next steps add

the model router (17), then stage C's resilience steps.
