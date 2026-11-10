# The agent gateway and its contracts

Since 10 November 2026 (step 11 of `docs/esmart-agent-execution-plan.md`) every run of the agent goes through one entry, `app.agent.gateway.run_request`, under one frozen `ExecutionContext` that the gateway builds and nothing else may build.
The three channels (the Teams consumer, `POST /analytics/agent/ask`, `POST /logs/debug/ask`) are adapters: they turn their input into an `AgentRequest` and the `AgentResult` back into the shape they always returned.
Nothing a person sees changed in this step.

## The contracts (`app/agent/contracts.py`)

| type | fields | notes |
| --- | --- | --- |
| `Principal` | `tenant`, `object_id`, `display_name`, `roles`, `kind` | the person by Entra object id, or a service caller (`service:<token name>`); roles arrive with the roles table |
| `AgentRequest` | `request_id`, `tenant`, `principal`, `profile`, `text`, `conversation_id`, `conversation_type`, `action`, `deadline_seconds` | no history field: the channel passes earlier turns separately, so a client can never put turns on the request |
| `RunSettings` | `model` (`ModelSettings`), `budgets` (`RunBudgets`), `public_base_url` | the gateway's snapshot of every setting a run needs |
| `ExecutionContext` | the request's identity plus `started_at`, `deadline_at`, `model`, `budgets`, `public_base_url`, `trace_id` | frozen, built once by `build_context`; the model never sees it |
| `AgentResult` | `answer`, `stop_reason`, `model`, `tool_calls`, `iterations`, `retries`, `evidence`, `evidence_data`, `withheld_figures`, `declined`, `request_id`, `trace_id`, `citations`, `usage` | `to_legacy_dict()` returns today's keys exactly; `status` is ok, declined or timeout |

## Who builds the context

`app/agent/gateway/context.py` is the only module under `app/agent` that reads settings (a layout test holds it to that).
`snapshot_settings()` reads the model name, the Ollama and Bedrock details, the token and iteration budgets, the run deadline and the public base url once; `build_context` turns a request and that snapshot into the context with an absolute deadline.
`run_request` snapshots when the channel did not, builds the context, binds the request id and tenant into the log context, runs the runtime and writes one `agent.run` line.

The runtime (`GroundedAgent.run`) refuses a context for another tenant, resolves the model and the topic gate from the context, keeps the deadline with `asyncio.wait_for` from `context.seconds_left()`, and returns an `AgentResult`.
A test replaces the settings object in every agent module with one that raises on any read and runs a question through both agents and all three adapters: nothing inside a run reads settings.

## The adapters

| channel | request id | principal | profile | earlier turns |
| --- | --- | --- | --- | --- |
| Teams consumer (`app/teams_consumer.py`) | the edge's job id | the sender's Entra object id and display name from the job | `analytics` | the conversation store, loaded by the consumer |
| `POST /analytics/agent/ask` | the bound `X-Request-ID` | the service token's name (`service:web`) until OIDC | `analytics` | the client's `history` field (transitional) |
| `POST /logs/debug/ask` | the bound `X-Request-ID` | the service token's name | `logspace` | the client's `history` field (transitional) |

## Transitional, by design

- The web sends its own earlier turns because it has no conversation store and no sign-in yet; the field is documented as transitional and goes when the web gets a store.
- `GroundedAgent.ask(question, history)` remains for the tests that still construct an agent directly; it builds a request with an anonymous principal and goes through the same `run_request`. It is removed in step 12 with the agent subclasses.
- The legacy Claude agent (`app/services/log_agent/agent.py`, selected by `TEAMS_AGENT=claude`) is not a runtime and still reads its own settings; retired in step 12.
- The logspace agent keeps `scope`, `tz_name` and `today` as constructor arguments, because `FeedScope` imports persistence models and may not enter `app/agent`; step 12 puts them on the request and the context as plain values.

## What the next steps add

Profiles as YAML and the tool registry (steps 12 and 13), the guards as a pipeline with stored traces (14), golden questions (15), admission and quotas in the gateway (16), the model router (17).
