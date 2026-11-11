# The model router

Since 10 November 2026 (step 17 of `docs/esmart-agent-execution-plan.md`) a router sits between the settings and the runtime.
The profile names what it needs of a model, the settings name the candidates, the router keeps the candidates that can serve the needs and pins the first for the run, and when the pinned provider fails mid-run the run is tried once more from the start on the next candidate, inside the same deadline.
Answers do not change; only what happens when a provider is down.

## The profile names needs (`model` key)

`model: {tools: true, context_tokens: 16000, gate: cheap}` in both `analytics.yaml` and `logspace.yaml`.
Needs, not provider names, because the same profile runs on Ollama on a laptop and on Bedrock on the server.
`tools` says the model must call tools; `context_tokens` is the smallest context that holds the prompt and the schemas; `gate` is `cheap` when the topic gate may run on a cheaper model if one is configured, `primary` when it must run on the pinned model.
The loader (`app/agent/router/policy.py`, `ModelPolicy.from_dict`) refuses any other shape.

## The settings name candidates

| setting | meaning |
| --- | --- |
| `ANALYTICS_AGENT_MODEL` | the primary, as before |
| `ANALYTICS_AGENT_FALLBACK_MODELS` | comma-separated model strings tried in order when the pinned provider fails; empty by default |
| `ANALYTICS_AGENT_GATE_MODEL` | a cheaper model for the topic gate; empty means the pinned model |

`snapshot_settings` copies them into `ModelSettings.fallback_models` and `ModelSettings.gate_model`, so nothing in the router reads settings and the "no settings inside a run" test keeps holding.

## The adapters (`app/agent/router/providers.py`)

One adapter per prefix of the model string: `ollama`, `bedrock_converse` and `bedrock`, `anthropic`, `openai`.
Each says whether it calls tools (all do today), how wide its context is (Ollama: the configured `num_ctx`; the hosted providers: 128k until refined per model), and builds the chat model with the exact arguments `build_model` always passed (Ollama: base url, reasoning off, `num_ctx`, temperature 0; Bedrock: region, temperature 0, max tokens; Anthropic and OpenAI: none).
A prefix no adapter knows is never pinned.

## The pick (`app/agent/router/router.py`)

`ModelRouter.pick(policy, model_settings)` walks the primary and then the fallbacks, skips a candidate with no adapter or that cannot serve the needs (one `agent.router.skipped model= why=` line each), pins the first that qualifies and keeps the rest as fallbacks.
No usable candidate is a `ValueError` naming the needs, raised when the run starts.
The gate's model is the configured cheap one only when the profile says `cheap`, else the pinned model.
The breaker, the bulkhead and the tenant's budget are the checks steps 18 and 19 add here.

## The rerun (`GroundedAgent._attempts`)

The pinned model runs the whole run.
A provider exception (anything but a timeout or a cancellation) with a fallback left and time left reruns the run from the start on the next candidate, after one `agent.fallback from= to= error=` line; never mid-run, because switching providers inside a run would mix tool-call ids.
A second failure, or no fallback, propagates to the channel as before.
The whole sequence stays under the one deadline; a primary that hangs gives the timeout answer and no fallback is tried.
The result's `model` names the model that answered; the trace keeps the tool calls of every attempt; the usage counts the model calls that completed.
The gate's model is built once per run and shared by every attempt, so a fallback does not change who screened the question.

A test model injected into `GroundedAgent(model=...)` bypasses the router, as every existing test relies on.

## The provider guards (step 18)

Since step 18 every model call of a run, the gate's and the loop's, goes through a guard owned per provider (`ollama`, `bedrock`, `anthropic`, `openai`) and shared by every run of the process: the router is one object per process (`app/teams_consumer.py` `_ROUTER`, `app/api/deps.py` `model_router()`) and holds the guards in its `ProviderHealth`.
The wrap is `GuardedChatModel` (`app/agent/router/guarded.py`): LangChain's agent loop requires a `BaseChatModel` and calls `bind_tools` then `ainvoke`, so the bound runnable is wrapped too; the sync path delegates unguarded because the agent only runs async.

| guard | rule | setting (default) |
| --- | --- | --- |
| call timeout | each call runs under `asyncio.wait_for`; a call that hangs is cancelled and raises `ModelCallTimeout`, a plain exception the step 17 rerun treats as a provider failure while the run deadline still bounds the whole run | `ANALYTICS_AGENT_CALL_TIMEOUT_SECONDS` (60, under the 120 s run deadline) |
| retry | only a throttled call (HTTP 429, Bedrock `ThrottlingException`, a `RateLimit...` error) is tried once more, after half a second plus a jitter of up to one second; a server error, a timeout and anything else go straight to the rerun on the next candidate | none |
| breaker | every failed call counts and a success resets; after the threshold of consecutive failures the provider is open for the cool-down and `pick` skips it (`agent.router.skipped why=breaker open`); after the cool-down one call probes it, a success closes the breaker and a failure reopens it | `ANALYTICS_AGENT_BREAKER_FAILURES` (3), `ANALYTICS_AGENT_BREAKER_COOLDOWN_SECONDS` (30) |
| bulkhead | calls in flight per provider per process; a full bulkhead skips the provider at pick (`why=bulkhead full`) and makes a call already pinned wait for a slot within its call timeout | `ANALYTICS_AGENT_PROVIDER_BULKHEAD` (8) |

The numbers travel in the snapshot (`RunBudgets.call_timeout_seconds`, `RunSettings.resilience`), never in a settings read inside a run.
The guard is a counter rather than an `asyncio.Semaphore`, because a semaphore binds to the first event loop that uses it and the test suite runs one loop per test; the loop is single-threaded, so a counter changed between awaits is race-free.
`classify` (`app/agent/router/failures.py`) reads the HTTP status on the exception or its response, botocore's error code, and failing those the class name.
Log lines: `agent.model.retry provider= wait=`, `agent.model.failed provider= kind= error=`, `agent.breaker provider= state=`.
Since step 19 the guard also reports every attempt (provider, model, latency, outcome, tokens) to the run's trace through `on_call`, which is where the usage accounting starts (`docs/agent-usage-accounting.md`); an injected test model goes through a `test` guard so the accounting is one path.
`tests/test_agent_provider_resilience_chunk168.py` holds the documented result of a timeout, a 429 and a 500 with fake providers and an injected clock.

## Fake-provider tests

`tests/test_agent_model_router_chunk167.py`: the primary raises a server error and the fallback answers; a failure after a tool call reruns from the start; both failing propagates the second error; no fallback propagates the first; a hanging primary gives the timeout answer; the gate runs on the cheap model and the loop on the primary.

## What later steps add

The capacity table (20); per-model context sizes and an embeddings model in the policy when something embeds; breakers on the connectors' side.
