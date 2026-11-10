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

## Fake-provider tests

`tests/test_agent_model_router_chunk167.py`: the primary raises a server error and the fallback answers; a failure after a tool call reruns from the start; both failing propagates the second error; no fallback propagates the first; a hanging primary gives the timeout answer; the gate runs on the cheap model and the loop on the primary.

## What later steps add

Retry on throttling, breakers and bulkheads per provider (18); per-attempt accounting and provider-aware failure sentences (19); per-model context sizes and an embeddings model in the policy when something embeds.
