# Usage accounting and the failure sentences

Since 10 November 2026 (step 19 of `docs/esmart-agent-execution-plan.md`) every model call the agent makes is recorded where it passes, the provider guard, summed per tenant, UTC day, provider and model into `usage_daily` after each run, priced from a list the operator supplies, and shown on an admin endpoint.
The sentence a Teams user reads when a run fails now depends on what any provider said, not only Anthropic.
Answers do not change.

## Where a call is recorded

The provider guard (`app/agent/router/health.py`) reports every attempt to the run's `RunTrace` through `on_call`: provider, model, latency, outcome, and the tokens the provider reported (`usage_metadata`: input, output, and input read from the provider's cache).
The gate's call, every loop call, a failed call and the calls of an attempt that later fell back are all there; an injected test model goes through a `test` guard, so the accounting is one path.
`RunTrace.usage` sums them: `model_calls`, `input_tokens`, `output_tokens`, `cached_tokens`, and `by_model` with one row per provider and model (`calls`, `failed_calls`, tokens, `latency_ms`). It is on the `AgentResult`, in the stored trace row, and is what the accounting reads.

The gateway owns the `RunTrace` and hands it to the runtime, so a run that raised still has its tool calls and model calls: the error trace row holds them and the calls are accounted.

## `usage_daily`

One row per tenant, UTC day of the run's start, provider and model: `calls`, `failed_calls`, `input_tokens`, `output_tokens`, `cached_tokens`, `cost_usd`, `latency_ms`.
`PostgresUsageStore.add` (`app/services/agent_usage/store.py`) is one upsert per row with atomic increments, so every worker and web process adds into the same row; `cost_usd` becomes NULL for good once any call of the row ran on an unpriced model, because a partial sum would read as a real total.
Migration `c9e5a1b63d04`; a tenant purge deletes the rows.
A store that fails is logged as `agent.usage_store_failed` and never changes the answer.

## Prices

`ANALYTICS_AGENT_PRICES` is a JSON object, USD per million tokens by model string:

```
{"bedrock_converse:<model id>": {"input_per_million": 0.2, "output_per_million": 0.8, "cached_input_per_million": 0.05}}
```

`cached_input_per_million` is optional and defaults to the input price.
Prices are the operator's data, never guessed by the code: a model not listed is recorded with no cost and one `agent.cost.unpriced model=` line per run at info level.
Malformed JSON is a `ValueError` naming the setting at the first request.
`snapshot_settings` parses the list into `RunSettings.prices`, so nothing reads settings inside a run.

## Seeing it

`GET /api/v1/analytics/agent/usage?days=7` with an admin service token and `X-Customer-Code` returns the tenant's days newest first:

```
{"tenant": "acme", "since": "2026-11-04",
 "days": [{"day": "2026-11-10", "calls": 3, "failed_calls": 0, "input_tokens": 150, "output_tokens": 15,
           "cached_tokens": 0, "cost_usd": null, "latency_ms": 20,
           "models": [{"provider": "bedrock", "model": "...", "calls": 1, ..., "cost_usd": null}, ...]}]}
```

A day's `cost_usd` is null when any of its rows is unpriced.

## The failure sentence

`classify` (`app/services/model_failures.py`, re-exported as `app.agent.router.failures`) names a failed call: `throttled`, `server_error`, `timeout`, `auth` (401 or 403, Bedrock `AccessDeniedException`, `UnrecognizedClientException`, `ExpiredTokenException`, Anthropic authentication or permission errors), `billing` (a 400 whose message says credit, billing, funds, quota exceeded or payment), `unreachable` (connection and endpoint errors, which carry no status) or `other`.
It lives beside the services because the Teams consumer (`app/services/teams/consumer.py`) uses it too and the services side may not import the agent package.

`failure_message` maps the kind to the sentence: `auth` and `billing` (and the old missing-key error) say the model is unavailable and the team has been alerted; `throttled`, `server_error`, `timeout` and `unreachable` say the service is busy, try again in a minute; anything else is the generic sentence.
The gateway logs `agent.run_failed trace_id= kind= error=` so the provider's own word is in the log even when the sentence is generic.

## What later steps add

The capacity table (20); a daily cost budget as an admission rule, now that the data exists; tenant-local days; per-model prices shipped as a reviewed file rather than one setting.
