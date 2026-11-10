# The tool registry

Since 10 November 2026 (step 13 of `docs/esmart-agent-execution-plan.md`) every tool the agent can call is a `ToolSpec` under a namespace in one `ToolRegistry` (`app/agent/capabilities/`).
A profile lists its namespaces, and the offered set for a run is built from that list alone.
Today's tools are registered unchanged: the same names, the same order per profile, the same schemas, and the same schema-and-recipe text the fabrication guard is given.

## The pieces (`app/agent/capabilities/`)

| type | what it is |
| --- | --- |
| `ToolSpec` | `namespace`, `name` (the model-visible name), `description`, `input_schema`, plus `side_effects`, `cost_class`, `timeout_seconds`, `sensitive_args` |
| `Namespace` | a family of tools with one `runner`, the exact `tool_text` the model is given for them, and an optional `describe` that says what the records are |
| `ToolRegistry` | `register`, `namespaces`, `offered`, `tool_text`, `bind` |
| `ToolSet` | what `bind` returns for one run: the bound tools, the text, the description |

A runner is `(context, request, name, args) -> str`: the tenant comes from the context, never from an argument the model produced, and the runner opens its own short session per call.
Only `timeout_seconds` is acted on in this step; `side_effects`, `cost_class` and `sensitive_args` are carried for the confirmation step, the budgets and the trace redaction to come.

## The three namespaces

| namespace | tools | text | built by |
| --- | --- | --- | --- |
| `logs` | the eight read-only log tools (`search_transactions` to `explain_freshness`) | none | `app/services/analytics_agent/namespaces.py` |
| `analytics` | the four release tools and the five at-risk tools | the schemas and recipes, as before | `app/services/analytics_agent/namespaces.py` |
| `logspace` | the ask box's six tools | the schemas | `app/services/logspace_agent/namespaces.py` |

The namespace builders live beside the tools because the tools read Postgres and the agent package may not import persistence.
`analytics.yaml` lists `[logs, analytics]`, `logspace.yaml` lists `[logspace]`.
`analytics` is one namespace rather than two dotted ones because its text is one JSON document; splitting it later is a data change that changes that text, not a change to the registry.

## The allow-list

- A profile reaches only the tools of the namespaces it lists; `bind` builds from `offered` and nothing else.
- An unknown namespace, a namespace listed twice, or two namespaces exposing the same model-visible name are refused. `logs` and `logspace` both expose `get_transaction` and `search_entries`, so no profile may select both.
- A profile whose `schema_tools` names a tool its namespaces do not offer is refused when the runtime is built, not at the first question.

## The executor

Every bound tool is wrapped once: the runner is called with the context, a `timeout_seconds` on the spec caps the call and turns a timeout into a tool error result the fabrication guard already treats as a failed call, and one line is written per call: `agent.tool ns=<namespace> name=<tool> ms=<duration> ok=<bool>`.
`ok` is false on an exception, a timeout, or a result that starts with `{"error"`.
No tool sets a timeout today.

## Who composes the registry

Each process composes its own registry with every namespace, so the profile's list and the clash rule mean the same everywhere: `app/teams_consumer.py` at module level, and `app/api/deps.py` `tool_registry()` for the two ask endpoints.
Tests compose one over a spy session factory with `tests/agent_helpers.py` `tool_registry(session_factory)`.

## What later steps add

Per-tenant enabled sources and health filtering of the offered set, confirmation before a side-effecting tool, the query connectors as new namespaces, and the dotted split of `analytics`.
