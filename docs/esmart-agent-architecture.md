# eSmart Agent: architecture for a multi-tenant agentic framework

Date: 2026-11-09, revised for clarity after an independent review and a second pass against the writing rules.
Scope: architecture only; no code is started by this document.
Visual: `docs/esmart-agent-architecture.html` in this repo (open in a browser), also published at https://claude.ai/artifact/MTUEwpW7iVVaDH3QqkT12h.
Related: the Teams edge repo `teams-agent-edge` (the Lambda in section 12) and the answer-blocks plan of 7 November (stage 7).

## 1. What this is for

eSmart Eye answers warehouse questions from Teams by running an agent over analytics data.
Today the agent, the ingestion and the analytics live in one backend repo, the API has no authentication, and the agent can only read the analytics database.
The goal is a framework in which the agent can grow for years: hundreds of concurrent users across many customer tenants, several kinds of data source including ones a customer brings, more than one model provider, memory of a user's preferences, and the resilience and security that a shared service needs.

Five facts about the current code shape the design.
The agent loop is LangChain `create_agent` with tool calling and no structured output, so typed output must come from tool calls, not from the model's prose.
The only typed output today, the evidence table, is already built in code from the tool trace; the design generalises that.
Tools hold one database session for the whole run on a pool of fifteen, which caps concurrency at about fifteen runs regardless of worker count.
The API and the queue path both trust a customer code sent by the caller, so tenancy is not enforced yet.
The SQS consumer has a heartbeat but no deadline and no idempotency, so a hung provider holds a slot forever and a redelivered message runs twice.

## 2. The shape

```
                      people and systems
   Teams tab   web app   REST client   scheduled jobs   (future: Slack, email)
       │          │           │              │
┌──────▼──────────▼───────────▼──────────────▼───────────────────────────────────┐
│ CHANNELS      translate in and out only                                        │
├────────────────────────────────────────────────────────────────────────────────┤
│ GATEWAY       who is calling, for which tenant, may they, have they already    │
├────────────────────────────────────────────────────────────────────────────────┤
│ RUNTIME       run one request under a deadline: profile, guards, loop, trace   │
│   services it composes: MODEL ROUTER · MEMORY · POLICY · PRESENTATION          │
├────────────────────────────────────────────────────────────────────────────────┤
│ CAPABILITIES  the tool registry and the connectors behind it                   │
├────────────────────────────────────────────────────────────────────────────────┤
│ DATA PLANES   analytics service · customer SQL and APIs · documents · agent    │
│               store                                                            │
└────────────────────────────────────────────────────────────────────────────────┘
   cross-cutting: identity and tenancy · secrets · observability · contracts
```

Dependencies point down and a layout test enforces it, so a change in a lower layer can never reach up and a new channel or connector cannot bypass the gateway.
The runtime is one shared, stateless program; everything that differs between tenants is configuration carried on the request.

## 3. Channels and gateway

A channel turns an outside event into an `AgentRequest` and an `AgentResult` back into the outside format.
Today's Teams SQS consumer and the web `/ask` route become two adapters with no logic of their own, so a third door (Slack, email) is one more adapter.

```
AgentRequest { request_id, tenant, principal, profile, text, conversation_id,
               conversation_type: personal | group | channel, action?, deadline }
AgentResult  { status, answer, blocks, evidence, citations, usage, trace_id, reason }
```

History always comes from the conversation store, never from the client.
A client that could send its own history could also plant instructions in it.

The gateway answers four questions before any model runs.

- Who is calling: a service token for the edge and internal jobs, an OIDC token for the web app.
- Which tenant: taken from the credential or the user-to-tenant table, never from a header, because a header is just a claim.
  On the queue path the backend re-resolves the customer from the Entra tenant id in its own binding table and refuses a mismatch.
- Who is the person: a `Principal{tenant, object_id, display_name, roles}` keyed by the Entra object id, which is stable, per tenant, and already in the Teams token.
  Roles (`viewer`, `operator`, `admin`) come from a backend table an admin edits.
  This was chosen over Entra group claims because it needs no setup inside each customer's tenant and keeps the audit in one place; a group-claim sync can feed the same table later.
- May they, and have they already: per-tenant and per-user counters for in-flight runs, hourly questions and daily cost; and an `agent_runs(tenant, request_id)` row claimed before the run.
  The row is what makes redelivery safe: a second delivery that finds the row `running` leaves the message alone, and one that finds it `done` re-posts the stored result instead of paying for the model again.

For Teams the refusal must happen before the edge enqueues, or the user polls for an answer that will never come.
So the tenant and user quotas are mirrored into the edge's DynamoDB the way bindings already are, and the edge gains a per-user hourly limit beside its per-tenant one.

## 4. Runtime

`AgentRuntime.run` does the same eight things for every request: claim the run, load the profile, assemble context, run the guards, run the tool loop under budgets, run the guards again, run presentation, persist and mark done.

A profile is data, not a class: a YAML file with the system prompt, the tool namespaces, the guard list, the model policy, the limits and the memory policy.
Profiles live in the repo so a prompt change is a reviewed commit, and the git hash is recorded on every trace so an answer can always be tied to the prompt that produced it.
Today's three agent subclasses become three files.

The tool loop stays LangChain `create_agent` behind one function; a second loop implementation would earn an interface, one does not.

Budgets are the runtime's safety: a wall-clock deadline shorter than the queue's visibility window so a hung provider frees its slot; iteration and token caps; and a tool-failure budget of two, because a model that retries a failing source ten times costs minutes and a slot for an answer it cannot give.

The trace is the unit of truth.
It records every model call and every tool call with timing, tokens and outcome.
Presentation and audit both read the trace, and nothing reads the model's prose for structure, which is why the output can be rendered safely and tested without a model.

Guards run before the model (manipulation patterns, topic gate) and after it (every figure in the answer or in a chart must appear in a tool result; secret redaction).
Both exist today and move unchanged into a pipeline a profile can pick from.

A regression harness keeps the agent honest as it evolves: a golden-question file per profile (question, required tool, forbidden figures, expected block kinds) runs in CI with a scripted model and nightly against the real router.

## 5. Capabilities and data sources

The model never touches data.
It calls typed tools; connectors do the reading under the tenant's credentials; code turns the results into the answer.

A tool is a `ToolSpec`: name, typed arguments, typed result, whether it has side effects, a cost class, a timeout, and whether its arguments are sensitive.
The registry holds every spec at import and builds the offered set per request: the profile's namespaces, filtered by the tenant's enabled sources, filtered again by health.
A source that fails its health check simply stops being offered, so the model is never handed a broken tool.

Two things are easy to confuse and must stay separate.
A connector kind is code: one package per source type (`analytics`, `sql`, `http_api`, `documents`; `nosql` later), each shipping the same five files: `client.py`, `catalogue.py`, `executor.py`, `tools.py`, `health.py`.
A connector instance is configuration: one row per tenant per source in `connector_configs` with its kind, its allow-list and a reference to its encrypted credential.
A customer adds a source through an admin endpoint with a test action that connects, introspects the catalogue and lets the admin prune the allow-list; nothing is deployed.

### One query model

Every structured source speaks one language, defined once in the contracts.
Tools, evidence, grounding and charts are written once against it, so a new source adds an executor and never a renderer.

```
DataQuery  { source, entity, measures: [{field, agg}], filters: [{field, op, value}],
             group_by: [field | {field, bucket}], sort, limit, window }
DataResult { columns: [{name, type, unit}], rows, totals, grain, truncated, provenance }
Catalogue  { source, entities: [{name, fields: [{name, type, unit, filterable, groupable}],
             measures, time_field}] }
```

Example.
The question "which three products were short most often last week" becomes:

```
DataQuery { source: "analytics", entity: "releases",
            measures: [{field: "units_short", agg: "sum"}],
            filters: [{field: "status", op: "eq", value: "short"}],
            group_by: ["item"], sort: [{field: "units_short", dir: "desc"}],
            limit: 3, window: {start: "2026-11-02", end: "2026-11-08"} }
```

The analytics executor sends it to `POST /analytics/query`; a SQL executor would compile the same object to a parameterised statement over allow-listed tables; an HTTP executor maps it to an allow-listed endpoint.
A connector that cannot express a clause says so in its catalogue, and the runtime rejects the query before the call instead of failing on it.
The catalogue is fetched per run with a sixty-second cache, so a field removed from a source disappears from what the model is offered within a minute.

Rules that hold for every executor: never free-form SQL or code from the model; read-only credentials; a statement timeout; row and byte caps; a circuit breaker per instance that opens within the run.

Documents are different.
Retrieval is `documents.search(query, filters, k)` over chunks in a table with a not-null tenant column and a tenant-led index, returning citations.
The existing document search is reused only after its tenant column and its broken pgvector call are fixed, and only when a document source is first needed.

The rule for a customer's own source: live state is a connector; anything to trend or forecast is ingested into the analytics service and reached through the analytics connector, because history and aggregation are what that service is for.

Actions are tools with side effects.
They are gated by role, confirmed by a form or a second message when the run has seen untrusted text, and written to the audit table.

## 6. Model router

A profile states what it needs; the router chooses the provider and model.

A `ModelPolicy` names a primary, fallbacks, a cheap model for the topic gate, and an embeddings model, each with the capabilities it requires such as tool calling and context size.
The router checks, in order: capability, circuit breaker closed, a free slot in the provider's bulkhead (a cap on in-flight calls so one slow provider cannot absorb every worker), and the tenant's daily budget.
It then pins the provider for the whole run.
If the provider fails mid-run, the run is retried once on the next fallback from the start, because switching providers inside a run would mix tool-call ids and price lists.

Every call records tokens in, out and cached, cost, and latency, summed per tenant per day.
Catalogue and tool results are cached per tenant and arguments for under a minute.
Answers are never cached: the data changes every minute and the same question means different things to users with different preferences.

Capacity is planned on the default Bedrock quotas in eu-west-2 and a 60 second p95.
A run is one gate call plus up to ten tool rounds, so a default quota in the low hundreds of requests per minute carries roughly ten to twenty concurrent runs.
Stage 2 measures the real number and files the quota increase.

## 7. Memory

Memory is explicit in version one, because inferred memory about employees is a behavioural profile that needs a lawful basis, a notice and a tenant switch before it exists.

- Conversation: the last turns per conversation, 30 days, replayed with tables stripped as today.
  In group chats the key includes the user, so one person's history is not replayed into another's question.
- User preferences: keyed by tenant and Entra object id; enum keys only (units, home site, default window, chart preference).
  Written only by a `preferences.set` tool after the page confirms, so a planted instruction cannot write them.
  Read only in personal conversations.
  Listed and clearable from the tab.
  Off by default per tenant.
- Tenant knowledge: glossary, site names, shift times, written by admins only.

A request without a `Principal` gets no memory at all.

## 8. Tenancy and scale

The harness is shared.
Every tenant runs through the same workers and the same code; what differs is the `TenantContext` built once per request: allowed namespaces, enabled sources, credentials, quotas, model policy.
No code reads global settings inside a run.
A tenant that needs guaranteed capacity gets a dedicated worker pool started with a tenant filter, same code, different processes.

Hundreds of concurrent users is a few dozen questions in flight.
A worker runs several async slots behind a plain semaphore; the pool scales by adding consumer processes.
Fairness is enforced at admission, before enqueue, not by bouncing queue messages between workers, because every bounce counts toward the dead-letter limit.
The real ceiling is the provider quota, which is why the capacity table in section 6 matters more than worker count.

Database rules: no session held across model calls; a session per tool call; explicit statement timeouts; and row-level security on the agent-store tables as a backstop, so a query that forgets the tenant filter returns nothing rather than everything.
Caches are keyed by tenant, profile version, catalogue hash and arguments.

## 9. Resilience

- Deadlines on the run, each model call, each tool call and the answer post.
- Retries with jitter only on throttling; tool retries only for idempotent reads.
- Circuit breakers and bulkheads per provider and per connector instance, effective within a run.
- Safe redelivery through the run row: a finished answer is re-posted, never recomputed.
- A degradation ladder, each step logged: full answer, then without blocks or evidence, then the fallback model, then a typed refusal with a reason.
- Tests with a fake provider for timeout, throttling and server error, a duplicate queue delivery and a slow connector; each must produce the documented result.

## 10. Security

Each control is tied to the attack it closes.

- Anyone on the network picks any tenant: authentication at the gateway and the tenant taken from the credential.
- A queue message names a customer it does not own: the binding check in section 3.
- Prompt injection through log text, documents or HTTP bodies: results arrive in a capped envelope; once a run has seen free text, no side-effecting tool executes without a confirmation; presentation strips URLs and phone numbers not on the tenant allow-list.
- A planted instruction tries to persist: preferences take enum keys only and tenant knowledge is admin-written.
- A user runs an action they should not: the role table, the actor taken from the `Principal` with the display name looked up, and an audit row.
- Credential exposure: per-tenant secrets encrypted with a data key that is itself encrypted by a master key (envelope encryption), fetched at use, never in prompts, logs, traces or queues; webhook URLs move into the same store.
- Cross-tenant leakage through caches or vectors: tenant-keyed caches; a not-null tenant column on the chunk table.
- Sensitive data in traces: redaction before persistence, argument hashes for sensitive tools, capped result text, ids and hashes only in audit.
- One user burns a tenant's budget: per-user limits at the edge and the gateway.

Mutual TLS and egress allow-lists come when workers run in containers.

## 11. Observability

JSON logs carry the request id, tenant, user hash, profile and an `area.action` key.
Metrics cover runs, latency percentiles, tokens and cost per tenant, tool errors per connector, breaker state, queue depth and age, cache hits.
One global dashboard and alerts on dead letters, open breakers and cost anomalies come first; tracing and per-tenant views later.

## 12. Deployment and layout

The agent stays a package in the backend repo, `app/agent/`, with the layout test guarding its imports; it is extracted to its own repo only when a second deployable consumer exists, because an extra service and a shared contracts package cost a small team more than they return today.
Contracts stay verbatim copies between the edge and the backend, the pattern that already works.

```
app/agent/
  channels/   gateway/   runtime/   router/   memory/
  capabilities/{registry.py, spec.py, envelope.py, connectors/*, actions/*}
  presentation/   policy/   secrets/   observability/   profiles/*.yaml
  contracts.py
```

Runtime pieces: the existing Lambda edge; the backend repo on the Matrix server with the API, ingestion workers and a pool of agent consumers; Postgres for analytics and the agent store; Bedrock in eu-west-2, Anthropic, Ollama on-prem and OpenAI embeddings behind the router.
Containers and autoscaling on queue depth come in the last stage.

## 13. Roadmap

Each stage ships on its own and has a test that says it is done.

1. Close the door.
   Authentication on the API and the queue binding check; a secrets store; request ids and JSON logs; the run deadline; a session per tool call; the run row; a per-user limit at the edge.
   Done when no unauthenticated call succeeds, a hung provider frees its slot, and a duplicate delivery never runs twice.
2. Runtime in process.
   Profiles as YAML, the registry, `AgentRequest` and `AgentResult`, the `Principal`, guards as a pipeline, trace redaction, with today's tools registered unchanged.
   Done when the layout test passes, Teams answers are unchanged and the golden questions are green.
3. Router and resilience.
   Pinned provider, rerun on fallback, breakers, bulkheads, accounting, the capacity table, the quota request, provider-aware failure sentences, fake-provider tests.
   Done when the chaos tests pass and cost per tenant is visible.
4. Query model.
   `DataQuery`, `DataResult` and `Catalogue` in the contracts; `POST /analytics/query` and `/catalog`; the HTTP executor; renderers rewritten once; the agent package no longer imports persistence.
   Done when the golden questions are green over HTTP and a SQL executor would need no renderer change.
5. Memory and tenancy.
   Explicit preferences with confirmation, tenant knowledge, the roles table, per-user admission, the quota mirror to the edge.
   Done when a busy user cannot starve a tenant, a user can view and clear memory, and group chats carry no preferences.
6. First customer source.
   Whatever kind the first customer has, with `connector_configs`, the test action and its catalogue; documents and the pgvector fix only when a document source is needed.
   Done when a source is added by configuration plus one connector package.
7. Presentation.
   Charts and forms as typed blocks, with grounding over chart numbers, per the blocks plan of 7 November.
8. Scale-out.
   Containers, autoscaled consumers, dashboards and alerts; dedicated pools and data regions on request; repo extraction if a second consumer exists.
   Done when a load test with hundreds of concurrent users stays within the agreed p95.

## 14. Decisions in one place

1. Shared runtime, per-tenant policy; dedicated pools on request.
2. The analytics service is the record for history; connectors are for live state.
3. No free-form SQL or code from the model.
4. One query model for every structured source; renderers written once.
5. The router picks and pins the provider; profiles name needs.
6. Memory version one is explicit, personal-conversation only, off by default per tenant.
7. Identity is the Entra object id with roles in a backend table.
8. Capacity assumes default Bedrock quotas and a 60 second p95 until measured.
9. Authentication and secrets come before any new data source.
10. One repo until a second consumer exists.
