# eSmart Agent: execution plan

Date: 2026-11-09.
Companion to `docs/esmart-agent-architecture.md`.
Decision: the separation is a package boundary inside this repo first (`app/agent/` with a layout test); a separate repository only when a second consumer exists.

## Rules for every step

- A failing test first, then the change, then green; the full suite stays green at every commit.
- One commit per step; the "done when" line is the commit's proof.
- No Teams-visible behaviour changes unless the step says so; the edge contract stays compatible at every point.
- No assumption about the code: every move is preceded by reading the importers and importees of what moves.
- Deploy points: after stage A (edge, then backend on the Matrix server), then after each stage.

## Baseline (recorded before step 1)

- Python 3.11.11 in `.venv`; `pytest` with `asyncio_mode = auto`, `testpaths = tests`, 153 test files.
- Tests need the local Postgres from `docker compose up -d postgres` (pgvector image, port 5432).
- No formatter or linter is configured in this repo.
- Agent code lives in `app/services/agent_core`, `analytics_agent`, `logspace_agent`, `log_agent`; the Teams consumer in `app/services/teams` and `app/teams_consumer.py`.
- Coupling that must be respected when the tool modules move (query model stage, not step 2): `app/services/teams/home_shape.py` and `home_snapshot.py` import `aggregate_releases` and `list_releases` from `app/services/analytics_agent/tools.py`.
  Those two functions are analytics reads used by the snapshot writer; when the tools leave `app/services`, these two go to an analytics module, not into the agent package.
- Every tool module (`analytics_agent/tools.py`, `at_risk_tools.py`, `logspace_agent/tools.py`, `log_agent/tools.py`, `log_agent/analytics_tools.py`) imports `app.persistence` models or repositories directly, and `log_agent/agent.py` imports `app.api.deps`.
  They are the data access layer today and stay outside `app/agent` until the query model replaces those reads.
- `agent_core/grounded.py` imports `analytics_agent.evidence`, and `guards.py` imports `notifications.redact`; both edges are inside what moves or are leaf utilities.

## Stage A: separate and close the door

| # | Step | Done when |
|---|---|---|
| 1 | Create `app/agent/` with a layout test: nothing under it imports `app.persistence`, `app.api`, `app.background` or `app.worker`; the analytics and Teams snapshot code never imports `app.agent` | the test passes on the empty package and fails on a planted bad import |
| 2 | Move the loop into the package with `git mv` and import rewrites only: `agent_core/grounded.py` and `guards.py` to `app/agent/runtime/`; `analytics_agent/evidence.py` and `logspace_agent/evidence.py` to `app/agent/presentation/`. The agent classes and every tool module stay in `app/services` for now, because the tools read the database through `app.persistence` directly and may not enter the package until the query model exists; the layout test lists them as transitional importers of `app.agent`, a list that must only shrink | every existing agent test passes unchanged; the layout test passes with the transitional list |
| 3 | A session per tool call: tools open a short session the way `services/teams/commands.py` does; the `db` constructor argument leaves `AnalyticsAgent` and `LogspaceAgent`; callers in `teams_consumer.py`, `api/v1/analytics_agent.py` and `api/v1/logs.py` updated | a run holds no session between model calls; a test runs 20 agent calls in parallel on the default pool |
| 4 | Run deadline: `asyncio.wait_for` around the whole run, shorter than the SQS visibility window; a typed timeout result and sentence | a fake provider that hangs yields the timeout answer and frees the slot |
| 5 | `agent_runs` table and a claim before the run; a finished run re-posts its stored result on redelivery | a duplicate SQS delivery never runs the model twice |
| 6 | Queue binding check: the consumer resolves the customer from `tenant_id` through `teams_tenant_bindings` and refuses a mismatch; `sender_object_id` required (contract v2, mirrored to the edge) | a job whose tenant does not own the customer is refused and logged |
| 7 | API authentication: a service token on `/api/v1`; `require_admin` made real; the customer header kept for the web app until OIDC | no unauthenticated call succeeds |
| 8 | Secrets store with envelope encryption for webhook URLs and connector credentials | the API returns no secret; rotation documented |
| 9 | Request ids, JSON logs, `area.action` keys across API, worker and consumer | every log line carries `request_id` and tenant |
| 10 | Edge: a per-user hourly limit beside the per-tenant one | edge test green; deployed |

## Stage B: the runtime takes shape, in process

| # | Step | Done when |
|---|---|---|
| 11 | `ExecutionContext`, `Principal`, `AgentRequest`, `AgentResult`; a gateway module builds the context; the consumer and `/ask` become adapters | channels build a context; no settings are read inside a run |
| 12 | Profiles as YAML for analytics and logspace; git hash on the trace | the three subclasses are deleted; answers unchanged |
| 13 | `ToolRegistry` and `ToolSpec`; existing tools registered under namespaces, unchanged | a profile selects by namespace; an allow-list test |
| 14 | Guards as a pipeline; trace redaction before persistence; `agent_traces` table | the guard list comes from the profile; traces stored |
| 15 | Golden-question harness per profile in CI with the scripted model | CI fails on a prompt regression |
| 16 | Shared atomic quotas: `quota_windows`, admission in the gateway, in-flight sweep | five workers enforce one limit in a concurrent test |

## Stage C: router and resilience

| # | Step | Done when |
|---|---|---|
| 17 | Provider adapters, `ModelPolicy`, the router: pin per run, rerun once on fallback | the profile names needs; fake-provider tests |
| 18 | Timeouts, retry on throttling only, breakers, bulkheads per provider | timeout, 429 and 500 tests give the documented result |
| 19 | Accounting to `usage_daily`; provider-aware failure sentences | cost per tenant per day visible |
| 20 | Capacity table measured; Bedrock quota increase filed | the measured concurrent-run ceiling is written down |

Later stages follow the roadmap in the architecture document: the query model and `POST /analytics/query`, memory and tenancy, the first customer source, presentation blocks, scale-out.
Each is detailed when the stage before it is done, because its shape depends on what the earlier stages measure.
