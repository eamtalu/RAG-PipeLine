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

## Stage D: the Teams app (roles, sites, advisories, tasks)

Added 12 November 2026.
The design pack and the step-by-step prompts live in the edge repo at `docs/design-handoff/` (DESIGN.md, PROMPTS.md, screens 01 to 12); the edge's `docs/design-handoff/PLAN.md` is the repo-local summary.
Stage D takes roadmap items 5 (roles table) and 7 (presentation blocks) forward together, because the design needs both at once.
The numbering follows PROMPTS.md so the two repos talk about the same step; D3 is PROMPTS step 3 and so on.
Rules that hold for every D step: the backend Postgres is the record; what the tab reads is mirrored into the edge's DynamoDB table under one key prefix `EYE#`; what the tab writes arrives as `QuestionJob.action` on the queue and is answered through `/internal/answers`; every contract addition is optional and an unknown action kind is answered "unsupported"; every new loop or mirror is behind a setting that defaults off, so `deploy.sh` with an unchanged `.env` changes nothing.
Steps D7, D9 and D10 are presented to the owner and wait for an OK before code.

| # | Step | Done when |
|---|---|---|
| D4 | Sites: `tenant_sites` lookup (Brighton 1, Bicester 2, London 3 for `tmp-live`, values verified against `analytics_settled_rows.warehouse` first); the Home snapshot computed per site and for all sites; `analytics_at_risk_deliveries.warehouse` filled from what the board already reads; `mirror_outbox` and a `MirrorWriter` shared by every mirror; freshness (`data_as_of`, `provisional` from the unsealed share) | `EYE#<cc>/SITE#<id>#HOME` rows exist for every active site and `HOME#<cc>/SNAPSHOT` is still written; the old tab is unchanged |
| D5 | Answer blocks: the shared contract gains `blocks`, `asked_at`, `answered_at`, `data_as_of`, `provisional`; presentation tools `show_chart`, `show_table`, `ask_form`, `suggest_followups` behind `analytics_agent_presentation`; `blocks_from_trace`; the ungrounded guard covers chart numbers | a scripted run yields one valid chart block with the fetched numbers; a chart with numbers from no tool is dropped and logged; the legacy `answer` is unchanged |
| D6 | History: turns keep the sender, the blocks and the times; a conversation index per person; purge at 30 days; mirrored under `EYE#<cc>#U#<oid>/HIST#*`; `history_refresh` re-runs a stored question | a person's own conversations list; another person's never do; a 31-day-old turn is gone |
| D7 | Access control: `roles`, `capabilities`, `role_capabilities` seeded from `access_matrix.yaml`; `users`, `user_sites`, `approvals`, `audit_events`; one `decide()`; `Principal.roles` and `ExecutionContext.site_ids` applied by the tool executor; the user profile and the Admin rows mirrored; `access_default_role` supervisor until the users are seeded | a supervisor cannot read another site and cannot approve; a model-supplied site outside scope is rewritten and noted on the trace; the matrix in the database equals screen 07 |
| D8 | Actions: `run_action` dispatch keyed on `action.kind` with the at-risk check folded in; the form registry with `create_user` end to end (permission, approval when the matrix says so, audit, receipt with undo); redelivery replays the stored answer | `create_user` by a supervisor opens an approval and returns a receipt; an ops manager's decision flips the user; an unknown kind is answered "unsupported" |
| D9 | Advisories: tables, the agent ledger, the runner loop behind `advisory_worker_enabled`; the Delivery agent on `analytics_at_risk_deliveries`; the other four seeded off; feed and roster mirrored; high severity to the customer's Teams webhook | planted at-risk rows yield one advisory per route and day, resolved when the rows close; the roster shows cadence, last and next run |
| D10 | Tasks: `reminders`, `schedules`, `task_runs` with a unique `(kind, task_id, due_at)`; the scheduler loop in the singleton worker behind `task_scheduler_enabled`; reminders re-run their query as the owner; conditions fire once and re-arm; schedules post to a channel through the notification dispatcher | two workers claim a due run once; a run left running is swept and retried; a condition fires once per crossing; a channel post is enqueued |
| D11 | Docs and the switch: `docs/stage-d-deploy-runbook.md`, the ER diagram, the handoff; `access_default_role` to none | the runbook has a rollback line per step and the switch is one Terraform apply on the edge |

Later stages follow the roadmap in the architecture document: the query model and `POST /analytics/query`, memory and tenancy, the first customer source, presentation blocks, scale-out.
Each is detailed when the stage before it is done, because its shape depends on what the earlier stages measure.
