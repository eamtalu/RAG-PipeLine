# Agent quotas: admission in the gateway

Since 10 November 2026 (step 16 of `docs/esmart-agent-execution-plan.md`) the gateway admits every agent run before any model call, against counters in Postgres that every consumer and web process changes by one atomic operation, so a tenant's limit is one number whoever enforces it.
Nothing a person sees changes unless a limit is hit; the refusal is a plain sentence through the normal answer path, like a manipulation decline.

## The limits

| setting | default | what it caps |
| --- | --- | --- |
| `AGENT_QUOTA_TENANT_PER_HOUR` | 60 | questions per tenant per hour |
| `AGENT_QUOTA_USER_PER_HOUR` | 20 | questions per person (Entra object id, or the service caller name) per hour |
| `AGENT_QUOTA_TENANT_IN_FLIGHT` | 8 | runs live at once per tenant, across every process |

The hourly numbers equal the Teams edge's (`rate_limit_per_hour`, `user_rate_limit_per_hour`), which refuses first for Teams so a Teams user the edge admitted is never refused here; the web has only these.
They are global today; per-tenant overrides come with the tenant knowledge table.

## How a run is admitted (`app/services/agent_quotas/admission.py`)

One transaction per request under `pg_advisory_xact_lock(hashtext('agent_admission:<tenant>'))`, which serialises one tenant's admissions and nothing else:

1. Sweep the tenant's `agent_in_flight` rows whose `deadline_at` has passed and its `quota_windows` rows older than a day. A crashed worker never releases, so a tenant can be pinned for at most one run deadline, with no sweeper process.
2. Count the live rows and read the hour's two counters (the tenant row has `user_id=""`).
3. Decide, naming the first limit the run would exceed, in the order `tenant_in_flight`, `tenant_per_hour`, `user_per_hour`.
4. On a refusal, roll back: a refused attempt writes nothing, so a person hammering refusals cannot push the tenant's own counter over its limit.
5. On admission, increment both counters (`INSERT ... ON CONFLICT DO UPDATE SET used = used + 1`) and insert the in-flight row, then commit.

`release` deletes the in-flight row in its own short session.
The test `tests/test_agent_quotas_chunk166.py` runs five admissions at once against an in-flight limit of three and sees exactly three admitted, which is the "five workers enforce one limit" proof.

## The gateway's part (`app/agent/gateway/run.py`)

`run_request(..., admission=None)` admits after it builds the context and before the runtime runs.
The port (`Admission` in `app/agent/gateway/ports.py`) takes plain values, so the Postgres implementation imports nothing from the agent package and the layout rule holds.

- Refused: `AgentResult(stop_reason="declined", declined="quota_<reason>", answer=<sentence>)`, a trace row with that decline, one `agent.refused` log line; the runtime never runs and nothing is released.
- Admitted: the slot is released in a `finally`, so a timeout or an exception frees it too.
- The quota store raising: the run goes through and one `agent.admission_failed` line is written. The quotas protect cost; an outage of the database already stops the tools, and refusing every answer on top would help no one.

The sentences (`QUOTA_REFUSALS`): busy right now, the warehouse's hourly limit, your hourly limit.

## Who passes the admission

`app/teams_consumer.py` builds one at module level beside its registry and trace store; `app/api/deps.py` has a cached `admission()` for both ask endpoints.
Tests that call `run_request` without one run unlimited.
A tenant purge deletes the rows of both tables.

## What later steps add

Per-tenant limits from the tenant knowledge table and their mirror into the edge's DynamoDB, a daily cost budget once step 19 accounts usage, a 429 on the web once the web app handles it.
