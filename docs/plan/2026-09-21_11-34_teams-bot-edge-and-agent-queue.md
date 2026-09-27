# Teams bot for the log debugging agent: AWS edge, outbound queue, tenant binding

Status: design agreed on 2026-09-21. Edge built the same day (`~/myworkspace/work/bec/teams-agent-edge`, no git yet). Backend built the same day: tables, admin API, agent history, consumer process. AWS infrastructure code and Azure registration not started.
Owner decisions recorded here so a later session does not re-open them.

## Decisions taken

- The bot is published to customers in other Microsoft 365 tenants.
- The bot registration is single-tenant in our Entra tenant, because Microsoft stopped new multi-tenant bot creation on 31 July 2025.
- Cross-tenant reach comes from a Teams store (AppSource) listing.
- The bot edge runs on AWS.
- The backend stays on the Ubuntu server behind the firewall and is never exposed to the internet.
- Connectivity between edge and backend is an outbound-only SQS queue polled by the existing worker process (question 1, option a).
- Follow-up questions in the same Teams conversation carry memory of earlier turns (question 2, yes).
- One default log space per customer tenant, set at onboarding (question 3, option a).
- Personal chat first, with the manifest and handlers designed so channel @mention can be switched on later (question 4).

## What already exists and is reused

- The Claude tool-use agent in `app/services/log_agent/agent.py` with eight SELECT-only tools, every one hard-scoped by customer code.
- The web endpoint `POST /api/v1/logs/debug/ask` in `app/api/v1/logs.py`, which stays unchanged for the frontend.
- The tenant registry `customers` table, where one row is one log space and `customer_code` is the tenant key used everywhere.
- The worker process `python -m app.worker`, which runs the background loops registered in `app/background.py` under a singleton advisory lock.
- The Adaptive Card style in `app/services/notifications/channels/teams.py`, so answers look like the existing alerts.

## Components

### 1. Bot edge (new repository, AWS)

A small FastAPI service built on the Microsoft 365 Agents SDK for Python, hosting package `microsoft-agents-hosting-fastapi` (verified at version 1.7.0).
It has no database access to the log store and holds no model keys.

Responsibilities:

- Receive activities on `POST /api/messages` and validate Microsoft's Bearer token with the SDK's JWT middleware.
- Read the customer tenant id from `activity.conversation.tenant_id`, falling back to `channelData.tenant.id`.
- Look up the tenant binding and reject unknown tenants with a fixed message.
- Enforce a per-tenant rate limit.
- Store the conversation reference so the answer can be sent later.
- Send a typing indicator and enqueue a job on SQS.
- Return 200 to Microsoft within a second.
- Expose `POST /internal/answers`, authenticated by a shared secret, for the worker to deliver the answer.
- Render the answer as an Adaptive Card and send it with the SDK's proactive `continue_conversation` API.
- Re-send the typing indicator every 15 seconds while a job is open, because Teams clears it after about 20 seconds.

Repository layout:

```
teams-agent-edge/
  app/
    main.py                # FastAPI app, middleware, routers
    settings.py            # pydantic-settings, env driven
    bot/
      handlers.py          # message handler: guard -> reference -> enqueue -> ack
      cards.py             # Adaptive Card builder for answers and error replies
      typing.py            # typing indicator keepalive per open job
    tenants/
      binding_store.py     # DynamoDB: tenant id -> customer code, default space, enabled
      rate_limit.py        # DynamoDB counters, sliding window per tenant
    queue/
      publisher.py         # SQS send with the job schema
    answers/
      router.py            # POST /internal/answers, shared-secret auth
      delivery.py          # continue_conversation + card send
    storage/
      dynamodb_storage.py  # SDK Storage protocol (read/write/delete) on DynamoDB
  infra/                   # Terraform or CDK for ALB, ECS, SQS, DynamoDB, Secrets
  teams-app/               # manifest.json, color.png, outline.png
  tests/
```

### 2. Backend changes (this repository)

Three contained changes.

**A. Tenant binding table** `teams_tenant_bindings`:

| column | type | notes |
|---|---|---|
| tenant_id | text, primary key | Entra tenant id of the customer |
| customer_code | text, indexed | soft reference to `customers.customer_code`, the default log space |
| enabled | boolean, default true | switch a customer off without deleting |
| display_name | text, nullable | customer name for logs |
| created_by | text, nullable | who onboarded |
| created_at, updated_at | timestamptz | as elsewhere |

The edge reads a copy of this table from DynamoDB.
Postgres is the source of truth and the worker publishes changes to DynamoDB on write, so the edge never needs to reach the LAN.
Admin API: `POST/GET/PATCH /api/v1/teams/bindings` behind `require_admin` in `app/api/deps.py`.

**B. Conversation memory** `teams_conversation_turns`:

| column | type | notes |
|---|---|---|
| id | uuid | |
| conversation_id | text, indexed | Teams conversation id |
| customer_code | text | tenant scope, always present |
| role | text | user or assistant |
| content | text | the question or the final answer |
| created_at | timestamptz | |

`LogDebugAgent.ask` gains an optional `history` parameter with the last N turns, default 6, bounded by a settings value.
The web endpoint passes nothing and behaves exactly as today.
The worker loads the turns for the conversation, passes them, and appends the new pair after the answer.

**C. Queue consumer, its own process** `app/services/teams/consumer.py`, entry `python -m app.teams_consumer` (REVISED after the capacity review: not a loop inside the singleton worker, so several copies can run):

- Long-polls the SQS queue with boto3 through `asyncio.to_thread`, or aioboto3 if we prefer native async.
- For each job: open a session with `async_session()`, resolve the customer through `CustomerRepository.get_by_code`, build `LogDebugAgent(db, customer_code)`, load history, run `ask`, store the turns, close the session.
- POSTs the answer to the edge's `/internal/answers` with the shared secret, then deletes the message.
- On agent failure it POSTs a short error answer so the user is not left waiting, then deletes the message.
- Visibility timeout on the queue is set above the agent's worst case, and the loop extends it while working.
- Runs as `deploy/fastapirag-teams-consumer.service`, N agent runs concurrently per process (`teams_consumer_concurrency`), scaled by starting more instances. Not under the singleton advisory lock.

Settings added to `app/settings.py`: queue URL, AWS region, credentials via the standard AWS environment variables, edge answer URL, edge shared secret, history turns, worker enabled flag.

Dependencies added to `requirements.txt`: `boto3` or `aioboto3`.

Migrations: one Alembic revision chained onto the current head, plus the required update of `docs/database-er-diagram.md`.

### 3. Azure (registration only, no compute)

- Entra app registration in our tenant, supported account types "any organizational directory", client secret with a calendar reminder for expiry.
- Azure Bot resource, single-tenant, messaging endpoint `https://<edge-domain>/api/messages`, Teams channel enabled.
- Teams app manifest with scopes `personal` now, `team` and `groupchat` present but the bot ignores them until channel support is switched on.

### 4. AWS resources

| resource | purpose |
|---|---|
| Route 53 record and ACM certificate | public HTTPS name for the edge |
| Application Load Balancer | TLS termination, health check on `/health` |
| ECS Fargate service, two tasks | the edge |
| SQS standard queue plus dead-letter queue | questions from edge to worker |
| DynamoDB tables: conversation references, tenant bindings, rate counters | edge state |
| Secrets Manager | bot client secret, shared answer secret |
| IAM user or role for the Ubuntu worker | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility` on the one queue, and `dynamodb:PutItem` on the bindings table |
| CloudWatch logs and alarms | dead-letter depth above zero, edge 5xx rate |

## Message flow

1. User sends a message in personal chat with the bot.
2. Microsoft POSTs the activity to the edge with a signed token.
3. Middleware validates the token; failure is 401 and nothing else runs.
4. Handler reads the tenant id and looks up the binding; unknown or disabled tenant gets the fixed reply and the turn ends.
5. Rate limit is checked; over limit gets a polite reply and the turn ends.
6. Conversation reference is stored; typing indicator sent; job enqueued with: job id, tenant id, customer code, conversation id, activity id, sender object id, sender name, question text, enqueued at.
7. Handler returns; Microsoft sees 200.
8. Worker receives the job, runs the agent with history, stores the turns.
9. Worker POSTs the answer, tool call count, and cited transaction ids to `/internal/answers`.
10. Edge builds the card and sends it through `continue_conversation`; the typing keepalive for that job stops.

## Security

- The backend has no inbound exposure. All traffic from the LAN is outbound HTTPS to SQS, DynamoDB, and the edge.
- The edge trusts only Microsoft-signed tokens for `/api/messages` and only the shared secret for `/internal/answers`.
- Tenant identity comes from Microsoft's token-backed activity, never from message text.
- The agent tools remain SELECT-only and tenant-scoped, unchanged.
- Rate limits protect the model bill; each question is several Opus calls.
- The two live API keys recorded in `docs/HANDOFF-2026-08-26.md` must be rotated before any customer can trigger the agent.

## Testing strategy

Failing test first, then implement, for every unit.

Backend:

- Repository tests for the binding and turn tables using the `db` fixture in `tests/conftest.py`.
- `LogDebugAgent.ask` with history: assert the message list sent to the Anthropic client contains the prior turns in order and bounded to N.
- Worker loop with a fake SQS client and a fake edge: one job in, one answer out, message deleted; agent failure yields an error answer and still deletes.
- `start_background_tasks` gate test in the style of `tests/test_background_workers_chunk10.py`.

Edge:

- Handler tests with a synthetic activity: known tenant enqueues and acks, unknown tenant replies and does not enqueue, rate-limited tenant replies and does not enqueue.
- Card builder pure tests.
- `/internal/answers` rejects a missing or wrong secret.
- DynamoDB storage adapter against DynamoDB Local in Docker.

End to end:

- Edge on a laptop behind a dev tunnel, Azure Bot pointed at the tunnel, worker on the laptop against the local Postgres, one real question from Teams in our own tenant.

## Rollout

1. Backend tables, agent history, worker loop, all behind the disabled flag; deploy with `deploy.sh`.
2. Edge on AWS with our tenant bound; test from our own Teams via custom upload.
3. Enable the worker flag on the server; first live question answered end to end.
4. Store submission through Partner Center; first external customer bound after approval.
5. Later: channel @mention by adding the `team` scope handling and mention stripping with `activity.remove_mention_text`.

## Revisions after the capacity review (2026-09-21)

- Consumer is a dedicated, horizontally scalable process, not a singleton-worker loop.
- Edge: activity-id deduplication (DynamoDB conditional put) so a redelivered Teams message is queued once.
- Edge: job watchdog replaces the typing keepalive; says "still working" after 2 minutes and closes the job with an apology after 10, so a queue delay is never silent.
- Agent: prompt caching on the system prompt and tool definitions; Anthropic client retries on rate limits (`log_agent_max_retries`).
- Backend: `teams_conversation_turns.position` orders the question and its answer written in the same instant.

## Open items to confirm before implementation

- Repository path for the edge, proposed `/Users/amintalukder/myworkspace/work/bec/teams-agent-edge`.
- AWS account, region, and any existing VPC or domain to reuse.
- Company name, privacy policy URL, and terms URL for the Teams manifest and store listing.
- Partner Center account existence.
