# Teams bot integration: the backend half

The Microsoft Teams bot is split in two.
The edge, repository `teams-agent-edge`, runs on AWS, receives Teams messages from Microsoft, and queues them on SQS.
This backend runs the consumer, behind the firewall, with no inbound port.
Design and decisions: `docs/plan/2026-09-21_11-34_teams-bot-edge-and-agent-queue.md`.

## What runs here

| piece | file | runs in |
| --- | --- | --- |
| tenant binding table and admin API | `app/persistence/models/teams_binding.py`, `app/api/v1/teams.py` | web tier |
| conversation memory | `app/persistence/models/teams_conversation_turn.py`, `app/services/teams/memory.py` | consumer |
| agent history and prompt caching | `app/services/log_agent/agent.py` (`ask(question, history=...)`) | web tier and consumer |
| SQS consumer | `app/services/teams/consumer.py`, entry `python -m app.teams_consumer` | its own process |
| binding mirror to the edge | `app/services/teams/binding_mirror.py`, `app/services/teams/binding_sweep.py` | web tier (on write) and consumer (sweep) |
| message contract | `app/services/teams/contracts.py`, copied verbatim from the edge | both |

## One question, end to end

1. The edge validates Microsoft's token, maps the Entra tenant id to a `customer_code` from its DynamoDB copy of `teams_tenant_bindings`, and publishes a `QuestionJob`.
2. The consumer receives it, starts a heartbeat that keeps extending the SQS visibility timeout, and checks the customer exists and is active.
3. It loads the last `teams_history_turns` turns of that Teams conversation and runs `LogDebugAgent.ask(question, history=...)`.
4. It records the question and answer as two `teams_conversation_turns` rows, then POSTs an `AnswerPayload` to the edge with the shared secret.
5. On success it deletes the SQS message.
   If the edge cannot be reached the message is left in place; SQS makes it visible again and the queue's redrive policy dead-letters it after a few attempts.
6. Any failure inside the agent still produces an answer with `status=error`, so the person in Teams is never left waiting.

## Onboarding a customer tenant

```
PUT /api/v1/teams/bindings/<entra tenant guid>
{"customer_code": "acme", "display_name": "Acme Ltd", "created_by": "amin"}
```

The row is saved and mirrored to the edge in the same request.
If the mirror fails the response says `mirrored: false` and the consumer's sweep pushes it within `teams_binding_mirror_sweep_seconds`.
`enabled: false` switches the bot off for that tenant without losing the mapping.

## Running the consumer

It is a separate systemd unit, `deploy/fastapirag-teams-consumer.service`, not one of the singleton worker's loops.
Run more than one copy to scale; SQS gives each copy different messages and the edge makes answer delivery idempotent.
Settings are the `TEAMS_*` variables in `.env.example`.
AWS credentials go in `/etc/fastapirag/teams-consumer.env`, read by the unit's `EnvironmentFile`, never in `.env`, whose loader rejects unknown keys.
Concurrency per process is `teams_consumer_concurrency`; the Anthropic client retries on rate limits with `log_agent_max_retries`.

## The evidence on the card

When the answer rests on rows, `AnswerPayload.evidence` carries them as data (title, columns, rows, facts, link) so the edge draws a real Adaptive Card table.
The contract and the card layout are in `docs/teams-evidence-card.md`; the edge must mirror the `Evidence` model into its `contracts.py`.

## Why the agent replays plain text only

Only the text of earlier questions and answers is sent back to the model, never earlier tool calls or results.
The model re-reads the data fresh for every question, so a follow-up can never quote a number from a stale tool result.

## The Home snapshot for the Teams tab (chunk 127)

The Teams app's Home tab is a page served by the edge on AWS, which cannot reach this server.
So the consumer computes "today so far" per bound, enabled customer every `TEAMS_HOME_SNAPSHOT_SECONDS` (60) and writes it to the edge's DynamoDB table as `HOME#<customer_code>` / `SNAPSHOT`, one JSON string with a one-hour TTL (`app/services/teams/home_snapshot.py`).
It is built from the same reads the analytics agent uses (`aggregate_releases`, `list_releases`) on the tenant's clock: releases today against yesterday at this time with an hourly sparkline, deliveries, fill rate, zero-picks and partials, the top zero-picked items today and chronic short items over seven days, customers today by lines, and the latest short lines.
Chunk 133 adds `shape`, the "Shape of the day" chart under the tiles (`app/services/teams/home_shape.py`): lines per hour over the last 24 clock hours, stacked by transaction kind (stock, jit, milk, frz, other, read from the transaction name the way the pick releases page reads it), and distinct pickers per hour.
It takes two grouped reads, `hour_start` by `transaction_name` and `hour_start` with `distinct:user_name`, and decides the axis ticks and the peak labels itself so the page only draws; a read problem leaves `shape` null and the rest of the snapshot intact.
A customer without a `pick_release` settlement gets no snapshot; a customer without the item or customer lookups gets the plain fields instead.
Questions typed in the tab arrive on the same queue with `source="tab"` and are answered the same way; the edge stores the answer on the job for the page to poll instead of sending it into a Teams conversation.
