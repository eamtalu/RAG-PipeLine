# Stage D deploy runbook: the Teams app (roles, sites, advisories, tasks)

One section per backend step of stage D (`docs/esmart-agent-execution-plan.md`), written when the step is built and updated when it is deployed.
The edge's side of each step is in the edge repo, `docs/rollback-runbook.md` and `docs/HANDOFF-2026-11-12.md`.
Rules for every step: `deploy.sh` with an unchanged `.env` changes nothing visible, because every new loop or mirror defaults off or writes rows nobody reads yet; migrations are additive only; the way back is `git checkout <tag>` plus a restart, never `deploy.sh` on a detached checkout.

## Before the first stage D deploy

- `cp .env .env.bak.20261112-stage-d` on the server.
- Baseline tags exist on both repos: `baseline-2026-11-12-pre-esmart-eye` and `deployed-backend-9c7fee9`.

## D4: sites, the Home snapshot per site, freshness, the mirror

What it adds: `tenant_sites`, `analytics_at_risk_deliveries.warehouse` (migration `d3b7e9f1a2c4`), the snapshot computed per site and mirrored under `EYE#<customer>` (`SITE#<site_id>#HOME`, `SITE#all#HOME`, `SITES`), the `freshness` block on every snapshot, settings `TEAMS_PROVISIONAL_SHARE` (0.20) and `TEAMS_MAX_SITES_PER_CUSTOMER` (3).
The old `HOME#<customer>/SNAPSHOT` row is still written, so the old page is unchanged.
The consumer's IAM user must already allow the `EYE#*` prefix (edge step 3 Terraform, applied 12 November 2026).

Deploy:

1. `./deploy.sh` (pulls, migrates, restarts the three units). The migration builds one index `CONCURRENTLY`.
2. Verify the rows the new page reads exist within a minute: in the edge's DynamoDB table, `EYE#tmp-live` / `SITE#all#HOME` and `EYE#tmp-live` / `SITES` (`aws dynamodb get-item --table-name <table> --key '{"pk":{"S":"EYE#tmp-live"},"sk":{"S":"SITES"}}'`).
3. Discover the site ids from the data before configuring any site:
   ```
   PYTHONPATH=. venv/bin/python scripts/teams_sites.py discover tmp-live
   ```
   It prints the distinct `warehouse` values on the settled pick-release rows and on the facts.
   A settled row carries the warehouse only when the `pick_release` settlement's `carry` list names it.
   If the facts have values and the settled rows have none, add `"warehouse"` to the settlement's carry list: `PATCH /api/v1/analytics/settlements/pick_release` with the full `carry` list (the ops token and `X-Customer-Code: tmp-live`).
   A changed carry list rebuilds every settled row under the new rules in that same call (the answer says `rebuilt N row(s)`; minutes on the live tenant); then run `discover` again.
   Only then do per-site figures exist; until then every site's Home shows zeros and the whole-customer view is the one to use.
4. Configure the sites with the ids the data carries (the owner believes 1 Brighton, 2 Bicester, 3 London; verify):
   ```
   PYTHONPATH=. venv/bin/python scripts/teams_sites.py set tmp-live <id> Brighton --order 1
   PYTHONPATH=. venv/bin/python scripts/teams_sites.py set tmp-live <id> Bicester --order 2
   PYTHONPATH=. venv/bin/python scripts/teams_sites.py set tmp-live <id> London --order 3
   PYTHONPATH=. venv/bin/python scripts/teams_sites.py list tmp-live
   ```
   Within a minute the consumer writes `SITE#<id>#HOME` per site and the `SITES` list; the journal shows no `home snapshot ... failed` line.
5. The at-risk rows learn their warehouse on the next evaluation (every minute); closed history stays NULL and counts under the whole customer.

Rollback: `git checkout deployed-backend-9c7fee9` and `sudo systemctl restart fastapirag fastapirag-worker fastapirag-teams-consumer`; the new table, column and index can stay, the `EYE#` rows expire within an hour or are ignored.

## D5: typed answer blocks and the presentation tools

What it adds: the analytics profile lists the `presentation` namespace (`show_chart`, `show_table`, `suggest_followups`), empty unless `ANALYTICS_AGENT_PRESENTATION=true`; the runtime turns those calls into `blocks` on the answer; the Teams consumer posts `blocks`, `asked_at`, `answered_at`, `data_as_of` and `provisional` with every answer (contract revision 3.5, `docs/contract_schema.json`).
No migration. The guards treat a drawn figure like a written one: a chart with no data call behind it is withheld like an invented number.
The edge must already run the step 5 image, so that the blocks are drawn; an older edge image drops the unknown fields and shows the evidence card, so the order is only about seeing the result.

Deploy:

1. `./deploy.sh` with the flag unset (the default, off). Ask one question in Teams: the answer arrives as before (text plus the card) and the journal shows no `answers` error.
2. Add `ANALYTICS_AGENT_PRESENTATION=true` to the server `.env`, then `sudo systemctl restart fastapirag-teams-consumer` (the web API reads the same flag; restart `fastapirag` too if the web assistant should draw).
3. Ask a chart question in Teams ("Show deliveries per day this week as a chart"): the answer carries a chart card with the Bars / Line / Table toggle. In `agent_traces` the run lists a `show_chart` call after the data calls.
4. If the model draws a chart from nothing, the answer is withheld with the honest sentence (the same path as an invented number) and the journal says so; that is the guard working, not a fault.

Rollback: remove the flag from `.env` and restart the consumer (answers go back to text plus card, nothing else changes); or `git checkout deployed-backend-<previous>` and restart the three units.

## D6: the Ask history per person, the purge, the progress row

What it adds: migration `e4c8a2b7d619` (seven nullable columns on `teams_conversation_turns` and the index `(customer_code, sender_object_id, created_at)`, built `CONCURRENTLY`); the consumer records every exchange with who asked, the site, the blocks and the times, then mirrors the person's last 30 days under `EYE#<customer>` (`USER#<id>#HISTORY`, one `USER#<id>#TURN#<job>` per answer, 31-day TTL); an hourly purge of turns older than `TEAMS_HISTORY_RETENTION_DAYS` (30); a `JOB#<id>#PROGRESS` row rewritten while a question runs.
No flag. Settings `TEAMS_HISTORY_RETENTION_DAYS`, `TEAMS_HISTORY_ENTRIES` (100), `TEAMS_HISTORY_PURGE_SECONDS` (3600) have defaults.
The consumer's IAM user already allows the `EYE#*` prefix.

Deploy:

1. `./deploy.sh` (pulls, migrates, restarts the three units). The index builds `CONCURRENTLY`; the table is small.
2. Ask one question in Teams. Within a minute `EYE#tmp-live` / `USER#<your object id>#HISTORY` exists in the edge's table and lists it; the journal shows no `teams.history_mirror_failed` line.
3. While a second question runs, `EYE#tmp-live` / `JOB#<job id>#PROGRESS` is rewritten a few times (`steps` with one `running`); the page shows the four steps.
4. The purge logs `teams.history_purged rows=N` only when something is older than 30 days; nothing on the first day.

Rollback: `git checkout deployed-backend-<previous>` and restart the three units; the columns and the index can stay (nullable, unused), the mirror rows expire.

## D6b: conversations as the unit of the Ask history

What it adds: `teams_conversations` (migration `f5d9b3c8e721`), the header of each thread, created on a thread's first question and bumped on every later one; the person's history row now lists conversations most recent first with their turns inside (the flat list stays), and a turn written before this change shows as a thread of its own. Headers go with their turns in the purge. No flag, no new setting.

Deploy:

1. `./deploy.sh` (pulls, migrates, restarts the three units).
2. Ask a question in Teams, then a follow-up in the same conversation: `EYE#tmp-live` / `USER#<id>#HISTORY` lists one conversation with two turns; the journal shows no `teams.history_mirror_failed` line.
3. Press "New conversation" on the page and ask again: a second conversation appears, and the agent's answer carries no memory of the first.

Rollback: `git checkout deployed-backend-<previous>` and restart the three units; the table can stay.

## D7a: access control (the record, the matrix, users, approvals, audit)

What it adds: migration `a6e1c4d9f832` (eight tables: roles and capabilities seeded from `app/services/access/matrix.yaml`, the matrix per customer, `teams_users`, their sites and overrides, `access_approvals`, `audit_events`); the consumer refuses a question before any model runs when the person has no user row, is deactivated, or asks about a site outside their scope (audited as `ask.refused`); an hourly-ish sweep (every `TEAMS_ACCESS_SWEEP_SECONDS`, 60) rewrites the access rows for the edge (`USER#<id>#ACCESS`, `ROLES`, `USERS`, `APPROVALS`, `AUDIT`), expires approvals older than 24 h and purges audit older than `TEAMS_AUDIT_RETENTION_DAYS` (90); the superadmin's API under `/api/v1/teams/access/<customer>/...` and `scripts/teams_users.py`.

**The owner's decision 9 flips here: a person with no user row cannot ask any more.** So the first Admin must be created right after the deploy, before anyone asks.

Deploy:

1. `./deploy.sh` (pulls, migrates, restarts the three units).
2. Make the first Admin at once, with the owner's Entra object id (the edge logs it as `sender=` on `tab.intake`; the bot's as the sender's `aadObjectId`):
   ```
   PYTHONPATH=. venv/bin/python scripts/teams_users.py set tmp-live <object id> "Amin Talukder" --role admin --all-sites --email amin.talukder@becsi.co.uk
   PYTHONPATH=. venv/bin/python scripts/teams_users.py list tmp-live
   PYTHONPATH=. venv/bin/python scripts/teams_users.py matrix tmp-live
   ```
   The `set` prints `mirrored 5 access row(s) to the edge` when the shell has the service's AWS credentials; from a plain shell it prints `saved; not mirrored from this shell` and the consumer's sweep writes the rows within a minute (seen on 13 November: the first run crashed on `NoCredentialsError` after saving the row; the sweep mirrored it anyway). Either way `EYE#tmp-live` / `USER#<object id>#ACCESS` exists in the edge's table within a minute.
3. Ask a question in Teams: it answers as before, and `teams_users.last_active_at` is stamped. Ask from a second account that has no user row: the answer is "You have no access to eSmart Eye yet…" and the journal shows `audit ask.refused`.
4. Add the other people with `teams_users.py set … --role supervisor --sites BRI` (or through the Admin tab once the edge step 7b/7c is live).

Rollback: `git checkout deployed-backend-<previous>` and restart the three units; the tables can stay. The refusal stops with the old consumer.

## D7c: the Admin tab's commands

What it adds: the five access commands on the question queue (`access_set_user`, `access_deactivate_user`, `access_set_matrix_cell`, `access_decide`, `access_request`), run by `app/services/access/commands.py` from the actor's own record; a change the matrix marks "with approval" becomes a request; the access rows are mirrored at once after each. Contract 3.7 (synced from the edge). No migration, no flag.

Deploy: `./deploy.sh`. Then, once the edge step 7c image is live, open the Admin tab as Admin, click one matrix cell: the answer line arrives within a few seconds and the journal shows `audit access.matrix_changed`.

Rollback: the previous checkout and a restart; the edge's commands are then refused as unknown until the edge is rolled back too.

## D7d: the executive Home

What it adds: `app/services/teams/exec_home.py` computes, every `TEAMS_EXEC_HOME_SECONDS` (300), the operation across every site for four periods (fill rate against the same stretch of the period before, on-time dispatch from the closed departures, lines, deliveries, pickers), the 30-day fill-rate series per site and the decisions waiting, and mirrors it as `EYE#<customer>` / `EXEC#HOME` (TTL two hours). `TEAMS_FILL_RATE_TARGET` (0.98) is the target the tiles show. The at-risk history counts take a site. No migration, no flag.

Deploy: `./deploy.sh`. Within five minutes `EYE#tmp-live` / `EXEC#HOME` exists; the journal shows no `executive home … failed` line. To see the page, give a test user the Executive role: `teams_users.py set tmp-live <object id> "<name>" --role executive --all-sites`.

Rollback: the previous checkout and a restart; the row expires within two hours.
