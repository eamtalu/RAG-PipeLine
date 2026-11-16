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

## D8: create_user through Ask

What it adds: the `actions` namespace on the analytics profile (one tool, `propose_create_user`, offered while `ANALYTICS_AGENT_PRESENTATION` is on), the form lifted off the trace, `access_set_user` answering with a receipt, the `access_undo` command, `TEAMS_UNDO_MINUTES` (10). Contract 3.8 synced from the edge. No migration, no new flag.

Deploy: `./deploy.sh`. Then, once the edge step 8 image is live, ask "Create a user for Dan Roberts" in Teams as Admin: the form comes back pre-filled (the trace shows one `propose_create_user` call and no data call); confirm it with an Entra object id: the receipt says created, and `teams_users` has the row with `created_by` your object id; Undo within ten minutes deactivates it (audited as `access.user_deactivated`).

Rollback: the previous checkout and a restart.

## D9: advisories (the agents, the runner, the actions)

What it adds: migration `b7f2d5e8c943` (`advisories`, `advisory_agents`, `advisory_agent_runs`), `app/services/advisories/` (the registry of seven agents, three of them live: Delivery from the at-risk board every 5 min, Stock controller from the Quick Stock Count facts every 15 min, Location from today's zero-picks every 30 min; the runner; the actions; the `ADVISORIES` mirror row), the consumer's `_advisories_loop` every `TEAMS_ADVISORIES_TICK_SECONDS` (30), the `advisory_act` command (acknowledge, assign, snooze, dismiss, resolve; audited as `advisory.<action>`), and new HIGH advisories announced once to the customer's notification channels as an `advisory` event (the existing outbox and webhook adapters deliver it).
Contract 3.9 synced from the edge.
No new flag: the loop runs wherever the edge table is set.

Deploy: `./deploy.sh` (the migration runs inside it).
Then watch the consumer for one minute:

```
sudo journalctl -u fastapirag-teams-consumer --since "-2 min" --no-pager | grep -i "advisory\|advisories"
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT agent, enabled, cadence_seconds, last_run_at, last_result FROM advisory_agents WHERE customer_code='tmp-live' ORDER BY agent;"
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT agent, severity, status, site_id, left(title,70) FROM advisories WHERE customer_code='tmp-live' ORDER BY opened_at DESC LIMIT 20;"
```

Expected: seven switches (delivery, stock, location enabled), one run per live agent within the first tick, and whatever the data holds (today's zero-pick locations and count variances are the likely first rows; a delivery row only while the board flags one).
The `ADVISORIES` row under `EYE#tmp-live` is written on the same tick (the edge step 9 image reads it).
To switch an agent off for a customer: `UPDATE advisory_agents SET enabled=false, updated_by='<you>' WHERE customer_code='tmp-live' AND agent='location';` (the Admin tab's roster toggle is a later step).

Rollback: the previous checkout and a restart; `alembic downgrade a6e1c4d9f832` drops the three tables.

Hotfix of 13 November 2026: `TEAMS_ADVISORIES_TO_CHANNEL` (default false) switches the channel posts off; the first live day had sent 73 cards to the one channel in six hours.
The advisories still open, resolve and show in the tab; only the Teams card stops.
D12 (notification groups) replaces the switch with routing by topic and severity, where nothing goes to a channel unless a group takes it, a new channel included.

## D10: reminders and schedules (the tasks loop)

What it adds: migration `c8e3f6a9d054` (`teams_tasks`, `teams_task_runs`), `app/services/tasks/` (the specs and their next-fire maths in the customer's time zone, the store, the runner the consumer ticks, the `task_create` / `task_update` commands, the per-person `USER#<id>#TASKS` mirror row with the suggestions from the person's own history), the consumer's `_tasks_loop` every `TEAMS_TASKS_TICK_SECONDS` (30) with the catch-up window `TEAMS_TASKS_CATCH_UP_HOURS` (6), the two proposing tools (`propose_reminder`, `propose_schedule`) on the analytics profile beside `propose_create_user`, the prompt snapshot regenerated.
Contract 3.10 synced from the edge.
No new flag: the loop runs wherever the edge table is set.

The scheduler is the table: every task row carries `next_fire_at`; the loop claims the due rows with `FOR UPDATE SKIP LOCKED`, writes the ledger row first (unique per task and planned moment), runs the attached question as the owner through the same gateway as a Teams question, and books the next firing from the planned time.
After a restart the loop picks up whatever is due; a task more than six hours late is noted as `missed` on the ledger and rebooked, not fired.

Deploy: `./deploy.sh` (the migration runs inside it).
Then, once the edge step 10 image is live, set a reminder in Teams a few minutes ahead and watch:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT kind, title, status, next_fire_at, fire_count FROM teams_tasks WHERE customer_code='tmp-live' ORDER BY created_at DESC LIMIT 10;"
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT planned_at, outcome, left(text,80) FROM teams_task_runs WHERE customer_code='tmp-live' ORDER BY planned_at DESC LIMIT 10;"
```

Expected: the reminder row with `next_fire_at` a few minutes ahead, then one ledger row `sent` within 30 s of that moment and the reminder `done`; a schedule row keeps `status active` and moves `next_fire_at` on.

Rollback: the previous checkout and a restart; `alembic downgrade b7f2d5e8c943` drops the two tables.

## D12: notification groups (routing by topic to channels and people)

What it adds: migration `d9f4a7b1e265` (`notification_groups`, `notification_group_channels`, `notification_group_members`, and `teams_tasks.target_group_id`), `app/services/groups/` (the routing rules, the store with the one lookup the dispatchers make, the commands `group_set` / `group_delete` / `group_join` / `group_leave` / `channel_set` / `channel_delete` / `channel_test`, the `GROUPS` and `USER#<id>#GROUPS` mirror rows written by the access sweep every minute and after every command), the advisories runner announcing every new advisory only to the groups that take its topic and severity (nothing without a group, a new channel included), a schedule posting to the one group it names, the schedule form offering the groups.
Contract 3.12 synced from the edge.
The hotfix switch `TEAMS_ADVISORIES_TO_CHANNEL` is gone: routing decides.

Deploy: `./deploy.sh` (the migration runs inside it).
Then, once the edge step 12 image is live, in Teams as an Admin or an Ops manager open Admin, "Channels and groups": add the channel (paste the webhook from the Teams channel's "Post to a channel when a webhook request is received" flow; Test sends one card), make a group (topics, severity, the channel, members).
From then on only what the group takes reaches that channel.
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT name, topics, min_severity, enabled FROM notification_groups WHERE customer_code='tmp-live';"
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT event_type, count(*) FROM notification_events WHERE created_at > now() - interval '1 hour' GROUP BY 1;"
```

Rollback: the previous checkout and a restart; `alembic downgrade c8e3f6a9d054` drops the three tables and the column. Without groups nothing is posted, as with the hotfix.

## D13: the per-group cap (how many, after step 12 decided who)

What it adds: migration `e0a5b8c2d376` (`notification_groups.max_per_hour`, default 5, and the ledger `advisory_announcements`), `app/services/groups/caps.py` (which of the groups that take a finding still have room this clock hour; the ledger lines sent / held / notice / summary; the notice card when a group reaches its cap; the summary card of what it held once the hour is over, written to the ledger before it is sent), the runner announcing within the cap (`notify()`), the quiet period for findings that come back (`teams_advisories_reannounce_hours`, default 2: a finding cleared and found again within that time of its last card keeps its stamp), `group_set` taking `max_per_hour` (1 to 60), the GROUPS row carrying `max_per_hour`, `sent_this_hour` and `held_this_hour` per group.
Contract 3.13 synced from the edge.
Worst case per group and hour: the cap, one notice, one summary.
Every open advisory already carries a stamp, so the first group made after this deploy receives only findings opened after it exists.

Deploy: `./deploy.sh` (the migration runs inside it).
Then the edge step 13 image (the group dialog's "Cards per hour" and the counters).
Check on the server, after the first group exists and a counting hour has passed:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT g.name, g.max_per_hour, a.window_start, a.status, count(*) FROM advisory_announcements a JOIN notification_groups g ON g.id = a.group_id WHERE a.customer_code='tmp-live' GROUP BY 1,2,3,4 ORDER BY 3,1,4;"
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT event_type, count(*) FROM notification_events WHERE created_at > now() - interval '1 hour' GROUP BY 1;"
```

Expected: per group and hour, `sent` at most `max_per_hour`, at most one `notice` and one `summary`; `advisory` events at most the cap per group per hour, plus `advisory_cap` and `advisory_summary` at most one each.

Rollback: the previous checkout and a restart; `alembic downgrade d9f4a7b1e265` drops the ledger and the column. The step 12 runner then announces every finding a group takes, with no cap: delete the groups first if the channel must stay quiet.

## D14a: the watchlist (watches, the tick, the rows the edge reads)

What it adds: migration `f1b6c9d3e487` (`watches`, `watch_events`, `watch_not_now`; any remaining condition schedule is stopped with a note), `app/services/watchlist/` (`registry.py` the pure rules: six kinds, cadences, status sets, default alerts, which changes are alerts, which are silent; `readers.py` one read per kind for all the watched things of a customer; `store.py` the rows, the limit of ten with the quietest first for the swap, the events, the Not now answers; `runner.py` the tick; `mirror.py` the `USER#<id>#WATCHES` and `WATCHABLES` rows; `commands.py` `watch_add` / `watch_update` / `watch_stop` / `watch_seen` / `watch_not_now`; `search.py` resolving ids and words; `suggest.py` the Suggested box from open HIGH advisories), the consumer's `_watches_loop` (settings `teams_watches_tick_seconds` 30, `teams_watches_settle_seconds` 120, `teams_watches_limit` 10), the Home snapshot keeping each site's fill rate in process for the measure watches (`home_snapshot.latest_fill_rate`), the Home rows carrying `departure_date`, `warehouse` and the attention items an `entity` for the eye toggle.
Retired: the condition schedules of step 10 (`when.kind == "condition"`, `_read_fill_rate`, `check_conditions`, the schedule form's "When fill rate drops below…"); a condition is a measure watch now.
Contract 3.14 synced from the edge (the watch kinds, the `entities`, `watch_suggestion` and `choice` blocks, the two rows).

Deploy: `./deploy.sh` (the migration runs inside it).
The watches loop has nothing to do until the edge step 14b image lets people add watches; the `WATCHABLES` row is written for every bound customer from the first minute.
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT version_num FROM alembic_version;" -c "SELECT count(*) FROM watches;" -c "SELECT count(*) FROM teams_tasks WHERE \"when\"->>'kind' = 'condition' AND status = 'active';"
```

Expected: `f1b6c9d3e487`, 0 watches, 0 active condition schedules.

Rollback: the previous checkout and a restart; `alembic downgrade e0a5b8c2d376` drops the three tables (a stopped condition schedule stays stopped).

## D14c: the watchlist in Ask (entities, the suggestion, the watch tools, the choice)

What it adds: `app/agent/presentation/entities.py` (the watchable things an answer was built on, read from the at-risk, releases, aggregate and advisory tool results in the trace, never from the text; the `entities` block leads the answer's blocks, `MAX_BLOCKS` 6), `app/services/watchlist/ask_hooks.py` (after the agent returns, each entity is marked watched or not and out of reach; at most one `watch_suggestion` for the thing the question named, never when watched, after Not now within a day, when the list is full, on a turn that already acts; a failure never costs the answer), `app/services/analytics_agent/watch_tools.py` (`watch_now` and `stop_watching` on the actions namespace: a plain watch is added at once as the asker and answers with the receipt, a rule with a level is the `add_watch` form, several matches or a rule without a level is one `choice` whose options carry the ready command, nothing matching or no access is a problem the model reads out), `app/services/watchlist/forms.py` (the add_watch form and how a confirmed one is read back; `watch_add` takes `form_id` + `values`), `framed_question` for `context.choice` (the words answer the model's own question), the profile's "Watchlist" paragraph (snapshot regenerated), and `watch_commands.run(job, db=…)` running on the caller's session (two sessions deadlocked on the lazily seeded access matrix).
No migration. No new setting.

Deploy: `./deploy.sh`, before the edge step 14c image (an older edge hides the three blocks).
Check in Teams after the edge: Ask "Where is delivery <a live number>?" (chips plus one suggestion), "watch route <code>" (a receipt), "tell me if fill rate drops below 95%" (the form), "stop watching <it>" (a receipt). On the server, `SELECT created_via, count(*) FROM watches GROUP BY 1;` shows `ask` once the first one is added that way.

Rollback: the previous checkout and a restart (watches added through Ask stay and keep being checked).

Deployed 13 November 2026 as `a35ffe8` (PR #23); the edge image `909b34d` followed.

## D15a: routines (the Tasks tab becomes Routines: runs by hand, ticks with a note, history, reuse)

What it adds: migration `a7c3e5f9b012` (`teams_task_runs` gains `trigger` time | hand | tick, `by_object_id`, `by_name`, `note`; outcome gains `done`), `store.shape_of` (check | report | action, from what the row holds; `kind` stays reminder | schedule in storage), `runner.run_by_hand` (Run now answers the question at once as the owner, writes the ledger row with the actor, leaves the clock alone, posts nothing), `runner.complete` (the person's own tick: done or skipped with a note; a one-off is finished by it, a repeating one keeps its clock), `store.copy_from` (Plan again and Copy as new make a new task from an old one, `source.task_id` naming it), the `repeat` row action (a one-off onto a weekday clock), `store.list_runs` and the new row `USER#<id>#TASK_RUNS` (the last 30 days newest first, blocks on the newest ten), the Today list and `counts.due_today` on `USER#<id>#TASKS`, `runner.purge_once` (hourly in `_tasks_loop`, setting `teams_task_runs_retention_days` 30), and `run_command(job, run_query=…)` so a Run now from the tab can answer a question (the consumer passes its agent runner).
Contract 3.15 synced from the edge (the `task_update` actions run_now | done | skip | plan_again | copy | repeat and the new row).
Behaviour change: `run_now` no longer moves the clock to now (it ran at once and is recorded); `done` on a repeating routine no longer ends it (the tick is logged, the clock stays).

Deploy: `./deploy.sh` (the migration runs inside it), before the edge step 15b image (an older edge still draws the Tasks tab from the same row; the new keys are extra).
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT version_num FROM alembic_version;" -c "SELECT trigger, outcome, count(*) FROM teams_task_runs GROUP BY 1,2 ORDER BY 1,2;"
```

Expected: `a7c3e5f9b012`; the existing runs all `time`.

Rollback: the previous checkout and a restart; `alembic downgrade f1b6c9d3e487` drops the four columns (by-hand runs and ticks keep their rows as plain runs).

Deployed 13 November 2026 as `f8fb4cf` (PR #24); the edge image `87af592` followed.

## D17a: advisory kinds (the Advisories tab becomes a grouped list; batch actions)

What it adds: migration `c4d8e2a7f531` (`advisories.kind` count_gap | chronic_short | empty_location | delivery_at_risk | other, and `advisories.about` {kind, id, label, where, area}; both backfilled from `source` so the open rows group on the first read), `app/services/advisories/kinds.py` (the kinds with their label and one-sentence meaning, `area_of`, the `about_*` builders, `kinds_payload`), the three agents naming their kind and subject (count findings name the item in words where the `item description` lookup knows it: "Ginger (104568) at A-01-02: counted 2, system 4"; unknown items keep their number), `store.reconcile` copying both, `advisory_payload` carrying `kind` and `about`, the ADVISORIES row's `kinds` list (label, meaning, open, high per kind with a live row), and `advisory_act` taking `advisory_ids` (up to 50; each advisory checked against the actor's sites and audited on its own; the answer "acknowledged 11 · 1 not yours to change"; a single id answers as before).
Contract 3.16 synced from the edge (the `advisory_act` payload doc; `minutes` named, which the doc had called `until`).
Behaviour change: count titles gain the item's description where one is known; nothing else changes for an older edge, which ignores the new keys.

Deploy: `./deploy.sh` (the migration runs inside it), before the edge step 17b image.
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT version_num FROM alembic_version;" -c "SELECT kind, count(*) FILTER (WHERE status='open') AS open, count(*) AS total FROM advisories GROUP BY 1 ORDER BY 1;" -c "SELECT about->>'label' AS label, about->>'area' AS area FROM advisories WHERE status='open' ORDER BY opened_at DESC LIMIT 5;"
```

Expected: `c4d8e2a7f531`; every row a kind other than `other` (count_gap, chronic_short, empty_location; delivery_at_risk among the resolved); labels "Item <number>" until the next stock run names them.

Rollback: the previous checkout and a restart; `alembic downgrade a7c3e5f9b012` drops the two columns (an edge at step 17b then shows every advisory under "Other findings").

Deployed 14 November 2026 as `aedffcb` (PR #25): migration `c4d8e2a7f531` at head, three units active, health 200, the open rows backfilled (37 count gaps, 10 chronic shorts, 15 empty locations); the edge image `17b0340` followed.

## D18a: Ask as a guided assistant (the catalogue, the action registry, demo actions, forecast and usage)

What it adds: `app/services/ask/` (the words and the wiring of Ask in one place: `catalogue.py` with the seven topics and their starters, filtered per record by `visible_topics`, and the "What can I ask" answer; `actions.py`, the registry of nine actions from which the agent's tools are generated, one `propose_<id>` per real action and one `demo_action` for the three demo stubs, each required argument with the question asked back when it is missing; `forms.py`, every form builder and `unwrap`, which turns a confirmed form `{form_id, values, via}` into its command's own fields; `slash.py`, the composer's commands read on the backend into the words the model sees; `demo.py`, the `demo_act` command that writes one audit line marked demo and changes nothing; `mirror.py`, the `ASK_CATALOGUE` row written with the access rows), the real actions change_user (`access_set_user`), decide_request (`access_decide`) and set_group (`group_set`), the forecast tool `forecast_ahead` (the Home card's figures as rows), the usage tool `app_usage` (people active, questions per day, top askers, actions, acknowledgements; for records that manage users or approve), `find_advisory` (an open advisory by the words of what it is about), the consumer answering "what can I ask" and slash commands before the model, and the profile's Actions, Advisories and Planning rules (snapshot regenerated once).
Contract 3.17 synced from the edge: the `topics` block, `FormField.labels`, `FormBlock.command_kind` and `mode`, the receipt status `demo`, the `demo_act` command, the `ask_catalogue` row.
No migration. Behaviour change: a confirmed form may arrive wrapped; the three step 8 and 10 proposers answer exactly as before.

Deploy: `./deploy.sh`, before the edge step 18b image (an older edge ignores the new block and the new fields; its forms still arrive unwrapped and are read as before). The consumer writes the catalogue row on its first access sweep.
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT action, decision, count(*) FROM audit_events WHERE action LIKE 'demo.%' GROUP BY 1,2;"
```

Expected: no rows until someone confirms a demo form; afterwards `demo.<action>` with decision `demo` only.

Rollback: the previous checkout and a restart; nothing in the schema changed.

Deployed 14 November 2026 at 17:42 UTC as `797a9f0` (main, PR #26): no migration (head `c4d8e2a7f531`),
`fastapirag`, `fastapirag-worker` and `fastapirag-teams-consumer` active, health 200, the
`ASK_CATALOGUE` row written for tmp-live (version 18.1, 7 topics, 9 actions, 3 shortcuts). The edge
step 18b went live as image `31fb5df` right after.

## D19: the presentation policy (the shared fragment, chart_from_data, kpis and findings, the golden set)

What it adds: `app/agent/profiles/fragments/presentation.md`, the presentation and communication policy every profile includes through the new optional `includes` key (the loader composes it into the prompt and versions the profile over its file plus its fragments; `scripts/profile_snapshots.py` regenerates the snapshots); `analytics.yaml` loses its inline "Presenting the answer" block; `chart_from_data` (`app/agent/presentation/charts.py`: the chart is drawn from the last data call's own result at answer time, the kind from the shape of the data, so no chart number is ever retyped); `show_kpis` and `state_findings` (the `kpis` and `findings` blocks, read by the figure guards, a finding's source held to the tools that answered); blocks in reading order with the cap at eight; `strip_tables` keeps bullets that carry fewer than two figures; the golden harness gains a `blocks` expectation and fifteen cases from the questions asked in Teams, `plant_analytics(week=True)` adds two earlier days, and `scripts/golden_questions.py` registers the presentation and actions namespaces (it raised before) and ends with a score line.
Contract 3.18 synced from the edge: `KpisBlock`, `FindingsBlock`.
No migration. Behaviour change: answers may carry `kpis` and `findings` blocks and a `## ` heading or bullets in the text; every profile's version changes (the prompt did).

Deploy: AFTER the edge step 19a image (an older edge drops the two blocks on arrival and stores the answer without them). `./deploy.sh`, then restart the three units.
Check on the server:

```
PGPASSWORD=rag psql -h localhost -U rag -d rag -P pager=off -c "SELECT profile_version, count(*) FROM agent_traces WHERE created_at > now() - interval '1 day' GROUP BY 1 ORDER BY 2 DESC;"
```

Expected: a new `profile_version` on every trace after the restart. Then in Teams, "Show deliveries per day this week as a chart" draws a line chart whose numbers equal the table's, with a findings block; "How many deliveries and lines were picked today?" shows key-figure tiles.
Score: `cd /opt/RAG-Pipeline/RAG-PipeLine && PYTHONPATH=. venv/bin/python scripts/golden_questions.py --profile analytics --tenant golden --plant` prints `analytics: N of 25 cases pass` (about 25 live questions of model cost); record N in the handoff.

Rollback: the previous checkout and a restart; nothing in the schema changed.

Deployed 14 November 2026 at 20:15 UTC as `d9fe52c` (main, PR #27), after the edge step 19a image
`d852963`: no migration (head `c4d8e2a7f531`), three units active, health 200, the analytics profile
composed at version `375506244fa1` with the fragment in its prompt. The score run needs the dev
requirements in the server venv (`venv/bin/pip install -r requirements-dev.txt`, owner's step);
without them `tests/golden/plant.py` fails on `import pytest`.

## D19c: the presentation policy tuned after the first live score

What it adds: the fragment calls for `chart_from_data` straight after the data call when a chart, graph or trend is asked for, and forbids the model's own arithmetic (a total, an average, a difference, a share); the fifteen step 19 golden questions name their date so the live run targets the planted day; a withheld answer keeps the blocks drawn from the data itself (the `chart_from_data` chart, the entities) and says so, instead of dropping a chart the model never typed. Snapshots regenerated (every profile's version changes).
Why: the first live score was 8 of 25 with most misses on empty November data, and the one live Teams chart question of the evening was withheld because the model totalled the rows by hand and skipped the chart tool.
No migration.

Deploy: `./deploy.sh`, then restart the three units. Then the score:

```
cd /opt/RAG-Pipeline/RAG-PipeLine && PYTHONPATH=. venv/bin/python scripts/golden_questions.py --profile analytics --tenant golden --plant 2>&1 | tail -30
```

Expected: the step 19 cases call `chart_from_data`, `show_kpis` or `state_findings` and draw their blocks; the score line well above 8 of 25. In Teams, "Show deliveries per day this week as a chart" draws the line chart with findings and no withheld sentence.

Rollback: the previous checkout and a restart.

Deployed 14 November 2026 at 20:50 UTC as `b78b564` (main, PR #28): three units active, health 200,
profile version `aa0de2ab87bc`. Live score: `analytics: 17 of 25 cases pass`, from 8 of 25 before the
tuning; the remaining misses are read in the handoff's D19c paragraph.

## D19d: the routing sentence and the tools named as calls

What it adds: one sentence in the analytics profile ("how many deliveries", "deliveries per day", "deliveries by volume" count the deliveries picked: `aggregate_releases`, never `at_risk_history`), and the fragment names each shape's tool as a call and says a tool is called, never written out. Snapshots regenerated; no code change, no migration.
Why: the second live score was 17 of 25; two misses and the owner's own Teams chart question went to the at-risk tool, and once the model wrote `show_kpis` as prose.

Deploy: `./deploy.sh`, then the score command of D19c. Expected: "Show deliveries per day … as a chart" calls `aggregate_releases` then `chart_from_data`; the score above 17 of 25.
## D19e: written-out calls lifted, the shape drawn by the lift

What it adds: `app/agent/presentation/written_calls.py` lifts a presentation call the model wrote as text (`state_findings findings: [...]`, `suggest_followups items: [...]`, `chart_from_data title: ... measure: ...`) off the answer into the trace, so it is validated, guarded and drawn as a real call and the reader never sees raw arguments; `charts.auto_chart` and `charts.auto_kpis` draw the shape when the model drew none: a line for a trend of three or more points, a bar for a ranking the model asked for (2 to 12 groups), key-figure tiles for a single total. Golden expectations that gain a chart or tiles updated.
Why: the owner's two observations on 14 November: a chart should appear without asking for one when the data has a shape, and the findings arrived as raw text (the live model wrote the calls as prose after the chart).
No migration.

Deploy: `./deploy.sh`, then the score command of D19c. Expected: "show me the top 10 pickers" (no "chart" in the words) draws the bar chart; a written-out `state_findings` becomes the findings block; the score above 17 of 25.

Rollback: the previous checkout and a restart.

## D20: who was active, when (the wms_activity tool, the heat map, the People and devices topic)

What it adds: `app/services/analytics_agent/activity_tools.py`, the `wms_activity` tool over `log_transactions` on the tenant's clock (distinct users, devices, calls and errors per hour, day or week, or by user, device, warehouse or method; first and last call per person; the day-by-hour matrix for an hour grain over up to 62 days), with five recipes in the tool text; the chooser reads its rows (measures users, devices, calls, errors), `charts.auto_heatmap` draws the heat map from its matrix and `auto_kpis` the totals as tiles; `heatmap` sits after `chart` in the reading order, and a withheld answer keeps the shapes the lift draws from the data; the analytics profile sends "who was active", "users per hour", "devices in use", "logged in" to it and the fragment names the heat map (snapshots regenerated); the catalogue gains "People and devices" (six starters, open to everybody), version 20.1; four golden cases over `plant_activity`.
Contract 3.19 synced from the edge: `HeatmapBlock`.
No migration. The queries read `log_transactions` by `customer_code` and `started_at` (both indexed); a 92-day window on tmp-live is about 650,000 rows grouped in Postgres.

Deploy: AFTER the edge step 20a image (an older edge drops the heat map on arrival). `./deploy.sh`. The consumer writes the catalogue row 20.1 on its next access sweep.
Check: in Teams, "Show active users per hour for the last 2 weeks" draws the heat map; "How many different users were active each day in the last 2 weeks?" the line with findings; "Who was active today, and when did each person start and finish?" a table; Browse shows "People and devices". Then the score command of D19c.

Rollback: the previous checkout and a restart.

## D20b: the data's shape wins; a formatted number is one figure

What it changes: `presentation_figures` passes table cells to the guards as whole words, one per line, so "1,928" is one figure (it was "1" and "928" and withheld correct answers); `blocks_from_trace` draws the heat map for any day-by-hour grid (replacing a model's chart of hours) and the trend chart for any time series of three or more points (beside a model's table), while rankings and totals still wait for the model to draw nothing; `charts.chart_from_result` keeps a long series' newest 48 points and marks the title "(latest 48 of N)".
Why: three live answers on tmp-live on 14 November (21:17 withheld for a comma, 21:27 a table with no chart, 21:19 a 48-hour line instead of the heat map), replayed over the production result before and after.
No migration, no contract change, no edge change.

Deploy: `./deploy.sh`. Check in Teams: "How many different users were active each day in the last 2 weeks?" gives the line chart (and a table if the model draws one); "Show active users per hour for the last 2 weeks" gives the heat map every time. Then the score command of D19c.

Rollback: the previous checkout and a restart.

## D20c: devices per day, the people table, the live score judged by what a person sees

What it changes: `wms_activity` keeps the series by time when a grain is asked together with a cut and returns the cut as `breakdown`; the tool description names "devices per day: grain day, no by"; `charts.auto_measure` charts devices for a device cut; `charts.auto_people_table` and the lift draw the people table from the tool's list when the model drew none; `tests/golden/harness.py` judges a live case that names its blocks by those blocks and its data tools.
Why: the live check of 14 November: "devices per day" answered with a total twice; "who was active today" sometimes came as bullets; the score counted another route to the same picture as a miss.
No migration, no contract or edge change.

Deploy: `./deploy.sh`. Check in Teams: "How many devices were in use per day in the last month?" draws the line of devices per day; "Who was active today, and when did each person start and finish?" draws the people table. Then the score command of D19c.

Rollback: the previous checkout and a restart.

## D21: the story of one delivery in Teams Ask (delivery_story)

What it adds: `delivery_story` in the analytics namespace (`app/services/analytics_agent/story_tools.py`), the log explorer's `trace` for one delivery, order, item, request id or pick-list line on the warehouse's clock, with the picks by outcome, the picks not exact, the issues with their message, the packages and the photos, and no LAN links; the lift draws the tiles and the transaction table from its rows (`charts.auto_story_table`, `auto_kpis`); the analytics profile routes "the details of ONE delivery …" to it first and keeps the at-risk tools for the van, the timing and the risk (snapshots regenerated).
Why: on 15 November "give me the details of this delivery number 33330" told the full story in the log explorer (logspace profile, `trace`) and only the at-risk status in Teams (analytics profile, `explain_at_risk_delivery`).
No migration, no contract or edge change.

Deploy: `./deploy.sh`. Check in Teams: "give me the details of this delivery number 33330" answers with the picks, the over-picks, the soft error, the tiles and the transaction table; "is delivery 33330 at risk" still answers from the at-risk board.

Rollback: the previous checkout and a restart.

## D22: facts as bullets, the order named as sorted

What it changes: the table stripper receives the rendered table and keeps up to four fact bullets (with their sub-points), removing only a longer list or a bullet that only repeats a whole table row; the policy fragment asks for a lead sentence and one bullet per fact, at most four, and to name the order the tool sorted in; snapshots regenerated.
Why: the owner asked for facts as bullets instead of one paragraph (15 November); the old stripper would have deleted any bullet with two figures beside a table; a live answer called the shortest time "the longest".
No migration, no contract change. Deploy after D21 (it is cut from D21). The edge's nested-bullet rendering may go before or after; an older edge draws a nested bullet at the first level.

Deploy: `./deploy.sh`. Check in Teams: "How long does a line take per picker today?" answers with a lead sentence and up to four bullets beside the table, and calls the smallest median the shortest.

Rollback: the previous checkout and a restart.

## D23: answer polish (no one-point chart, no follow-ups in the text, facts and labels)

What it changes: a chart needs at least two points, both for `chart_from_data` (dropped at the lift) and for `show_chart` (refused with a reason the model can act on); the follow-up questions the chips carry are taken off the answer text, with their lead-in ("**Follow-up questions:**", "You could ask:") and a rule line above it; the policy fragment asks that each bullet is a fact from the data, not a note on the method, that each count goes by its own label (packages are not photos), and that follow-ups are never also written in the text; snapshots regenerated.
Why: the owner's live check of 15 November found a one-bar chart (only BCHAM settled on a Sunday), follow-up questions written as text beside the chips, a bullet that only described the measurement, and "9 packages" for delivery 33330 where the data has 8 packages and 9 photos.
No migration, no contract change, no edge change.

Deploy: `./deploy.sh`. Check in Teams: "How long does a line take per picker today?" with one picker settled shows no chart; "Show deliveries per day this week as a chart" shows the chart and the follow-ups only as chips; "give me the details of this delivery number 33330" says 8 packages and 9 photos.

Rollback: the previous checkout and a restart.

Deployed 15 November 2026 23:30 as `c9ddb1c`; live check passed for the three questions above; four follow-up observations are recorded in the handoff as step 24.

## D24: answer integrity (finished answers kept, figures read right, totals from the tool)

What it changes: the step limit lets ten tool rounds end with an answer; a closing remark with no figure gives way to the answer written beside the last drawing call; the figure guard reads "1,042" in a result whole, ignores the year of a written date and checks a figure before a full stop; a bulleted follow-up lead-in, a dangling one above the table and "[… chart …]" placeholder lines are removed; a grouped aggregate returns `overall` (sums over all groups) with a note; the follow-up acknowledgement ends the turn and a run that only repeats presentation calls after it keeps its answer; the delivery story gives each not-exact pick its difference.
Why: the live check of 15 November and the live scores after D23 (answers lost at the limit or to a remark, forecast answers wrongly withheld, totals the model added up, tool loops).
No migration, no contract change, no edge change. Merge the D23b hotfix (PR #38) first, or this PR alone, which contains it.

Deploy: `./deploy.sh`. Check in Teams: "Which products were short today, and by how many units?" gives the total from the tool and is not withheld; "What is coming tomorrow?" is not withheld; "give me the details of this delivery number 33330" answers in full; "what is available?" answers.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 09:07 GMT together with D25 as `e884752` (main; PR #39 merged as `124940c`, PR #40 as `e884752`): three units active, health 200; live check through the real model on tmp-live, twice each, all ended normally with no retry, none withheld.

## D25: the ranking keeps its chart, all-groups stats from the tool

What it changes: the auto chart is drawn from the last grouped result, so reading the release list after a ranking keeps the ranking's chart; a grouped aggregate's `overall` also carries each stat asked for (a median over all rows, not a median of the groups' medians).
Why: the D24 live runs lost the short-products chart whenever the model read the release list, and one answer called one picker's median the median across all pickers.
No migration, no contract change, no edge change. Merge D24 (PR #39) first.

Deploy: `./deploy.sh`. Check in Teams: "Which products were short today?" shows the ranking chart; "How long does a line take per picker on 13 November 2026?" gives 51.1 s as the median across all pickers, with each picker's own median in the bullets.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 09:07 GMT as `e884752` with D24. Live: line time per picker on 13 November gave 51.1 s as the median across all pickers 2 of 2; products short on 13 November 2 of 2 answered, one quoting the 352-unit total from the tool; golden 30 of 39 with no run out of tool calls, the short-products case drawn.

## D26: the delivery story covers every day the delivery appears

What it changes: with no day named, Teams' `delivery_story` gathers the 7 days up to today at once, so a delivery picked one day and loaded the next is one story; the story says each day, and dates its times when it spans days.
Why: on 16 November delivery 33330 got the picking in the log explorer (its page filter was 15 November) and only the loading in Teams (today first, then it stopped).
No migration, no contract change, no edge change; the log explorer is unchanged.

Deploy: `./deploy.sh`. Check in Teams: "give me the details of this delivery number 33330" tells both the picking of 15 November and the loading of 16 November; "give me the details of delivery 33330 on 15 November 2026" keeps to that day.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 10:10 GMT as `cd3191c` (main, PR #41): three units active, health 200; live check passed (33330 tells both days; a day named keeps to it; the at-risk question uses the at-risk tool).

## D27: the release tools read the window the model meant

What it changes: a bare `end` date covers that whole day; with no start or end, a `where` on event_time or business_date sets the window instead of the last 24 hours; quoted filter values are read. Both release tools share the one rule.
Why: the golden run of 16 November answered "I could not get that" for a past day and "No items were short" for a week whose last day had 11 units short.
No migration, no contract change, no edge change.

Deploy: `./deploy.sh`. Check in Teams: "how many picked on 13 November 2026" gives the day's figure; "which items were short in the week to 13 November 2026?" includes the 13th.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 10:40 GMT as `92ded18` (main, PR #42): three units active, health 200; live check passed (a past day's total, a week to a date including that date, 33330, products short today).

## D30: the Teams Forecast page's row

What it changes: a new consumer loop writes one `site_forecast` row per site and for "all" every five minutes, built from the functions behind `/analytics/forecast/*`; contract 3.20.
Why: the edge's Forecast page (edge step 24), opened from Overview's forecast card, reads it.
No migration. Deploy before or with edge step 24: an edge without the row says "no forecast page yet"; rows without the edge are simply unread.

Deploy: `./deploy.sh` (it restarts the Teams consumer, which starts the loop). Check: after five minutes `SITE#all#FORECAST` exists for tmp-live, and the consumer log has no `teams.forecast_detail_failed`.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 13:04 GMT as `5209c82` (main, PR #43) with edge `1780ac8`: three units active, health 200; the four forecast rows of tmp-live written in about 22 s (255 to 260 KB) and rewritten five minutes later; Overview's rows unaffected.

## D31: the Teams Pick releases page's row

What it changes: the detail loop also writes one `site_releases` row per site and for "all" every five minutes, with three windows computed by a Python port of the web Pick releases page's arithmetic; contract 3.21.
Why: the edge's Pick releases page (edge step 25), opened from Overview's shape of the day, reads it.
No migration. Deploy before or with edge step 25: an edge without the row says "no pick releases page yet"; rows without the edge are simply unread.

Deploy: `./deploy.sh`. Check: after five minutes `SITE#all#RELEASES` exists for tmp-live, and the consumer log has no `teams.releases_detail_failed`.
After a change to the arithmetic, run the parity check in `scripts/releases_parity/README.md`.

Rollback: the previous checkout and a restart.

Deployed 16 November 2026 15:34 GMT as `1cb53ff` (D33b list backfill, D34 all-sites row); D31 and D33 at 14:23 as `e8a471a`. Then the pick-line repair (see the handoff). Rollback: the previous checkout (`91285b7` keeps the Pick releases rows) and a restart.

Deployed 16 November 2026: D35 (`8b6292f`, hourly forecast per warehouse) at 16:20; the forecast run as of 15 November then completed at 22:35 (a run takes about 6 hours and a restart kills it, so no deploy between 03:00 and about 09:30). D37 (`9a7217e`, Overview snapshot drops Needs attention and Customers today) at 23:26; `e0734fe` (all-sites volume line without headcount) at 23:34. Rollback: the previous checkout and a restart.
