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

