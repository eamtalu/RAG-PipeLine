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
