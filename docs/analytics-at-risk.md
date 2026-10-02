# Deliveries at risk (chunks 143-150)

A board of the deliveries that are behind their route's normal rhythm before the van leaves, for each tenant with a `delivery_route` settlement.
It learns the rhythm from history, flags what is behind, lets a person mark a flag as checked, and keeps an honest record of whether flagged deliveries really ended late.

## What it reads

Four bounded reads, in `app/services/analytics_at_risk/board_store.py`, none of them against `log_transactions`:

1. The `delivery_route` settlement: one settled row per delivery as `GetNextDeliveryByRoute` last saw it, with the WMS departure date and time (`resp.DeparatureDate`, `resp.DeparatureTime`, the WMS spelling), the route and the customer.
   The latest observed departure wins, because departures move.
2. The `pick_release` settled rows grouped by `delivery_number`: lines confirmed, lines that moved stock, lines short, and the last pick.
3. The `pick line` lookup read the other way round: how many pick lines name this delivery.
   This is the expected count, known a few minutes before picking starts, because `ListPickLinesByUser` lists the lines first.
4. The facts of the last 36 hours for `ConfirmPickLine`, `NewDeliveryPackage`, `LoadDeliveryPackage` and `LoadDeliveryPackageList`: packages known and loaded.
   The packages known for a delivery are the distinct package numbers on its pick confirmations plus any package created by hand; measured live, most packages are born at pick time and `NewDeliveryPackage` alone undercounts them on three deliveries in four.
   The milk load's deliveries live inside the `PackagesToLoad` JSON string and are parsed in Python.
   The loading docks seen on the loads name the routes that have a loading step at all.

### Routes without a loading step

The BRILA routes (the Gatwick run among them) are picked and packed but never scanned onto a van: not one load in thirty days of history.
Judging them on loading would flag every one of them every day, so the board decides per route whether loading is expected: a route has a loading step when it loaded anything in the last 36 hours or when its profile holds any loaded delivery.
A delivery on a route without a loading step is judged on picking alone, never reaches `at_risk` through loading, and closes as `picked_in_time` or `picked_late` by its last pick instead of `loaded_in_time`, `loaded_late` or `never_loaded`.
The decision is kept on the delivery row (`loading_expected`, migration `f1c2d3e4a5b6`).

## The rule

A delivery is judged against its departure instant and its route's thresholds (`model.tier_for`):

- `watch`: picking is still open and the departure is closer than the route's pick lead.
- `at_risk`: loading is still open and the departure is closer than the route's load lead.
- `late`: the departure has passed and either is still open.

A lead is learned per route as a coverage quantile over the closed deliveries of the last 28 days: the lead that nine in ten loaded deliveries met or beat, which is the tenth percentile of lead minutes.
Below 20 closed deliveries the learned lead is unknown.
The effective threshold is the larger of the learned lead and the tenant's floor (120 minutes to load, 180 to pick by default), so a slow week can never teach the system to hide risk.
With the expected line count unknown, only a delivery with no picks at all counts as picking open, so a lookup gap never flags every delivery.

## What it writes

- `analytics_at_risk_deliveries`: one row per delivery per departure date.
  The worker writes the state (progress, tier, history, thresholds, outcome); the API writes the five check columns.
  The two writers touch disjoint columns.
- `analytics_at_risk_checks`: the acknowledgement ledger, append-only (`checked`, `unchecked`, `reopened`).
- `analytics_at_risk_route_profiles`: one row per route per day with what the history taught.
- `analytics_at_risk_settings`: the tenant's floors and knobs; no row means the defaults.
- `analytics_at_risk_tenant_state`: one row for `/status`.

## The three plain words

Every closed delivery is also given one of three words (`model.category_for`, mirrored in SQL by `delivery_store.category_expr` so a filter and a count agree with the row):

- `missed`: the van left without it. A package was never loaded, or lines were never picked.
- `delayed`: it got away, but behind the route's rhythm (flagged Watch or At risk before the departure) or after the departure time.
- `fine`: in time and never flagged.

Two edge cases exist for honesty: `unknown` when the board lost sight of the delivery before it closed, and `open` while it has not closed.
The delivery row also keeps the picking screens its lines went through (`transaction_names`, migration `a2b3c4d5e6f7`), so a supervisor can look at one kind of picking at a time; a delivery that spans two kinds appears under both.
`GET /analytics/at-risk/history` filters on `category` (a comma list), `transaction` and `delivery` (the start of a number), and returns `counts` per category and the `transactions` seen over the whole range under every filter except the category one, which is what the stacked bar beside the table is drawn from.

## Acknowledgements

A person marks a flagged delivery as checked with an optional note.
The web app sends its self-declared logspace name; the Teams tab sends the signed-in display name.
The check is kept when the tier later rises: the row is counted re-opened, the ledger says so, and the screens show "checked at Watch, now At risk".

## Outcomes

A row closes once its departure plus a grace (180 minutes) has passed: `loaded_in_time` when every package was on the van before the departure, `loaded_late` when the last one went on after, `never_loaded` otherwise.
`outcome_lead_min` is the minutes between the last load and the departure, negative when late.
The WMS records no actual departure: the standard load screen answers OK per package, the milk list answers that all packages are loaded, and sign-off only ends the session.
The nearest real event is the last package scanned onto the route's loading dock on the departure day, kept on every row of that route and day as `route_loaded_at` (migration `b3c4d5e6f7a8`) and shown beside the target departure as "route loaded".
Each row also keeps its own `last_load_at`, the moment its last package went on the van.
A row still open a day after its departure closes as `unknown`.
`GET /analytics/at-risk/accuracy` scores flags against outcomes: precision is the share of flagged deliveries that really ended late, recall the share of late deliveries that had been flagged.
Both are null, not zero, when nothing is scorable.

## Backfilling the days before the switch-on

The worker can only judge a delivery whose routing call was folded after the departure fields were approved, because approving a field never rewrites old facts.
So on the day the feature is switched on the history is empty, and the first closed rows appear three hours after the next departures.
`app/services/analytics_at_risk/backfill.py` fills the days before that, run once by hand:

```
cd /opt/RAG-Pipeline/RAG-PipeLine && PYTHONPATH=$PWD venv/bin/python -m app.tools.at_risk_backfill tmp-live 2026-09-25 2026-10-02
```

For each day, oldest first, it reads the departures straight from the routing calls' response text in `log_transactions` (the latest call per delivery wins, as on the board), reuses the board's reads for picks, expected lines, packages and loads up to the day's close, replays the tier rule over the clocks (`model.replay_tiers`: one tick after each lead is crossed, at each completion, at the departure and at the close), and writes a closed row per delivery with the outcome, the three plain words and the tier story.
Each day is judged with the profiles learned up to the day before, and its own profile is learned once it is written, so the words match what the live rule would have said.
Rows written this way carry `reconstructed = true` (migration `c4d5e6f7a8b9`).
They fill the history and teach the route profiles, but the accuracy score leaves them out, because their flags were computed from the clocks rather than observed minute by minute.
A live row is never overwritten, a day whose deliveries have not all closed is skipped, and each day commits on its own, so an interrupted run resumes by running it again.
The raw log retention bounds how far back it can go: 60 days of `log_transactions`, and on tmp-live the ingest only became complete on 14 Sep 2026.

## When it runs

`app/services/workers/analytics_at_risk_worker.py`, behind `ANALYTICS_AT_RISK_WORKER_ENABLED`, on the worker process.
It wakes every `ANALYTICS_AT_RISK_POLL_SECONDS` (60) and evaluates every tenant with an enabled `delivery_route` settlement whose settings row is not switched off.
Once per tenant-local day, after `ANALYTICS_AT_RISK_PROFILE_HOUR_LOCAL` (03:00), it learns the route profiles as of the day before.
`POST /api/v1/analytics/at-risk/evaluate` runs one pass now; it is sub-second, so there is no run to poll.

## Endpoints

All under `/api/v1/analytics/at-risk`, tenant-scoped by `X-Customer-Code`: `board`, `deliveries/{n}/check` (POST and DELETE), `history`, `checks`, `accuracy`, `settings` (GET and PUT), `routes`, `status`, `evaluate`.
`app/api/v1/analytics_at_risk.py` documents the parameters and shapes.
The board carries `stale: true` once the worker has missed three polls, so a quiet board is never mistaken for a calm one.

## The Teams Home block

`app/services/teams/home_at_risk.py` writes an `at_risk` block into the Home snapshot every minute: a summary line, up to twelve flagged deliveries worst first with every figure as text, a person's check, and the accuracy line once twenty departures have closed.
A "Mark checked" action in the tab travels as a `command` on a `QuestionJob`; the consumer records it through `app/services/teams/commands.py` and never runs the agent for it.

## Enabling on the server

Tenant configuration first, through the existing API with `X-Customer-Code: tmp-live`:

1. `GET /api/v1/analytics/registry/fields?limit=2000`, find the two rows for method `GetNextDeliveryByRoute`, source `response`, fields `resp.DeparatureDate` and `resp.DeparatureTime`, and `PATCH /api/v1/analytics/registry/fields/{id}` with `{"captured": true, "reviewed_by": "amin"}` for each.
   The fold re-folds the retained 60 days.
2. `PATCH /api/v1/analytics/lookups/pick%20line` adding `DeliveryNumber`, `LineStatus` (`latest_wins`, not stable) and `ExpectedQty` from the same list source, then `POST /api/v1/analytics/lookups/pick%20line/backfill?days=60`.
3. Once the re-fold has passed today, `POST /api/v1/analytics/settlements` with the `delivery_route` document (`tests/at_risk_fixtures.py` holds the same declaration as code).
4. `GET /api/v1/analytics/at-risk/status` must show every readiness flag true.

Then the code:

1. Stop the worker: the facts index on a partitioned parent is a plain build.
2. `alembic upgrade head` (migration `e9a4c7d21f36`).
3. Set `ANALYTICS_AT_RISK_WORKER_ENABLED=true` in `.env`; only the worker process starts loops.
4. Restart `fastapirag`, start `fastapirag-worker`, restart `fastapirag-teams-consumer`.
5. Read `GET /api/v1/analytics/at-risk/board` and `/routes`: on day one every route runs on the floor until twenty closed deliveries accrue.
6. Optionally run the backfill above for the days before the switch-on, so the history and the route learning start full.

## Assumptions

- Default floors of 120 minutes to load and 180 to pick, a sample floor of 20, a 28 day window, a 180 minute close grace and 0.90 coverage.
  All are per-tenant settings.
- Expected lines come from the pick-line lookup; a delivery whose lines were never listed shows `expected: null`.
- A delivery is on the board only once a routing call has named it; Milk deliveries listed only by `ListDeliveriesByRoute` are not, because that list response is truncated at 500 characters.
