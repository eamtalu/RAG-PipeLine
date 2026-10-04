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
4. The facts of the last 36 hours for `ConfirmPickLine`, `LoadDeliveryPackage` and `LoadDeliveryPackageList`: packages known and loaded.
   The packages known for a delivery are the distinct package numbers on its pick confirmations that moved stock.
   Measured over a live week, 3,160 of those 3,167 packages were loaded; a package made by hand through `NewDeliveryPackage` that no pick ever filled is an empty box (2 of 11 were loaded), and a short line's package number is noise, sometimes another delivery's.
   Rule version `at-risk-v3` made that explicit; v2 counted both and half of its "never loaded" rows were one of those.
   The milk load's deliveries live inside the `PackagesToLoad` JSON string and are parsed in Python.
   The loading docks seen on the loads name the routes that have a loading step at all.

### Routes without a loading step

The BRILA routes (the Gatwick run among them) are picked and packed but never scanned onto a van: not one load in thirty days of history.
Judging them on loading would flag every one of them every day, so the board decides per route whether loading is expected: a route has a loading step when it loaded anything in the last 36 hours or when its profile holds any loaded delivery.
A delivery on a route without a loading step is judged on picking alone, never reaches `at_risk` through loading, and closes as `picked_in_time` or `picked_late` by its last pick instead of `loaded_in_time`, `loaded_late` or `never_loaded`.
The decision is kept on the delivery row (`loading_expected`, migration `f1c2d3e4a5b6`).

## The rule: the van is the clock

The WMS departure time (11:30 on weekdays, 12:00 on Saturdays) is a planning time.
Measured over a live week, every BRI route's van was fully loaded four to five hours before it (the dock's last scan landed between 05:50 and 07:50), so a lead measured back from 11:30 said "at risk" and "4 h 31 before" about the same delivery.
What a route does have is a rhythm: its dock's last scan lands at much the same time of day, day after day (BRI04 by 06:56 on nine days in ten, BRI06 by 07:48, BRI08 by 07:31).

Each route learns the time its van is usually ready: the coverage quantile (0.90 by default) of the daily van-ready time over the last 28 days, unknown below 5 days of history.
A delivery is judged against that instant on its own departure day (`model.tier_for`, with the clock from `profile_store.clock_for`):

- `watch`: picking is not finished and the van is usually ready within the warning window (30 minutes by default).
- `at_risk`: packages are still off the van and the van is usually ready within the window, or the usual time has passed.
- `left_behind`: the usual time has passed and the dock has been quiet for the gone window (20 minutes by default), so the van is taken as gone, and this delivery is not on it.

A route with too few days of history has no rhythm yet, and its WMS departure stands in (`usual_ready_source = wms_departure`), which flags late but never falsely.
A route that never scans a load (the BRILA runs) learns from its last pick of the day instead and is judged on picking alone.
Nothing is flagged before the warning window, however slow the morning looks.

A pick line is done once it is confirmed, whether it moved stock or was declared short: a short pick is the warehouse's answer for that line, not a line still waiting.
With the expected line count unknown, only a delivery with no confirmation at all counts as picking open, so a lookup gap never flags every delivery.
Rule versions: `at-risk-v1` judged on lines that moved stock and flagged one closed delivery in four; `at-risk-v2` counted confirmed lines; `at-risk-v3` knew a package only from a pick that moved stock; `van-v1` made the van the clock.

### Routes without a loading step

The BRILA routes (Shake Shack Gatwick daily, BRILA1/2/3) are picked and packed but never scanned onto a van, measured over 30 days.
Their deliveries carry `loading_expected = false`, the last pick decides the tier and the outcome (`picked_in_time` / `picked_late`), and the route's clock is the time its picking is usually done.

## What it writes

- `analytics_at_risk_deliveries`: one row per delivery per departure date.
  The worker writes the state (progress, tier, history, the van clock, outcome); the API writes the five check columns.
  The two writers touch disjoint columns.
- `analytics_at_risk_checks`: the acknowledgement ledger, append-only (`checked`, `unchecked`, `reopened`).
- `analytics_at_risk_route_profiles`: one row per route per day: when its van is usually ready, learned from its days.
- `analytics_at_risk_settings`: the tenant's windows and knobs (warning window, quiet window, days of history needed, coverage, learning window, close grace); no row means the defaults.
- `analytics_at_risk_tenant_state`: one row for `/status`.

## The three plain words

Every closed delivery is given one of three words (`model.category_for`, mirrored in SQL by `delivery_store.category_expr` so a filter and a count agree with the row):

- `missed`: the van went without it. A package was never loaded, or lines were never confirmed (picked or declared short).
- `held`: it was on the van, but `held_after_min` minutes or more (60 by default) after the van's usual ready time, or after the WMS departure. The van ran noticeably late and this delivery was one of those still going on. The history shows "held the van".
- `fine`: on the van before the usual time.

Two edge cases exist for honesty: `unknown` when the board lost sight of the delivery before it closed, and `open` while it has not closed.
The tiers a delivery passed through on the way do not decide the word; the clocks do.
The delivery row also keeps the picking screens its lines went through (`transaction_names`), so a supervisor can look at one kind of picking at a time; a delivery that spans two kinds appears under both.
`GET /analytics/at-risk/history` filters on `category` (a comma list), `transaction` and `delivery` (the start of a number), and returns `counts` per word and the `transactions` seen over the whole range under every filter except the category one, which is what the stacked bar beside the table is drawn from.
Each row also carries `before_van_min` (minutes between its last load and the van being ready) and `van_late_min` (minutes the van ran past its usual time, negative when early).
`GET /analytics/at-risk/vans` aggregates the same rows per route per day: when loading started, when the van was ready, when it is usually ready, how late it ran and how many deliveries it carried, held or went without.

## Acknowledgements

A person marks a flagged delivery as checked with an optional note.
The web app sends its self-declared logspace name; the Teams tab sends the signed-in display name.
The check is kept when the tier later rises: the row is counted re-opened, the ledger says so, and the screens show "checked at Watch, now At risk".

## Outcomes

A row closes once its departure plus a grace (180 minutes) has passed: `loaded_in_time` when every package was on the van before the departure, `loaded_late` when the last one went on after, `never_loaded` otherwise.
`outcome_lead_min` is the minutes between the last load and the departure, negative when late.
The WMS records no actual departure: the standard load screen answers OK per package, the milk list answers that all packages are loaded, and sign-off only ends the session.
The nearest real event is the last package scanned onto the route's loading dock on the departure day, kept on every row of that route and day as `route_loaded_at` and shown as "van ready"; the dock's first scan is `route_loading_from`, "loading from".
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

For each day, oldest first, it reads the departures straight from the routing calls' response text in `log_transactions` (the latest call per delivery wins, as on the board), reuses the board's reads for picks, expected lines, packages and loads up to the day's close, replays the tier rule over the clocks (`model.replay_tiers`: one tick after the warning window opens, after the usual time, after the gone window, at each completion and at the close), and writes a closed row per delivery with the outcome, the three plain words and the tier story.
Each day is judged with the van clock learned up to the day before, and its own profile is learned once it is written, so the words match what the live rule would have said.
Rows written this way carry `reconstructed = true` (migration `c4d5e6f7a8b9`).
They fill the history and teach the route profiles, but the accuracy score leaves them out, because their flags were computed from the clocks rather than observed minute by minute.
A live row is never overwritten, a day whose deliveries have not all closed is skipped, and each day commits on its own, so an interrupted run resumes by running it again.
`--replace` deletes the range's reconstructed rows first and writes them again, which is how a rule change is applied to the backfilled history.
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

- Defaults: a 30 minute warning window, a 20 minute quiet window, 5 days of van history before a route's rhythm counts, 60 minutes past the usual time before a delivery "held the van", a 28 day learning window, a 180 minute close grace and 0.90 coverage.
  All are per-tenant settings.
- Expected lines come from the pick-line lookup; a delivery whose lines were never listed shows `expected: null`.
- A delivery is on the board only once a routing call has named it; Milk deliveries listed only by `ListDeliveriesByRoute` are not, because that list response is truncated at 500 characters.
