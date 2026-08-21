# Warehouse analytics platform: merged implementation plan

> **SUPERSEDED - merged into [`docs/analytics-ml-architecture/final_architecture.md`](../analytics-ml-architecture/final_architecture.md)**
>
> This document has been folded into the single canonical architecture document.
> Do not implement from this file. Kept for history only.
> Alembic migrations for this work should cite `docs/analytics-ml-architecture/final_architecture.md`.

## Context

The goal is an analytics platform over the live WMS log data on the Matrix host, delivering three things.
A real-time running total per item, folded as each record arrives.
Easy daily, weekly and monthly aggregation across multiple metrics, scalable to millions of rows.
A second pipeline for machine learning and agentic AI on the same foundation.

**This document supersedes two earlier plans and is the single source of truth.**

- `docs/plan/2026-08-11_00-11_real-time-warehouse-consumption-analytics.md` in the backend repo, whose core mechanism is adopted here largely unchanged.
- `~/.claude/plans/2026-08-11_09-30_analytics-platform-grains-metrics-ml.md`, whose critique and extensions are folded in.
- `~/.claude/plans/2026-08-10_19-26_realtime-warehouse-consumption-analytics-module.md` contributes its 55-metric catalogue and data-quality findings; its settled-only append design is **withdrawn** for reasons measured below.

**Done 2026-08-18:** this document now lives in the backend repo at `docs/plan/2026-08-17_22-32_merged-warehouse-analytics-platform-plan.md`, and the 2026-08-11 plan carries a superseded banner in both its `.md` and `.html`. Repo convention has Alembic migrations cite their plan document, so migrations written for this work should cite this path.

**Still outstanding:** neither plan file is committed to git yet, so the citation is not yet durable.

**Migration base, checked 2026-08-18:** deployed Alembic head is `e4b28f5c9107`, which is also the repo head across 43 revisions, so **there are no pending migrations**. The three notification revisions (`b3d914c7ea52`, `c7a02f68b1d4`, `d5c81b60a473`) that an earlier document warned were undeployed have since been applied, verified by the presence of `consumer_cursors`, `ix_log_transactions_created_at` and `notification_rules.cursor_at`. Phase 1 therefore lands on a clean head using `deploy.sh`'s ordinary pull, migrate, restart order.

## Why the design looks unusual

`log_transactions` is not an append-only table and not a time series.
It is a **mutable derived projection**: each transaction is stitched from roughly 15 raw log lines, and when a late line arrives the row is deleted and rebuilt, possibly with different values.

Measured on the live server:

| Measurement | Value |
|---|---|
| Transaction rows written more than 5 min after their own entries | **22,183 of 22,465 (98.7%)** |
| Sealed rows, average gap from newest entry to row write | **6,098 s (1.7 h)** |
| Sealed rows, worst gap | **19,312 s (5.4 h)** |
| Rows built promptly (within 60 s) | 126 (0.6%) |

So rebuilding is the norm, and a row is not final until hours after the event.
A naive additive consumer would count rebuilt rows again and produce plausible-looking wrong totals.
Any design that waits for rows to settle is also unusable, because sealing is computed against the newest log timestamp rather than the wall clock (`derive_transactions.py:535-538`), so the wait has no clock-based bound and stalls entirely when ingestion pauses.

**Consequence for implementation:** the version hash on each contribution is load-bearing, not an optimisation. With a 98.7% rebuild rate almost every re-diff must be absorbed as a no-op by a matching fingerprint, or the system generates a constant stream of pointless aggregate writes.

## Verified ground truth

Read from the live server and from code. Do not re-derive.

**Volume and shape.** 72,682 transactions, ~1,100,516 raw log entries, 1,474,393 parsed M3 records inside `log_entries.fields`. Peak 68 derived transactions/minute, 13 pick transactions/minute. Tenants `tmp-live` and `tmp-test`. 692 distinct items with consumption, 395 on the busiest day, against 25,008 in the item master, so **cardinality grows far more slowly than volume**.

**Aggregate scan cost on this hardware: ~2.5 µs per row.** 1.04 M rows took 2,541 ms. Planning cost is driven by partition count, not data volume: 7 ms against one partition versus ~85 ms against 21. Prepared statements amortise it to ~9 ms, and asyncpg prepares by default.

**Consumption source.** Filter on `method`, never `transaction_type`. `ConfirmPickLine` is 1:1 with `attributes->>'QuantityPicked'` across all 7,327 rows. `transaction_type` contains WMS-supplied placeholders (`xxxxxx`, `XXXXX`, `00xxxx`, `0050XX`), and `AddStockCountLine` carries real `CountedQuantity` under a placeholder type, so a `transaction_type` filter silently drops real data.

**Quantity fields.** `QuantityPicked` and `ExpectedQuantity` are 100% clean. `QuantityToBePicked` is 91% empty strings and `attributes.Weight` is 100% empty. Quantities are fractional (for example 8639.392735), so `NUMERIC` throughout, never float. `ExpectedQuantity` arrives as `30.000000` while `QuantityPicked` arrives as `30.0`, so comparisons must be numeric, never string.

**`ExpectedQuantity` is mutable per instruction, not an order-line total**, so fill rate is not derivable from it. Use `OIS100MI/LstLine.ORQA` from the M3 layer instead.

**Identity.** Re-verified against the current implementation on 2026-08-18, which **corrected an earlier statement in this plan that had it backwards.**

A *new* group's id is `uuid5(fixed_namespace, anchor_entry_hash)` (`derive_transactions.py:445`), where the anchor is the REQUEST entry if there is one and the earliest entry otherwise. A *rebuilt* group **inherits** its id from the transaction that owned the plurality of its entries, via `continuity.assign(...)` in `_resolve_ids` (`derive_transactions.py:555-582`). The 1.3% exposure figure in `continuity.py` is the measurement that **motivated** that fix, not a live exposure.

**But continuity is wired into only one of the three rebuild paths.** Traced through every `_persist` call site:

| Path | Deletes | Continuity supplied | Ids on rebuild |
|---|---|---|---|
| `regroup_window` (`:858`, `:895`) | all in the padded range, **sealed included** | **yes** | **inherited** |
| `regroup_incremental` (`:780`, `:801`) | unsealed only, no time bound | **no** | recomputed from anchor |
| `regroup_all` (`:677`, `:698`) | everything | **no** | recomputed from anchor |

So id stability holds for the **backfill and repair path**, which is the one the 1.3% measurement was about. It does **not** hold on the live incremental path or on a full rebuild, where an unsealed transaction gaining a late REQUEST line will change id.

Note the docstring at `derive_transactions.py:575-576` asserts "Only `regroup_window` can supply one, because only it frees transactions." That is **inaccurate**: `regroup_incremental:780` and `regroup_all:677` both delete transactions. Worth reporting upstream, since the comment would mislead the next reader into thinking the live path is protected.

**Consequence for this design: none, and that is the point.** The range diff reverses a vanished id and applies a new one, so an id change is handled identically to a merge or split. This is further evidence that the range diff is mandatory and a per-id upsert would be wrong. It does mean the ledger will see more identity churn than a reading of `continuity.py` alone suggests.

**Source uniqueness is `UNIQUE NULLS NOT DISTINCT (id, started_at)`, not unique on id.** Confirmed in the model (`log_transaction.py:43-44`, named `uq_log_transactions_id`) and in the migration that created it (`a1f6d70b3e92:126`). The model comment gives the reason: `started_at` is the partition key and is nullable, and a primary key would force it `NOT NULL`, making timestamp-less rows un-insertable. A live `pg_constraint` re-read was not possible on 2026-08-18 because the host was unreachable, but a duplicate-id check on 2026-08-17 returned zero rows.

**Live checks, run 2026-08-17.** NULL `started_at`: **0**. Duplicate id pairs: **0**. Transactions with NULL `ended_at`: **0**. Unsealed at any moment: 1,638. `log_regroup_pending`: 7,407 tickets, **0 pending, 0 abandoned, 0 retries**, roughly one every 70 seconds. Orphaned entries with no assignment row: **847 to 1,079**, all past the abandon window, from two files (`TMP-AZ-BEC02/eSmartServerLog.txt`, `TMP-AZ-BEC01/eSmartServerLog.txt`), of which **only 7 are pick-related**.

**Infrastructure.** Stock PostgreSQL 16, 48 available extensions, **no TimescaleDB, no Citus, no columnar, no hll, no tdigest, no pg_partman**. `work_mem` is 4 MB. `shared_buffers` 8 GB. `app/persistence/partitioning.py` supports **daily bounds only**. Frontend has **no chart library**. `next.config.mjs` rewrites specific API prefixes, so a new `/api/v1/analytics/*` route 404s until added.

## Adopted unchanged from the 2026-08-11 plan

These were correct and are preserved verbatim in intent.

The contribution ledger with delta application. The transactional dirty-window ticket. `NUMERIC(38, 9)` for quantities. Refusing Flink and Kafka with measured justification. Rejecting an additive cursor over `created_at`, database triggers, in-memory totals, direct aggregate queries per request, and WebSocket-first delivery. Revision polling before SSE. Keeping the four web workers read-only and running the loop in the singleton `app.worker`. Deploying schema first, then ticket publication with the worker disabled, then one tenant. Freshness indicators beside business metrics. `EXPLAIN (ANALYZE, BUFFERS)` evidence before finalising index order.

## The eleven fixes

### F1. Ticket bounds must be derived from the delete, not from the new entries

**Defect.** The plan requires ticket bounds to be "the same padded range Stage 2 used". `regroup_window` has explicit `[lo-pad, hi+pad]` bounds, but the live path `regroup_incremental` deletes `WHERE sealed IS FALSE` with **no time predicate** (`derive_transactions.py:779-780`). If bounds are inferred from the incoming entries, an older unsealed row that was deleted and rebuilt falls outside the ticket, is never re-diffed, and drifts permanently. This path runs every ~70 seconds.

**Fix.** Every publisher computes bounds from **the set actually freed**. In `regroup_incremental`, the unsealed ids are already selected into `unsealed_stmt` before the delete; take `min(started_at)` and `max(started_at)` over that same set and publish `[min - pad, max + pad]`. In `regroup_window`, publish the padded window it already computes. If the freed set is empty, publish no ticket.

**Invariant to test:** no transaction may be deleted by any path without a committed ticket whose range contains its `started_at`.

### F2. Reconciliation must check completeness against `log_entries`, not only the projection

**Defect.** The plan proves correctness by recomputing from `log_transactions` and comparing. Both the incremental total and the recomputation read the same projection, which is currently missing ~1,000 orphaned entries. **Reconciliation passes while under-counting.**

**Fix.** Two independent checks, both scheduled.

1. **Aggregate reconciliation**, as the plan describes: ledger-derived totals versus direct recomputation over `log_transactions` for the same window.
2. **Projection completeness**, new: count `log_entries` rows older than the abandon window with **no row in `log_entry_assignment`**, grouped by `source_file` and `mi_program`. Non-zero is a defect, and the current value is non-zero.

Surface both in `warehouse_analytics_state` and on the dashboard. Completeness failures are a source problem, not an analytics problem, but analytics is the only place that will notice.

**Immediate action, independent of this build:** run a scoped regroup over 2026-08-15 to 2026-08-17 for the two named files, and investigate the cause before assuming a regroup is a fix, because whatever caused it will recur.

### F3. Ledger identity must mirror the source constraint

**Defect.** The plan keys contributions `(customer_code, source_transaction_id)` and instructs that `source_started_at` be stored "as content, not identity". Source uniqueness is `(id, started_at)`, and `continuity.py` states two rows can share an id in different partitions "silently and undetectably". A ledger keyed on id alone collapses them into one receipt and under-counts.

**Fix.** Primary key `(customer_code, source_transaction_id, source_started_at)`.
Safe because `started_at` is the minimum over a transaction's entries, no transaction spans more than the pad, and the ticket range is padded on both sides, so a rebuild that moves `started_at` keeps both old and new rows inside the diffed window.
Zero duplicate pairs exist today; the extra column costs nothing and removes a class of silent under-count.

### F4. Freshness needs two numbers: analytics lag and data settledness

**Defect.** The plan targets sub-5-second freshness and achieves it, but freshness there means "how far analytics lags the projection". Measured settling time is **1.7 h average, 5.4 h worst**. The UI can truthfully display "updated 2 seconds ago" about a number that will still move materially, which manufactures false confidence and is worse than an honest stale warning.

**Fix.** Two distinct indicators, both server-computed and both in the status response.

- **Copy freshness**: seconds between the analytics processed watermark and the source write watermark. This is the plan's existing metric.
- **Settledness**: for the displayed window, the share of contributing transactions still unsealed, and the age of the oldest unsealed one. A window with unsealed contributors is labelled *provisional*, not *stale*, because the distinction matters to an operator.

Any figure covering the last several hours is provisional by construction. Say so rather than implying finality.

### F5. The status endpoint must read exactly one row

**Defect.** The browser polls status every 2 seconds per tab across four web workers. The response includes regroup-pending and analytics-pending state, which are counts over other tables, so the cheapest request becomes the most frequent expensive one.

**Fix.** The worker denormalises every status field into the single `warehouse_analytics_state` row on each cycle: revision, watermarks, lag, pending ticket count and oldest ticket age, quarantine count, completeness count, unsealed share. The status endpoint is one primary-key read plus `ETag`/`If-None-Match`. Add a test asserting the endpoint issues exactly one query.

### F6. The retention cursor must publish a write-time position

**Defect.** Retention gating goes through `consumer_cursors`, whose `position` is a `log_transactions.created_at` value compared against UTC day bounds. The analytics worker is driven by event-time ticket ranges and has no natural write-time position. Left unresolved it either publishes a meaningless value or gates nothing, and the second loses data during exactly the backlog that caused it.

**Fix.** The worker tracks, per tenant, the **maximum `created_at` it has observed among fully processed source rows**, and publishes `min` of that across tenants under consumer name `analytics:warehouse-v1`. Because a ticket is only acknowledged once its whole range is diffed, every row inside an acknowledged range has been read, so its `created_at` is safely behind. When no tickets are pending, publish the source write watermark.

Register a second cursor `ml:features-v1` when the ML pipeline lands, which is the naming example `consumer_cursor.py:35` already gives.

**Dependency to watch: update-in-place would invalidate this fix.**
`docs/plan/2026-08-08_19-43_stable-transaction-identity-by-continuity.md` proposes a deferred follow-up. Now that transaction ids are stable, the delete-and-reinsert in Stage 2 could become an `UPDATE`. The plan states the consequence directly: `created_at` would stop churning, and the cursor would need to read a new `updated_at` column instead, which it argues is more correct anyway because "new OR changed since I looked" is what an incremental reader actually wants.

If that ships, **this fix breaks silently**: a cursor tracking `created_at` would stop advancing on changed rows, and retention could drop partitions the analytics worker still needs.

Two consequences for us.

- The cursor field must be a **single named constant**, not `created_at` inlined at each use, so the switch to `updated_at` is one edit.
- The 98.7% rebuild rate that sizes this whole plan is a **direct artefact of delete-and-reinsert**. If update-in-place lands, that figure collapses and the fold volume drops sharply. It would make the design cheaper, never wrong, but the load assumptions in "Designing for scale" should be re-measured rather than trusted.

Its own caveats are worth noting, since they may keep it deferred indefinitely: an `UPDATE` that moves `started_at` across a day boundary moves the row between partitions, and `log_transactions` carries roughly 15 indexes, so in-place updates reintroduce write amplification on the hot tail.

### F7. Mirror the existing ticket table rather than inventing one

**Defect.** The plan specifies `warehouse_analytics_dirty_window` with UUID key, `customer_code`, event-time bounds, creation time, availability time, attempt count, last error, and consumed or abandoned time. `log_regroup_pending` already has exactly those columns and is healthy in production: 7,407 tickets, zero pending, zero abandoned, zero retries.

**Fix.** Copy its schema, column names, index shape and retry semantics field for field, so operators recognise it and the proven claim query can be reused.

**Resolved: analytics gets its own table. Do not share `log_regroup_pending`.** This was left as an evaluation in an earlier draft; two findings close it.

1. **`consumed_at` is a single-consumer field.** The stitcher stamps it when the stitcher is done. A second consumer stamping the same column means whichever runs second finds the window already closed and skips work it never performed.
2. **The existing tickets do not cover the path that matters most.** `log_regroup_pending` rows are written in **Stage 1**, one per ingested file, at `parse_insert.py:188-190`, with `range_start`/`range_end` taken from the min/max `log_entries.timestamp` of that file. They therefore describe *ingest* ranges, and they only ever drive `regroup_window`. The live path `regroup_incremental` deletes every unsealed transaction regardless of when it happened, and **no Stage 1 ticket describes that**, because Stage 1 only knows about the file it just read.

So the analytics ticket is a distinct signal with a distinct meaning: written in **Stage 2**, per re-assembly, with bounds from the min/max `log_transactions.started_at` of the rows actually freed. Same shape, same retry semantics, separate table and separate `consumed_at`.

| | `log_regroup_pending` (exists) | `warehouse_analytics_dirty_window` (new) |
|---|---|---|
| Written by | Stage 1, per ingested file | Stage 2, per re-assembly |
| Bounds from | `log_entries.timestamp` min/max | `log_transactions.started_at` min/max of freed rows |
| Consumed by | the stitcher | the analytics worker |
| Covers `regroup_incremental` | no | **yes** |

### F8. Zero-quantity picks are attempts, not consumption

**Defect.** 9.2% of `ConfirmPickLine` confirmations record `QuantityPicked = 0` with `status = success`: the line was confirmed but nothing was taken, typically an empty location. "Validated successful physical picks" reads as including them, which inflates pick counts by about 9%.

**Fix.** The semantic contract states three counters explicitly, and every rollup stores all three.

- `quantity` - sum of `QuantityPicked`, unaffected by zeros.
- `pick_count` - confirmations with quantity greater than zero.
- `attempt_count` - all confirmations.

`zero_pick_rate` derives as `(attempt_count - pick_count) / attempt_count`. It is a first-class metric, not a footnote: it points at specific empty locations and is the reliable substitute for the fill rate that `ExpectedQuantity` cannot provide.

### F9. Add daily and monthly grains; derive weekly

**Defect.** The plan stores lifetime totals and hourly buckets only. A month-to-date query summing hourly is roughly 1.08 M item-hour rows at realistic cardinality, about 2.7 seconds and growing.

**Fix.** Cascade `hourly → daily → monthly`, each built from the grain below so the ledger is read once. Weekly derives from daily at read time and needs no table, using **ISO Monday-start weeks on the tenant-local business date** (confirmed 2026-08-18).

Each grain exists **per registered metric definition**, not once globally, so a definition's dimensions and measures determine its own rollup rows. See the metric registry section for the generic storage shape.

Grouping key for daily and above is the **tenant-local business date**, because `date` is computed as `to_display(started).date()` (`derive_transactions.py:172`) while `started_at` is UTC. For a UK warehouse these diverge by an hour for half the year. Every query must still carry a `started_at` predicate for partition pruning, because `date` is not the partition key (`logs.py:711-713`).

Sizing at 50,000 picks/day and 5,000 active items:

| Grain | Retention | Rows at 5 years |
|---|---|---|
| Hourly | 90 d | 3.2 M |
| Daily | forever | 9.1 M |
| Monthly | forever | 300 K |

Read-layer rule: choose the coarsest grain covering the window, targeting under 100,000 rows scanned. A 12-month top-items query is 60 K rows from monthly versus 1.8 M from daily.

**Additivity is a schema rule, not a guideline.** A rollup stores components, never finished answers.

| Measure | Composes | Stored as |
|---|---|---|
| Quantity, pick count, attempt count | yes | directly |
| First and last event | yes | `min`, `max` |
| Average, rate | no | `sum` + `count` |
| Std dev, coefficient of variation | no | `sum`, `sum_sq`, `count` |
| Median, p95 | no | 20-bucket log histogram; bucket counts are additive |
| Distinct items, operators, orders | no | computed per period from the ledger, never cascaded |
| Top-N | no | full item set for the window |

No `hll` or `tdigest` is available, so distinct counts have no sketch fallback. They are computed per period directly from the ledger, which is cheap because the ledger and the monthly grain share a partition boundary.

### F10. Keep contribution history, or ML is impossible later

**Defect.** The ledger's primary key retains only the latest contribution, so a rebuild overwrites the previous value. A training set built from it is not reproducible: the same query returns different features next week, no experiment is replayable, and offline metrics will not match online behaviour. **Discarded versions cannot be recovered**, so this cannot be retrofitted.

**Fix.** Two tables.

- `warehouse_consumption_contributions` - latest value per identity, as the plan describes, serving the delta computation.
- `warehouse_consumption_contribution_history` - **append-only**, one row per version, carrying `source_version_hash`, `revision`, `valid_from` and the superseded values. Retention independent of raw partitions and longer than them.

Training sets pin a `revision`, which makes them reproducible byte for byte.
Storage is modest: at 50,000 picks/day with an average of a few versions each, a few hundred million rows over five years is the pessimistic bound, and the table is append-only, well partitioned and never read by the serving path.

### F11. Load testing needs a synthetic tenant

**Defect.** Phase 3's exit criterion requires worker lag to hold at 100 times the measured pick rate, roughly 78,000 transactions/hour, with no described way to generate it without polluting production data.

**Fix.** A dedicated synthetic tenant, `synthetic-load`, seeded by a fixture generator that produces realistic entries and drives Stage 2 normally, so tickets and rebuilds are exercised rather than bypassed. It must be excluded from every production read path and from reconciliation alerting. The generator also produces the deliberate defect fixtures listed under verification, so correctness and load share one harness.

## Additional hardening

Findings from the earlier critique that the eleven headline items do not cover. All still apply.

**A1. Quarantine must never halt a tenant.** The plan halts a tenant's window on an ambiguous duplicate id. One bad row then freezes every metric until a human intervenes. This also contradicts the codebase's own decision in `consumer_cursors.py`, which explicitly chooses "the survivable failure, and making it loud rather than letting it be discovered later". Quarantine the row, count it in the state row, surface it, and continue.

**A2. The analytics tenant lock must not reuse the stitcher's key.** An earlier draft of this item said lock keys were unallocated and could collide with `app/worker.py:34-69`'s `pg_try_advisory_lock(0x7A9B, 1)`. **That reasoning was wrong** and is corrected here: that is a two-argument advisory lock, which PostgreSQL keeps in a separate space from single-argument locks, so it cannot collide with either key below.

The real hazard is different. `finalize_pending` holds `pg_advisory_xact_lock(hashtext(customer_code))` around every regroup sub-window (`derive_transactions.py:1000-1005`). If the analytics worker adopted the same expression it would **serialise against ingestion for that tenant**, so a slow analytics fold would stall log stitching. Analytics falling behind is survivable; ingestion stopping is not.

Derive a distinct key, for example `pg_advisory_xact_lock(hashtext('analytics:' || customer_code))`, and record both keys in one place so a third subsystem does not have to guess.

**A3. The ticket table must be provably constraint-free.** Because the ticket is written inside Stage 2's transaction, any failure of that insert fails ingestion. Insert-only, no foreign key, no unique constraint a retry could violate, no trigger. State it as an invariant so a later "improvement" cannot add one.

**A4. Routine reconciliation must be windowed.** The plan's own table says full reconciliation is proportional to all retained picks, which becomes a multi-hour job that stops being run. Routine mode covers a rolling recent range plus a rotating slice of history so everything is eventually covered. Full reconciliation stays an explicit operator action.

**A5. One authoritative revision.** There is a per-row `revision` on the aggregates and a per-tenant revision in the state row. The tenant revision is authoritative for `ETag` validation and must bump in the same commit as any aggregate change; row revisions are diagnostic only. Otherwise a valid `ETag` returns 304 over changed data.

**A6. Normalise the warehouse sentinel once.** The plan applies a sentinel for missing warehouse "only in the bucket table" while the ledger keeps nullable columns. That places the null-to-sentinel mapping in two code paths, and any divergence silently splits one item's aggregate. Normalise at the ledger boundary and let every grain inherit it.

**A7. Every source read carries `UtcWindow.covers`.** From `app/services/mnp_log_ingestion/pipeline/time_bounds.py`, with `include_null=True`, because a range predicate is FALSE for NULL and NULL `started_at` is a documented possibility even though zero rows have it today.

**A8. Align the worker cadence.** "Poll every second" against 68 transactions/minute mostly wakes to find nothing, while the existing notification worker polls at 10 seconds. Since tickets are durable, latency is bounded by ticket availability. Match the existing convention unless a measurement justifies otherwise.

**A9. Set `work_mem` per analytics transaction.** `SET LOCAL work_mem = '64MB'`, since the global 4 MB will spill hash aggregates over thousands of items and the global value is shared with ingestion workers.

**A10. Enable `pg_stat_statements`.** Available and not installed. Without it, slow analytics queries are guesswork.

## Metric registry

**Decided 2026-08-18: this is the centre of the design, not an extension.** The requirement is that the *user* chooses what is measured and how it is sliced, from the interface, and that the measure list is not fixed now. So nothing about dimensions or measures may be hardcoded into a rollup schema.

### The consequence that cannot be deferred

If measures are chosen later, the fact row must capture **every potentially useful field now**. Raw `log_transactions` and `log_entries` are dropped at 60 days, so a measure invented next year can only be backfilled across history if the fields it needs were already captured. A consumption-only ledger would make "average duration by device" permanently impossible beyond 60 days, with no way to recover.

**So the contribution ledger widens from a consumption ledger into a full fact row.** Alongside `quantity` and the version hash it carries the complete dimensional context of the transaction:

| Group | Fields |
|---|---|
| Identity | `source_transaction_id`, `source_started_at`, `source_version_hash`, `revision` |
| Time | `event_time`, `business_date` (tenant-local), `duration_ms` |
| Operation | `method` (49 values), `transaction_name` (22 values), `transaction_type`, `status` |
| Subject | `item_number`, `lot_number`, `order_number`, `delivery_number` |
| Place | `warehouse`, `warehouse_id`, `from_location`, `to_location` |
| Actor | `user_name`, `device_id`, `device_name` |
| Measures | `quantity` where present, `attempt`/`pick` classification, plus a typed slot per additional measure as registered |

This is a wider row than the 2026-08-11 plan specified, and it is deliberate. The fact table is the only durable record, so anything not written here is lost at 60 days.

### Dimensions confirmed

Aggregate by **both** `method` and `transaction_name`, per the answers of 2026-08-18. `method` gives API-level detail, 49 values; `transaction_name` gives the operator's screen, 22 values. Both are low cardinality, so both rollup families together stay under roughly 1,700 hourly rows per day, which is negligible.

Only **two of the 49 methods carry quantities**: `ConfirmPickLine` (14,654 rows, all with `QuantityPicked`) and `ReportCount` (9,076, with `CountedQuantity`). For the other 47 the meaningful measures are volume, duration, status breakdown, and optionally operator and device. A registered metric must therefore declare which measures it supports, and the interface must not offer a quantity measure for a method that has none.

### How a user-defined metric works

1. The user picks a dimension set, a measure, a filter and a grain in the interface.
2. That writes a **definition row**: identifier, dimensions, measure, filter predicate, grain set, lifecycle `draft`/`active`/`inactive`.
3. The worker begins maintaining rollups for that definition on the next cycle.
4. **A backfill populates its history from the fact table**, which is possible only because the fact row is wide and retained. This is the payoff for the widening above.

Rollups are stored generically: a definition identifier plus a fixed number of dimension slots plus additive measure slots, rather than a bespoke table per metric. Adding a metric is then a row plus a backfill, never a migration.

**The honest limit.** Fully arbitrary slicing over years cannot be pre-aggregated, because the combinations explode. Registered definitions are pre-aggregated and fast at any range. Genuinely ad-hoc exploration falls back to a bounded scan of the fact table, and the interface must make that distinction visible rather than silently running a slow query.

### Grains and selection, confirmed

Grains are hourly, daily, weekly and monthly. Weekly and monthly group on the **tenant-local business date with ISO Monday-start weeks**, using the timezone already stored per customer. Weekly derives from daily; it needs no table of its own.

Selection is **toggleable**: a chosen set of types can render either as one series per type or as a single combined total. Both are valid from the same rollup because counts and sums are additive, which is exactly why the additivity rule under F9 is a schema constraint rather than advice. Percentiles across a selection require the histogram, not a stored percentile.

### Why one pipeline rather than one per metric

Building each metric as its own ledger, worker and tables would duplicate the whole pipeline per metric.

Metric definitions become rows, following the shape already proven by `NotificationRule` (`app/persistence/models/notification.py:82-111`): an identifier, a kind, a `match JSONB` of parameters, a `draft`/`active`/`inactive` lifecycle, and a per-definition cursor. One ledger, one worker, one fold. Adding a metric is a data change.

This requires opening the closed dispatch: `build_evaluator` (`app/services/notifications/rules/base.py:46-58`) becomes a registry rather than an if-chain, and `_validate_rule` (`app/api/v1/notifications.py:163-178`) validates from the registry. That refactor also removes the current three-place edit needed to add a notification rule type, so it pays for itself.

**Prerequisite, to avoid a third copy.** The transaction filter vocabulary exists twice already, covering the same 13 dimensions: `_TXN_FILTERS` / `_txn_conditions` (`app/services/log_agent/tools.py:29-221`) and the `conds` chain in `list_transactions` (`app/api/v1/logs.py:759-787`). Factor into one filter-spec module before analytics becomes the third. Likewise the duplicated window-index arithmetic (`rules/engine.py:143-148` and `notifications/rollup.py:78-85`) should collapse to one `window_index`.

**Naming.** `app/services/notifications/rollup.py` already owns "rollup" for alert-burst summarisation. New modules use different words; the 2026-08-11 plan's `warehouse_analytics` and `fold.py` are fine.

## ML and agentic pipeline

**Now, because it cannot be added later:** the append-only contribution history from F10, with independent retention and a `revision` that training sets pin.

**Cheap and early:** analytics tools added to `TOOLS` and `_DISPATCH` in `app/services/log_agent/tools.py`. The agent already exists and works: a manual multi-turn Claude tool-use loop (`agent.py:71`), `settings.log_agent_model` default `claude-opus-4-8`, `max_iterations` 12, exposed at `POST /api/v1/logs/debug/ask`, with `customer_code` injected server-side and never model-exposed (`tools.py:184-188`). The metric registry is what makes those tools generic rather than one per metric.

**Deferred, with the seam ready:** forecasting and anomaly detection. No data-science dependency is installed or declared; `numpy` is present only transitively via `qdrant-client`. Anomaly detection reuses the existing notification rules engine, channels and delivery tracking rather than a parallel alerting path. `docs/data-architecture-scale-ml.md` sketches `transaction_features` and `transaction_predictions`; neither exists yet, and both should follow the same additivity and reproducibility rules as above.

**Out of scope but should be fixed independently.** `PgVectorStore.ensure_collection()` runs `CREATE EXTENSION IF NOT EXISTS vector` from an always-on worker (`app/background.py:82-86` → `embedding_worker.py:173-180`); the server has no pgvector, so it retries forever every ~4 seconds on both web and worker processes, masking real errors. And `PgVectorStore.query()` lacks the `text_match` parameter that `search_service.py:167-172` passes, so hybrid search raises `TypeError` on the default backend.

## Read API and frontend

Endpoints as the 2026-08-11 plan specifies, in `app/api/v1/analytics.py`, registered in `app/api/v1/router.py`, every one using `Depends(get_current_customer)` and `Depends(get_session)`. Keyset pagination with default and hard maximum limits, no default `COUNT(*)`, `ETag` with `If-None-Match` returning 304, `202 Accepted` for backfill and reconcile.

Additions from this merge: the status endpoint is a single-row read (F5), responses carry both freshness numbers (F4) and the completeness count (F2), and grain selection happens server-side in one repository method so no caller can scan a fine grain over a wide window (F9).

Frontend as specified, plus three facts to design around. There is **no chart library** and adding one would be the repo's first in two years, so hand-rolled inline SVG matches convention. Styling is one 1,168-line global CSS file with tokens in `:root` and an `nx-` prefix for new work; `src/components/notifications/ActivityTab.tsx` is the only existing table-plus-stat-tiles-plus-polling page and should be the template. **`next.config.mjs` needs a rewrite entry for `/api/v1/analytics/*`** or every request 404s.

## Delivery sequence

Codex's eight phases, revised. Exit criteria are unchanged except where a fix demands more.

**Phase 0. Semantic contract and fixtures.** Record the consumption definition including the three counters from F8. Capture sanitised fixtures for success, **zero pick**, short pick, error, incomplete, late backfill, rebuild, **merge**, **split**, and **a multi-confirmation order line whose `ExpectedQuantity` changes**. Build the synthetic tenant generator from F11 here, so it serves both correctness and load.

**Phase 1. Schema, models, ER diagram.** **Wide** fact ledger with the F3 key, carrying the full dimensional context from the metric registry section, **history table from F10**, generic per-definition grains from F9, the metric definition table, quality, and the state row with every denormalised status field from F5. Ticket table mirroring `log_regroup_pending` (F7) and provably constraint-free (A3). Register each partitioned table in `partitioning.py` and give each its own retention lag in `log_partition_worker.droppable_days`. Update `docs/database-er-diagram.md` in the same change.

The width of the fact row is the load-bearing decision in this phase. Anything omitted here cannot be backfilled once raw partitions age out at 60 days, so err toward capturing a field rather than leaving it.

**Phase 2. Ticket publication.** From every path that creates, deletes or rebuilds, with bounds computed from the freed set (**F1**). Tests for overlap, rollback, late data, and the specific case of an old unsealed row rebuilt by the live path.

**Phase 3. Normalizer, diff, worker.** Range diff, never per-id upsert, because merges and splits make ids appear and vanish. Quarantine without halting (**A1**). Advisory lock namespace (**A2**). Write-time cursor publication (**F6**). `work_mem` per transaction (**A9**).

**Phase 4. Backfill and reconciliation.** Windowed routine reconciliation plus explicit full runs (**A4**), and the **completeness check against `log_entries`** (**F2**). Gate source retention on healthy state.

**Phase 5. Read APIs.** Grain selection, both freshness numbers, single-query status.

**Phase 6. Next.js experience.** Provisional versus stale distinction (**F4**), and the `next.config.mjs` rewrite.

**Phase 7. Rollout.** Schema, then ticket publication with the worker disabled, then backfill and report-only reconciliation, then one tenant, then API and UI behind a feature flag.

## Verification

Grouped by consequence. Every item below is a failure that produces a plausible wrong number rather than an error.

**Correctness of the total**

1. **Reconciliation**, windowed and scheduled: ledger totals equal direct recomputation over `log_transactions` for the window.
2. **Completeness**: entries past the abandon window with no assignment row, by file and program. Currently non-zero, so this test starts red and that is correct (F2).
3. **Restatement**: force a rebuild of an already-folded window and assert totals unchanged; change a quantity at source and assert the old contribution is reversed exactly once.
4. **Merge and split**: assert a merged transaction's vanished id is reversed and a split's new id is applied. A per-id upsert passes the restatement test and fails this one.
5. **Ticket coverage** (F1): rebuild an old unsealed row via the live path and assert its `started_at` falls inside a committed ticket.
6. **Identity** (F3): assert the ledger key includes `source_started_at`, and that two rows sharing an id with different `started_at` produce two receipts.
7. **Idempotence**: run the same fold twice and assert values unchanged. Catches an additive upsert, which looks correct until the first retry.
8. **Crash safety**: kill the worker mid-batch; no gap, no duplicate.

**Data that cannot be recovered**

9. **Retention independence**: drop a 61-day-old raw partition and assert the ledger, history and grains survive.
10. **Cursor gating** (F6): assert the published position blocks retention while behind, and that a stalled worker is logged CRITICAL then stops blocking.
11. **Reproducibility** (F10): build a training set at revision N, restate a contribution, rebuild at revision N, assert byte-identical output.
12. **Aggregate rebuildability**: delete a grain entirely and assert it rebuilds from the ledger to identical values.

**Traps specific to this data**

13. **Attempts versus picks** (F8): zero-quantity confirmations excluded from `pick_count`, included in `attempt_count` and the zero-pick rate.
14. **Additivity** (F9): for a fixed month, every metric from the monthly grain equals the same metric computed from the ledger, including distinct count, p95 and average.
15. **Top-N is not composable**: a month's top 20 comes from the full item set, not merged daily top-20 lists.
16. **Local business date**: assert a pick at 00:30 local during BST lands on the correct local day, not the UTC day.
17. **Numeric fidelity**: `NUMERIC` end to end with fractional quantities; comparisons numeric, never string.
18. **Placeholder rejection**: `xxxxxx`-family transaction types and empty-string quantities quarantined with a reason, never dropped silently.

**Registry and user-defined metrics**

18a. **Fact-row completeness.** Assert every field listed in the metric registry table is populated on the fact row wherever the source has it. A field silently omitted is unrecoverable once raw partitions age out, so this guards the one irreversible schema decision.
18b. **A newly defined metric backfills correctly.** Define a metric from the interface for a dimension and measure combination never previously registered, run its backfill, and assert its history equals a direct aggregate over the fact table for the same range. This is what proves user-defined metrics are real rather than forward-only.
18c. **Selection is additive both ways.** For a chosen set of types, assert the combined-total series equals the sum of the per-type series, and that a percentile across the selection comes from merged histograms rather than averaged percentiles.
18d. **Quantity measures are refused where the field does not exist.** Assert a quantity measure cannot be registered against a method with no quantity, since 47 of the 49 have none.
18e. **ISO week boundaries.** Assert a transaction at 00:30 local on a Monday during BST lands in the correct ISO week and local business date, not the UTC one.

**Performance**

19. **Status is one query** (F5).
20. **Queries are prepared**, worth ~100 ms per call.
21. **Grain selection**: a 12-month top-items request resolves to monthly, never daily.
22. **Index-only scans** on grain reads, with `EXPLAIN (ANALYZE, BUFFERS)` proof.
23. **Load** (F11): synthetic tenant at 100× measured rate, worker lag inside target.

**End to end**

24. In the real UI: watch a live pick arrive and confirm the total updates within target; confirm a window with unsealed contributors reads as *provisional* and not as final; check empty and loading states, fractional number formatting, and both light and dark rendering.

## Inputs still needed

1. **Expected production volume**: picks per day at full rollout, and distinct active items per day. Every grain, retention and index above is sized from an assumption of 50,000 picks and 5,000 items. Today's ~1,300 picks and 692 items are a warehouse mid-rollout. An order-of-magnitude difference means re-cutting the cascade before building.
2. **Tenants at full rollout.** Everything keys on `customer_code` and there are two. Ten busy tenants multiplies every row count tenfold and may justify per-tenant partitioning.
3. **Cause of the two orphaned files**, before treating a regroup as the fix.
4. **Chart approach**: hand-rolled SVG, consistent with the repo's zero-dependency history, or accept the first chart dependency.
