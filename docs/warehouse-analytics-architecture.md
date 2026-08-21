# Warehouse analytics and ML platform: low-level architecture

> **SUPERSEDED - merged into [`docs/analytics-ml-architecture/final_architecture.md`](analytics-ml-architecture/final_architecture.md)**
>
> This document has been folded into the single canonical architecture document.
> Do not implement from this file. Kept for history only.
> Alembic migrations for this work should cite `docs/analytics-ml-architecture/final_architecture.md`.

Companion to `docs/plan/2026-08-17_22-32_merged-warehouse-analytics-platform-plan.md`.
The plan says **what to build and why**.
This document says **how it is structured**: components, the tables each one owns, who may write to what, and the flows between them.

An HTML twin of this document exists at `docs/warehouse-analytics-architecture.html`.
If the two disagree, this Markdown wins.

## Naming

The plan inherited names from an earlier consumption-only design (`warehouse_consumption_contributions`, `warehouse_item_consumption_hourly`).
The design has since widened: the store is a general transaction fact row, and rollups are generic per metric definition rather than per item.
So the names are re-cut.

Final scheme, arrived at after two rejected attempts:

| Plan name | Final name |
|---|---|
| `warehouse_analytics_dirty_window` | `analytics_pending_windows` |
| `warehouse_consumption_contributions` | `analytics_facts` |
| `warehouse_consumption_contribution_history` | `analytics_fact_ledger` |
| `warehouse_item_consumption_hourly` | `analytics_hourly_rollups` |
| `warehouse_item_consumption_daily` | `analytics_daily_rollups` |
| `warehouse_item_consumption_monthly` | `analytics_monthly_rollups` |
| (new) | `analytics_metrics` |
| `warehouse_analytics_state` | `analytics_tenant_state` |
| `warehouse_analytics_quality_issues` | `analytics_quality_issues` |
| (new, ML) | `analytics_feature_sets`, `analytics_predictions` |

### Why this scheme and not the obvious ones

**The prefix is `analytics_`, not `warehouse_analytics_`.** A 20-character prefix breaks PostgreSQL's 63-character identifier limit once `partitioning.py` appends a date and the concurrent-index recipe appends columns.
Measured:

```
80  OVER  warehouse_analytics_rollup_hourly_2026_08_18_customer_code_bucket_start_item_idx
64  OVER  warehouse_analytics_rollup_hourly_2026_08_18_customer_bucket_idx
55  ok    analytics_hourly_rollups_2026_08_18_customer_bucket_idx
```

`partition_name` raises past the limit, so this fails at partition creation rather than quietly.
`analytics_` also matches the form of the existing `notification_` and `log_` prefixes, where the repo uses a single domain noun.

**Names are plural**, because the repo is predominantly plural: `log_entries`, `notification_rules`, `consumer_cursors`, `log_regroup_runs`, `saved_views`, `jobs`, `customers`, against only `log_entry_assignment`, `log_regroup_pending` and `logspace_presence`.

**`analytics_pending_windows`, not `dirty_window`.** "Dirty window" is imported data-engineering jargon.
The repo's equivalent table is `log_regroup_pending`, named for what is pending rather than for a term of art.

**`facts` and `fact_ledger`, not `contributions`.** These are not synonyms and the distinction is load-bearing.

- **`contributions`** means "what this row contributed to a total".
  Correct when the design was consumption-only, too narrow now: **47 of the 49 methods carry no quantity at all**.
  A `GetAccessToken` row contributes to no sum but still has a duration, a status and an operator.
- **`ledger`** implies entries and their reversals sitting side by side.
  `analytics_facts` holds one current row per transaction; the reversal is a computation the worker performs, not a row it stores.
  Using "ledger" for that table would give a reader the wrong expectation of what a query returns.
- **`facts`** means one row per event carrying dimensions and measures, which is accurate for all 49 methods.

But "ledger" *is* the right word for the **history** table, which genuinely is append-only, one row per version, showing how each fact changed over time.
Hence `analytics_fact_ledger`, which also tells the reader what it is for in a way that `_history` did not.

The plan's prose describing "the contribution ledger with delta application" stays as it is.
That phrase describes what the worker *does*; a table name should describe what the table *contains*.

**Module names follow the tables.** The repo pairs `services/notifications/` with `notification_*` tables and `notification_worker.py`.
The exact parallel here is `services/analytics/` with `analytics_*` tables and `analytics_worker.py`.

**Still reversible.** Renaming before Phase 1 costs nothing; renaming after the first migration costs a migration.

## Component map

Twelve components.
Five already exist and are reused unchanged.
Seven are new.

| # | Component | Status | Module |
|---|---|---|---|
| E1 | Log fetch and parse (Stage 1) | exists | `services/mnp_log_ingestion/pipeline/parse_insert.py` |
| E2 | Transaction stitcher (Stage 2) | exists | `services/mnp_log_ingestion/pipeline/derive_transactions.py` |
| E3 | Retention cursor registry | exists | `services/consumer_cursors.py` |
| E4 | Partition manager | exists | `persistence/partitioning.py`, `services/workers/log_partition_worker.py` |
| E5 | Alerting engine | exists | `services/notifications/` |
| N1 | Ticket publisher | new | `services/analytics/pending_windows.py` |
| N2 | Fact normaliser | new | `services/analytics/normalizer.py` |
| N3 | Analytics worker | new | `services/workers/analytics_worker.py` |
| N4 | Metric registry | new | `services/analytics/registry.py` |
| N5 | Rollup folder | new | `services/analytics/fold.py` |
| N6 | Read layer | new | `persistence/repositories/analytics_repository.py` |
| N7 | API and agent tools | new | `api/v1/analytics.py`, additions to `services/log_agent/tools.py` |
| M1 | ML pipeline | later | `services/analytics_ml/` |

## Table ownership

The single most important rule in this architecture: **exactly one component may write each table.** Two writers to one table is how aggregates silently diverge.

| Table | Written by | Read by | Partitioned | Retention |
|---|---|---|---|---|
| `log_entries` | E1 | E2, N3 | daily on `timestamp` | 60 days |
| `log_entry_assignment` | E2 | E2, N3 | daily on `entry_ts` | 60 days |
| `log_transactions` | E2 | E2, N3, N6 | daily on `started_at` | 60 days |
| `log_regroup_pending` | E1 | E2 | no | pruned |
| `consumer_cursors` | N3, M1, E5 | E4 | no | forever |
| `analytics_pending_windows` | **N1 only** | N3 | no | pruned after consume |
| `analytics_facts` | **N3 only** | N5, N6, M1 | monthly on `event_time` | **forever** |
| `analytics_fact_ledger` | **N3 only** | M1 | monthly on `recorded_at` | **forever** |
| `analytics_metrics` | **N7 only** | N3, N5, N6 | no | forever |
| `analytics_hourly_rollups` | **N5 only** | N5, N6 | daily on `bucket_start` | 90 days |
| `analytics_daily_rollups` | **N5 only** | N5, N6 | yearly on `business_date` | **forever** |
| `analytics_monthly_rollups` | **N5 only** | N6 | none needed | **forever** |
| `analytics_tenant_state` | **N3 only** | N6, N7 | no | forever |
| `analytics_quality_issues` | **N3 only** | N6, N7 | monthly | 1 year |
| `analytics_feature_sets` | **M1 only** | M1 | monthly | forever |
| `analytics_predictions` | **M1 only** | N6, M1 | monthly | forever |

Note `consumer_cursors` has three writers, but each owns a distinct row keyed by consumer name (`analytics:warehouse-v1`, `ml:features-v1`, `notifications`), so there is no contention.
That pattern is already established in the codebase.

**No component writes to any `log_*` table.** The analytics platform is strictly a reader of the ingestion pipeline, with the single exception of N1 inserting a ticket, which lives inside E2's own transaction.

## Component detail

### N1. Ticket publisher

**Job.** Record that a bounded event-time range of a tenant's transactions changed.

**Where it runs.** Inside E2, in the same database transaction as the change.
Not a separate process.

**Publishes from three sites** in `derive_transactions.py`:

| Site | Bounds |
|---|---|
| `regroup_window` | the padded window it already computes |
| `regroup_incremental` | min and max `started_at` over the freed unsealed set, padded |
| `regroup_all` | min and max over everything freed, padded, or one ticket per day of the span |

**Constraints.** Insert-only.
No foreign key, no unique constraint a retry could violate, no trigger.
Because it commits inside ingestion's transaction, any failure here fails ingestion.

**Table shape** mirrors `log_regroup_pending` field for field: `id`, `customer_code`, `range_start`, `range_end`, `created_at`, `consumed_at`, `attempts`, `last_error`, `last_attempt_at`, `abandoned_at`, `available_at`, with index `(customer_code, consumed_at)`.

It is a **separate table**, not a shared one, because `consumed_at` is single-consumer and because Stage 1's tickets describe ingest ranges that never cover `regroup_incremental`.

### N2. Fact normaliser

**Job.** One `log_transactions` row plus its `attributes` JSONB to one typed fact row, or a quarantine record with a reason.

**Pure.** No database, no clock, no configuration.
This is where correctness is won, and it is why the module has no I/O.

**Responsibilities.**
- Reject placeholder `transaction_type` values (`xxxxxx`, `XXXXX`, `0050XX`).
- Cast quantities to `NUMERIC`, never float, treating empty string as absent rather than zero.
- Classify each row as `pick` (quantity above zero), `attempt` (quantity zero), or non-quantity.
- Compute `business_date` in the tenant timezone.
- Compute the version fingerprint over every field that affects a measure.
- Emit a typed record for every field in the fact row, present or explicitly absent.

**Absent is never zero.** A missing field means "not supplied", which is a different fact from zero and must survive as such.

### N3. Analytics worker

**Job.** Turn tickets into fact rows and aggregate deltas, exactly once in effect.

**Where it runs.** Inside the existing singleton `python -m app.worker` process, alongside the other workers.
Never in the four web workers, which stay read-only for analytics.

**One cycle, all in a single transaction per tenant:**

1. Claim available tickets for one tenant, using the proven predicate `consumed_at IS NULL AND abandoned_at IS NULL AND available_at <= clock_timestamp()`.
2. Coalesce their ranges into disjoint runs.
3. Take `pg_advisory_xact_lock(hashtext('analytics:' || customer_code))`.
   **Distinct from the stitcher's `hashtext(customer_code)`**, or a slow fold would stall log stitching.
4. `SET LOCAL work_mem = '64MB'`.
5. Read current `log_transactions` rows in the range, carrying `UtcWindow.covers(..., include_null=True)`.
6. Normalise via N2.
7. Read existing `analytics_facts` rows in the same range.
8. **Range diff**, never per-row upsert.
9. Apply the four outcomes below.
10. Append every changed version to `analytics_fact_ledger`.
11. Hand deltas to N5.
12. Write quarantine rows.
13. Update `analytics_tenant_state` with every status field.
14. Publish the retention position to `consumer_cursors`.
15. Stamp tickets consumed.

**The diff, which is the heart of the component:**

| Condition | Action | Total |
|---|---|---|
| in both, fingerprint equal | skip | unchanged |
| in both, fingerprint differs | reverse old, apply new | moves by the difference |
| in fact, absent from source | **reverse** | decreases |
| in source, absent from fact | apply | increases |

The third row is why the diff must span a range.
A merge makes one id disappear, and a per-id update would never look for it, leaving its contribution stranded permanently.

**Failure policy.** A poisoned row is quarantined and the tenant continues.
A failed run leaves its tickets open, bumps `attempts`, and dead-letters at the configured maximum.
One bad row never halts a tenant, following the precedent set in `consumer_cursors.py`.

**Retention position.** The maximum `created_at` observed among fully processed rows, published under `analytics:warehouse-v1`.
Held in a single named constant, because a deferred upstream change to update-in-place would require switching to `updated_at`.

### N4. Metric registry

**Job.** Hold what is measured, as data rather than code.

**Definition row:** `id`, `customer_code`, `name`, `dimensions` (ordered list), `measure` kind, `filter` predicate, `grains`, `status` in `draft`/`active`/`inactive`, `created_by`, `backfilled_through`.

Shape follows `NotificationRule`, which already proved the pattern in this codebase.
Dispatch must be a **registry, not an if-chain**, unlike `build_evaluator` today.

**Validation rules the registry enforces:**
- A quantity measure may only be registered against a dimension filter whose methods actually carry quantities.
  Only `ConfirmPickLine` and `ReportCount` do, out of 49.
- Dimensions must exist on the fact row.
- A definition cannot go `active` until its backfill has run, or its chart would show a false start date.

### N5. Rollup folder

**Job.** Maintain the grain cascade per active definition.

**Cascade.** `fact → hourly → daily → monthly`.
Each level reads only the level below, so the fact table is read once per cycle.

**Every write is recompute-and-replace**, never increment.
`ON CONFLICT ... DO UPDATE SET value = EXCLUDED.value`.
An additive upsert double-counts on the first retry.

**Additive components only.** Sums and counts stored directly.
Averages as `sum` plus `count`.
Variance as `sum`, `sum_sq`, `count`.
Percentiles as a 20-bucket log histogram, because bucket counts add and percentiles do not.
Distinct counts are **not cascaded**; they are computed per period from the fact table, which is cheap because the fact table and the monthly grain share a partition boundary.

**Weekly has no table.** ISO Monday weeks derive from daily at read time.

### N6. Read layer

**Job.** Answer every question, and be the only component that does.

**Grain selection.** Choose the coarsest grain covering the requested window, targeting under 100,000 rows scanned.
A twelve-month request resolves to monthly, never daily.

**Two-tier read for current periods.** Pre-aggregated rollups for settled ranges, unioned with a bounded live scan of the recent tail.
Both halves use **one boundary value read from the persisted cursor**, never a freshly computed one, or a lagging worker produces double counts or gaps.

**Ad-hoc fallback.** A query no definition covers falls back to a bounded fact-table scan, and the response marks itself as such so the interface can show it rather than silently running slow.

All queries parameterised so asyncpg prepares them, worth roughly 100 ms per call.

### N7. API and agent tools

**Endpoints** in `api/v1/analytics.py`, registered in the existing router, every one using `Depends(get_current_customer)` and `Depends(get_session)`.

| Endpoint | Notes |
|---|---|
| `GET /analytics/status` | **exactly one row read** plus `ETag`, because the browser polls it every 2 seconds per tab |
| `GET /analytics/metrics` | list and manage definitions |
| `POST /analytics/metrics` | create a definition, returns `202` with a backfill job |
| `GET /analytics/series` | one series per selected type, or one combined total, toggleable |
| `GET /analytics/breakdown` | top-N by dimension for a window |
| `POST /analytics/backfill` | `202 Accepted`, tracked |
| `POST /analytics/reconcile` | `202 Accepted`, tracked |

**Agent tools.** Added to `TOOLS` and `_DISPATCH` in `services/log_agent/tools.py`.
The agent already exists as a Claude tool-use loop at `POST /api/v1/logs/debug/ask`, with `customer_code` injected server-side and never model-exposed.
Because metrics are registry rows, the tools are generic: `list_metrics`, `query_metric`, `explain_freshness`.
No new tool per metric.

**Frontend** needs a `next.config.mjs` rewrite for `/api/v1/analytics/*` or every request 404s.
Charts are hand-built inline SVG; the repo has no chart library and adding one would be its first in two years.

### M1. ML pipeline

**Job.** Reproducible training and prediction on the same foundation.

**Reads `analytics_fact_ledger` at a pinned `revision`**, not the current fact table.
This is what makes a training run repeatable months later, and it is the reason the history table must exist from day one rather than being added when ML starts.

**Writes** `analytics_feature_sets` (features plus the pinned revision and a code version) and `analytics_predictions` (output keyed by subject, horizon and model version).

**Registers its own cursor** `ml:features-v1`, so retention will not drop history it still needs.

**Predictions are served by N6** through the same read layer, so a forecast and an actual are never fetched by two different code paths that could disagree.

**Anomaly detection reuses E5** rather than building a parallel alerting path.
The rules engine, channels and delivery tracking already exist.

## Data flows

### Flow A: change to ticket

```
E1 parse ─┐
          ├─► log_entries ─► E2 stitch ─► log_transactions
E2 rebuild┘                       │
                                  └─► N1 ─► analytics_pending_windows
                                      (same transaction as the change)
```

The ticket and the change commit together or neither commits.
That single property is what makes the coverage argument hold.

### Flow B: ticket to fact

```
dirty_window ─► N3 claim + coalesce + lock
                     │
                     ├─read─► log_transactions (current truth for the range)
                     ├─read─► analytics_facts (existing rows, same range)
                     ├─► N2 normalise
                     ├─► range diff
                     ├─write─► analytics_facts          (upsert)
                     ├─write─► analytics_fact_ledger  (append)
                     ├─write─► analytics_quality_issues
                     ├─► N5 apply deltas
                     ├─write─► analytics_tenant_state
                     ├─write─► consumer_cursors
                     └─write─► dirty_window.consumed_at
                     ────── one transaction ──────
```

### Flow C: fact to rollups

```
analytics_facts
        │  per active definition in analytics_metrics
        ▼
   rollup_hourly ──► rollup_daily ──► rollup_monthly
                          │
                     weekly derived at read time, no table
```

Each level reads only the one above.
Any level can be deleted and rebuilt from the fact table, which is what makes rollups safe to treat as disposable.

### Flow D: read

```
request ─► N7 ─► N6
                  ├─ pick coarsest grain covering the window
                  ├─read─► rollup_monthly | _daily | _hourly     (settled)
                  ├─read─► log_transactions tail                 (live, bounded)
                  ├─ union on ONE boundary from consumer_cursors
                  └─read─► analytics_tenant_state             (freshness)
                                    │
                        ┌───────────┴───────────┐
                     Frontend                Agent tools
```

### Flow E: ML training

```
analytics_fact_ledger @ pinned revision
        ├─► M1 build features ─► analytics_feature_sets
        ├─► M1 train ─────────► model artifact on disk
        └─► M1 infer ─────────► analytics_predictions ─► N6 ─► N7
```

Pinning a revision is what makes step 1 reproducible.
Without the history table it is not.

### Flow F: retention safety

```
N3 ─► consumer_cursors('analytics:warehouse-v1')  ┐
M1 ─► consumer_cursors('ml:features-v1')          ├─► E4 min position
E5 ─► consumer_cursors('notifications')           ┘        │
                                                            ▼
                                        drop partitions older than min
```

A component that stops reporting is excluded and logged critical rather than blocking retention forever.
That trade is already decided in `consumer_cursors.py` and is not re-litigated here.

## Invariants

Each of these fails **silently**, producing a plausible wrong number rather than an error.
Each maps to a test in the plan.

1. Exactly one component writes each table.
2. No transaction is deleted by any path without a committed ticket whose range contains its `started_at`.
3. Ticket and change commit in the same transaction.
4. A ticket is stamped consumed only after its entire range is diffed.
5. The diff spans a range, so a vanished id is reversed.
6. A matching fingerprint writes nothing, which is what makes retries free.
7. Rollup writes are recompute-and-replace, never increment.
8. Rollups store additive components, never finished answers.
9. Both halves of a read use one boundary value from the persisted cursor.
10. A missing source field is recorded as absent, never as zero.
11. The fact row is written wide, because omissions cannot be backfilled after 60 days.
12. Analytics uses its own advisory lock namespace, so it can never stall ingestion.
13. Quarantine never halts a tenant.
14. The analytics cursor field is one named constant, so an upstream move to `updated_at` is a single edit.

## Open questions

1. **Table naming.** The scheme above is a proposal.
   Confirm before Phase 1, since renaming afterwards costs a migration.
2. **Definition scope.** Are metric definitions per tenant, or global templates a tenant can enable? Per tenant is assumed above.
3. **Who may create metrics.** There is no authentication in this codebase yet (`api/deps.py:34-42` is a permit-all placeholder).
   If metric creation should be restricted, that needs an answer before N7.
4. **Production volume and tenant count**, still outstanding from the plan, which size every grain and index above.
