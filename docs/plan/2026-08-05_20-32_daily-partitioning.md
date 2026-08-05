# Daily partitioning: 60-day retention

**Decision:** daily UTC partitions, 60 days hot.
**Constraint:** non-live, outage acceptable.

Everything below is grounded in tests run against the actual PostgreSQL 17 instance and a full audit of every query touching the three tables. Where a claim came from measurement rather than reasoning, the measurement is quoted.

---

## 1. Why daily rather than weekly

Measured pre-wipe: **~1.5 KB per entry** (2,832 MB / 1.9M rows incl. indexes), **~150-200k entries/day** currently, against a 1M/day design target.

| | Daily | Weekly |
| --- | --- | --- |
| Partitions at 60 days | 60 | ~9 |
| Size each (now / at 1M-day) | ~300 MB / ~1.5 GB | ~2 GB / ~10 GB |
| Retention precision | exact 60 days | 56-70 days |
| Day-filtered query | 1 partition | 1 partition (no difference) |
| Assignment churn freezes after | 24 h | 7 days |
| Repair / re-export unit | 1 day | 7 days |

Daily wins on the two things that matter here: the disk has bad sectors, so a corrupt or unreadable partition costs one day rather than seven; and `log_entry_assignment` is the churn hotspot (delete+reinsert on every finalize), so freezing it after 24 h rather than 7 days matters.

60-90 partitions is well inside PostgreSQL's comfortable range - planning overhead becomes a problem in the thousands, not the tens.

---

## 2. What the tests proved

Run against the real database, not reasoned about.

### 2.1 A foreign key blocks `DROP PARTITION` - the decisive finding

```
ALTER TABLE ... DETACH PARTITION  -> ERROR: violates foreign key constraint
DROP TABLE <partition>            -> ERROR: other objects depend on it
```

`log_entry_assignment` currently holds `ON DELETE CASCADE` foreign keys into **both** tables that would be partitioned. While those exist, a partition can be neither detached nor dropped - which removes the entire point of partitioning.

**They must become soft references**, with both tables co-partitioned on the same key so they are dropped together. Verified working: with no FK and matching daily partitions, dropping day D from entries and assignment succeeds and leaves the rest intact.

### 2.2 Primary-key column ORDER is a 240x difference

Three hot functions look entries up by `entry_id` alone (`load_transaction_by_entry`, `is_unassigned`, `belongs_to_transaction`). Measured on 300k rows:

| Primary key | Lookup by `entry_id` |
| --- | --- |
| `(entry_id)` - today | 0.045 ms, index scan |
| `(entry_ts, entry_id)` | **10.8 ms, sequential scan** |
| `(entry_id, entry_ts)` | 0.046 ms, index scan |

The partition key must be *in* the PK, but it does not have to be *first*. **`(entry_id, entry_ts)`** satisfies partitioning and preserves the lookups.

Worth noting: the codex design uses `(event_day, customer_code, id)` - partition key first. Following it here would have caused exactly this regression.

### 2.3 Dropping the FKs makes writes ~4x faster

```
INSERT 200k assignments WITH FKs : 1,060 ms   (2 FK triggers x 200,000 calls)
INSERT 200k assignments NO FKs   :   249 ms
```

Assignments are written on every regroup of the live tail, so this is the hottest write path. The FK removal is a gain in its own right, independent of partitioning.

### 2.4 A unique constraint must contain the partition key

```
ERROR: unique constraint on partitioned table must include all partitioning columns
DETAIL: UNIQUE constraint lacks column "timestamp" which is part of the partition key.
```

`UNIQUE(customer_code, entry_hash)` becomes `UNIQUE(timestamp, customer_code, entry_hash)`.

Safe because `entry_hash = sha256(raw_body)` and `raw_body` includes the timestamp text, so an identical replay routes to the same partition. Confirmed: an identical re-insert dedups; the same hash on a *different* timestamp inserts a second row.

**The hole:** the `timestamp` COLUMN is derived by applying the customer's configured timezone (`parse_insert.py`). Change that config via `PATCH /customers/{code}` and re-ingest, and the same line yields a different UTC instant -> different partition -> duplicate row. Narrow, but reachable. See §7.

### 2.5 NULL timestamps need a DEFAULT partition and must stay out of the PK

```
INSERT with NULL timestamp -> ERROR: no partition of relation found for row
```

A `DEFAULT` partition accepts them, but only while `timestamp` is nullable - putting it in the primary key forces `NOT NULL` and makes such entries un-insertable. The parser can produce them (`parse_insert.py` handles `ts is None`).

### 2.6 Pruning works

A day-filtered query touches exactly one partition, and the `DEFAULT` partition is correctly excluded from range predicates.

---

## 3. Step 1 - fix `log_entry_assignment` (ships alone)  -  **DONE 2026-08-05**

Delivered in migration `f04b7c29ae13`, tests in `tests/test_assignment_soft_refs_chunk21.py`.

One correction to what is written below: the plan said "primary key `(entry_id, entry_ts)`". That is
impossible - PostgreSQL silently forces PK columns to `NOT NULL`, and `entry_ts` must stay nullable
because `log_entries.timestamp` is. It shipped as `UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)`
instead, which keeps the guarantee (including for timestamp-less entries, where a plain `UNIQUE`
would not) and keeps `entry_id` seekable.

Independently valuable: ~4x faster writes even if partitioning never happens.

**Schema**
- Drop both foreign keys -> soft references
- Add `entry_ts timestamptz` (denormalised from the entry; the table has no time column today and cannot otherwise be partitioned)
- Primary key `(entry_id)` -> **`(entry_id, entry_ts)`** - order per §2.2

**Code - the four delete paths that lose their cascade**

| Site | Was |
| --- | --- |
| `logspace_cleanup.py:106` | tenant purge, via jobs -> entries |
| `logs.py:576` | full wipe, via jobs |
| `logs.py:601-602` | date-range delete |
| `derive_transactions.py:553, 590` | regroup deletes |

Each needs an explicit assignment delete. This is where a miss leaves orphan rows, so it is where the tests concentrate.

**Trade being made:** a database guarantee is exchanged for application discipline. Consistent with the rest of the schema (`log_ssh_fetch_runs.source_id`, `customer_code` everywhere are already soft), but it is a real reduction in enforced integrity and the tests must carry the weight.

---

## 4. Step 2 - bound the unbounded queries (ships alone)  -  **PART A DONE 2026-08-05**

Shared arithmetic lives in `app/services/mnp_log_ingestion/pipeline/time_bounds.py`: a half-open
`UtcWindow`, built either from instants a caller already holds (`from_instants`) or from a
customer-LOCAL date range (`from_local_dates`), and rendered as a predicate by `covers()`.
Two hazards it exists to close, both of which drop rows silently rather than raising:
a range predicate is FALSE for NULL, so an entry with an unparsable timestamp vanishes unless the
NULL branch is asked for; and `log_transactions.date` was computed with whatever display zone the
customer had when the row was written, so a later timezone change slides the local day away from the
UTC instants - the local-date window is padded 27 hours (wider than the UTC-12..UTC+14 spread) to
stay a strict superset.

Done in part A:

| Site | What it carries now |
| --- | --- |
| `assignments.load_entries` | a `window` on BOTH `log_entry_assignment.entry_ts` and `log_entries.timestamp` - the join key is `entry_id`, which prunes neither. All three callers derive it from the transactions' own `started_at`/`ended_at`, so it is exact by construction. NULLs included on purpose |
| `derive_transactions._cutoffs` | a bounded `max(timestamp)` probe over the last `log_cutoff_lookback_days` (7), falling back to the full scan on a miss - without the fallback an idle or back-dated tenant would never seal |
| `logs.py` `view_transactions` | `_day_conds` adds the padded `started_at` window beside the exact `date` filter |
| `logs.py` date-range delete | same window on the transaction side; the entry side already stated `timestamp` |

Guarded by `tests/test_partition_bounds_chunk22.py` (27 tests, mutation-checked), including a source
guard that fails if any `load_entries` call site is left without an explicit window.

Part B is now DONE too (see below). Original note:

Still open (part B, the Stage 2 live path - each needs a batch-edge guard so a bound cannot split a
transaction, so it is deliberately its own pass): the `regroup_incremental` live-tail read and its
`SELECT DISTINCT customer_code` anti-join, the unsealed-transaction delete, `_persist` loading every
transaction id for the tenant, plus `list_entries`, agent `search_entries`, `regroup_all` and the two
`db.get(LogTransaction, id)` point lookups.


Thirteen sites lack the partition key and would scan all 60 partitions. Each is also faster today with a bound.

**Highest impact**

| Site | Problem |
| --- | --- |
| `assignments.py:139` `load_entries` | joins `log_entries` by PK with no timestamp - **the feed's hot path**. Derive the window from the transaction's `started_at` +/- the seal window: 2 partitions maximum |
| `derive_transactions.py:609-617` | `regroup_incremental` live-tail read - unbounded anti-join, no LIMIT |
| `derive_transactions.py:590` | `DELETE WHERE sealed IS false` - unbounded, every cycle |
| `derive_transactions.py:454` `_cutoffs` | `max(timestamp)` per regroup call |
| `derive_transactions.py:495` `_persist` | loads every transaction id for the tenant into a Python set, per sub-window |
| `logs.py:707-732` `view_transactions` | filters on `date` (customer-LOCAL) not `started_at` (UTC) - **prunes nothing**. Needs a `started_at` range alongside |
| `logs.py:593-601` date-range delete | same `date` vs `started_at` problem |

**Also**: `list_entries`, agent `search_entries` (optional time filters), `regroup_all` (unbounded by design), and two `db.get(LogTransaction, id)` point lookups.

### Part B - Stage 2's own queries  -  **DONE 2026-08-05**

Partitioning made these worse, not better: each opened ~130 partitions instead of scanning one table.

| Site | Was | Now |
| --- | --- | --- |
| `_persist` clash check | loaded EVERY transaction id the tenant ever had into a Python set, per sub-window - 109k rows in production, growing forever, and an Append over 25 partitions locally | asks only about the ids being written, inside a padded window. **25 partitions -> 1**, and memory now scales with the batch, not the tenant's history |
| `regroup_incremental` tenant discovery | `SELECT DISTINCT customer_code ... WHERE unassigned` - a whole-table anti-join - even though both real call sites already name the customer | an existence check for that one code; the `None` path keeps the scan |
| `regroup_incremental` live tail | every unassigned entry for the tenant, no time bound, no LIMIT | bounded to the abandon window + pad behind the tenant's newest entry |

The `_persist` fix is exact, not approximate, and the argument matters: a transaction's id is `uuid5`
of its anchor entry's hash, so a colliding row was built from the SAME anchor and shares its
timestamp. Its `started_at` is the min over its own entries, which the system guarantees span at most
one pad - so it lies in `[anchor_ts - pad, anchor_ts]`. Asking about exactly the candidate ids within
that padded window cannot miss a real clash.

Two edges are load-bearing and each has a test that fails without it: a NULL `started_at` (DEFAULT
partition) would be invisible to a plain range predicate and the clash missed; a NULL entry timestamp
would be dropped from the live tail and that entry never grouped and never reported.

The one real cost of bounding the live tail: a tenant whose entire backlog predates the window now
looks idle. That is logged as a WARNING naming the tenant and telling the operator to run a full
regroup, because nothing else would reveal it.

Deliberately NOT bounded, with reasons:

- `list_entries` and agent `search_entries` already take optional `time_from`/`time_to` on the
  partition key. Forcing a default window would silently hide older results from a user who asked for
  an unfiltered search.
- `db.get(LogTransaction, id)` in `logs.py` and `tools.py` - the id arrives as a URL parameter with no
  day attached, so there is nothing to derive a bound from. Correct, just unpruned: ~130 index probes.
- The unsealed-transaction delete stays unbounded on purpose. Bounding it could leave an old unsealed
  transaction permanently unfreed, so its entries would never regroup - a silent stuck state.

`_persist` also dropped from CRAP 12 to 5 by extracting `_resolve_ids`, `_cap_over_length` and
`_write_transaction`. 17 new tests in `tests/test_stage2_bounds_chunk24.py`, all six bound decisions
mutation-checked. 293 tests green.

---

## 5. Step 3 - partition  -  **DONE 2026-08-05** (migration `a1f6d70b3e92`)

One migration for all three tables, not one each: PostgreSQL DDL is transactional, so a failure
anywhere rolls the whole thing back rather than leaving entries partitioned and their assignments not.

Per table: rename -> create bare parent `PARTITION BY RANGE (key)` -> DEFAULT partition + one per day
the data spans (widened to today + precreate) -> `INSERT INTO new SELECT * FROM old` -> **verify the
copied row count against the source and abort if they differ, while the originals are still on disk**
-> drop old -> add constraints and indexes. Indexes last: building them after the bulk load is much
faster, and the old table holds their NAMES until it is dropped.

What the implementation had to handle that the plan above did not anticipate:

- `log_transactions.started_at` is nullable, exactly like `entry_ts`. So NONE of the three tables can
  keep a PRIMARY KEY - all three become `UNIQUE NULLS NOT DISTINCT` with the key present but not
  leading. `db.get(Model, id)` keeps working because the ORM key stays the id column alone.
- `parse_insert.py`'s `ON CONFLICT` had to name all three dedup columns. Against the old two-column
  target it fails outright with "no unique or exclusion constraint matching the ON CONFLICT
  specification" - caught by a test, not in production.
- A single corrupt timestamp would have turned into hundreds of thousands of partitions. The build now
  refuses any span over ten years and tells the operator how to find the offending rows.
- `LIKE` copies columns, NOT NULL and defaults but NOT foreign keys, so `job_id -> jobs ON DELETE
  CASCADE` is re-added explicitly. `logspace_cleanup` purges tenants through that cascade.
- Partition bounds carry an explicit `+00`. A bare date bound is resolved in the session's TimeZone at
  CREATE time, which on a Europe/London server puts every partition an hour off its own name.

Verified: upgrade -> downgrade -> upgrade with zero row loss, downgrade restoring the exact prior
schema (plain tables, original PKs, two-column dedup, FKs intact). The downgrade must be paired with a
code rollback. 275 tests green; 38 in `tests/test_partitioning_chunk23.py`.

Management (`app/persistence/partitioning.py`) is shared by the migration, the step-4 worker and the
step-6 status endpoint, so the three cannot drift.


One offline migration per table: rename existing -> create partitioned parent -> pre-create partitions covering existing data + 30 days -> `INSERT INTO ... SELECT` -> drop old.

| Table | Partition key |
| --- | --- |
| `log_entries` | `timestamp` (UTC day) |
| `log_transactions` | `started_at` (UTC day) |
| `log_entry_assignment` | `entry_ts` (UTC day, matching its entry) |

**Key changes**
- `log_entries` unique -> `(timestamp, customer_code, entry_hash)`; `parse_insert.py:58-61` `ON CONFLICT` inference target must match
- `log_transactions` PK -> must include `started_at`. Note the id is a deterministic `uuid5` relied on for idempotency, and `db.get(LogTransaction, id)` plus `DELETE WHERE id IN (...)` currently use id alone - both need the day threading through
- `DEFAULT` partition on entries for NULL timestamps, monitored
- `log_entries.job_id` cascade already seq-scans (its index was dropped); post-partitioning that becomes 60 seq-scans - revisit during this step

---

## 6. Step 4 - partition management  -  **DONE 2026-08-05**

`app/services/workers/log_partition_worker.py`, registered in `background.py`, hourly. Both halves are
idempotent so a missed tick is harmless.

It is deliberately NOT gated on there being work to do, unlike the two queue workers. Those have a
durable backlog to fall back on; this one does not. If it never runs, ingestion stops the moment the
existing runway is exhausted, so disabling it is a decision to provision partitions by hand and the
startup log says exactly that.

Creation runs FIRST and independently of the drop: a drop failure reclaims no disk and retries next
tick, which is survivable, while a creation failure eventually stops ingestion. Letting the first
prevent the second would turn a survivable problem into an outage. A creation failure logs CRITICAL
rather than ERROR because nothing breaks at the time - it is invisible until the runway runs out days
later - and the remaining runway is reported on EVERY tick, since "created 0" alone cannot distinguish
"fully provisioned" from "creation has been broken for a week".

**Verified against the real database, not just unit tests.** One `run_once` on the local database
dropped 103 expired partitions and left exactly the right boundaries:

- `log_transactions` oldest surviving day `2026-06-06` (today - 60)
- `log_entries` oldest surviving day `2026-06-05` (today - 61) - the midnight rule, one day of lag
- `2026-05-19` held back entirely, because five OPEN `log_regroup_pending` windows still covered it -
  gate 2 firing on real data rather than a fixture

24 tests in `tests/test_partition_worker_chunk25.py`; all six gate decisions mutation-checked
(removing the lag, unlinking assignments from entries, dropping gate 2, widening gate 2 to consumed
windows, swallowing a creation failure, and letting a drop failure abort creation each fail the
suite). All functions A-grade complexity. 320 tests green.

**Note for deployment:** the first tick on production will immediately drop every partition older than
60 days. That is the designed behaviour, but it is a one-way door - worth confirming the retention
number is right before the worker starts.


New background loop `app/services/workers/log_partition_worker.py`, registered in `background.py` alongside the stitch and parse workers. The worker process's singleton advisory lock already guarantees one instance. **Hourly**; both jobs are idempotent so a missed tick is harmless.

**Create** - for today through today+30:

```sql
CREATE TABLE IF NOT EXISTS log_entries_2026_08_06
PARTITION OF log_entries
FOR VALUES FROM ('2026-08-06') TO ('2026-08-07');
```

30 days of runway means the worker can be down a week without stopping ingestion.

**Drop** - partitions older than 60 days, behind three gates:

1. day < today - 60
2. no open `log_regroup_pending` overlapping that day
3. **entries lag transactions by one day**

Gate 3 is the midnight rule. A transaction spans at most the seal window (`log_seal_window_seconds = 900`, enforced), so one starting at 23:58 owns entries up to ~00:13 the next day. Dropping day N's entries while a day N-1 transaction still references them would leave that transaction rendering half-empty. So per cycle: drop `log_transactions` for day D, then `log_entries` + `log_entry_assignment` for day **D-1**. Costs one extra day of entry storage; the bound is exact, not a guess.

**Failure modes**
- Creation fails -> ingestion stops once runway is exhausted. The dangerous one; alarmed below.
- Drop fails -> nothing breaks, disk not reclaimed, retries next tick.

---

## 7. Step 5 - the timezone dedup hole  -  **DONE 2026-08-06**

The hole was reproduced against the real database before anything was written, rather than taken on
trust from §2.4:

```
identical entry_hash, SAME tz     -> inserted 0   (dedup works)
identical entry_hash, CHANGED tz  -> inserted 1   (a second row, different partition)
```

Option (1) as recommended, with one addition: it is a locked door with a key, not a wall.

`app/services/timezone_change_guard.py` decides and explains; `PATCH /customers/{code}` turns a reason
into **409** and owns the override. Splitting it that way keeps the rule testable without HTTP.

Blocked only when BOTH hold - the tenant has entries, AND the change moves the EFFECTIVE zone. Two
cases are deliberately allowed, because blocking them would be noise rather than safety:

- setting a zone on a tenant with no entries (the normal post-creation case);
- `null -> "Europe/London"` when the global default already was `Europe/London`. `null` never meant
  "no timezone", it meant "the default", so nothing about any instant changes.

The rejection message names the SAFE remedy (purge the log data, set the zone, re-ingest) as well as
the override - a block that only says "no" is an obstacle, not a guard.

`allow_mixed_timezones=true` proceeds and logs **CRITICAL**, because after that point nothing in the
data itself records where the derivation changed; that log line is the only evidence the seam exists.

Zones are compared by IANA NAME, not by resolved offset. Two differently-named zones that agree today
can diverge on any future DST rule change, so treating them as equivalent would be a bet on politics;
making that rare case need the override is the cheaper mistake.

Guarded at the only door: `repo.set_timezone` now has exactly ONE caller, pinned by a test, so a
future endpoint cannot reopen the hole silently. Creation-time timezone setting needs no guard - a
brand-new tenant has no entries.

16 tests in `tests/test_timezone_change_guard_chunk26.py`, all six decisions mutation-checked, the
guard module 100% covered at max complexity 3. `update_customer` came down from CRAP 15 to 6 by
extracting `_permanent_fields`, `_require_something_to_update` and `_set_timezone_guarded`; the
remaining 6 is the endpoint's inherent branching over three independent optional field groups.
335 tests green.


Per §2.4, changing a customer's timezone after data exists can duplicate entries on re-ingest.

Options, cheapest first:

1. **Reject the change** when the tenant has entries - a guard in `PATCH /customers/{code}`
2. **Warn and require a flag** on the request
3. **Accept it** and document the behaviour

Recommend (1). Changing a tenant's timezone after ingestion already silently changes the meaning of every stored timestamp, so blocking it is defensible independently of partitioning.

---

## 8. Step 6 - partition status on `GET /logs/regroup/status`  -  **DONE 2026-08-06**

Shipped as planned. Live response:

```json
"partitions": {
  "days_ahead": 14,
  "oldest_day": "2026-05-19",
  "newest_day": "2026-08-19",
  "retention_days": 60,
  "default_partition_rows": 0,
  "healthy": true
}
```

Additive - every existing field on the endpoint is unchanged.

`healthy` is server-side so the threshold matches the worker's own CRITICAL alarm
(`log_partition_min_runway_days`); a threshold baked into a React component cannot be changed without
a deploy, and the card would eventually show green while the worker paged.

The block is a passenger, not the purpose: if the catalogue read fails it degrades to `null` and the
card still renders the STITCHING status it primarily exists for, rather than 500ing the whole widget.
`null` rather than an absent key, so the frontend can tell "unavailable now" from "an older backend".

Two things measurement changed from the plan:

- **The runway must be read from the live catalogue, not echoed from config.** The first version of
  the test could not tell the two apart, because in a healthy database they are both 14 - a version
  that simply returned the setting looked correct. The test now forces them apart before asserting.
- **A comment claiming `WHERE timestamp IS NULL` would scan every partition was WRONG.** Measured,
  PostgreSQL prunes it to the DEFAULT partition on its own. The code still addresses the partition
  directly - nothing then rests on the planner continuing to do that - but the comment was corrected
  rather than left asserting something false.

13 tests in `tests/test_partition_status_chunk27.py`; six mutations checked, of which one turned out
to be an equivalent mutation (above) and one exposed the weak test (above). `regroup_status` came
down from CRAP 8 to 4 by extracting `_partition_status_or_none` and an `_iso` helper.

**Frontend**: one line, briefed in `docs/plan/2026-08-06_frontend-partition-status-line.md`.


Additive block:

```json
"partitions": {
  "days_ahead": 30,
  "oldest_day": "2026-06-06",
  "newest_day": "2026-09-04",
  "retention_days": 60,
  "default_partition_rows": 0,
  "healthy": true
}
```

A `pg_class` catalogue read, not a data scan - safe to poll at the card's existing cadence.

**Note:** this endpoint is tenant-scoped but partitions are global, so the block is identical for every customer. It goes here anyway because the AUTO-POLL card already polls this endpoint via `RegroupContext`, so no second request is needed.

`healthy` is computed server-side (`days_ahead >= 7 && default_partition_rows == 0`) so the frontend does not encode the thresholds.

**Frontend** - one line in `PollingStatus.tsx`, beneath the existing `pstat-sub`:

```tsx
{status?.partitions && (
  <div className={status.partitions.healthy ? "pstat-sub" : "pstat-warn"}>
    {status.partitions.healthy
      ? `storage ready ${status.partitions.days_ahead}d ahead`
      : `⚠ storage partitions only ${status.partitions.days_ahead}d ahead — ingestion stops when this reaches 0`}
  </div>
)}
```

Plus the optional field on the `RegroupStatus` type in `logsApi.ts`, so nothing breaks before the backend ships it.

Rendered:

> 🟢 **Up to date**
> last updated 8/5/2026, 8:27:38 PM
> 2 servers auto-polling · last poll 8/5/2026, 8:14:11 PM
> storage ready 30d ahead

The three fields that matter operationally: `days_ahead` (hits 0 -> ingestion stops), `default_partition_rows` (growth means the parser is emitting NULL timestamps), `oldest_day` (is retention actually running).

**Alarm when `days_ahead < 7`.**

---

## 9. Verification

**Correctness**
- Re-ingest the same file -> zero new entries (the dedup linchpin, under the new 3-column unique)
- Rotation re-read -> no duplicates
- Two tenants, identical content -> two rows
- A transaction spanning 23:58 -> 00:03 stitches whole, renders whole, and has assignments in both daily partitions
- NULL-timestamp entries land in the DEFAULT partition and do not fail the insert
- `log_transactions` identical before and after the migration (deterministic uuid5 makes this exact)

**Retention**
- `DROP PARTITION` reclaims space instantly with no scan
- After dropping day D transactions and day D-1 entries, no surviving transaction has lost entries
- A partition with open `log_regroup_pending` overlapping it is NOT dropped

**Performance**
- `EXPLAIN` shows pruning on the feed, the day view, the transaction timeline, and the Stage 2 padded read
- No query touches all 60 partitions
- Assignment insert throughput before/after the FK removal (expect ~4x)

**No regression**
- Tenant purge and full wipe leave no orphan assignment rows (the four delete paths from §3)
- Deleting an SSH source still preserves ingestion evidence

---

## 10. Sequencing

Steps 1 and 2 are independently useful and de-risk step 3. Stopping after either leaves a working, faster system.

Step 3 is the only irreversible one and the only one needing downtime.

Steps 4-6 follow it and are additive.

**Do not** attempt this as a single migration - the FK/DROP interaction in §2.1 means a big-bang approach fails on the first retention run, after the data has already moved.
