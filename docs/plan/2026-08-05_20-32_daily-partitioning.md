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

## 4. Step 2 - bound the unbounded queries (ships alone)

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

---

## 5. Step 3 - partition

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

## 6. Step 4 - partition management

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

## 7. Step 5 - the timezone dedup hole

Per §2.4, changing a customer's timezone after data exists can duplicate entries on re-ingest.

Options, cheapest first:

1. **Reject the change** when the tenant has entries - a guard in `PATCH /customers/{code}`
2. **Warn and require a flag** on the request
3. **Accept it** and document the behaviour

Recommend (1). Changing a tenant's timezone after ingestion already silently changes the meaning of every stored timestamp, so blocking it is defensible independently of partitioning.

---

## 8. Step 6 - partition status on `GET /logs/regroup/status`

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
