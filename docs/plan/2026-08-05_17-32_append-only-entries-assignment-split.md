# Make `log_entries` append-only: move the assignment into its own table

**Scope:** Stage 2's write path only.
Nothing about partitioning, retention, `jobs`, or ML - though this is the prerequisite for all of them.

**Why now:** this is the actual cause of the outage that started this whole workstream.
Everything shipped so far (the two queue splits) is plumbing around it.

---

## 1. The problem, measured

From `pg_stat_user_tables` on production, 2026-08-05:

| Table | live rows | dead | % dead | updates | HOT updates | % HOT |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `log_entries` | 1,907,592 | 345,382 | 15.3% | **105,838,123** | 162 | **0.0%** |
| `log_transactions` | 109,127 | 14,188 | 11.5% | 2,137 | 63 | 2.9% |

**105.8 million updates on a 1.9 million row table** - roughly 55 rewrites per row, and essentially none of them HOT.

A non-HOT update rewrites the heap tuple *and every index entry for that row*.
`log_entries.transaction_id` is indexed, so each of those 105M updates touches the index too.
That is the write amplification, the dead-tuple churn, and the vacuum pressure that took the box down.

### Where it comes from

Stage 2 writes the grouping result back onto the raw table:

```python
# derive_transactions.py:522-524
for i, e in enumerate(b.entries):
    e.transaction_id = txn.id
    e.seq = i
```

And the delete side relies on a foreign-key cascade to clear it:

```python
# log_entry.py:53
ForeignKey("log_transactions.id", ondelete="SET NULL"), nullable=True, index=True
```

So every regroup of the unsealed tail rewrites the same entries again.
The tail is regrouped repeatedly before it seals, which is where the ~55x comes from.

### The fix

Move `transaction_id` and `seq` off `log_entries` into `log_entry_assignment(entry_id, transaction_id, seq)`.

The raw table becomes genuinely append-only: Stage 1 inserts, and nothing ever updates it again.
The churn moves to a small, purpose-built table designed to be replaced.

---

## 2. Why this is provable, not hopeful

Transaction IDs are deterministic - `uuid5(customer_code + anchor_entry.entry_hash)` - so regrouping the same entries reproduces the same ID.

That gives an exact correctness test: **after the split, `log_transactions` must be byte-identical.**
Not "looks right" - identical rows, identical IDs.
If it is not, the change is wrong and the test says so.

This is the strongest verification available on any of the remaining work, and it is why this piece should go before partitioning.

---

## 3. The change surface

Every site, located and verified.

### 3.1 The write path

`derive_transactions.py:522-524` - `_persist` sets the columns on each entry.
Becomes: insert `log_entry_assignment` rows instead.

### 3.2 The "needs grouping" signal - three sites

`transaction_id IS NULL` currently means "unassigned":

| Site | Context |
| --- | --- |
| `derive_transactions.py:596` | `regroup_incremental` - `SELECT DISTINCT customer_code WHERE transaction_id IS NULL`, **whole-table** |
| `derive_transactions.py:609` | `regroup_incremental` - per-customer entry select |
| `derive_transactions.py:671` | `regroup_window` - the live path |

After the split, "unassigned" means "no row in `log_entry_assignment`":

```sql
NOT EXISTS (SELECT 1 FROM log_entry_assignment a WHERE a.entry_id = e.id)
```

**Only window-scoped.**
`:671` is already bounded by `timestamp BETWEEN lo_p AND hi_p` and is fine.
`:596` is a whole-table scan today and must not become a whole-table anti-join - it has to be time-bounded or dropped.

### 3.3 The delete/reselect contract

`regroup_window:659-676` deletes transactions and then reselects unassigned entries **in one transaction, no intermediate commit**, relying on the `ON DELETE SET NULL` cascade being visible to that same-transaction reselect.

Replace with an explicit `DELETE FROM log_entry_assignment WHERE transaction_id IN (<deleted ids>)` inside the same window transaction.
MVCC visibility is identical; it is simply written down rather than inferred from a cascade.

### 3.4 Readers - the ones that break if missed

| Site | Reads |
| --- | --- |
| `app/api/v1/logs.py:760` | `LogEntry.transaction_id.in_(ids)` - the feed's entry fetch |
| `app/api/v1/logs.py:810-811` | `transaction_id == ?` ordered by `seq` - transaction detail |
| `app/api/v1/logs.py:867` | `"seq": e.seq` in the response payload |
| `app/api/v1/logs.py:64` | `_entry_sort_key` uses `e.seq` |
| `app/services/log_agent/tools.py:288-289` | agent tool: entries by transaction, ordered by `seq` |
| `app/services/log_agent/tools.py:316` | `"seq": e.seq` in tool output |
| `app/services/log_agent/tools.py:341` | agent filter on `transaction_id` |

The agent tools are the easy ones to forget - they are a separate read path from the API.

### 3.5 Scalability fix that must ride along

`derive_transactions.py:494` loads **every** transaction ID for the customer into a Python set on every `_persist` call:

```python
existing: set[uuid.UUID] = set((await db.execute(
    select(LogTransaction.id).where(LogTransaction.customer_code == customer_code)
)).scalars().all())
```

At 109k transactions that is already wasteful; it grows without bound.
Scope it to the window being rebuilt.
This is not strictly part of the split, but it is in the same function and the same test run proves it.

---

## 4. The new table

```sql
CREATE TABLE log_entry_assignment (
    entry_id uuid PRIMARY KEY
        REFERENCES log_entries(id) ON DELETE CASCADE,
    transaction_id uuid NOT NULL
        REFERENCES log_transactions(id) ON DELETE CASCADE,
    seq integer NOT NULL,
    customer_code varchar(64) NOT NULL,     -- soft tenant key, consistent with every other log table
    assigned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX ix_log_entry_assignment_txn
    ON log_entry_assignment (transaction_id, seq);
```

### Deliberate choices

**`entry_id` is the primary key.**
One current assignment per entry, enforced by the database rather than by convention.

**Both foreign keys CASCADE.**
Deleting a transaction removes its assignments (replacing today's `SET NULL` behaviour); deleting an entry removes its assignment. This keeps the existing purge path working unchanged - `logspace_cleanup.py` deletes jobs, which cascades to entries, which now also cascades to assignments.

**No FK to the SSH source.**
A source delete must never cascade through assignments. It has no such column, so this holds by construction.

**`clock_timestamp()`, not `now()`.**
Same lesson as the two queues: written and compared by the database clock only.

---

## 5. Steps

Each ships and is verified independently.

### Step 1: migration only

Create the table and its index. Nothing reads or writes it.
Purely additive - no change to `log_entries`, so no rewrite of the 2.8 GB heap.

### Step 2: dual-write, behind a flag

`_persist` writes assignment rows **as well as** setting the columns.
Reads still use the columns.

This is the safety net: with both populated, a comparison query proves they agree on real production data before anything depends on the new table.

```sql
-- must return 0
SELECT count(*) FROM log_entries e
LEFT JOIN log_entry_assignment a ON a.entry_id = e.id
WHERE e.transaction_id IS DISTINCT FROM a.transaction_id
   OR e.seq IS DISTINCT FROM a.seq;
```

### Step 3: switch reads

Point all seven reader sites at the assignment table.
Detection sites move to the anti-join.
`regroup_window` gets the explicit delete.

Still dual-writing, so rollback is a flag flip.

### Step 4: stop writing the columns

`_persist` no longer sets `transaction_id` / `seq`.
**`log_entries` is now append-only.**

Watch `pg_stat_user_tables`: `n_tup_upd` on `log_entries` should stop climbing.

### Step 5: drop the columns

Separate release, after a rollback window.
Drops the `transaction_id` index too, which is the last piece of the write amplification.

**This is the only step that rewrites `log_entries`.**
Consider folding it into the partitioning pass, which rewrites the table anyway - the same reasoning that deferred `jobs` to `log_ingestion_batches`.

---

## 6. Verification

**Correctness - the strong one**

- `log_transactions` byte-identical before and after, on a real window. Deterministic `uuid5` IDs make this exact, not approximate.
- The dual-write comparison query above returns 0 across the whole table.
- Existing view/pagination/detail tests pass unchanged.

**The point of the exercise**

- `n_tup_upd` on `log_entries` stops increasing after Step 4. Measure before and after; it is currently 105,838,123.
- `n_dead_tup` on `log_entries` falls after the next autovacuum.
- `log_entry_assignment` is the new churn hotspot (the unsealed tail is delete+reinsert every finalize) - confirm autovacuum keeps up. It is small, so it should.

**No regression**

- Backfill: `regroup_all` reproduces identical IDs and assignments, and is now read-only on `log_entries`.
- Purge: deleting a customer removes assignments via the cascade chain, leaving no orphans.
- Delete one SSH source: entries, transactions and assignments all survive.
- Late/back-dated data: a window rebuild deletes and recreates assignments, not entries.
- The agent tools return the same ordered entries as the API for the same transaction.

---

## 7. Out of scope

- Partitioning and retention. This lands first because it is independently provable; Step 5 should probably ride with it.
- Replacing `jobs` with `log_ingestion_batches`.
- ML.
- The `./uploads` historical sweep - unrelated, and smaller.
