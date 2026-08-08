# Stable transaction identity: assign it once, carry it forward

## Context

Amin asked how Stage 2 deleting and recreating transactions affects notifications, and flagged the
15-minute overlap. Investigating it found a root cause that reaches well beyond notifications, so this
plan fixes that rather than the symptoms.

He has said explicitly: no patchwork, outage is acceptable, the product is not live, find the real fix.

### The root cause, in one sentence

**A transaction's identity is derived from its content, and its content can change.**

`derive_transactions.py:433-446`:

```python
def _anchor(entries):
    req = next((e for e in entries if e.entry_type.value in ("request", "request_body")), None)
    e = req or entries[0]           # <- the anchor can be a DIFFERENT entry after a rebuild
    return f"{e.customer_code}:{e.entry_hash}"

def _txn_id(entries):
    return uuid.uuid5(_TXN_NS, _anchor(entries))
```

`regroup_window` deletes every transaction anchored in `[lo-pad, hi]` (sealed included) and rebuilds
from the entries present at that moment. If the membership changed - a backfilled file adds an earlier
entry, or supplies the REQUEST line that was missing - the anchor changes, so the id changes.

Anything that remembered the old id is now wrong: notification dedupe (re-alerts), alert deep links
(404), agent citations, saved frontend links.

### Measured on production, one full day, 16,153 transactions

| | count | reqid | entries | note |
| --- | --- | --- | --- | --- |
| Has a REQUEST entry - stable anchor | 15,937 (98.7%) | 15,933 | 275,326 | id cannot change |
| No REQUEST entry - **fragile anchor** | **216 (1.3%)** | **0** | 1,962 | id CAN change |

The two populations are exactly the same rows: no REQUEST entry means no request params, hence no
`reqid`. Their contents: 1,138 info, 276 mi_result, 232 sql, **161 response**, 155 mi_call - real WMS
activity whose REQUEST line did not parse, not garbage.

`reqid` is otherwise near-perfect as a natural key (15,931 distinct across 15,933), but the 1.3% that
lack one are precisely the unstable ones, so it cannot solve this.

### What is already fine, and must not regress

- **No alert is ever missed.** A rebuild re-stamps `created_at` to now, so a rebuilt transaction always
  re-enters the notification cursor's feed AHEAD of the bookmark, never behind it.
- **Rebuild churn is bounded.** 533 unsealed transactions right now; the seal gate already excludes the
  143 `incomplete` ones, leaving ~390 re-read until they seal. Dedupe drops them all.

### Why the two obvious fixes are both wrong

**"Always anchor on `entries[0]`."** Still content-derived, so still unstable - an earlier entry can
still join. And it changes every existing id ONCE, re-firing every open alert and breaking every saved
link, to fix 1.3%.

**"Only create a transaction when a REQUEST exists."** Would hide 216 groups and 1,962 entries of real
activity, including 161 responses, from the feed entirely.

Neither addresses the actual defect, which is deriving a stable thing from an unstable one.

## The fix: identity by continuity

Stop recomputing identity on every rebuild. Assign it once, then carry it forward by matching rebuilt
groups to the transactions that already own those entries.

`log_entry_assignment` already records exactly which entries belonged to which transaction, so the
information needed is present - it is simply being thrown away before the rebuild.

### One bulk read, not one per record

The map is read for **every** entry in the window, in a **single** query - never one lookup per
transaction or per entry. An N+1 here would be the whole cost of the change; as one bulk read it is
noise.

The repo already has this exact shape twice, and the new helper is a sibling of both:

- `assignments.load_seq_by_entry` (`assignments.py:130`) - `{entry_id: seq}`, one bulk query
- `assignments.load_transaction_by_entry` (`assignments.py:146`) - `{entry_id: transaction_id}`,
  keyed by a list of entry ids

The new one is `load_owners_in_window(db, customer_code, window)` - the same projection as
`load_transaction_by_entry` but bounded by the window rather than by an id list, because the entry
ids are not known until after the read. It reuses `UtcWindow.covers(..., include_null=True)`
(`time_bounds.py`), the same predicate `_existing_ids_stmt` and `entries_stmt` already use.

### The steps, in order

Inside `regroup_window`, exactly one new step and one changed step:

```
  1. compute `freed`                          ← unchanged
  2. READ the owner map, ONE query            ← NEW. Position is load-bearing: step 3
     {entry_id -> transaction_id}                destroys exactly the rows being read.
  3. delete assignments for `freed`           ← unchanged
  4. delete transactions in `freed`           ← unchanged
  5. re-read unassigned entries in window     ← unchanged
  6. group them (state machine)               ← unchanged
  7. resolve ids                              ← CHANGED (in `_resolve_ids`)
  8. insert transactions + assignments        ← unchanged
```

Step 7, per group, in memory:

```
  owners = Counter(map[e.id] for e in group.entries
                   if map.get(e.id) in freed_set)     ← the safety guard, see below
  winner = owners.most_common(1)

  no winner              -> _txn_id(entries)          exactly as today
  winner unclaimed       -> reuse it
  winner already claimed -> larger group keeps it, this one falls back to _txn_id
```

Groups are resolved in a deterministic order (descending match count, then `_txn_id` as tiebreak) so
the same input always produces the same output - a rebuild must be reproducible.

### Why there is no data loss

**Raw log entries are never touched.** `regroup_window` deletes only `log_transactions` and
`log_entry_assignment` rows. `log_entries` - the source of truth - is append-only and this change does
not go near it. Anything mis-grouped is recoverable by re-running the regroup; anything mis-identified
is recoverable the same way. That is the floor under everything below.

**The map is read before the delete.** Reading it after would return an empty dict and silently
degrade to today's behaviour. The ordering is the correctness condition, so a test asserts the map is
non-empty for a window that had transactions.

**Only `freed` ids may be reused - this is the one that matters.** The unique constraint is
`UNIQUE NULLS NOT DISTINCT (id, started_at)`, not unique on `id` alone (the partition key must be in
it). So the database will **not** stop a second row with the same id landing in a different partition.
The `DELETE` is `WHERE id IN (freed)` with no `started_at` bound, so a freed id is definitely gone and
safe to reuse; a non-freed id is not. Restricting matches to `freed_set` is what makes reuse safe, and
it costs one Python set lookup.

In practice the entries read in step 5 are only the unassigned ones, so they can only have come from
`freed` or from nothing - but relying on that would make correctness depend on a property two
functions away. The guard states it where it is needed and makes it testable.

**The split tiebreak is correctness, not cosmetics.** Two groups reusing one id would write
`(T, started_a)` and `(T, started_b)`. Different `started_at`, so the constraint permits it and two
rows with the same id exist silently. The "already claimed" branch is what prevents that, and it gets
its own test.

**Nothing new is skipped.** The clash check (`_existing_transaction_ids`) still runs on the resolved
ids. A reused id was deleted in the same transaction, so it cannot collide with itself.

### Performance - measured on production, not assumed

One added query per regroup window. Two candidate shapes were measured on live data; the window-bounded
one wins clearly, because it prunes partitions at PLAN time:

| shape | planning | execution |
| --- | --- | --- |
| `transaction_id IN (SELECT ...)` - no time bound | **109.1 ms** | 4.98 ms |
| **`customer_code` + `entry_ts` window (include NULL)** | **23.3 ms** | **0.50 ms** |

The first form cannot prune: without a predicate on the partition key the planner opens all ~20
`log_entry_assignment` partitions and plans an index scan into each, and that planning dominates. The
second bounds `entry_ts` to the window `regroup_window` already has, so the planner touches two
partitions plus the default. **That is the form to implement.**

Context for the 0.5 ms: `regroup_window` already deletes those transactions, deletes those assignment
rows, re-reads every entry in the padded window, re-runs the grouping state machine, and inserts the
results. The added read is on rows that are about to be deleted anyway.

Everything else is in-memory: one dict lookup per entry and one `Counter` per group, O(entries).

**No new index, no new table, no extra write, no extra round trip per record.** Stage 1, Stage 2
throughput and every read path are untouched.

### The seam already exists

`derive_transactions._resolve_ids` is the single place ids are decided:

```python
async def _resolve_ids(db, builders, customer_code):
    for b in builders:
        b.entries.sort(key=_entry_stream_order)
    ids = [_txn_id(b.entries) for b in builders]          # <- becomes: continuity first, _txn_id as fallback
    existing = await _existing_transaction_ids(db, customer_code, ids, window=_clash_window(...))
    return ids, existing
```

Only this function and `regroup_window` change. `_group`, `_persist`, `_is_sealed`, the evaluators and
the whole notification pipeline are untouched.

### What it gives, permanently

- Transaction ids never change -> notification dedupe is sound forever; the 1.3% double-alert is gone
- Deep links never die, because a rebuilt transaction keeps its row identity
- Agent citations and saved frontend links stay valid indefinitely
- `_txn_id` stays exactly as it is for genuinely new transactions, so existing ids are NOT rewritten -
  no mass re-alert, no broken links on deploy

## A second change this unlocks: update in place

Once identity is stable, the DELETE + INSERT can become an UPDATE. Then `created_at` stops churning,
and the ~390 unsealed transactions stop re-entering the notification feed on every rebuild.

That requires the cursor to read a new `updated_at` column instead of `created_at`, which is strictly
more correct anyway - "new OR changed since I looked" is what an incremental reader actually wants.

**Recommended as a SEPARATE, later step**, not part of this one:

- an UPDATE that changes `started_at` across a day boundary moves the row between partitions;
- `log_transactions` carries ~15 indexes, so in-place updates reintroduce write amplification on the
  hot tail - the exact problem the assignment split removed from `log_entries`, at 50k rows rather
  than 1.9M, but it must be measured rather than assumed.

Identity stability is the fix. Update-in-place is an optimisation that depends on it.

## Files

- `app/services/mnp_log_ingestion/pipeline/assignments.py` - new `load_owners_in_window`, a sibling of
  the existing `load_seq_by_entry` / `load_transaction_by_entry` bulk loaders
- `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` - `_resolve_ids` (continuity
  matching) and `regroup_window` (capture the owner map before deleting)
- `tests/test_transaction_identity_chunk35.py` - new
- `docs/database-er-diagram.md` and `docs/plan/2026-08-08_notification-architecture.html` - the
  identity model is currently documented as content-derived

## Testing

Failing-first, per the standing bar, targeting CRAP 1-3. The cases that matter:

- a rebuild with identical entries keeps the id (no regression on the 98.7%)
- **a rebuild that gains an earlier entry keeps the id** - the bug, reproduced first
- a rebuild that gains the missing REQUEST line keeps the id
- a merge keeps the larger contributor's id; a split keeps it for the larger half
- a genuinely new group still mints a `_txn_id`, so existing production ids are unchanged
- an old transaction whose entries all moved away is deleted

Data-loss and safety cases, each mapped to a specific failure:

- **the map is read BEFORE the delete** - reading it after returns `{}` and silently reverts to
  today's behaviour, so assert it is non-empty for a window that had transactions
- **an id outside `freed` is never reused** - would write a second row with the same id in a different
  partition, which `UNIQUE (id, started_at)` does NOT catch
- **two groups never share one reused id** - same silent-duplicate failure, via the split path
- `EXPLAIN` on the owner-map query shows partition pruning, so the 109 ms planning form cannot come
  back unnoticed (same technique as `_existing_ids_stmt` and `entries_stmt`, which are already
  EXPLAIN-asserted)
- entries with a NULL `entry_ts` (the default partition) are still found - the `include_null` branch
- **no `log_entries` row is deleted or modified by a regroup**, before or after this change
- end to end: a notification alerts once across a backfill-triggered rebuild
- no regression: `regroup_all` is still idempotent, and the deterministic id still holds for new data

## Verification

1. `pytest -q` green three consecutive runs (this repo has had order-dependent flakes).
2. Mutation-check each decision - drop the continuity lookup, invert the plurality rule, drop the
   split tiebreak, remove the `freed_set` guard, move the map read after the delete - and confirm a
   test fails for each.
3. Reproduce the real scenario against the local database: ingest a file, stitch, then backfill an
   earlier file covering the same window, and assert every transaction id is unchanged.
4. Assert directly that no id is ever duplicated:
   `SELECT id FROM log_transactions GROUP BY id HAVING count(*) > 1` returns zero rows after a
   regroup - the check the partitioned unique constraint cannot make for us.
5. Confirm on a copy of production data that applying this does NOT change any existing id, and that
   `log_entries` row counts are identical before and after.

## Explicitly not in scope

Update-in-place and the `updated_at` cursor (above). Changing `_anchor`. Anything in the notification
pipeline - once identity is stable, nothing there needs to change.

## Is it safe to execute now?

Yes. Nothing about this change needs a window, a migration, or an outage.

**No schema change.** No new table, no new column, no new index, so no Alembic migration and nothing
to roll forward or back at the database level. `deploy.sh`'s ordinary pull-then-migrate-then-restart
path is sufficient.

**No existing data is rewritten.** `_txn_id` is untouched and stays the fallback, so every transaction
id in production today keeps the value it already has. Deploying this re-fires no alert and breaks no
saved link. That is the property that makes it safe to ship at any time rather than during a quiet
period.

**It is a pure code change in one call path.** `regroup_window` and `_resolve_ids` in
`derive_transactions.py`, plus one new bulk loader in `assignments.py`. Stage 1 ingest, the read
endpoints, the notification subsystem and the partition worker are all untouched.

**Rollback is a revert.** With no migration and no data rewrite, reverting the commit restores exactly
today's behaviour. New transactions created while it was live keep working, because a continuity id is
an ordinary uuid and nothing downstream distinguishes it from a `_txn_id` one.

### Two things to be aware of, neither blocking

**The three notification migrations are still undeployed** (`b3d914c7ea52`, `c7a02f68b1d4`,
`d5c81b60a473`). That deploy needs stop-workers -> migrate -> deploy -> start, NOT `deploy.sh`'s order.
This change is fully independent of it and can ship before, after, or with it - but the two should not
be conflated when planning the deploy.

**The `docs/` updates are part of the change, not a follow-up.** `docs/database-er-diagram.md` is a
maintained artifact under this repo's CLAUDE.md, and the notification HTML currently documents identity
as content-derived. Both are in the Files list above.

### Housekeeping

The plan file is still at the harness default path
`~/.claude/plans/okay-in-the-backend-bright-blossom.md`, which does not follow the required naming
rule. On execution it is copied into the repo as
`docs/plan/2026-08-08_19-43_stable-transaction-identity-by-continuity.md`.
