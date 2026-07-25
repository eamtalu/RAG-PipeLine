# logs.py - `view_transactions` query flow, the sort problem, and the fix

A focused reference for the feed endpoint `GET /api/v1/logs/transactions/view`, implemented in
`app/api/v1/logs.py` (`view_transactions`).
It records, for the record: how the endpoint runs step by step, exactly where the sorting problem
was, and what we changed to fix it.

For the wider incident this came out of (worker timeouts, table bloat, the failing disk), see
`docs/transactions-view-load-spike-and-db-concepts-primer.md`.
For the original outage and the composite index on `log_transactions`, see
`docs/debugging-worker-timeout-outage.md`.

---

## What the endpoint does

Given a required `date` (plus optional `user`, `hour`, `status`, `order_number`, `item_number`,
`verbose`), it returns ONE page of that day's transactions rendered as human-readable text,
oldest -> newest, paginated by `limit` + `offset`.

The data is nested: one `log_transactions` row (the summary) has many `log_entries` rows (the
request line, each step, the response).
So rendering a page needs both tables: the transaction summaries for the page, and all of their
entries.

---

## How it was implemented - step by step

### Step 1 - COUNT query (table: `log_transactions`)

Count how many transactions match the filters, for the pager total.

```python
conds = [LogTransaction.customer_code == customer, LogTransaction.date == date]
# ... optional user / hour / status / order_number / item_number appended to conds ...
total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))
```

Cheap: an index-only count over `(customer_code, date)`.

### Step 2 - PAGE query (table: `log_transactions`)

Fetch the up-to-`limit` transaction summaries for this page, oldest -> newest.

```python
txns = (await db.execute(
    select(LogTransaction).where(*conds)
    .order_by(LogTransaction.started_at.asc().nullslast(), LogTransaction.id.asc())
    .limit(limit).offset(offset)
)).scalars().all()
```

Cheap: served by the composite index `ix_log_transactions_customer_date_started`.
`id` is a stable tiebreak so offset paging is deterministic.

### Step 3 - ENTRY-FETCH query (table: `log_entries`)  <-- this is where the problem was

Take the page's transaction ids and fetch all their entries, bounded by a runaway cap.

```python
ids = [t.id for t in txns]                       # up to 500 transaction ids
entry_rows = (await db.execute(
    select(LogEntry).where(LogEntry.transaction_id.in_(ids))
    .order_by(LogEntry.seq.asc().nullslast(), LogEntry.line_number.asc())   # <-- the costly sort
    .limit(MAX_RENDER_ENTRIES + 1)               # MAX_RENDER_ENTRIES = 50_000
)).scalars().all()
```

### Step 4 - group + render (in Python)

Bucket the entries by transaction, then render each transaction's block in `txns` order.

```python
by_txn = {}
for e in entry_rows:
    by_txn.setdefault(e.transaction_id, []).append(e)
blocks = [render_transaction(t, by_txn.get(t.id, []), verbose=verbose) for t in txns]
```

The overflow guard: if `len(entry_rows) > MAX_RENDER_ENTRIES` the endpoint returns a
"too many entries - use a smaller `limit`" notice and renders nothing.

---

## Where the issue was (Step 3's ORDER BY)

The `ORDER BY seq, line_number` in Step 3 sorted **all** of the page's entries **globally, across
every transaction at once**, before anything was grouped.

Two problems with that:

1. **It sorted the wrong dimension.**
   Step 4 immediately re-buckets the entries by transaction and renders per transaction, so the
   ordering that actually matters is *within* each transaction.
   The global *cross-transaction* order the sort produced was thrown away by the grouping.

2. **The global sort spilled to disk.**
   Postgres sorts in a small RAM budget (`work_mem` = 4 MB on this box).
   A busy page's entries exceed that, so the sort spilled to a temporary file on the (slow, failing)
   disk - observed in the query plan as:
   ```
   Sort Method: external merge  Disk: 6904kB
   ```
   And because `LIMIT` sits on top of the sort, Postgres had to sort everything before the limit
   could apply - the cap could not short-circuit the work.

Illustration of the wasted work:

```
   global sort by (seq, line_number) interleaves transactions:
     seq1-A  seq1-B  seq1-C  seq2-A  seq2-B  ...      (A/B/C = different transactions)
        |       |       |
   then Step 4 splits them straight back apart, per transaction:
     A: [seq1-A, seq2-A, ...]   B: [seq1-B, ...]   C: [seq1-C, ...]
   -> the cross-transaction ordering we paid to produce is discarded.
```

---

## What we changed (the fix)

Stop asking the database for a global sort.
Fetch the entries **unordered**, and restore the only ordering that matters - **within each
transaction** - in Python, where each bucket is tiny (~17 entries) and sorting is free.

### Step 3, after

```python
# no ORDER BY: the global sort spilled to disk and its cross-transaction order is discarded by
# _render anyway. Fetch unordered (no Sort node, LIMIT can short-circuit, no temp-file), and
# restore per-transaction order in Python below.
entry_rows = (await db.execute(
    select(LogEntry).where(LogEntry.transaction_id.in_(ids))
    .limit(MAX_RENDER_ENTRIES + 1)
)).scalars().all()
```

### Step 4, after - sort each bucket in Python

```python
by_txn = {}
for e in entry_rows:
    by_txn.setdefault(e.transaction_id, []).append(e)
for entries in by_txn.values():
    entries.sort(key=_entry_sort_key)            # per-transaction order (was the SQL ORDER BY)
blocks = [render_transaction(t, by_txn.get(t.id, []), verbose=verbose) for t in txns]
```

### The sort key (the one thing that must be exactly right)

`seq` and `line_number` are both nullable, so the key must reproduce
`seq ASC NULLS LAST, line_number ASC NULLS LAST` and must never compare `None` to `None`:

```python
def _entry_sort_key(e):
    return (e.seq is None, e.seq or 0, e.line_number is None, e.line_number or 0)
```

- The `x is None` flags put NULLs last (`False < True`).
- The `x or 0` substitutions ensure Python never compares `None` to `None`.
  A naive `(e.seq, e.line_number)` key raises `TypeError` the moment it meets a NULL, and even
  `(e.seq is None, e.seq, ...)` raises when two rows both have `seq = None`.

---

## Why there is no regression

- **Cross-transaction order** (which transaction renders first) comes from `txns`, not from the
  entry sort - unchanged.
- **Within-transaction order** is reproduced by `_entry_sort_key` using the same ordering the SQL
  used - identical bucket contents.
- **The overflow guard refuses instead of truncating.**
  In the pathological `> 50k` case both versions return the same "too many entries" notice and
  render nothing, so *which* rows the `LIMIT` kept is irrelevant.
  Under the cap (the normal case, real max ~8.4k) both fetch every matching row regardless of order.
  Output is byte-identical either way.

`EXPLAIN` confirms the structural difference: with the `ORDER BY` the plan has a `Sort` node; without
it, there is none.

### What was deliberately NOT changed

`_load_transaction_entries` (the single-transaction detail endpoint,
`GET /transactions/{id}/view`) keeps its `ORDER BY seq, line_number`.
It is a single-transaction equality lookup (already cheap, no cross-transaction sort), and when
truncated it renders an *ordered prefix* (`rows[:MAX_RENDER_ENTRIES]`) - so there the ordering is
load-bearing and must stay in SQL.

---

## Tests

`tests/test_transactions_view_entry_ordering_chunk16.py`:

- Pure unit tests of `_entry_sort_key`: seq-then-line ordering, NULL `seq` last, **two** NULL `seq`s
  (must not raise), NULL `line_number` last, all-NULL rows, and permutation-independence.
- An end-to-end test that seeds a transaction whose entries are INSERTED out of order (including a
  NULL-`seq` entry) and asserts the rendered feed shows them in seq/line order.

---

## Key code references

- `app/api/v1/logs.py`
  - `view_transactions` - the endpoint (Steps 1-4).
  - `_entry_sort_key` - the NULL-safe per-transaction sort key.
  - `MAX_RENDER_ENTRIES = 50_000` - the runaway render cap.
  - `_load_transaction_entries` - the single-transaction path, left on SQL ordering on purpose.
- `app/services/mnp_log_ingestion/render.py` - `render_transaction` / `_steps` (consume per-txn order).
