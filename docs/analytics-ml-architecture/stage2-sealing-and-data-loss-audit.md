# Stage 2 sealing and data-loss audit

A beginner-readable walkthrough of how log entries become log transactions, what `sealed` actually does today, and the eleven ways an entry can end up belonging to no transaction.

Date: 2026-08-20.
Verified against alembic head `e4b28f5c9107`.
Project root for every path below: `/Users/amintalukder/myworkspace/personal/python work/RAG FAST API/`.

## What this document answers

Four questions:

1. Does anything get *sealed* while a log transaction is being built from log entries, and if so, what?
2. How does the sealing actually happen, end to end?
3. Is `sealed` still used by regroup, or has it quietly stopped mattering?
4. Can data be lost at this stage?

## How to read this

Every claim here comes from **executable code**: function bodies, SQL, ORM column definitions, alembic migrations, config defaults, and test assertions.
Docstrings, `#` comments, other documents in `docs/`, and commit messages were deliberately excluded as evidence.
That was the point of the exercise, and it is why several of the findings below are places where a docstring states a guarantee the code does not make.
Where that happens it is called out explicitly.

Sections 1 to 5 build up the mental model from nothing.
Section 6 answers question 3.
Section 7 answers question 4.
Section 8 is the measured evidence from a real database.
Sections 9 and 10 are what to do about it.

If you already know the pipeline, skip to section 6.

## Status of the findings

Nothing in this document has been fixed.
It is a record of the code as it stood on 2026-08-20.
The fixes in section 9 are recommendations for a separate change, not work that has happened.

---

## 1. The three tables, in plain words

The WMS server writes a text log file.
Every **line** of that file becomes one row in `log_entries`.

A single user action, say scanning an item, produces **many** lines: a REQUEST line, some internal work lines, some M3 calls, then a RESPONSE line.
Those lines together are one **transaction**.

| Table | One row is | Written by |
|---|---|---|
| `log_entries` | one line of the log file | Stage 1, `app/services/mnp_log_ingestion/pipeline/parse_insert.py` |
| `log_transactions` | one complete request/response cycle | Stage 2, `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` |
| `log_entry_assignment` | "entry #47 belongs to transaction X, at position 3" | Stage 2 |

The important part: **`log_transactions` is not read out of the file, it is computed.**
Stage 2 reads entries in timestamp order, works out which lines belong together, and writes the transaction rows.

The *link* between an entry and its transaction lives in a third table, `log_entry_assignment`.
That third table is what everything in section 7 turns on.
If an entry has no row in it, the entry belongs to no transaction.
The line is still in the database, and `GET /logs/entries` still returns it with `transaction_id: null` (`app/api/v1/logs.py:286`, `:296`), but the transaction view cannot see it.

This split is deliberate and recent.
Stage 2 used to write the grouping back onto an indexed `log_entries.transaction_id` column, which meant every rebuild rewrote both heap and index.
`app/services/mnp_log_ingestion/pipeline/assignments.py:11-14` records the production measurement that motivated moving it out: 105.8M updates at 0.0% HOT.

---

## 2. Why Stage 2 does its work over and over

Log files rotate.
The REQUEST line can be at the end of `log.1` and its RESPONSE at the start of `log.2`.
When Stage 2 first runs it may only have the REQUEST, so it writes a transaction with `status = incomplete`.

Later the RESPONSE arrives.
Now Stage 2 has to throw that incomplete transaction away and rebuild it as a complete one.

So Stage 2's normal mode is: **delete some transactions, then rebuild them from their entries**, repeatedly, on a loop.

That is only safe because transaction ids are computed from content rather than randomly generated.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:445-446`

```python
445  def _txn_id(entries: list[LogEntry]) -> uuid.UUID:
446      return uuid.uuid5(_TXN_NS, _anchor(entries))
```

`_anchor` (`derive_transactions.py:433-442`) prefers the REQUEST line's `entry_hash`, which is a sha256 over the raw line including its millisecond timestamp.
So the id is a fingerprint of the transaction's opening line.
Rebuild the same transaction a thousand times and you get the same UUID every time, which is why saved or cited ids stay valid across rebuilds.

---

## 3. What `sealed` means

If Stage 2 rebuilt every transaction on every cycle it would re-read the whole table forever, which does not scale.
So there is a flag whose meaning is: **nothing new can ever join this transaction, so stop reprocessing it.**

`app/persistence/models/log_transaction.py:69`

```python
69  sealed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False, index=True)
```

It arrived in `alembic/versions/d4e7a1b9c206_add_sealed_to_log_transactions.py:24-29` as `NOT NULL DEFAULT false` with no explicit backfill, so every row that already existed became unsealed at migration time.
The index is recreated after the partitioning rewrite at `alembic/versions/a1f6d70b3e92_partition_log_tables_by_utc_day.py:97`, so it still exists today.

### The predicate

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:541-552`

```python
541  def _is_sealed(values: dict, seal_cutoff: datetime | None, abandon_cutoff: datetime | None) -> bool:
547      ended = values.get("ended_at")
548      if ended is None or seal_cutoff is None:
549          return False
550      if values["status"] == LogTransactionStatus.incomplete:
551          return abandon_cutoff is not None and ended < abandon_cutoff
552      return ended < seal_cutoff
```

Three rules:

1. Line 549: a transaction with no usable timestamp is **never** sealed.
2. Line 552: a *finished* transaction, meaning it has a RESPONSE or a hard error, seals once it is older than `seal_cutoff`.
3. Line 551: an *unfinished* one waits much longer, until `abandon_cutoff`. The reasoning is that a slow response might still be coming, so do not close the door early.

`ended_at` is the newest timestamp among the transaction's own entries, computed at `derive_transactions.py:106-108`.
It is pure **event time**, never ingest or write time.
`status` comes from the same function at `:112-131`; `incomplete` means no RESPONSE entry and no ERROR entry.

### Where the cutoffs come from

This is the part most people get wrong, so read it slowly.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:529-538`

```python
534      max_ts = await _max_entry_ts(db, customer_code)
535      if max_ts is None:
536          return None, None
537      return (max_ts - timedelta(seconds=settings.log_seal_window_seconds),
538              max_ts - timedelta(seconds=settings.log_abandon_window_seconds))
```

`max_ts` is **not the wall clock**.
It is the newest timestamp found inside *that customer's* log data, which is to say the log's own notion of "now".

That is deliberate.
If you import a log file from January, then "now" for that customer is January, so January transactions seal correctly instead of all looking brand new.
It also means one customer's stale logs still seal while another customer's active stream does not drag them along.

With the defaults from `app/settings.py:80` and `:84`, which are 900 and 3600 seconds:

```
  customer's newest log line
                            |
  ...-----------------------|
     ^                 ^    ^
     |                 |    max_ts
     |                 max_ts - 15min  = seal_cutoff
     max_ts - 1 hour           = abandon_cutoff

  finished transaction ending here  -> SEALED
                    <---------------|
  unfinished one ending here        -> SEALED (gave up waiting)
      <-----------|
  unfinished one ending here        -> still UNSEALED (response may still come)
                       |----------->
```

`_max_entry_ts` (`derive_transactions.py:513-526`) tries a bounded probe first, floored at `now(UTC) - log_cutoff_lookback_days` (7 days, `app/settings.py:91`) so a partitioned `log_entries` prunes instead of taking the max of every partition, and falls back to an unbounded `MAX` when the probe returns NULL.
The wall clock enters only as that pruning floor.
It cannot change the resulting value, only which query finds it.

### Where the flag is written

Exactly one place in the entire codebase.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:644` and `:610`

```python
644          is_sealed = _is_sealed(values, seal_cutoff, abandon_cutoff)
...
610      txn = LogTransaction(id=tid, sealed=is_sealed, **values)
```

Line 610 is an INSERT.
There is **no `UPDATE ... SET sealed`** anywhere in `app/`.
So a transaction never becomes sealed in place.
It is deleted and re-inserted, and the flag is recalculated from scratch each time.

`sealed` is also never serialized into any API response.
The transaction dict at `app/api/v1/logs.py:317-343` has no `sealed` key.

---

## 4. The live path, step by step

This is what actually runs in production.
Five steps.

### Step 1: a file is ingested, and a "please stitch this" ticket is written

`app/services/mnp_log_ingestion/pipeline/parse_insert.py:133-143`

```python
133          async def flush(b: list[dict]) -> int:
134              nonlocal lo, hi
135              ts_list = await _insert_dedup(db, b)
136              for ts in ts_list:
137                  if ts is None:
138                      continue
139                  if lo is None or ts < lo:
140                      lo = ts
141                  if hi is None or ts > hi:
142                      hi = ts
143              return len(ts_list)
```

`lo` and `hi` are the earliest and latest timestamp of the rows just inserted.
Line 137 skips rows whose timestamp could not be parsed, which matters later.

`app/services/mnp_log_ingestion/pipeline/parse_insert.py:188-190`

```python
188          if inserted and lo is not None and hi is not None:
189              db.add(LogRegroupPending(customer_code=customer_code, job_id=job_id,
190                                       range_start=lo, range_end=hi))
```

A row in `log_regroup_pending` is a **ticket**.
It says "I added data between `lo` and `hi`, somebody please group that time range".
Nothing else triggers Stage 2.
No ticket, no grouping.

### Step 2: a background worker picks up the ticket

`app/services/workers/log_stitch_worker.py:90-94`

```python
90  async def _stitch_customer(cc: str) -> dict:
93      async with async_session() as db:
94          return await finalize_pending(db, cc)
```

This worker is the only automated Stage 2 driver.
It is registered at `app/background.py:102` and imports only `finalize_pending` (`app/services/workers/log_stitch_worker.py:37`).

### Step 3: `finalize_pending` reads the open tickets

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:978-989`

```python
978      pend = list((await db.execute(
979          select(LogRegroupPending).where(
980              LogRegroupPending.customer_code == customer_code,
981              LogRegroupPending.consumed_at.is_(None),
982              LogRegroupPending.abandoned_at.is_(None),
987              LogRegroupPending.available_at <= func.clock_timestamp(),
988          ).order_by(LogRegroupPending.range_start.asc())
989      )).scalars().all())
```

Note what is **not** in that WHERE clause: the word `sealed`.
Work selection is driven entirely by the ticket table.

Tickets are then coalesced into disjoint runs (`_coalesce_pending`, `:923-935`) and each run is split into sub-windows of at most `log_regroup_max_window_seconds` (6 hours, `app/settings.py:126`) so per-window memory stays bounded (`_split_run`, `:938-951`).

### Step 4: `regroup_window` does the actual rebuild

The heart of it, in three parts.

**4a. Widen the window by a safety pad.**

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:837-838`

```python
837      pad = _regroup_pad()
838      lo_p, hi_p = lo - pad, hi + pad
```

`_regroup_pad()` is `max(log_regroup_pad_seconds, log_seal_window_seconds)`, which is `max(900, 900) = 900` seconds with the defaults (`:809-812`).
The reason for the pad is that a new entry might belong to a transaction that *started* before the ticket's range, so look a little wider on both sides.

**4b. Delete every transaction anchored in the window.**

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:845-851`

```python
845      freed = list((await db.execute(
846          select(LogTransaction.id).where(
847              LogTransaction.customer_code == customer_code,
848              LogTransaction.started_at >= lo_p,
849              LogTransaction.started_at <= hi,
850          )
851      )).scalars().all())
```

Look hard at lines 847 to 849.
**There is no `sealed` condition.**
This query takes sealed and unsealed transactions alike, and both are deleted along with their assignment rows (`:863-865`).

This is the single most important fact in the audit, and section 6 returns to it.

Before the delete, the current ownership map is captured so ids can be inherited rather than re-minted (`:858-861`, `assignments.load_owners_in_window`).
The ordering matters: reading it after the delete would return an empty map, which would not fail loudly, it would silently revert to minting a fresh id per rebuild.

**4c. Re-read the now-unassigned entries.**

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:873-884`

```python
873      rows = list((await db.execute(
874          select(LogEntry).where(
875              LogEntry.customer_code == customer_code,
876              assignments.is_unassigned(),
877              LogEntry.timestamp >= lo_p,
878              LogEntry.timestamp <= hi_p,
879          ).order_by(
880              LogEntry.timestamp.asc().nullslast(),
881              LogEntry.source_file.asc(),
882              LogEntry.line_number.asc(),
883          )
884      )).scalars().all())
```

`is_unassigned()` is just a NOT EXISTS against the link table.

`app/services/mnp_log_ingestion/pipeline/assignments.py:36-38`

```python
36      return ~select(LogEntryAssignment.entry_id).where(
37          LogEntryAssignment.entry_id == LogEntry.id
38      ).exists()
```

So step 4c means: give me every entry in this time range that currently belongs to no transaction.

The upper read bound is `hi_p`, not `hi`, because a freed transaction anchored at `hi` can own entries up to `pad` later and all of them are needed to stitch it whole.

The delete in 4b and the read in 4c happen in the **same** database transaction with no intermediate commit, so the freeing is visible to the read and readers never see a torn state.
That part is correct and worth knowing, because two of the other regroup functions do not do this.

### Step 5: group them and write

`_group` (`:236-430`) runs the REQUEST/RESPONSE state machine and returns a list of builders, one per transaction.
It is thread- and user-aware, keying an open transaction by `(thread, user)` so the .NET async server's interleaved streams do not cross-stitch.
Importantly, `_group` never discards an entry: every leftover open builder and every unmatched pending request is emitted as its own builder at `:425-429`.

Then `_persist` writes each builder.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:638-650`

```python
638      for b, tid in zip(builders, ids):
639          if tid in seen or tid in existing:
640              skipped += 1
641              continue
642          seen.add(tid)
643          values = _cap_over_length(b.compute(), customer_code)
644          is_sealed = _is_sealed(values, seal_cutoff, abandon_cutoff)
645          txn = await _write_transaction(db, tid=tid, values=values, is_sealed=is_sealed,
646                                         entries=b.entries, customer_code=customer_code)
```

And `_write_transaction` writes both the transaction and the links.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:610-614`

```python
610      txn = LogTransaction(id=tid, sealed=is_sealed, **values)
611      db.add(txn)
612      await db.flush()
613      await assignments.write(db, transaction_id=txn.id, entries=entries,
614                              customer_code=customer_code)
```

That is the whole live path.
Ticket in, transactions out.

---

## 5. The full live path in one diagram

```
  file
    |
    v
+---------------------------------------------------+
| 1. parse_insert -> log_entries                    |
|    ON CONFLICT DO NOTHING ... RETURNING           |
+---------------------------------------------------+
    |
    v
+---------------------------------------------------+
| 2. log_regroup_pending [range_start, range_end]   |
|    the ONLY work queue Stage 2 reads              |
+---------------------------------------------------+
    |
    v
+---------------------------------------------------+
| 3. log_stitch_worker -> finalize_pending          |
|    coalesce tickets, split into <=6h sub-windows  |
+---------------------------------------------------+
    |
    v
+---------------------------------------------------+
| 4. regroup_window                                 |
|    a. pad the window by 900s                      |
|    b. DELETE txns anchored in [lo-900, hi]        |
|       (sealed and unsealed alike)                 |
|    c. re-read unassigned entries in [lo-900,hi+900]|
+---------------------------------------------------+
    |
    v
+---------------------------------------------------+
| 5. _group -> _persist -> assignments.write        |
|    sealed is COMPUTED and INSERTED here           |
+---------------------------------------------------+
    |
    v
   log_transactions + log_entry_assignment
```

---

## 6. Is `sealed` still used for regroup?

**No, not by anything the system runs on its own.**

Notice that nowhere in section 4 did the code *read* `sealed`.
It computed it at line 644 and wrote it at line 610, but never read it back to decide anything.

There is exactly one regroup function that reads it, and it is not on the live path.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:779-780`

```python
779      unsealed_stmt = select(LogTransaction.id).where(LogTransaction.sealed.is_(False))
780      free_stmt = delete(LogTransaction).where(LogTransaction.sealed.is_(False))
```

That is `regroup_incremental`.
It says "free only the unsealed ones, leave sealed rows alone", which is exactly what `sealed` was invented for.
But it is reachable from only two places, both manual HTTP endpoints:

- `app/api/v1/logs.py:358`, `POST /logs/regroup?incremental=true`. Note the endpoint's own default is `incremental=False`, which routes to `regroup_all`, and that ignores `sealed` entirely.
- `app/api/v1/logs.py:728`, the tail of the date-range delete endpoint.

`regroup_incremental` appears nowhere in `app/background.py` or `app/worker.py`.
So in normal running that branch never executes.

### What still depends on `sealed`

The notification system, for a completely different reason.

`app/services/notifications/rules/stability.py:40-52`

```python
40  def is_alertable(status: LogTransactionStatus | None, *, sealed: bool) -> bool:
46      if status is None:
47          return False
48      if sealed:
49          return True
50      if settings.notification_alert_only_sealed:
51          return False
52      return status not in UNSTABLE_WHILE_UNSEALED
```

and the same rule as SQL, so the churn is never fetched in the first place:

`app/services/notifications/rules/stability.py:63-68`

```python
63      if settings.notification_alert_only_sealed:
64          return LogTransaction.sealed.is_(True)
65      return or_(
66          LogTransaction.sealed.is_(True),
67          LogTransaction.status.notin_(list(UNSTABLE_WHILE_UNSEALED)),
68      )
```

Consumed at `app/services/notifications/rules/engine.py:114-117` as a filter on the engine's window query.

The logic: do not alert "transaction incomplete!" while the transaction is unsealed, because minutes later the RESPONSE arrives and it becomes a success.
`dedup_key` is stable per (rule, transaction), so once that alert is out no correction ever follows, and the channel keeps a permanent record of a problem that resolved itself.
`UNSTABLE_WHILE_UNSEALED` is `frozenset({incomplete})` (`stability.py:37`), and `settings.notification_alert_only_sealed` defaults to `False` (`app/settings.py:285`).

This behaviour is pinned by `tests/test_notification_seal_gate_chunk29.py`: rule assertions at `:56-94`, WHERE-clause shape at `:112` and `:127-128`, a database round trip at `:185-204`, and end-to-end engine tests at `:297-331`.

### The answer, stated plainly

`sealed` is no longer what protects regroup.
It is now a "this status is final, safe to alert on" marker for notifications.
Regroup gets its safety from a different mechanism: the ticket table for *what* to rebuild, and deterministic plus inherited ids for *identity* across rebuilds.

Identity stability is carried by `log_entry_assignment` and `continuity`, which is what `sealed` used to provide by never freeing sealed rows.
`tests/test_transaction_identity_chunk35.py` is the evidence: it asserts id continuity across rebuilds at `:113-131`, `:133-144`, `:192-205` and `:377-395`, every rebuild goes through `regroup_window`, and the word `sealed` appears in that file only in its module docstring.

But Stage 2 must still keep computing `sealed` correctly, because notifications read it.
It is not dead, it has changed owner.

### Four stale statements that say otherwise

| Location | What it claims | Reality |
|---|---|---|
| `app/api/v1/logs.py:350` | Query description: "only regroup the unsealed live tail (fast, what the worker runs)" | The worker runs `finalize_pending` -> `regroup_window`. This text is user-facing OpenAPI. |
| `app/services/mnp_log_ingestion/pipeline/derive_transactions.py:11-13` | "regroup_incremental ... This is what the worker runs" | Same falsehood. |
| `app/persistence/models/log_transaction.py:67-68` | "so incremental Stage 2 never recomputes it" | `regroup_window` recomputes sealed rows every cycle. |
| `app/services/mnp_log_ingestion/pipeline/derive_transactions.py:652-653` | Warns "skipped N builder(s) with an already-sealed id" | The check behind it (`_existing_ids_stmt`, `:473-482`) filters on id existence and `started_at`, never on `sealed`. |

---

## 7. How data goes missing

First the good news.

**Log entries are never deleted by Stage 2.**
`log_entries` is insert-only.
So "data loss" here does not mean the line disappears.
It means:

> the entry has no row in `log_entry_assignment`, so it belongs to no transaction, so the transaction view cannot show it.

The line is still there.
It is still visible at `GET /logs/entries` with `transaction_id: null`.
But as far as the product is concerned it is invisible, and there is no reconciliation that would tell you (leak 11).

There is one exception where the raw row really does disappear, and that is leak 8, where retention drops the partition.

Three leaks are walked slowly below, because once you see the shape the other eight are the same shape.

### Leak 1: an entry with no timestamp is never grouped

Some log lines have a timestamp the parser cannot read.
`log_entries.timestamp` is then NULL, and the row lives in the DEFAULT partition.

Look again at step 4c:

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:877-878`

```python
877              LogEntry.timestamp >= lo_p,
878              LogEntry.timestamp <= hi_p,
```

Here is the SQL gotcha.
In SQL, `NULL >= anything` is not TRUE and not FALSE, it is NULL, and a WHERE clause treats NULL as "no".

```
  entry.timestamp = NULL
     NULL >= lo_p   ->  NULL   ->  row is not returned
  No error. No warning. The entry is simply not in the result set.
```

The entry is never read, so it is never grouped, forever.

**The codebase knows about this trap.**
There is a helper built specifically for it.

`app/services/mnp_log_ingestion/pipeline/time_bounds.py:72-80`

```python
72      def covers(self, column, *, include_null: bool) -> ColumnElement[bool]:
79          inside = and_(true(), *self._range_predicates(column))
80          return or_(inside, column.is_(None)) if include_null else inside
```

`include_null=True` adds the `OR column IS NULL` branch, and it *is* used correctly in the neighbouring queries: `assignments.py:184`, `assignments.py:226-229`, and `derive_transactions.py:481`.
The live path's own entry read at line 877 is the one place that omits it.

There is even a sibling function that gets it right:

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:755`

```python
755          stmt = stmt.where(or_(LogEntry.timestamp >= floor, LogEntry.timestamp.is_(None)))
```

That is `_live_tail`, used only by `regroup_incremental`, the manual path nobody runs.
It is covered by a test, `tests/test_stage2_bounds_chunk24.py:240-250`, and there is no equivalent test for the `regroup_window` path.

And it gets worse, because of line 137 in step 1.
If *every* new entry in a file has a NULL timestamp then `lo` stays None, so line 188's `if inserted and lo is not None` is false, and **no ticket is written at all**.
Stage 2 is never even asked.

For the concrete question "when does an entry actually end up with no timestamp?", see [Appendix A](#appendix-a-where-null-timestamps-come-from).
There are exactly two code paths and six realistic triggers, all verified by running the parser.

### Leak 2: a transaction longer than 15 minutes gets cut in half

Step 4 assumed something: that no transaction lasts longer than the 900-second pad.
If that holds, then deleting everything that *started* in `[lo-900, hi]` and reading everything in `[lo-900, hi+900]` covers every affected transaction completely.

Does it hold?
Here is the only thing that closes a transaction on a timeout.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:320-328`

```python
320      gap = timedelta(seconds=settings.log_open_gap_seconds)
322      def evict_stale(now: datetime) -> None:
325          horizon = now - gap
326          for k in [k for k, bd in open_by_key.items()
327                    if (lt := last_ts(bd)) is not None and lt < horizon]:
328              builders.append(close(k))
```

Read line 327 carefully.
`last_ts(bd)` is the timestamp of the builder's **most recent** entry (`:317-318`).
So the rule is "close this transaction if nothing has been added for `log_open_gap_seconds`", which is 300 by default (`app/settings.py:112`).

That is a rule about the **gap between entries**, not about **total length**.
Every new line resets the clock.
A transaction that logs one line every 4 minutes stays open indefinitely.

So a transaction can span an hour, and then:

```
  pad = 900s

  delete transactions started in :  [lo-900 .............. hi]
  read entries in                :  [lo-900 ..................... hi+900]

  T, actual span 3600s:  |R======================================E|
                          ^
                          started_at falls inside the delete range,
                          so T IS DELETED and its assignments wiped
                                                  hi+900 ^
                          |<--- re-read, rebuilt --->|<-- NOT re-read -->|
                                                          orphans, and T
                                                          is rebuilt truncated
```

Two bad outcomes at once: the entries past `hi+900` become orphans, and the transaction that gets rebuilt is missing its tail.

The docstring at `derive_transactions.py:828` states the guarantee as fact:

```
828      Lossless because the system guarantees no transaction spans more than pad: ...
```

The code does not make that guarantee.
Nothing does, and no test asserts it.
The same claim is repeated at `app/services/workers/log_partition_worker.py:44-46` to justify the one-day retention lag, and at `derive_transactions.py:458-470` to claim the clash window is "exact, not an approximation".

### Leak 3: the skip that silently eats entries

Back to step 5, line 639.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:638-641`

```python
638      for b, tid in zip(builders, ids):
639          if tid in seen or tid in existing:
640              skipped += 1
641              continue
```

`existing` means "a transaction with this id is already in the database", from `_existing_transaction_ids` (`:485-498`).
The intent is a safety valve: do not insert a duplicate.

But look at what `continue` skips.
It skips line 645, `_write_transaction`, which is the *only* thing that writes to `log_entry_assignment` (line 613).

```
  builder with 12 entries -> tid = X -> X already exists -> continue
     |
     +-- no transaction row written        (fine, one already exists)
     +-- NO assignment row written         <-- the 12 entries are now orphans
     |
  next cycle: same 12 entries are still unassigned
              -> same anchor line -> same id X -> skipped again
              -> and again, forever
```

It is a stable, self-perpetuating loop.
The only trace is one log line.

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:651-653`

```python
651      if skipped:
652          logger.warning("Stage 2: skipped %d builder(s) with an already-sealed id (out-of-order/bulk "
653                         "ingest). Run a full regroup (POST /logs/regroup) to rebuild cleanly.", skipped)
```

Two problems with that message.
It says "already-sealed id", but the check at line 639 never looks at `sealed`, only at whether the id exists.
And it reports a count rather than *which* entries, so you cannot repair just those.

### The other eight

Same shape: an entry ends up with no assignment row and nothing notices.

| # | What happens | Where | Fix |
|---|---|---|---|
| 4 | `regroup_window`'s **delete** query also omits the IS NULL branch, so a transaction built entirely from timestamp-less entries can never be freed, rebuilt, or corrected by the live path | `derive_transactions.py:848-849` | add `covers(..., include_null=True)`, as `_existing_ids_stmt:481` already does |
| 5 | `continuity.assign` can hand the same id to two groups in one batch, and the loser then hits leak 3. `_award` (`continuity.py:129-135`) guarantees an *inherited* id goes to one group but never checks it against the other groups' *fallbacks* | `continuity.py:145-147` | compare awarded ids against every fallback and re-mint on collision |
| 6 | `regroup_incremental` deletes unsealed transactions with **no time bound** and commits, then rebuilds from a read that **does** have a floor, so anything older is deleted and never rebuilt | delete `:780`, commit `:787`, floor `:750` | bound the delete the same way, and do delete plus rebuild in one transaction |
| 7 | A `ValueError`, `TypeError` or `KeyError` marks a ticket abandoned on the **first** attempt, with no retries, because those are classified permanent | `derive_transactions.py:1064-1071`, `app/services/queueing/retry_policy.py:28-37` and `:65-69` | give them the normal `log_regroup_max_attempts` budget |
| 8 | Abandoned tickets stop protecting their day from retention, so around day 61 the raw entries are genuinely dropped by `DROP PARTITION` | `app/services/workers/log_partition_worker.py:107-108`, drop at `:138-148` via `:173` | let abandoned tickets keep blocking the drop, and alert instead of reclaiming |
| 9 | Re-ingesting the file to repair things does nothing, because `ON CONFLICT DO NOTHING ... RETURNING` returns zero new rows, so `inserted == 0` and no ticket is written | `parse_insert.py:70-73` plus `:188` | write a ticket from the file's parsed min/max even when `inserted == 0` |
| 10 | `regroup_all` deletes **every tenant's** transactions and commits, then rebuilds in a loop with no try/except and no LIMIT, so if tenant 3 of 20 fails, tenants 3 to 20 are left empty | delete `:676-681`, loop `:689-701` | per-tenant try/except, per-tenant delete inside the same transaction as its rebuild, bounded read |
| 11 | **Nothing anywhere reports orphaned entries.** The count is computed and thrown away | `:701`, `:804`, `:898-900` | ship an unassigned-entry count per tenant and day |

Leak 11 is why the other ten are hard to see:

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:898-900`

```python
898      _merge_stats(stats, {**result, "entries_scanned": len(rows),
899                           "orphan_entries": len(rows) - result["entries_assigned"]})
900      logger.info("Stage 2 regroup (window): %s", stats)
```

Line 899 knows exactly how many entries went missing.
Line 900 writes it to a log file.
No metric, no endpoint, no alert.
Grepping `app/api/` and `app/services/workers/` for "unassigned" returns nothing, and `assignments.is_unassigned()` has exactly three call sites (`derive_transactions.py:721`, `:753`, `:876`), all of them work-finding queries inside a regroup, none of them reporting.

### Where each leak sits on the path

```
  file
    |
    +--X  1   ts IS NULL -> skipped from lo/hi -> no ticket
    +--X  9   re-ingest -> 0 new rows -> no ticket
    v
  log_regroup_pending
    |
    +--X  7   permanent-type error -> abandoned on attempt 1
    v
  regroup_window
    |
    +--X  1   entry read has no include_null branch
    +--X  4   freed-select has no include_null branch
    +--X  2   entries beyond hi+pad never re-read
    v
  _group -> _persist
    |
    +--X  3   id already exists -> continue -> no assignment row
    +--X  5   continuity.assign emits the same id twice in one batch
    v
  log_entry_assignment
    |
    +--X  6   regroup_incremental: unbounded DELETE, floored re-read
    +--X 10   regroup_all: global DELETE committed before rebuild
    v
  retention
    |
    +--X  8   abandoned tickets do not hold retention -> DROP PARTITION
    v
   gone

  11 = no reconciliation anywhere, so every X above is silent
```

### What was checked and found sound

Worth recording so nobody re-audits it:

- `_group` discards no entry. Every leftover builder and pending request is emitted (`:425-429`).
- Assignment writes are an idempotent upsert on `(entry_id, entry_ts)` with NULLS NOT DISTINCT, so a rebuild replaces rather than duplicates (`assignments.py:73-79`, `app/persistence/models/log_entry_assignment.py:77-78`).
- `regroup_window`'s delete and re-read are in one transaction, so readers never see a torn state (`:863-884`).
- Ticket rows are marked `consumed_at` only after every sub-window of the run has committed (`:1017-1025`), which is the safe ordering: a crash re-does idempotent work rather than skipping it.
- The `except Exception` in `finalize_pending` (`:1026`) advances no cursor. Tickets stay open, or are abandoned loudly. The classic "swallow and advance" bug is not present; the loss in leak 8 comes from the dead-letter policy, not the handler.
- `_split_run` sub-windows overlap by at least `pad` at their seams and rebuild identically thanks to deterministic ids (`:938-951`).
- Over-length promoted strings are capped rather than raising, so one bad value no longer aborts a batch (`_cap_over_length`, `:592-604`). This was a real outage; see `docs/stage2-stitching-stall-postmortem-and-fix.md`.
- Retention drops transactions one day *before* entries and assignments, so the ordering cannot strand a transaction without its body (`log_partition_worker.py:61-76`).
- `is_transient` correctly defaults *unrecognised* errors to transient (`retry_policy.py:99`). Only the explicitly named permanent types short-circuit.

### One non-loss issue found on the way

`derive_transactions.py:254-256` claims "a response can never be stitched onto another user's request".
The fallback 145 lines later contradicts it:

`app/services/mnp_log_ingestion/pipeline/derive_transactions.py:400-401`

```python
400                  if not u_keys and not u_reqs:
401                      u_keys, u_reqs = keys, pending_reqs
```

When the per-user filter leaves no candidate, all candidates are restored, so a response can close another user's transaction.
That is cross-user contamination, not loss, but it is worth knowing when reading a suspicious transaction.

Also: `_coalesce` (`:904-920`) is dead code.
Grepping `app/`, `tests/` and `scripts/` finds only its definition; only `_coalesce_pending` is called.

---

## 8. Measured evidence from the local dev database

Read-only queries against `ragfastapi-postgres-1` on 2026-08-20.

```
log_entries total ................ 12,596   (mnp: 8,595   VERIFY_REAL: 4,001)
entries with NO assignment ....... 8,595    <-- every single mnp entry
log_entry_assignment rows ........ 4,001    <-- all VERIFY_REAL, zero for mnp
log_transactions ................. 397      (mnp: 270, of which 228 sealed)
SUM(mnp.entry_count) ............. 6,509    <-- the column claims 6,509 entries
open log_regroup_pending ......... 5        (none of them for mnp)
abandoned log_regroup_pending .... 0
mnp transactions created ......... 2026-06-22
VERIFY_REAL transactions created . 2026-08-08
alembic head ..................... e4b28f5c9107
max transaction span ............. 172.5 seconds
transactions spanning > 900s ..... 0
entries with NULL timestamp ...... 0
```

Read that carefully.
Tenant `mnp` has 270 transactions that each claim to contain entries.
Zero of those entries are actually linked.
**All 270 render empty, and all 8,595 entries are orphans.**

The cause is a missing backfill.
`log_entry_assignment` was introduced by this migration, and all it does is create the table and two indexes:

`alembic/versions/c8f21a06d349_add_log_entry_assignment.py`

```
37   op.create_table(...)
58   op.create_index("ix_log_entry_assignment_txn", ...)
61   op.create_index("ix_log_entry_assignment_customer", ...)
```

The old scheme stored the link in a `log_entries.transaction_id` column.
The migration created the new table but never copied the old links across.
`mnp`'s transactions were built 2026-06-22, before that cut-over; `VERIFY_REAL`'s were built 2026-08-08, after it.

And because `mnp` has no open ticket, the live path has no reason to look at it.
Nothing reports it.
That is leak 11 in practice.

### What did not reproduce

Stated plainly, because an audit that only lists hits is not trustworthy:

- **Leak 1 has no live instance.** There are zero NULL-timestamp rows in this database. The code path is wrong, but nothing is currently falling through it.
- **Leak 2 is not currently breached.** The longest transaction is 172.5 seconds against a 900-second pad, and nothing exceeds the pad. It is an unenforced invariant, not an active fire.
- **Leak 8 has no live instance.** There are zero abandoned tickets.

---

## 9. Recommended fixes, in priority order

None of these has been done.

1. **Reconciliation first (leak 11).** Add an unassigned-entry count per tenant and day, as an endpoint plus a worker metric, reusing `assignments.is_unassigned()`. It is the smallest change on the list and it is the reason none of the rest is visible. The dev DB shows the failure mode is already live.
2. **Fix `regroup_window`'s two NULL holes (leaks 1 and 4).** Use the existing `time_bounds.UtcWindow.covers(..., include_null=True)` at `derive_transactions.py:877-878` and `:848-849`, and write a ticket at `parse_insert.py:188` even when `lo is None`. Two-line changes using a helper that already exists.
3. **Make the clash-skip loud and recoverable (leaks 3 and 5).** Record the skipped entry ids rather than only counting them, auto-enqueue a repair ticket for their range, and add the duplicate-fallback check in `continuity.assign`.
4. **Enforce the span invariant the design already assumes (leak 2).** Cap a builder's *total* span at `pad` in `evict_stale`, or derive `pad` from the largest span actually observed. Cheap to do now, while the observed max is 172.5s.
5. **Reclassify the dead letter (leaks 7 and 8).** Either give permanent types the normal attempt budget, or keep the classification and stop letting abandoned tickets release their retention hold.
6. **Bound and isolate `regroup_all` (leak 10).** Per-tenant try/except and a bounded read instead of `.scalars().all()`, which also brings it in line with rule 3 of the project CLAUDE.md.
7. **Make re-ingest a valid repair (leak 9).** Write a ticket from the file's parsed min/max even when nothing new was inserted.
8. **Decide what to do about the missing backfill (section 8).** Either backfill `log_entry_assignment` from the pre-migration state if production looks like the dev DB, or accept that those transactions are permanently bodiless and record that decision.
9. **Fix the stale narrative** in section 6, especially the user-facing OpenAPI description at `app/api/v1/logs.py:350` and the two docstrings asserting a span guarantee the code does not make.

---

## 10. How to verify any of this

A local Postgres is available (`ragfastapi-postgres-1`, `postgresql+asyncpg://rag:rag@localhost:5432/rag`, `app/settings.py:36`), and `tests/conftest.py` drives the application's real engine.
So each finding is reproducible as a failing test before its fix, which is the right order.

| Finding | Test to write |
|---|---|
| 1 and 4 | Insert an entry with `timestamp=None`, enqueue a ticket, run `finalize_pending`, assert the entry ends up assigned. Mirror `tests/test_stage2_bounds_chunk24.py:240-250` but against `regroup_window`. |
| 2 | Build a transaction whose entries are 299 seconds apart across 2,000 seconds, run `regroup_window` over a narrow window, assert no entry is left unassigned and the transaction is not truncated. |
| 3 and 5 | Construct the split-with-minority-anchor case, assert `transactions_skipped == 0`. |
| 6 | Two-phase ingest, an old backfill then live data, call `regroup_incremental`, assert the old transactions survive or are rebuilt. |
| 7 and 8 | Make a window raise `KeyError`, assert `attempts` increments rather than `abandoned_at` being set on the first failure. |
| 9 | Ingest the same file twice, assert a ticket exists after the second ingest. |
| 11 | Assert the new reconciliation query reports the orphan count the dev DB already has. |

---

## Appendix A: where NULL timestamps come from

Leak 1 and leak 4 both turn on `log_entries.timestamp` being NULL.
This appendix answers the obvious follow-up: in what scenario does that actually happen?

Everything below was verified by running `M3DotNetLogParser().parse()` over crafted inputs, not by reading the regex and reasoning about it.

### There are exactly two code paths

**Path 1: the header shape does not match.**

`app/services/mnp_log_ingestion/parsers/m3_dotnet_parser.py:86-89`

```python
86        m = _HEADER.match(header_line)
87        if not m:
88            # Unparseable header — keep it as a raw info record so nothing is lost
89            return LogRecord(line_number=line_no, entry_type="info", message=header_line, raw_body=raw_body)
```

Line 89 builds a `LogRecord` with no `timestamp` argument, and the field defaults to None (`app/services/mnp_log_ingestion/parsers/data_class/log_record.py:18`).

The subtlety is that two *different* regexes are involved.

`app/services/mnp_log_ingestion/parsers/m3_dotnet_parser.py:25` and `:28-34`

```python
25  _TS_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ")

28  _HEADER = re.compile(
29      r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
30      r"\((?P<user>.*?)\) "
31      r"\[(?P<thread>[^\]]*)\] "
32      r"(?P<level>\w+)\s+"
33      r"(?P<rest>.*)$"
34  )
```

A line becomes a new entry if it passes the *loose* `_TS_START` (at `:67`), and then has its fields read by the *strict* `_HEADER`.
Anything that passes the first test and fails the second lands on line 89 with a NULL timestamp.

**Path 2: the header matches but the date is impossible.**

`app/services/mnp_log_ingestion/parsers/m3_dotnet_parser.py:172-177`

```python
172      @staticmethod
173      def _parse_ts(ts: str) -> datetime | None:
174          try:
175              return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f")
176          except ValueError:
177              return None
```

The regex already guarantees the right *number* of digits, so the only way to reach line 177 is an out-of-range value.

### Verified samples

A well-formed line for reference:

```
2026-05-19 13:42:33,362 (BECWHLO) [94] INFO  M3.Managers.ItemManager MoveNext - REQUEST: /api/Item?ItemNumber=ABC
|- timestamp --------| |- user -| |th| |lvl| |- logger ------------| |method|   |- message ---------------|
```

Path 1, shape mismatch. `raw_timestamp` comes out as None:

| Sample line | Why it fails |
|---|---|
| `2026-05-19 13:42:33,362 (BECWHLO` | truncated line, the header is cut off mid-field |
| `2026-05-19 13:42:33,362 is the ShipDate value inside the REQUEST BODY` | a body line that itself begins with a timestamp |
| `2026-05-19 13:42:33,362 [94] INFO  M3.X Y - hello` | no `(user)` field |
| `2026-05-19 13:42:33,362 (BECWHLO) INFO  M3.X Y - hello` | no `[thread]` field |
| `2026-05-19 13:42:33,362 (BECWHLO) [94] WARN-2  M3.X Y - hello` | level has a non-word character, and `(?P<level>\w+)` stops at the hyphen |
| `2026-05-19 13:42:33,362 (BEC?HLO? [94] INFO  M3.X Y - hello` | a U+FFFD replacement character broke the `(user)` parentheses |

Path 2, impossible date. `raw_timestamp` is preserved:

| Sample line | Why it fails |
|---|---|
| `2026-13-19 13:42:33,362 (BECWHLO) [94] INFO  M3.X Y - hello` | month 13 |
| `2026-05-32 13:42:33,362 ...` | day 32 |
| `2026-05-19 25:42:33,362 ...` | hour 25 |
| `2026-05-19 13:42:60,362 ...` | second 60 |
| `2026-02-30 13:42:33,362 ...` | 30 February |

### Which scenarios realistically occur

Three of the above are plausible in normal operation.
The rest are corruption.

**1. A body line that itself starts with a timestamp.**
This is the likeliest trigger and it needs no corruption at all.
The parser folds continuation lines into the current entry (`m3_dotnet_parser.py:73-76`), but a continuation line that *looks* like a header is torn off as its own entry instead.
A `REQUEST BODY:` JSON blob, an M3 `Record:` block, or a stored-procedure text containing a `YYYY-MM-DD HH:MM:SS,mmm ` string at the start of a line will do it.
Confirmed: the sample produced two records, the correct `request` plus one NULL-timestamp `info`.

**2. A file read while it is still being written.**
The last line is cut mid-header.
The SSH fetch pulls live files, so this is a real exposure.
`app/services/mnp_log_ingestion/pipeline/parse_insert.py:92` also does `data.decode("utf-8", errors="replace")`, which is exactly how the U+FFFD case arises on the degraded disk documented in `docs/disk-io-resilience.html`.

**3. A server on a different log4net layout.**
If any customer's `log4net.config` uses a pattern without `(user)` or `[thread]`, or emits a level such as `WARN-2`, then **every line from that server** becomes a NULL-timestamp `info` entry.
That is the scenario that would actually hurt: not a few stray rows, but a whole tenant silently never grouping.

### Two side findings from this check

**`raw_timestamp` is captured but never stored.**
The parser sets it at `m3_dotnet_parser.py:106` and the field exists at `log_record.py:19`, but `parse_insert.py:154-175` does not include it in the row dict and `log_entries` has no such column.
So from the database you cannot tell path 1 from path 2, and have to read `raw_body` instead.
Worth adding if this is ever investigated, because the two paths have completely different causes and different fixes.

**A near-miss that behaves differently and would not show up in a NULL-timestamp count.**
If the separator after the milliseconds is not a single space, a tab for instance, then `_TS_START` never matches, so the line is not torn off as its own entry at all.
Mid-file it is silently folded into the *previous* entry's body, so it is preserved but attributed to the wrong entry.
At the very top of a file it is dropped outright by `m3_dotnet_parser.py:74-76`.
Neither outcome produces a NULL timestamp, so neither would appear in any count built for leak 1.

### How to watch for it

The metric already exists and is the right one.

`app/api/v1/logs.py:471-473`

```python
471          # Growth here means the parser is silently failing to read timestamps on some log format.
472          "default_partition_rows": default_rows,
473          "healthy": _partitions_healthy(days_ahead=days_ahead, default_rows=default_rows),
```

`GET /logs/partitions` counts rows in the DEFAULT partition, which is exactly where NULL-timestamp entries land.
In the dev database that count was **0** on 2026-08-20, which is why leak 1 has no live instance there.

Note that this metric tells you NULL-timestamp entries *exist*.
It does not tell you whether they were ever grouped, which is the actual damage leak 1 causes, and which only the leak 11 reconciliation would surface.

### One thing that is sound

The dedup constraint at `app/persistence/models/log_entry.py:65-66` uses `postgresql_nulls_not_distinct=True`, so NULL timestamps compare as equal.
Re-ingesting a file will not insert duplicate copies of these entries.

## Related documents

- `docs/stage2-stitching-stall-postmortem-and-fix.md` - the over-length `ItemNumber` outage that motivated `_cap_over_length` and the dead-letter design.
- `docs/plan/2026-08-05_17-32_append-only-entries-assignment-split.md` - why the grouping moved from `log_entries.transaction_id` to `log_entry_assignment`.
- `docs/plan/2026-08-08_19-43_stable-transaction-identity-by-continuity.md` - the identity mechanism that replaced what `sealed` used to provide.
- `docs/plan/2026-08-05_20-32_daily-partitioning.md` - the UTC-day partitioning that makes the NULL-partition trap in leaks 1 and 4 possible.
- `docs/deletion-and-cleanup-semantics.md` - the delete paths referenced in leaks 6 and 10.
- `docs/database-er-diagram.md` - the schema these three tables live in.
