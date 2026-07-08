# Stage 2 stitching stall - postmortem, investigation runbook, and fix

Date: 2026-07-08.
Environment: Ubuntu deployment (`fastapirag.service` on the VM whose Postgres is reachable at `192.168.0.142`, database `rag`).
Affected tenant: `tmp-live` (live BEC servers). `tmp-test` and `mnp` were unaffected by the stall.

This document is a reference for the next time transactions stop appearing while raw log entries keep arriving.
It records the symptom, the exact investigation steps (including how the server and database were inspected), the definitive root cause, the fix for each item, and how the fix was verified.

---

## 1. The issue we set out to solve

The automatic SSH poller is meant to pull log files from the remote Windows/WMS servers on an interval, ingest them as `log_entries` (Stage 1), then stitch those entries into `log_transactions` (Stage 2).

Reported symptom, in the user's words: the poller "fetches data once but does not poll again", and later, "I can see log entries in the table but not the transactions - not sure if stitching is happening."

So: raw entries were visibly growing, but transactions had stopped.

---

## 2. How the investigation was done

Everything below is read-only diagnosis. No production data was modified during investigation.

### 2.1 Confirming the code path

Explored the Stage 1 / Stage 2 / poller code to establish how work flows:

- Poller supervisor: `app/services/workers/ssh_log_fetcher.py` runs one asyncio task per customer (not per OS process).
- Fetch + finalize: `app/services/mnp_log_ingestion/remote/remote_fetcher.py` (`fetch_now` -> `_do_finalize` -> `finalize_pending`).
- Stage 2 stitching: `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` (`finalize_pending`, `regroup_window`, `regroup_all`, `regroup_incremental`, `_persist`).
- Parser: `app/services/mnp_log_ingestion/parsers/m3_dotnet_parser.py`.

Key early realisation: the background poller records each successful tick as `log_ssh_sources.last_ok_at`, and does NOT write `log_ssh_fetch_runs` rows.
`log_ssh_fetch_runs` only logs manual/on-demand fetches.
So a "fetch runs" view looks frozen even while background polling is perfectly healthy.
The poller's true heartbeat is `log_ssh_sources.last_ok_at`.

### 2.2 Inspecting the database (read-only, from a dev machine)

The project virtualenv has `asyncpg`, so ad-hoc read-only queries were run against the production database directly:

```python
# .venv/bin/python
import asyncio, asyncpg
async def main():
    c = await asyncpg.connect("postgresql://rag:rag@192.168.0.142:5432/rag")
    print(await c.fetchval("select now()"))
    # ... SELECT-only queries ...
    await c.close()
asyncio.run(main())
```

The queries that mattered:

- Poller health: `log_ssh_sources` (`enabled`, `last_ok_at`, `last_attempt_at`, `consecutive_failures`, `auto_disabled_at`).
  Result: `last_ok_at` updating every cycle, `fails=0`, not auto-disabled - the poller was healthy and connecting.
- Stage 1 freshness: `log_entries` count and `max(timestamp)` / `max(created_at)` per `customer_code`.
  Result: `tmp-live` entries arriving every minute - Stage 1 healthy.
- Stage 2 output: `log_transactions` count, `max(created_at)`, and `created_at` grouped by hour.
  Result: all existing `tmp-live` transactions were created in a single hour, then creation stopped.
- Backlog: `log_regroup_pending` open (unconsumed) rows, and `log_entries` with `transaction_id IS NULL`.
  Result: a large backlog of unassigned entries spanning weeks, with the pending queue growing every poll.
- Offending data: expanded the entry `fields` JSONB with `jsonb_each_text` to find any value whose length exceeds a promoted column width.

### 2.3 Logging into the Ubuntu server

The database evidence proved Stage 2 was failing, but not why.
The exact exception lives in the service log on the server, so we logged in.

The server is not reachable by raw port probe from the dev sandbox, but outbound TCP works (the asyncpg queries above succeeded), so SSH works too.
`sshpass` was not installed; `expect` was, so an expect script drove the login and ran a single read-only diagnostic bundle.
Credentials were provided by the user out-of-band for this session; do not commit them.

The expect script (saved to a scratchpad file, not the repo) did, in order:

1. `spawn ssh -o StrictHostKeyChecking=no amin@192.168.0.142`, sending the password at the prompt.
2. `sudo -v` to cache sudo, sending the password again.
3. one combined command:

```bash
echo '=====UNIT====='; systemctl cat fastapirag.service | grep -iE 'ExecStart|WorkingDirectory'
echo '=====PROCS====='; ps -ef | grep -i gunicorn | grep -v grep
echo '=====MEM====='; free -h; echo cpus=$(nproc)
echo '=====EXC====='; sudo journalctl -u fastapirag.service --since '2026-07-08 12:11' --until '2026-07-08 12:13' --no-pager \
  | grep -A 30 'poll loop error for tmp-live' | head -45
```

You can also read the server terminal from an adjacent tmux pane with `/peek <pane>` if you already have a session open there.

### 2.4 What the server told us

- `ExecStart=... gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker` - four worker processes.
- `ps` confirmed 4+ gunicorn workers, each logging its own `SSH poll loop started` lines.
- `free -h`: 47 GiB RAM, 21 GiB free, 8 CPUs - memory was never the constraint.
- The exception (the decisive evidence):

```
asyncpg.exceptions.StringDataRightTruncationError: value too long for type character varying(64)
  ... in finalize_pending -> by_window = [regroup_window(...) ...]
  ... in regroup_window -> _persist(_group(rows))
```

---

## 3. Findings

1. The poller is healthy and Stage 1 ingestion is healthy. Entries keep arriving.
2. Stage 2 stitching fails on every cycle. No transactions are created after the failure began.
3. The exception is a data-length violation, not memory and not the worker count: a promoted string value exceeds a `varchar(64)` column.
4. The offending column is `log_transactions.item_number`. A JSONB scan found `ItemNumber` values of 70 to 75 characters.
5. The parser is faithful, not buggy. The raw request URL itself contains a doubled value:
   `...&ItemNumber=BEC%7cV1%7c...%7c521BEC%7cV1%7c...%7c521&...`.
   `_flat_qs` (`parse_qs`) just percent-decodes `%7c` to `|` and stores the single value verbatim.
   The doubling originates in the WMS device's request URL.
6. Why one bad row froze everything:
   - `finalize_pending` ran all pending windows in one all-or-nothing transaction.
   - It always processes the oldest pending window first.
   - So it hit the poison window every cycle, the `INSERT` raised, the whole batch rolled back, nothing after it committed, and the poller retried forever.
7. The `-w 4` gunicorn config means four full copies of every background service run at once. This did not cause the stall (a single worker fails identically), but it multiplied the retry load and is a real, separate problem (duplicate pollers, duplicate finalizers, potential duplicate notifications).

Root cause, one line:
A composite/doubled `ItemNumber` (70 to 75 chars) from the WMS overflowed the `varchar(64)` `item_number` column; the unguarded `INSERT` raised `StringDataRightTruncationError`, which aborted the all-or-nothing, oldest-first Stage 2 finalize and permanently blocked all stitching for the tenant.

---

## 4. The fix, item by item

Items 1, 2, 3, and 5 are implemented in this change.
Item 4 (workers) is deliberately deferred - see section 6.

### Item 1 - Length-safe persistence (the root-cause fix)

`_persist` now caps every promoted string value to its column width before building the row, using a limit map derived once from the ORM mapping (`_txn_str_limits`).
So any over-length dimension value (of any column, not just `item_number`) is safely truncated and logged, and can never again raise `StringDataRightTruncationError`.
The full value is always preserved on the raw `log_entry`; only the queryable promoted column is capped.

File: `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` (`_txn_str_limits`, and the cap loop in `_persist`).

Because we also widen `item_number` (Item 3), a normal 70 to 75 char value is preserved in full; the cap only bites beyond 128.

### Item 2 - Failure isolation in `finalize_pending`

`finalize_pending` was rewritten so that:

- each coalesced run is processed on its own short-lived session and split into padded sub-windows of at most `log_regroup_max_window_seconds` (default 6 h);
- each sub-window is a `regroup_window` that commits on its own, so memory and transaction size stay bounded no matter how large the backlog is, and progress persists incrementally;
- a run that fails is caught, logged, and reported under `failures`; its pending rows stay open for a retry while every other run still completes and consumes its own pending;
- each sub-window still takes `pg_advisory_xact_lock(hashtext(customer_code))` inside its own transaction, so concurrent finalizes for the same customer serialise at window granularity (`regroup_window` is idempotent via deterministic ids, so interleaving is safe).

`remote_fetcher._do_finalize` was updated to surface a `failures` result as `agg["finalize_error"]`, preserving the existing "do not swallow finalize failures" guarantee (and its tests).

Files: `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` (`finalize_pending`, new `_coalesce_pending`, `_split_run`); `app/services/mnp_log_ingestion/remote/remote_fetcher.py` (`_do_finalize`).

### Item 3 - The over-length value itself

Investigation (section 3, points 4 and 5) proved the parser is correct and the long value is real source data, so we must not silently corrupt it.
`log_transactions.item_number` is widened from `varchar(64)` to `varchar(128)` so the real composite value is preserved.
The length guard from Item 1 remains the backstop for anything longer still.

Files: `app/persistence/models/log_transaction.py`; Alembic migration `alembic/versions/a2c7e9d13f5b_widen_log_transaction_item_number.py`.
The ER diagram (`docs/database-er-diagram.md`) types this column generically as `string`, so no diagram change is required.

### Item 5 - Recovery and safety net

- Safety net: an optional per-statement Postgres timeout, `settings.db_statement_timeout_ms` (env `DB_STATEMENT_TIMEOUT_MS`), applied in `app/config/database.py`. Default 0 (off, so no behaviour change); recommended 300000 (5 min) in production now that every statement is bounded by the sub-window cap. A genuine runaway is then killed and surfaced instead of spinning.
- Recovery: after deploying Items 1 to 3, the poller's own finalize drains the backlog automatically - the widened column plus the length guard let the previously-poison window commit, sub-windowing keeps each commit bounded, and the pending queue drains. No manual chunked-drain script is required; if you want to force it immediately, `POST /logs/regroup/finalize` for the tenant does the same work on demand.

---

## 5. How the fix was verified (local, no regression)

- Static: all touched modules parse; `_txn_str_limits()` returns the expected map with `item_number = 128` and correctly excludes `Enum`/`Text` columns; `_split_run` splits a 20 h span into four 6 h windows and leaves a 30 s span as one.
- Migration: applied to the local database (`item_number` -> 128), and the down/up round-trip verified (64 -> 128 -> 64 -> 128).
- New regression tests, `tests/test_stage2_item_number_length_chunk9.py`:
  1. a real 75-char `ItemNumber` now groups into a transaction with the value preserved and no exception;
  2. a value beyond 128 is capped to 128 rather than raising;
  3. `finalize_pending` isolates a failing run: the other run still completes and only its pending is consumed.
- Full suite: `PYTHONPATH=. .venv/bin/python -m pytest -q` -> 62 passed (59 pre-existing + 3 new). No regressions.

Run the suite with:

```bash
cd "<repo root>"
PYTHONPATH=. .venv/bin/python -m pytest -q
```

---

## 6. Deferred: run background services once (the `-w 4` problem)

Deferred by request, to be done as a separate, carefully explained change.

The problem: `gunicorn -w 4` starts four OS processes, each running the full `lifespan` in `app/main.py`, so there are four copies of the poller, finalizer, embedding worker, watcher, and notification worker.
Per-customer concurrency is meant to be asyncio tasks inside one process; it is not meant to be multiple worker processes.

Options to evaluate when we do this (details to be written up before implementing):

1. Single-instance election via an advisory lock at startup: only the worker that wins `pg_try_advisory_lock(<singleton key>)` starts the background tasks; the others serve HTTP only. Minimal, robust, fits the existing locking style.
2. Split into two systemd units: a web service (`gunicorn -w N`, background tasks disabled by an env flag) and a single-process worker service. The cleanest long-term architecture.
3. `gunicorn -w 1`: simplest, but loses HTTP concurrency.

Recommended direction: option 1 as the low-risk step, option 2 as the eventual architecture.

Important: deploying Items 1 to 3 and 5 does NOT change the worker count, so it is safe to ship the stall fix first and address the workers separately.

---

## 7. Quick runbook for "entries are growing but transactions are not"

1. Is the poller alive? Check `log_ssh_sources.last_ok_at` per enabled source (NOT `log_ssh_fetch_runs`, which is manual-only).
2. Is Stage 1 alive? Check `max(log_entries.created_at)` for the tenant.
3. Is Stage 2 stalled? Compare `max(log_transactions.created_at)` to now, and check `log_regroup_pending` open-row count (growing == not draining).
4. How big is the unstitched backlog? `count(*) from log_entries where transaction_id is null` for the tenant.
5. Get the exact error: `sudo journalctl -u fastapirag.service --since '<start>' --no-pager | grep -A 30 'poll loop error for <tenant>'`.
6. If it is a length violation, find the field: expand `fields` and `fields->'params'` with `jsonb_each_text` and look for values longer than the target column.
7. Remember: individual `regroup (window)` success lines can appear even while the batch fails - the failure is the `finalize_pending` line and the traceback, not the per-window info logs.
