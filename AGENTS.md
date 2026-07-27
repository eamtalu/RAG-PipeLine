# Project Memory

Instructions for Codex when working in this repository.

## Keep the ER diagram in sync with the schema

The file `docs/database-er-diagram.md` is a human-facing ER diagram of the entire database.
It is generated from the ORM models and must be treated as a maintained artifact, not a one-off.

**Whenever you add, remove, or modify anything about the database schema, you must update `docs/database-er-diagram.md` in the same change.**

The database schema is defined in these places:

- SQLAlchemy 2.0 ORM models under `app/persistence/models/` (registered in `app/persistence/models/__init__.py`).
- The base declarative class in `app/config/database.py`.
- The raw-SQL `embeddings` table (pgvector) created in `app/persistence/vectorstore/pgvector.py`.
- Alembic migrations under `alembic/versions/`.

A schema change that requires updating the diagram includes any of the following:

- Adding, renaming, or dropping a table or model.
- Adding, renaming, retyping, or dropping a column.
- Adding, changing, or removing a primary key, foreign key, unique constraint, or index that the diagram documents.
- Changing an `ON DELETE` behavior (CASCADE / SET NULL).
- Changing an enum's allowed values where the diagram lists them.
- Turning a soft reference into an enforced foreign key, or vice versa.
- Adding or removing a subsystem.

### How to update the diagram

Read `docs/database-er-diagram.md` first, then apply the change in the right place so the document stays internally consistent.

- Reflect the change in the relevant per-subsystem `erDiagram` block (columns, PK/FK markers, cardinality).
- Update the master overview diagram and the tenant-partitioning diagram if a table or an enforced foreign key was added or removed.
- Update the "Full relationship reference" tables at the bottom (enforced foreign keys and soft references).
- Preserve the document's core convention: solid lines (`--`) are database-enforced foreign keys, and dashed lines (`..`) are logical "soft" references such as the `customer_code` tenant key.
- If you add a new subsystem, add both a short prose description and its own `erDiagram` block, matching the existing structure.

### Verify after editing the diagram

- Confirm every `erDiagram` block still parses: braces balanced inside each entity block, and every relationship uses a valid crow's-foot cardinality token (for example `||--o{` for enforced, `||..o{` for soft).
- Cross-check the edited tables and relationships against the actual model files so nothing is invented or missed.
- The diagrams use Mermaid, which renders on GitHub and in VS Code Markdown preview; a PNG/SVG can be exported by pasting a block into the Mermaid live editor.

## Documentation conventions

- Do not use the em dash. Use a plain dash instead.
- In Markdown prose, put each full sentence on its own physical line while keeping normal Markdown structure.

## Data-intensive queries and async API design

Follow these when building or changing any endpoint that reads/aggregates a lot of rows or does heavy work.
They come from a real outage and its fix; the full postmortem is `docs/debugging-worker-timeout-outage.md`, read it if the "why" is unclear.
The stack is FastAPI on `gunicorn -w N` uvicorn workers, async SQLAlchemy 2.0 over asyncpg, Postgres.

### 1. Never block the event loop

Each gunicorn worker runs ONE event-loop thread that serves all its requests.
If a request handler does heavy SYNCHRONOUS work (a big loop, building a large string, materializing tens of thousands of ORM objects, CPU-bound parsing) with no `await`, it freezes that loop; the worker misses gunicorn's heartbeat and is killed after the timeout, and every in-flight request on it fails with `ECONNRESET`.
So: keep synchronous work in a handler small and bounded.
For unavoidable heavy CPU/formatting work, offload it: `result = await asyncio.to_thread(fn, ...)`.
Use `asyncio.to_thread`, NOT a bare `loop.run_in_executor`, because `to_thread` copies the current `contextvars.Context` into the worker thread; this repo carries the per-request display timezone in a ContextVar (`app/services/mnp_log_ingestion/timefmt.py`), which a bare executor would drop.
Reference implementation: `view_transactions` / `get_transaction_view` in `app/api/v1/logs.py`.

### 2. `await` is for I/O; it is not a substitute for offloading CPU

An `await`ed database or network call does NOT block the loop; the worker stays responsive while waiting, so a slow query alone will not trip the worker timeout.
The danger is synchronous CPU work between/after the awaits.
Do not assume "it's async so it's fine" - check whether the expensive part is the DB (safe to await) or Python (must be bounded and/or offloaded).

### 3. Always bound result sets - never fetch unbounded

Never ship `.scalars().all()` (or equivalent) on a set that can grow with data.
Every list/read endpoint takes a `limit` (with a sane default and a hard max, as in `list_transactions` / `list_entries` in `app/api/v1/logs.py`) and paginates.
When you must materialize rows to render or serialize, fetch `limit + 1` to detect overflow, and enforce a runaway guard (see `MAX_RENDER_ENTRIES` in `app/api/v1/logs.py`) so a pathological selection cannot balloon memory or CPU.
For very large reads prefer lighter loading (specific columns / Core rows / `.execution_options(yield_per=...)`) over full ORM objects.

### 4. Index for the exact access pattern, and prove it with EXPLAIN

A query like `WHERE customer_code = ? ORDER BY started_at DESC LIMIT n` needs a composite index that matches both the filter and the sort, e.g. `(customer_code, started_at DESC NULLS LAST)`.
Without it Postgres scans every matching row and sorts them just to return a page; on a 200k+ row table that is seconds per call (this was the real latency cause here, fixed by migration `b9d4f2a7c318`).
Before shipping a new hot query, run `EXPLAIN` on it against realistic data and confirm an index scan with no large `Sort`.
Composite indexes for this multi-tenant DB should lead with `customer_code`.
Add indexes via an Alembic migration using `CREATE INDEX CONCURRENTLY` inside `op.get_context().autocommit_block()` so the build does not lock the live table (see `alembic/versions/b9d4f2a7c318_*`).

### 5. Avoid `COUNT(*)` on hot paths

A filtered `COUNT(*)` scans all matching rows even when the page is small, so it is often the slowest part of a "list + total" endpoint.
Make totals opt-in (`?with_total=true`), or use an estimate, or drop them from paths that do not need them.

### 6. Keep sessions and transactions short-lived

Acquire, do the DB work, commit/close.
Never hold a transaction open across a long `await` (network, SSH, CPU) or for a process lifetime.
A long-open transaction pins the database-wide vacuum horizon (autovacuum cannot reclaim dead tuples, so tables bloat and slow down) and blocks `CREATE INDEX CONCURRENTLY` and other online DDL.
Session-scoped resources such as `pg_advisory_lock` survive a commit (they are tied to the connection, not the transaction), so commit even when you intend to hold a lock for the connection's life; see the fix in `app/worker.py`.

### 7. Long or unbounded-duration work belongs in the background, not the request

If an operation can take more than a couple of seconds (bulk fetch, full rebuild, embedding, multi-file ingest), do not make the client wait synchronously.
Return `202 Accepted` with a `run_id` and a poll URL, run the work as a tracked background task, and let the client poll for status.
Follow the existing pattern: `POST /logs/fetch-remote` and `POST /logs/regroup/finalize` in `app/api/v1/logs.py` / `app/api/v1/log_sources.py`.

### 8. Fail fast and degrade gracefully

Set a per-statement safety net (`db_statement_timeout_ms`, wired in `app/config/database.py`) so a runaway query aborts instead of hanging a worker.
This is a WEB-TIER guardrail — it applies to read / feed queries. The background log-ingestion path deliberately RELAXES it per-transaction (`SET LOCAL statement_timeout = 0` in `parse_insert.py`) because an index-heavy `log_entries` insert on the slow/failing production disk legitimately runs longer than the cap; a genuinely unreadable block still surfaces as an I/O error and is skipped (`is_disk_io_error` in `app/services/mnp_log_ingestion/io_errors.py`; see `docs/disk-io-resilience.html`).
Return a clear, bounded response when a request exceeds a guard rather than doing unbounded work.

### 9. Remember there are multiple worker processes

`gunicorn -w N` means N separate processes, each with its own memory.
An in-memory dict/cache/global lives in ONE worker and is invisible to the others, and it does not survive a restart.
Use Postgres (or another shared store) for any state that must be shared across requests or workers; use module globals only for per-process concerns (like the in-flight-task sets in `app/api/v1/logs.py`).

### 10. Measure before and after

Time the endpoint (`curl -w 'ttfb=%{time_starttransfer}s'`) and separate DB time from Python time.
If `/health` (no DB) is fast but the endpoint is slow, and the active DB query is short, the cost is in Python (bound/offload it); if the DB query itself is long, fix the query/index.

### Pre-ship checklist for a data-heavy endpoint

- Result set is bounded (`limit` + max + pagination), no unbounded `.all()`.
- The hot query has a matching composite index; `EXPLAIN` shows an index scan, no big sort.
- No synchronous heavy CPU/render on the event loop (offloaded via `asyncio.to_thread` if unavoidable).
- No `COUNT(*)` on the default path unless required.
- Transactions are short; nothing holds one open across awaits or for the process life.
- Anything that can exceed a few seconds is `202` + poll, not a blocking request.
- Measured ttfb on realistic data, and it meets the target under both quiet and catch-up load.
