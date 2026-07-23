# Issue: `WORKER TIMEOUT` outage - site not responding at https://192.168.0.142

- **Status:** Change 1, Change 2, and Change 3 (Postgres write-smoothing + shared_buffers) applied. Optional `synchronous_commit=off` still pending a durability decision; faster disk is the remaining long-term item.
- **Severity:** High (full site unavailability, intermittent).
- **Component:** `fastapirag.service` (gunicorn + uvicorn web tier), Postgres, `fastapirag-worker.service` (ingest + stitch), host `192.168.0.142` (Ubuntu VM on VMware).
- **Reported:** 2026-07-22, "https://192.168.0.142 is not responding".
- **First fixes shipped:** 2026-07-22.
- **Load investigation (Change 3):** 2026-07-23.
- **Related docs:** [`debugging-worker-timeout-outage.md`](./debugging-worker-timeout-outage.md) (full step-by-step walkthrough), [`log-pipeline.html`](./log-pipeline.html) (visual reference for the ingest/stitch pipeline), [`stage2-stitching-stall-postmortem-and-fix.md`](./stage2-stitching-stall-postmortem-and-fix.md).

---

## 1. Summary

The web app became intermittently unreachable.
The root cause was gunicorn workers being killed by the `WORKER TIMEOUT` watchdog while serving the transactions feed.
The feed endpoint rendered an unbounded result set synchronously, which blocked the worker's async event loop long enough for gunicorn to consider the worker dead and kill it.
A missing composite index made the underlying query slow enough to make this easy to trigger.
Both problems were fixed (Change 1 and Change 2).

Underneath that, the box also has a disk that is saturated on writes by the log ingest and stitch cycle.
That saturation is the second availability trigger (it degrades even `/health` during catch-up bursts) and is the remaining open item (Change 3).

---

## 2. Impact and symptoms

- `https://192.168.0.142` intermittently returned 502 from nginx, or hung, then recovered.
- nginx access log showed `502` on `/` and `/api/v1/logs/regroup/status`, and `500` on `/api/v1/logs/transactions/view`.
- The Next.js frontend proxy logged `socket hang up` / `Failed to proxy ... /transactions/view`.
- The backend system log (`journalctl -u fastapirag`) showed repeated `[CRITICAL] WORKER TIMEOUT (pid:NNNN)` followed by the worker being reaped and respawned.
- Worst hits were on the largest logspace `tmp-live` and on large page requests (for example `limit=500`).

---

## 3. Root cause

### 3.1 Primary: worker event loop blocked by an unbounded synchronous render

The transactions feed endpoint (`app/api/v1/logs.py`, `view_transactions`) fetched all matching transactions and their entries and rendered them into text on the request path.
This is a large amount of pure-Python work with no `await` in the middle.
Under Python's GIL, that work blocks the single event-loop thread of the uvicorn worker.
gunicorn's arbiter pings each worker; when a worker does not respond within the timeout (default 30 s), the arbiter treats it as hung and kills it with `WORKER TIMEOUT`.
So a slow feed render did not just make one request slow, it took the whole worker (and every other request on it) down.

### 3.2 Contributing: missing composite index

The feed query is `WHERE customer_code = ? ORDER BY started_at DESC LIMIT n`.
Before the fix there was no composite index for that shape, so on `tmp-live` (about 250k transactions) it did a large row scan plus an in-memory sort.
That pushed render time into the seconds-to-tens-of-seconds range, which is what made the timeout easy to hit.

### 3.3 Underlying: disk write saturation from the ingest + stitch cycle

Independent of the feed, the host disk is saturated on writes during the worker's ingest and stitch cycle.
This is the second availability trigger and the subject of Change 3 (see findings below).

---

## 4. Findings (evidence)

### 4.1 Ruled out first

- Network / TLS were fine (ports open, TLS handshake completed).
- Not out of memory, not disk-full, not CPU-starved at rest (8 cores, load about 2 to 3, tens of GB RAM free, root disk well under capacity).
- `steal = 0` in `vmstat`, so this is not VMware host contention.

### 4.2 The worker-timeout evidence

- `journalctl -u fastapirag` showed `WORKER TIMEOUT` kills correlated exactly with heavy `/transactions/view` requests.
- Reproduced end to end: firing `/transactions/view?limit=500` on `tmp-live` reliably produced a multi-second-to-tens-of-seconds response and a worker kill.
- Proved it was Python, not the database: while the request hung, the longest active DB query in `pg_stat_activity` was short or idle, so the time was being spent in the Python render, not in Postgres.

### 4.3 Data shape (why `tmp-live` is the hot spot)

Pulled live on 2026-07-23:

| Table | Rows | Size |
|---|---|---|
| `log_entries` | 5,702,146 | 47 GB |
| `log_transactions` | 253,588 | 2.6 GB |
| `log_regroup_pending` | 15,929 | 6.3 MB |

Per-customer `log_entries`:

| Customer | Entries | Span |
|---|---|---|
| `tmp-live` | 4,303,585 (about 75%) | 2026-06-26 to now, roughly 150k entries/day |
| `a-safe` | 888,116 | 2026-07-03 to 2026-07-14 (stopped) |
| `mnp` | 296,426 | 2026-03-18 to 2026-06-30 (stopped) |
| `tmp-test` | 203,092 | 2026-05-18 to 2026-07-21 |
| `asafe` | 11,097 | 2026-06-16, one hour only |

Notes from the data:

- `log_entries` is 47 GB for 5.7M rows (about 8 KB/row), dominated by `raw_body` text and the `fields` JSONB. This large, growing table on a slow disk is the core of the write pressure.
- `tmp-live` alone is about 75% of all entries. Capacity planning should size around its rate, not the total.
- Stale open pending window: the oldest unconsumed `log_regroup_pending.range_start` is `2026-07-03 13:09:41`, which matches `a-safe`'s first entry to the millisecond. It is an orphaned dirty window from a customer that went quiet; harmless because the 30-min-gap clustering keeps it isolated, but it will stay open until a finalize is nudged for that customer.
- Data-hygiene smell: `a-safe` vs `asafe` look like the same tenant under two codes.

### 4.4 Disk saturation evidence (Change 3)

- `iostat`: `sda` around 95% utilization at only about 2 MB/s, with about 55 ms average I/O wait, so the device is latency-bound, not throughput-bound.
- `sar` / `vmstat`: iowait spikes to 45 to 60% every roughly 10 to 20 minutes, aligned with the worker's ingest (a few thousand entries) plus stitch (roughly 9k entries) roughly every 8 minutes.
- Amplified by an under-tuned Postgres: `shared_buffers` 128 MB, `max_wal_size` 1 GB, `wal_compression` off, `synchronous_commit` on. These force frequent, heavy checkpoint write storms on a disk that cannot absorb them.

---

## 5. Resolution

### Change 1 - Frontend resilience (matrix-log-explorer) - shipped 2026-07-22

- `useTransactions` retries once on a transient 5xx or network blip, does not retry 4xx / supersede / timeout.
- Friendly, non-raw error messages, plus a Retry button.
- AbortController + sequence guard so a stale/slow response can never overwrite a newer one.
- Apply Filters always refetches (even with identical criteria) and is disabled while a request is in flight.

### Change 2 - Backend guardrails - shipped 2026-07-22

- **Composite index** `ix_log_transactions_customer_started` on `log_transactions (customer_code, started_at DESC NULLS LAST)` (migration `b9d4f2a7c318`). This was the real latency fix. `GET /transactions/view?limit=100` went from seconds/at-risk to about 0.7 s; `limit=500` went from about 46 s (plus worker kill) to about 3.4 s.
- **Bounded, offloaded render** in `view_transactions`: date is required, results are paginated (offset + ascending `started_at ASC, id ASC`), a `MAX_RENDER_ENTRIES` cap protects against pathological inputs, and the render runs via `asyncio.to_thread(...)` so it never blocks the event loop.
- **Service config** (`/etc/systemd/system/fastapirag.service`): `--timeout 120 --graceful-timeout 30 --max-requests 1000 --max-requests-jitter 100`.
- **Statement timeout** in `.env`: `DB_STATEMENT_TIMEOUT_MS=30000`.
- **Web/worker split**: `RUN_BACKGROUND_WORKERS=false` on the web tier (drop-in), so the web workers never run ingest/stitch.
- Also fixed a 13-day idle-in-transaction leak in `app/worker.py` by committing after `pg_try_advisory_lock`.

### Change 3 - Tame the disk load spikes - APPLIED 2026-07-23 (Postgres 16.14, cluster 16/main)

The investigation is complete (Section 4.4). Applied via `ALTER SYSTEM` as the `postgres` superuser; reload-only settings took effect on `pg_reload_conf()`, `shared_buffers` on a Postgres restart. Verified live afterwards (all `pending_restart = f`, app `/health` 200, DB serving).

| Setting | Before | After | Takes effect |
|---|---|---|---|
| `max_wal_size` | 1 GB | 8 GB | reload |
| `min_wal_size` | 80 MB | 2 GB | reload |
| `wal_compression` | off | on (pglz) | reload |
| `effective_cache_size` | 4 GB | 32 GB | reload |
| `checkpoint_timeout` | 5 min | 15 min | reload |
| `checkpoint_completion_target` | 0.9 | 0.9 (already) | - |
| `shared_buffers` | 128 MB | 8 GB | **restart** |

Rationale: box has 47 GB RAM (44 GB in page cache), so caching and WAL headroom were badly under-provisioned. Larger `max_wal_size` + longer `checkpoint_timeout` + `completion_target=0.9` spread checkpoint writes into a gentle trickle instead of storms; `wal_compression` cuts the bytes fsync'd; the `shared_buffers` and `effective_cache_size` bumps cut read I/O that was competing with writes on the saturated disk.

NOT applied (deliberate):

| Lever | Effect | Decision |
|---|---|---|
| `synchronous_commit=off` | Removes the per-commit WAL fsync wait - the most direct fix for the fsync-bound latency | Pending. Trades durability (lose <1 s of commits on a crash); low-risk here because the ingest is idempotent and re-fetches from source logs, but left as an explicit opt-in. |
| Shrink the stitch window (lower `log_regroup_max_window_seconds`) | Lower peak per commit but raises TOTAL writes due to the ±15-min pad overlap tax | Avoid. Counterproductive on a write-bound disk. See `log-pipeline.html` section F. |
| Faster disk (SSD / NVMe) | The disk is the real bottleneck | The durable long-term fix, but it is hardware. |

**Reproducible source:** these settings live as a runnable, documented file at [`deploy/postgres-tuning.sql`](../deploy/postgres-tuning.sql) so a rebuilt host reproduces them (`sudo -u postgres psql -d rag -f deploy/postgres-tuning.sql`, then reload, then restart for `shared_buffers`). It is a one-time provisioning step, deliberately NOT wired into `deploy.sh`. Values are sized for the 47 GB VM; scale `shared_buffers` (~15-25% RAM) and `effective_cache_size` (~60-75% RAM) for a different box.

**Rollback:** `ALTER SYSTEM RESET <name>;` then `SELECT pg_reload_conf();` (and a restart to revert `shared_buffers`).

---

## 6. The one-line takeaway

A slow, unbounded, synchronous render on a hot endpoint blocked the worker's event loop long enough for gunicorn to kill the worker, which read as a site outage.
Bounding + offloading the render and adding the composite index fixed the visible outage; the disk that is write-saturated by the ingest/stitch cycle is the remaining structural risk.

---

## 7. Deferred / watch items

- The JSON list endpoint `GET /api/v1/logs/transactions` still computes `total = count(*)` (a full scan) on every call, so it stays about 3 to 5 s even with the index. The frontend does not use it, so it was left alone. Make `total` opt-in or use a `reltuples` estimate if a client starts depending on it.
- Background-worker catch-up after downtime floods Stage-2 stitching and can briefly starve the web tier. If it still bites after the Postgres tuning, throttle the worker's regroup cadence or `nice` / `ionice` the worker process.
- Clear the orphaned `a-safe` open pending window and reconcile the `a-safe` vs `asafe` customer codes.
- Comet browser cannot reach the LAN IP (`ERR_ADDRESS_UNREACHABLE`) because of its VPN/proxy routing; Chrome works. This is a client issue, not a server issue.

---

## 8. Related follow-up: worker recycling dropped in-flight requests under load (applied 2026-07-23)

During stress testing the **Remote Log Servers (Manage)** panel intermittently showed "Internal Server Error" and fell back to "no servers yet".
Verified cause: the `--max-requests 1000` guardrail (added in Change 2) recycles each gunicorn worker about every ~1000 cumulative requests.
On recycle the worker shuts down and drops its in-flight sockets - gunicorn logs `Error while closing socket [Errno 9] Bad file descriptor` - which the Next.js proxy sees as `ECONNRESET` / "socket hang up" and returns as a `500`.
Evidence: nginx logged `GET /api/v1/logs/ssh-sources` 500s at 17:50 and 19:02; the proxy logged simultaneous `ECONNRESET` on `ssh-sources`, `saved-views`, `customers`, and `transactions/view` (all in-flight on the recycling worker); gunicorn logged `Maximum request limit of 1060 exceeded. Terminating process.` with the replacement worker taking ~19 s to cold-start.

Fix applied (server systemd unit `fastapirag.service` ExecStart):
- `--max-requests 1000` → **`--max-requests 20000`**, `--max-requests-jitter 100` → **`--max-requests-jitter 2000`**.
- The anti-leak recycle stays but now fires ~20× less often, so the "dropped in-flight request" window becomes rare.
- Client-side complement (matrix-log-explorer repo): every API call now goes through a shared `resilientFetch` wrapper that retries once on a transient failure (5xx / dropped connection) for idempotent reads - see that repo's `docs/api-retry-logic.md` and `CLAUDE.md` → "API RETRY LOGIC". So even a rare recycle-hit self-heals invisibly.

Follow-up (not done): the ~19 s worker cold-start (heavy ML imports at process start) makes every recycle and deploy expensive; consider lazy-importing heavy modules so boot is a few seconds.
