# Automatic Log Fetch (SSH/SFTP pull-ingestion)

How the backend automatically pulls log files from each customer's Windows Server over SSH and feeds them into the existing ingestion pipeline.

This is a load-into-context reference.
It reflects the code as of this writing; verify file paths and symbols still exist before relying on them.
For the full design rationale, edge cases, and the frontend API contract, see `docs/ssh-log-fetch-hardening-and-per-customer-poller.md`.

## What it is

Instead of a human uploading a log file, the backend reaches out over SSH/SFTP to each registered Windows Server, reads only the *new tail* of the matching log files, and feeds those bytes through the **same Stage 1 ingestion** that the upload / scan / watcher paths use, then runs Stage 2 `finalize_pending` once so the entries are stitched into transactions.

The poller supervisor is **ON by default** and idle until a source is enabled — auto-poll is controlled entirely from the frontend via each source's `enabled` flag. `settings.ssh_log_fetcher_enabled` (default true) is only a global kill-switch to stop all background polling.

Two entry points into one shared engine (`fetch_now` in `app/services/mnp_log_ingestion/remote/remote_fetcher.py`):

- **Background poller** - a supervisor running one independent loop per customer (`app/services/workers/ssh_log_fetcher.py`).
- **On-demand trigger** - `POST /logs/fetch-remote`, works regardless of the enable flag (`app/api/v1/log_sources.py`).

## Correctness model (two layers)

- **Layer 1 - byte checkpoint** (`log_ssh_file_checkpoints`): per `(source_id, remote_path)`, remembers `last_size / last_mtime / last_offset / head_fingerprint` so unchanged files are skipped and grown files transfer only their new tail. A bandwidth optimization.
- **Layer 2 - content dedup**: every entry is stored with `entry_hash = sha256(raw_body)` under a unique `(customer_code, entry_hash)` (`ON CONFLICT DO NOTHING`), so re-reading overlapping/whole files never duplicates rows. This is what makes re-reads, rotation re-reads, and timestamp over-selection safe.

## Key files

- `app/persistence/models/log_ssh_source.py` - `log_ssh_sources`; one row per Windows Server per customer (+ `consecutive_failures` / `auto_disabled_at` / `last_attempt_at` for the breaker + status).
- `app/persistence/models/log_ssh_file_checkpoint.py` - per source+file cursor (+ `head_fingerprint`).
- `app/persistence/models/log_ssh_fetch_run.py` - tracked-run row; `LogSshFetchMode`, `LogSshFetchPhase`, `LogSshFetchRunStatus` (`running`/`completed`/`failed`/`cancelled`).
- `app/services/workers/ssh_log_fetcher.py` - supervisor + per-customer poll loops + global semaphore.
- `app/services/mnp_log_ingestion/remote/remote_fetcher.py` - the engine (`fetch_now`, `_fetch_source`, `_pull_range`, `_host_lock`, `_save_ckpt`, `_record_success/_record_failure`, `sweep_stale_runs`, `run_ssh_fetch_tracked`).
- `app/services/mnp_log_ingestion/remote/ssh_client.py` - asyncssh connect/SFTP, host-key pinning, keepalive, `op()` per-operation timeout.
- `app/services/mnp_log_ingestion/remote/secrets.py` - Fernet encrypt/decrypt for inline key material (fail-closed).
- `app/api/v1/log_sources.py` - the frontend API (CRUD, test, fetch-remote, run history, cancel).
- `app/main.py` - lifespan: startup `sweep_stale_runs`, starts the poller when enabled, cancels workers + in-flight fetches on shutdown.
- `app/settings.py` - all `ssh_*` settings.

## The background poller (per-customer isolation)

`run_ssh_log_fetcher()` is a **supervisor** started in the app lifespan by default (unless the `ssh_log_fetcher_enabled` kill-switch is set false).
Every `ssh_poll_reconcile_seconds` it reconciles the set of customers with >= 1 enabled source against a dict of running tasks - spawning a loop per new customer, cancelling loops for departed ones, restarting any that finished - and cancels all children on shutdown.

Each per-customer loop, under a global `asyncio.Semaphore(ssh_poll_max_concurrent)`, runs `fetch_now(..., mode=incremental, enabled_only=True, skip_if_busy=True, drive_breaker=True)`, then sleeps its cadence (min non-null `poll_interval_seconds` across the customer's enabled sources, else `ssh_log_fetcher_poll_seconds`).
A slow or unreachable server for one tenant never blocks another; a crashing loop is caught and respawned.

## The engine: `fetch_now(...)`

Per customer it loads the target sources, releases the caller's DB connection (detach + rollback) before the network loop, then per source - under a **per-host advisory lock** (`_host_lock`, keyed on `host:port`, guaranteeing at most one SSH connection per server) - calls `_fetch_source`, isolating failures. It records success/failure via `_record_success` / `_record_failure` (which also drive the circuit breaker), and runs `finalize_pending(...)` once at the end.

`_fetch_source` holds a single SFTP connection for the whole file loop (no DB session pinned across reads); every SFTP op is bounded by `ssh_client.op`. Per file it fingerprints the head, then decides skip / tail-read / re-read-whole (see the design doc §5.1). Checkpoints are upserted; stale checkpoints for vanished paths are pruned.

## Fetch modes (`LogSshFetchMode`)

- **incremental** - per-file byte tail via checkpoints (the poller's mode).
- **timestamp** - "ensure coverage from time T"; pulls only files whose mtime could contain T, and (on a manual request) seeds the other present files' checkpoints to EOF for a forward-only resume. A manual request sets `force_remote` so it always hits the server.
- **full** - re-pull every matching file whole (first sync / repair); dedup drops the overlap.

## Manual vs automatic (ownership) + outage handling

- `enabled=True` -> the poller owns the source; a manual fetch of it is 409'd. `enabled=False` -> manual-only (poller ignores it); a "fetch all" pulls only disabled sources.
- **Circuit breaker**: `ssh_auto_disable_after_failures` consecutive poller failures flips a source to `enabled=False` + `auto_disabled_at`, moving it to manual-only (no retry storm / no surprise backfill). Re-enabling (PATCH) re-arms the breaker.
- **Windowed resume after an outage**: with the source disabled, `POST /logs/fetch-remote {mode: timestamp, from_timestamp}` pulls a bounded recent window and seeds the rest to EOF; then re-enable to resume forward-only.

## Frontend API (prefix `/api/v1/logs`)

- `GET/POST /ssh-sources`, `GET/PATCH/DELETE /ssh-sources/{id}` - CRUD. `POST /ssh-sources/{id}/test` - active connectivity probe (pins the fingerprint).
- `POST /fetch-remote` (202) - trigger a run; enforces the ownership 409s.
- `GET /fetch-remote/runs` - run history (tenant-scoped, `source_id`/`status` filters).
- `GET /fetch-remote/runs/{id}` - poll one run. `POST /fetch-remote/runs/{id}/cancel` - cancel an in-flight run.
- Each source carries a **server-computed `status`** (`live`/`stale`/`degraded`/`pending`/`auto_disabled`/`disabled`) plus `last_ok_at`, `last_attempt_at`, `last_error`, `consecutive_failures`, `auto_disabled_at` for the health view. Key material is never serialized out.

## Security model

- Auth by private key only; prefer `private_key_path` (file on the backend host). Inline material is Fernet-encrypted at rest with `settings.ssh_secret_key` (fail-closed).
- Host-key pinning (TOFU): first connect pins the fingerprint; later mismatches are rejected. Changing host/port/username clears the pin.
- Connection hygiene: one connection per source fetch (reused across files), one connection per `host:port` at a time (the lock), keepalive + per-op timeouts, deterministic teardown, bounded concurrency, no reconnect storms.

## Relevant settings (`app/settings.py`)

`ssh_log_fetcher_enabled`, `ssh_log_fetcher_poll_seconds`, `ssh_connect_timeout_seconds`, `ssh_max_file_size`, `ssh_secret_key`, `ssh_operation_timeout_seconds`, `ssh_keepalive_interval_seconds`, `ssh_keepalive_count_max`, `ssh_fingerprint_bytes`, `ssh_checkpoint_retention_days`, `ssh_fetch_lock_wait_seconds`, `ssh_poll_max_concurrent`, `ssh_poll_reconcile_seconds`, `ssh_auto_disable_after_failures`.

## End-to-end flow

```
[per-customer timer]  or  [POST /logs/fetch-remote]
        |
        v
   fetch_now(customer)   # release caller's DB conn; per source under a per-host lock
        |
        v
   _fetch_source  --SFTP (one connection)-->  Windows Server (OpenSSH)
        |  list glob, fingerprint head, compare vs LogSshFileCheckpoint
        |  pull only the new byte tail (or re-read whole on rotation/shrink), newline-aligned
        v
   LogIngestion.ingest  ->  Stage 1 parse + entry_hash dedup  ->  log_entries
        |
        v
   finalize_pending (once)  ->  Stage 2 grouping  ->  log_transactions
        |
        v
   entries now visible to reads / the debugging agent
```
