# SSH/SFTP Automatic Log Fetch - Design, Hardening & Operations

This document is the master reference for the SSH/SFTP log-fetch subsystem: what it is, how automatic and manual fetching behave, how it stays correct and safe against a live production server, the frontend administration surface, and the concrete implementation plan to harden it.
It supersedes the older `docs/automatic-log-fetch.md` overview and captures every design decision agreed in planning.

> **Status:** design + hardening plan, not yet implemented. Section 10 lists the concrete changes and build order; no code has been changed yet.

---

## 1. Master overview

### 1.1 What it does

Instead of a human uploading a log file, the backend reaches out over SSH/SFTP to each customer's Windows Server (running OpenSSH), reads only the *new tail* of the matching log files, and feeds those bytes through the **same Stage 1 ingestion** the upload / scan / watcher paths already use.
Stage 2 finalize then stitches the new entries into transactions so they are visible to reads and the debugging agent.

There are two ways a fetch is triggered, both landing in one shared engine (`fetch_now`):

- **Automatic** - a background poller pulls on a timer, per customer.
- **Manual** - an operator (via the frontend) triggers an on-demand pull, optionally bounded by a start timestamp.

### 1.2 The core correctness model (two independent layers)

This is the single most important idea in the whole subsystem.
Correctness never depends on the network transfer being exactly-once.

- **Layer 1 - the byte checkpoint (a bandwidth optimization).**
  Per remote file we remember how far we have read, so unchanged files are skipped and grown files transfer only their new tail.
- **Layer 2 - content dedup (the correctness guarantee).**
  Every parsed entry is stored with `entry_hash = sha256(raw_body)` under a unique `(customer_code, entry_hash)` constraint via `INSERT ... ON CONFLICT DO NOTHING`.
  Re-reading overlapping or whole files therefore *never* creates duplicate rows.

Because Layer 2 makes re-reading always safe, the design can afford to err toward re-reading (shrink, rotation, timestamp over-selection, full re-sync) without any risk of duplication.

### 1.3 Architecture at a glance

```mermaid
flowchart TD
    subgraph Frontend
      UI[Admin UI: register / test / enable / fetch / monitor]
    end
    subgraph Backend
      API[/log_sources API/]
      SUP[Poller supervisor<br/>one loop per customer_code]
      ENG[fetch_now engine]
      SSH[ssh_client<br/>asyncssh SFTP]
      ING[Stage 1 ingest + entry_hash dedup]
      FIN[Stage 2 finalize_pending]
    end
    subgraph DB[(Postgres)]
      SRC[log_ssh_sources]
      CKPT[log_ssh_file_checkpoints]
      RUN[log_ssh_fetch_runs]
      ENT[log_entries / log_transactions]
    end
    WIN[(Windows Server<br/>OpenSSH, rolling logs)]

    UI --> API
    API -->|on-demand| ENG
    SUP -->|per customer, on timer| ENG
    ENG --> SSH --> WIN
    ENG --> ING --> ENT
    ENG --> FIN --> ENT
    API --- SRC
    ENG --- CKPT
    API --- RUN
    ENG --- RUN
```

### 1.4 Key files

- `app/persistence/models/log_ssh_source.py` - `log_ssh_sources` (one row per Windows Server per customer).
- `app/persistence/models/log_ssh_file_checkpoint.py` - `log_ssh_file_checkpoints` (per source+file byte cursor; gains `head_fingerprint`).
- `app/persistence/models/log_ssh_fetch_run.py` - `log_ssh_fetch_runs` (tracked-run row; `LogSshFetchMode`, `LogSshFetchPhase`, `LogSshFetchRunStatus`).
- `app/services/workers/ssh_log_fetcher.py` - the background poller (rewritten to supervisor + per-customer loops).
- `app/services/mnp_log_ingestion/remote/remote_fetcher.py` - the engine (`fetch_now`, `_fetch_source`, `_pull_range`, `_save_ckpt`, `run_ssh_fetch_tracked`).
- `app/services/mnp_log_ingestion/remote/ssh_client.py` - asyncssh connect / SFTP, host-key pinning, timeouts/keepalive.
- `app/services/mnp_log_ingestion/remote/secrets.py` - Fernet encrypt/decrypt for inline key material (fail-closed).
- `app/api/v1/log_sources.py` - the frontend-facing API (CRUD, test, fetch-remote, runs).
- `app/main.py` - lifespan startup/shutdown of the poller + stale-run sweep.
- `app/settings.py` - all `ssh_*` settings.

---

## 2. Data model

### 2.1 `log_ssh_sources` - what to fetch (one row per server)

- Identity: `id` (uuid PK); unique `(customer_code, name)` - the tenant-local label (e.g. `prod-wms-1`) addresses a source.
- Connection: `host`, `port` (default 22), `username`.
- Auth (key only): `private_key_path` (a file on the backend host, preferred - no secret in the DB) OR inline Fernet-encrypted `private_key_enc` / `key_passphrase_enc`.
- What to pull: `remote_log_dir` + `file_glob` (POSIX style even on Windows OpenSSH, e.g. `C:/logs/m3` + `*.log`).
- Poller control: `enabled` (drives the automatic poller AND the manual/auto ownership contract), `poll_interval_seconds` (now wired up - see §4.1).
- Security: `host_key_fingerprint` (pinned on first successful connect).
- Bookkeeping: `last_ok_at`, `last_error`, `created_at`, `updated_at`.

### 2.2 `log_ssh_file_checkpoints` - the incremental cursor (per source+file)

- `id` (uuid PK); `source_id` (FK -> `log_ssh_sources.id`, ON DELETE CASCADE); `customer_code`.
- `remote_path` (full remote path); unique `(source_id, remote_path)`.
- `last_size`, `last_mtime`, `last_offset` (newline-aligned bytes ingested), `last_fetched_at`.
- **NEW: `head_fingerprint`** - `sha256` of the first N bytes of the file, used to detect log rotation (a path reused by different content).
  Nullable, backfilled lazily.

The identity is the **full remote path**, never the basename - so `M3.log`, `M3.log.1`, `M3.log.2` are tracked independently, and the same basename on two servers never collides.

### 2.3 `log_ssh_fetch_runs` - a tracked manual run

- `id` (uuid PK); `customer_code`; `source_id` (nullable, no FK - null means "all sources").
- `mode` (`incremental` | `timestamp` | `full`); `requested_from`.
- `status` (`running` | `completed` | `failed`); `phase` (`listing` | `fetching` | `regrouping` | `done`); `progress` (JSONB).
- Aggregates: `files_considered`, `files_fetched`, `bytes_fetched`, `entries_ingested`.
- `error`, `result` (JSONB), `created_at`, `finished_at`.

The automatic poller does not create run rows; only the manual/tracked path does, so the frontend can poll progress.

---

## 3. Configuration (`app/settings.py`)

Existing:

```python
ssh_log_fetcher_enabled: bool = False          # master switch for the background poller
ssh_log_fetcher_poll_seconds: float = 60.0      # default per-customer cadence when no source interval is set
ssh_connect_timeout_seconds: float = 20.0        # bounds the initial TCP/SSH handshake only
ssh_max_file_size: int = 200 * 1024 * 1024       # per read-window cap (mirrors the upload cap)
ssh_secret_key: str = ""                          # Fernet key for inline private-key material
```

New (added by this plan):

```python
ssh_operation_timeout_seconds: float = 60.0     # per SFTP op (glob/stat/open/read/close) hard ceiling
ssh_keepalive_interval_seconds: float = 15.0     # asyncssh keepalive probe cadence
ssh_keepalive_count_max: int = 3                 # drop the connection after this many missed probes
ssh_fingerprint_bytes: int = 4096                # head bytes hashed to detect rotation (one small read/file/poll)
ssh_checkpoint_retention_days: int = 30          # prune checkpoints for vanished paths older than this
ssh_fetch_lock_wait_seconds: float = 30.0        # on-demand: max wait to acquire the per-host fetch lock
ssh_poll_max_concurrent: int = 8                 # global cap on concurrent per-customer fetches (protects the DB pool)
ssh_poll_reconcile_seconds: float = 30.0         # how often the supervisor re-scans the set of customers to poll
```

Tuning note: a full 200 MB read window over a slow link can exceed the 60 s op-timeout; on slow links raise `ssh_operation_timeout_seconds` or lower `ssh_max_file_size`.

---

## 4. Fetch scenarios

This section is the operational heart: what actually happens in each situation.

### 4.1 Automatic fetching (the poller)

**Ownership:** the poller only ever touches sources with `enabled = True`.
A per-`customer_code` model gives full tenant isolation - a slow or unreachable server for one tenant never blocks another.

```mermaid
flowchart TD
    SUP[Supervisor loop] -->|every ssh_poll_reconcile_seconds| SCAN[scan customers with >=1 enabled source]
    SCAN --> DIFF{diff vs running loops}
    DIFF -->|new| SPAWN[spawn per-customer loop]
    DIFF -->|removed| REAP[cancel loop]
    DIFF -->|died| RESTART[restart loop]
    SPAWN --> LOOP
    subgraph LOOP[per-customer loop]
      direction TB
      A[acquire global semaphore] --> B[fetch_now incremental, enabled_only, skip_if_busy]
      B --> C[release semaphore]
      C --> D[sleep customer interval]
      D --> A
    end
```

- **Supervisor** (`run_ssh_log_fetcher`, started in lifespan when `ssh_log_fetcher_enabled`): every `ssh_poll_reconcile_seconds` it reconciles the desired set of customers (those with >=1 enabled source) against a `dict[customer_code, asyncio.Task]`, spawning, reaping, and restarting loops. Wrapped so it never dies; cancels all children on shutdown.
- **Per-customer loop**: acquires the global `asyncio.Semaphore(ssh_poll_max_concurrent)` only around the fetch, runs `fetch_now(..., enabled_only=True, skip_if_busy=True)`, catches everything except `CancelledError`, then sleeps its interval.
- **Per-customer cadence** (wires up `poll_interval_seconds`): interval = the minimum non-null `poll_interval_seconds` across that customer's enabled sources, else `ssh_log_fetcher_poll_seconds`.
- **Isolation:** a crash is caught per tenant (and the supervisor respawns); a hung server is bounded by SFTP timeouts (§7) and occupies only one semaphore slot; within a tenant, sources are processed sequentially (a tenant's slow server only delays that tenant's other servers).
- **Mode:** always `incremental` (per-file byte tail via checkpoints - see §5).

### 4.2 Manual fetching (on-demand)

Triggered by the frontend via `POST /logs/fetch-remote`, which returns `202` + a `run_id` to poll.
Runs in a background task on its own session, records progress on the `log_ssh_fetch_runs` row, and never raises (failures land as `status=failed`).

Three modes:

- **incremental** - "pull whatever is new right now" (same per-file tail logic as the poller).
- **timestamp** - "ensure coverage from time T" (the windowed catch-up - see §4.4).
- **full** - re-pull every matching file whole (first sync / repair); dedup drops the overlap.

Mode defaults: `timestamp` when `from_timestamp` is provided, else `incremental`; an explicit `mode` in the body wins.

```mermaid
sequenceDiagram
    participant UI as Frontend
    participant API as /logs/fetch-remote
    participant ENG as fetch_now (bg task)
    participant WIN as Windows Server
    UI->>API: POST {source_id?, mode?, from_timestamp?}
    API->>API: reject if target source enabled=True (409)
    API->>API: reject if a run is already running (409)
    API-->>UI: 202 {run_id}
    API->>ENG: launch run_ssh_fetch_tracked
    ENG->>WIN: SFTP list + pull (per-host lock)
    ENG->>ENG: Stage 1 ingest + dedup, then finalize
    ENG->>API: update run row (progress, then completed/failed)
    loop poll
      UI->>API: GET /fetch-remote/runs/{run_id}
      API-->>UI: status/phase/progress/aggregates
    end
```

### 4.3 Manual vs automatic - the ownership contract

`enabled` cleanly separates who owns a source:

- `enabled = True` -> **the poller owns it**. A manual fetch of that specific source is rejected with `409` ("disable auto-polling before fetching manually").
- `enabled = False` -> **the operator owns it** (manual only). The poller ignores it; manual fetch works.
- A "fetch all" manual request (`source_id = null`) pulls **only the disabled sources**, silently skipping the enabled ones the poller owns.

This is a policy for clarity and predictable UX; concurrency is already made safe by the per-host lock (§6), so the contract is not a correctness requirement - it just prevents confusing "busy, try later" bounces and keeps a single owner per source.

### 4.4 Manual bounded resume after an outage (avoid backpressure)

When a source has been off (disabled, or its server unreachable) for a long time, you usually do not want to backfill 30 days of rotated files.
The `timestamp` mode gives an operator-controlled, volume-bounded catch-up.

Procedure:

1. Keep the source `enabled = False` while it is off (poller ignores it).
2. Trigger `POST /logs/fetch-remote { source_id, mode: "timestamp", from_timestamp: <e.g. now-24h> }`.
3. Only files whose `mtime >= from_timestamp` (plus the single newest older file, i.e. the active file) are pulled - so "last 24h" touches ~1-2 rotation files, not a month. The window is file-granular, so you get whole recent files (dedup drops overlap), never the old backlog.
4. **Forward-only seed:** the resume seeds checkpoints for *all* currently-present files up to their current end (size, mtime, offset=EOF, fingerprint) without ingesting the old ones. So when you later flip `enabled = True`, the incremental poller only appends new bytes - no backfill burst of the pre-window rotations.
5. Flip `enabled = True` to resume automatic polling.

Two supporting details:

- `force_remote`: an explicit manual timestamp request always hits the server, bypassing the "local Postgres already covers T" short-circuit (which would otherwise misfire on resume because old local data exists). The automatic path keeps the short-circuit.
- Cold-resume correctness: on the first pass, the active file and any reused rotation paths are detected as changed via the head fingerprint (§5.2) and re-read from 0; dedup drops overlap. You recover everything the server *still retains*; anything the server itself already rotated away during the outage is permanently gone (a remote-retention limit, mitigated server-side by a larger keep-count).

---

## 5. The engine internals

### 5.1 Incremental checkpoint decision (per file, keyed on full remote_path)

Each poll stats every currently-present file, reads its head bytes (§5.2), and decides:

| Current file vs checkpoint | Decision | Why |
|---|---|---|
| same `size` AND same `mtime` AND fingerprint matches | **skip** (no transfer) | genuinely unchanged |
| `size` grew, fingerprint matches | read `[last_offset, size)` | append-only growth - just the new tail |
| fingerprint differs | re-read from `0` | path reused by different content (rotation/replace) |
| `size` shrank (`< last_size`) | re-read from `0` | truncation/rotation |
| metadata changed but `offset >= size` | update checkpoint, no pull | nothing new to read |

For `timestamp`/`full` the file is read whole from 0 (dedup drops overlap); `timestamp` additionally narrows to selected files and seeds the rest (§4.4).

### 5.2 Head fingerprint - closing the rotation miss window

Identity is path-based, so without a content signal a path reused by different content (log rotation) could, in a rare lagging-poller + mtime-collision case, skip never-seen bytes.
Fix: store `head_fingerprint = sha256(first ssh_fingerprint_bytes)`, and fingerprint **every considered file each poll** (even unchanged size+mtime).
If the stored fingerprint differs from the current one, the file was rotated/replaced -> re-read from 0.
This makes cold resume and fast rotation lossless (dedup absorbs any overlap).
Cost: one small (default 4 KB) `open`+`read`+`close` per considered file per poll - negligible.

### 5.3 Newline alignment (`_pull_range`)

Reads `[start, size)` in windows of at most `ssh_max_file_size`.
A window that does not reach EOF is trimmed back to its last newline, so a partial trailing line is never ingested; the offset advances only to that boundary.
A full window with no newline (one absurdly long line) is ingested whole to guarantee forward progress.

### 5.4 Two-layer dedup (recap)

- Layer 1 (checkpoint) decides whether to transfer bytes at all.
- Layer 2 (`entry_hash`, `ON CONFLICT (customer_code, entry_hash) DO NOTHING`) guarantees no duplicate rows even when bytes are re-read.
  Dedup is per customer - two tenants emitting an identical line are two distinct rows; the same line for one tenant across two files (e.g. rotation overlap) is stored once.

---

## 6. Concurrency & locking model

- **Per-host fetch lock (one connection per server).** Each fetch takes a session-scoped `pg_try_advisory_lock` in the two-int keyspace, keyed on normalized `host:port`, on a dedicated `engine.connect()` connection held (idle) for the fetch. Closing that connection auto-releases the lock. This guarantees **at most one live SSH connection to any single server at any moment**, across the poller and manual fetches and even across tenants sharing a server. It also prevents the checkpoint race. Different hosts use distinct keys, so they fetch in parallel.
  - Poller: non-blocking try-lock; if busy, skip that source this tick (records `skipped`, not an error).
  - Manual: blocking with a bounded `ssh_fetch_lock_wait_seconds` wait; on timeout the run fails with "another fetch for this host is in progress".
- **Two-int keyspace** is disjoint from the single-arg keyspace used by `finalize_pending`'s `pg_advisory_xact_lock(hashtext(customer_code))`, so the fetch lock and the finalize lock provably cannot deadlock or collide.
- **Checkpoint upsert.** `_save_ckpt` becomes `pg_insert(...).on_conflict_do_update(index_elements=["source_id","remote_path"], ...)`, so any residual overlap can never raise `IntegrityError`.
- **Global fetch concurrency cap.** `ssh_poll_max_concurrent` bounds how many per-customer fetches run at once (protects the DB pool).

---

## 7. Connection hygiene & remote-server protection (hard requirement)

The remote servers carry sensitive traffic; the design must never multi-connect, leak, or hammer a server.

- **One connection per source fetch, reused across all its files.** `_fetch_source` holds a single SFTP session for the whole file loop; files are `open`/`read`/`close` on that one connection, never a connection per file. The session refactor (§8, gap 3) keeps this - only DB sessions become short-lived.
- **Deterministic teardown on every path.** `connect()` closes and `await conn.wait_closed()` in `finally`; `sftp()` `client.exit()`s in `finally`. Success, error, timeout, and cancellation all close the connection. No leak.
- **At most one connection per `host:port`** (the per-host lock, §6).
- **Dead / half-open detection** via asyncssh keepalive (~60 s); **hung operations abandoned** via per-op `wait_for`, then closed. A `wait_for`-cancelled channel is abandoned, not reused.
- **Bounded total concurrency** via the global semaphore; **no reconnect storms** (one attempt per fetch, full-interval backoff on failure).
- **Conservative connect options:** `known_hosts=None` (we pin ourselves), only the needed `client_keys`, no agent/x11/port forwarding; one connection = one SFTP channel.
- **Reading the active (currently-written) file is safe.** We only `read()` - never write/lock/truncate/rename - so the app's writes and its own network traffic are untouched; we add only brief disk/sshd load for the incremental tail. **Decision: read the active file directly** (best freshness). The one version-dependent nuance: on strict/old Win32-OpenSSH builds that open reads without `FILE_SHARE_DELETE`, our brief read handle could momentarily block the writer's *rotation* (not its writes). Mitigated by tail-then-close (tiny open window); a strict-share writer that denies our open surfaces as a per-source error we skip and retry. Verify per §11.

---

## 8. Security model

- **Auth by private key only.** Prefer `private_key_path` (file on the backend host) so no private material lands in the DB.
- **Inline key material** is Fernet-encrypted at rest with `settings.ssh_secret_key`. If that key is unset/invalid, encryption is refused (fail-closed, never plaintext).
- **Host-key pinning (TOFU).** asyncssh's own `known_hosts` is disabled; the first successful connect pins the server fingerprint, and every later connect must match or is rejected (`SshHostKeyMismatch`). Changing `host`/`port`/`username` clears the pin so it re-pins next connect.
- **The API never serializes key material back out** (see §9).

---

## 9. Frontend administration surface

All administration is driven by the frontend against these endpoints (router prefix `/logs`, under `/api/v1`).
Reads use the current-customer dependency; mutations use the active-customer dependency; every call is tenant-scoped.

### 9.1 Source CRUD

- `GET /logs/ssh-sources` -> `{ sources: [SourceOut, ...] }` - list the tenant's sources.
- `POST /logs/ssh-sources` (201) -> `SourceOut` - create. Body: `name`, `host`, `port` (default 22), `username`, `remote_log_dir`, `file_glob` (default `*.log`), `enabled` (default false), `poll_interval_seconds` (>=5, nullable), and auth: `private_key_path` OR `private_key` (PEM, stored encrypted) + optional `key_passphrase`. 409 if the name exists; 400 if no key is provided.
- `GET /logs/ssh-sources/{id}` -> `SourceOut`; 404 if not found.
- `PATCH /logs/ssh-sources/{id}` -> `SourceOut` - partial update. Changing `host`/`port`/`username` clears the pinned fingerprint (re-pins next connect). Setting a key path and inline PEM are mutually exclusive (one clears the other).
- `DELETE /logs/ssh-sources/{id}` (204) - cascades its checkpoints.

`SourceOut` (what the frontend renders) includes: `id`, `customer_code`, `name`, `host`, `port`, `username`, `remote_log_dir`, `file_glob`, `enabled`, `poll_interval_seconds`, `auth_method` (`path`|`inline`|`none`), `host_key_fingerprint`, `last_ok_at`, `last_error`, `created_at`, `updated_at`.
It **never** returns `private_key_path` contents, `private_key_enc`, or `key_passphrase_enc`.

### 9.2 Test connectivity

- `POST /logs/ssh-sources/{id}/test` -> `{ ok, fingerprint, matched_files, sample: [paths...] }`.
  Connects, lists the remote dir, and pins the fingerprint on first success.
  `409` on host-key mismatch; `502` on connection/config/secret errors (with a human-readable reason).
  The frontend should call this after create and surface the fingerprint + sample so the operator can confirm the target before enabling.

### 9.3 Trigger a fetch and monitor it

- `POST /logs/fetch-remote` (202) -> `{ run_id, status, mode, poll }`.
  Body: `source_id` (omit for "all"), `from_timestamp`, `mode`.
  Enforces the ownership contract (§4.3): 409 if the target source is `enabled=True`; 409 if a run for it is already `running`.
- `GET /logs/fetch-remote/runs/{run_id}` -> the run row: `status`, `phase`, `progress` (current source/file, files done/total, bytes/entries so far), aggregates, `error`, `result`, timestamps.
  The frontend polls this until `status` is `completed` or `failed`.

### 9.4 Suggested frontend workflows

- **Onboard a server:** create (disabled) -> test -> if OK, enable (or leave disabled for manual-only).
- **Manual catch-up after downtime:** ensure disabled -> fetch with `mode=timestamp`, `from_timestamp` = desired window -> monitor run -> enable to resume auto-polling.
- **Force a one-off pull on a manual source:** fetch with `mode=incremental` (or `full` to repair) -> monitor run.
- **Health view:** per source show `enabled`, `last_ok_at`, `last_error`, `host_key_fingerprint`; a red state when `last_error` is recent and `last_ok_at` is stale.
- **Enable/disable toggle** is the primary control: it decides poller ownership and gates manual fetch.

---

## 10. Implementation plan (changes, grouped by the gaps they close)

Ordering is bottom-up so each step lands on a stable base.

1. **Settings** - add the eight new `ssh_*` settings (§3).
2. **Schema (gap 5)** - add nullable `head_fingerprint VARCHAR(64)` to `log_ssh_file_checkpoints`:
   - Alembic migration, `down_revision = "e5a2c9f10b34"` (current head), no backfill.
   - Model column + docstring.
   - `docs/database-er-diagram.md`: add `string head_fingerprint "sha256 of file head; rotation guard"` to the `log_ssh_file_checkpoints` block (no FK/relationship change).
3. **SSH client (gap 1)** - add keepalive kwargs to `asyncssh.connect`; add a `with_timeout`/`_op` wrapper and route `glob`/`stat`/`open`/`read`/`close` through it.
4. **Engine session refactor (gap 3)** - `_fetch_source` drops the long-lived `db`: preload the source's checkpoints once, then use short `async_session()` blocks for the fingerprint pin, each `_save_ckpt`, the prune, and the `last_ok_at`/`last_error` stamp. Give `_ingest_chunk` its own session (it currently rides the outer `db` at `remote_fetcher.py:82`). The single SFTP connection still wraps the whole per-source loop (§7).
5. **Concurrency (gap 2)** - per-`host:port` advisory lock on a dedicated `engine.connect()` (try-lock for poller, bounded-blocking for manual); `_save_ckpt` -> upsert; fix the misleading docstring at `remote_fetcher.py:51`.
6. **Rotation (gap 5 logic)** - head-fingerprint read + decision change in `_fetch_source`; store the fingerprint on every `_save_ckpt`.
7. **Prune (gap 6)** - after a non-empty listing, best-effort delete this source's checkpoints for paths not present AND older than `ssh_checkpoint_retention_days`.
8. **Stuck runs (gap 4)** - `sweep_stale_runs()` (mark `running` -> `failed`) called at lifespan startup; cancel `_fetch_tasks` on shutdown; catch `CancelledError` in `run_ssh_fetch_tracked` to mark `failed`.
9. **Per-customer poller (gap 7)** - rewrite `ssh_log_fetcher.py` as supervisor + per-customer loops + global semaphore (§4.1); wire per-customer cadence.
10. **Windowed resume + ownership (gap 8 + contract)** - `force_remote` on `fetch_now`; timestamp-mode forward-only seed; request-time 409s and "fetch all -> disabled only" in `log_sources.fetch_remote`.
11. **Docs** - update `docs/automatic-log-fetch.md` to describe the supervisor/per-customer model, the ownership contract, and windowed resume.

### Critical files

- `app/settings.py`
- `alembic/versions/<new>_add_ssh_ckpt_head_fingerprint.py` (`down_revision="e5a2c9f10b34"`)
- `app/persistence/models/log_ssh_file_checkpoint.py`, `docs/database-er-diagram.md`
- `app/services/mnp_log_ingestion/remote/ssh_client.py`
- `app/services/mnp_log_ingestion/remote/remote_fetcher.py` (bulk)
- `app/services/workers/ssh_log_fetcher.py` (rewrite)
- `app/api/v1/log_sources.py`, `app/main.py`, `docs/automatic-log-fetch.md`

---

## 11. End-to-end verification

Primary harness: an in-process asyncssh SFTP server (deterministic, no external deps), plus a local OpenSSH `sshd` smoke test.

1. **Happy path / incremental:** write lines, fetch, append, fetch again -> only the new tail is read; DB count == source line count.
2. **Timeout (gap 1):** mock SFTP `read()` sleeps past `ssh_operation_timeout_seconds` -> `SshConnectionError`, poller moves on; assert keepalive kwargs are passed.
3. **Concurrency (gap 2):** two simultaneous fetches for the same host -> one runs, the other skips (poller) or waits/times-out (manual); no `IntegrityError`; a concurrent finalize for the same customer does not deadlock.
4. **Pool (gap 3):** `pool_size=1, max_overflow=0`; a multi-window fetch completes (proves no connection held across `read`).
5. **Stuck runs (gap 4):** insert a `running` row, restart -> flips to `failed`; cancel an in-flight task -> best-effort `failed`.
6. **Rotation (gap 5):** replace a file with same-size, same-mtime, different content -> fingerprint mismatch forces re-read; dedup means no duplicate rows but new content is ingested; legacy null-fingerprint rows backfill then protect.
7. **Prune (gap 6):** stale + vanished checkpoint deleted; present/recent survive; empty listing prunes nothing.
8. **Per-customer isolation (gap 7):** a hung tenant never blocks a healthy one; supervisor spawns/reaps within `ssh_poll_reconcile_seconds`; `ssh_poll_max_concurrent=1` serializes without deadlock; a source-level `poll_interval_seconds` is honored.
9. **Windowed resume + ownership (gap 8):** month of rotations + `mode=timestamp, from_timestamp=now-24h` pulls only ~24h, short-circuit does not suppress, all present files seeded to EOF; enabling then only appends; manual fetch of an `enabled=True` source returns 409; "fetch all" pulls only disabled sources.
10. **Connection hygiene:** server-side, at most one connection per `host:port` at any moment even with poller + "fetch all"; count returns to zero after each fetch; a forced timeout/cancel leaves no half-open socket; two sources on one host serialize; a refused/slow port -> one attempt then full-interval backoff.
11. **Active-file coexistence (Windows):** with a process appending to `app.log`, repeated tails do not interrupt writes or truncate the file, and a rotation succeeds while a read is in flight (target OpenSSH opens reads with delete-share); note and mitigate if a build blocks rotation.
12. **Suite + migration:** run the existing tests; `alembic upgrade head` / `downgrade` round-trips the new column.

---

## 12. Risks & assumptions

- **Single app instance assumed** for the startup stale-run sweep and the request-time 409; the advisory lock itself is cross-instance-safe. Revisit if scaled out.
- **Pool sizing under "fetch all":** one idle lock connection per concurrently-fetching host; keep `ssh_poll_max_concurrent` within `pool_size + max_overflow` (default 15).
- **Op timeout vs large windows:** tune `ssh_operation_timeout_seconds` / `ssh_max_file_size` together for slow links.
- **Remote retention:** files the server itself rotated away during an outage are unrecoverable; mitigate server-side with a larger keep-count.
- **Active-file rotation coexistence** is version-dependent on Win32-OpenSSH share modes (§7); mitigated by tail-then-close and verified in §11.
- **Timestamp window is file-granular** (whole recent files), so you may ingest slightly more than the exact window - never less, never the old backlog.
