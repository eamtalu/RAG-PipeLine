from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


# https://docs.pydantic.dev/latest/concepts/pydantic_settings/
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- App ---
    app_name: str = "RAG Backend"
    debug: bool = False

    # --- Logging ---
    # The app logs to stdout/stderr by default. Set log_file to ALSO write to a rotating file (useful
    # when the process is started bare, e.g. `uvicorn main:app`, where stdout isn't captured durably).
    # Empty => no file handler. Rotation keeps log_file_max_bytes per file, log_file_backup_count files.
    log_level: str = "INFO"
    log_file: str = ""                       # e.g. "/var/log/rag-backend.log" or "~/rag-backend.log"
    log_file_max_bytes: int = 10 * 1024 * 1024   # 10 MB per file before rotating
    log_file_backup_count: int = 5               # keep this many rotated files

    # --- Display timezone ---
    # Log timestamps are stored as UTC instants (timestamptz). They are CONVERTED to this zone for
    # human display only (API isoformat output, the §6 text view, agent answers, notification bodies)
    # — storage/grouping/comparisons stay in UTC. Use an IANA name so DST is automatic (Europe/London
    # → BST in summer, GMT in winter), never a fixed offset. See timefmt.py.
    display_timezone: str = "Europe/London"

    # --- Multi-tenant (per-customer log segregation) ---
    # Tenant of pre-existing rows at migration time (logs ingested before customer_code existed), and
    # the fallback code for document-pipeline jobs (which have no tenant concept). Valid tenants are
    # now governed by the `customers` registry table, not a static list.
    default_customer_code: str = "legacy"

    # --- Postgres --- user/pass and database name all as "rag"
    database_url: str = "postgresql+asyncpg://rag:rag@localhost:5432/rag"

    # --- Object storage ---
    upload_dir: Path = Path("./uploads")

    # --- Embedding ---
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    embedding_batch_size: int = 32
    openai_api_key: str = ""

    # --- Chunking ---
    chunk_size: int = 512
    chunk_overlap: int = 64

    # --- Vector store: "pgvector" | "qdrant" | "pinecone" ---
    vector_store_backend: str = "pgvector"

    # --- Qdrant (optional) ---
    qdrant_url: str = "http://localhost:6333"
    qdrant_collection: str = "documents"

    # --- Pinecone (optional) ---
    pinecone_api_key: str = ""
    pinecone_index: str = "documents"

    # --- Worker ---
    worker_poll_seconds: float = 2.0

    # --- Transaction log ingestion ---
    log_format: str = "m3_dotnet"
    log_incoming_dir: Path = Path("./logs/incoming")
    log_processed_dir: Path = Path("./logs/processed")
    log_failed_dir: Path = Path("./logs/failed")
    # Read-only source dir for /logs/scan (e.g. live rotating logs). Files here are NEVER moved.
    log_source_dir: Path = Path("./logs/source")
    log_watcher_poll_seconds: float = 5.0
    # Background incremental-regroup worker: polls log_entries and regroups the live tail after each
    # new file. Set False to stop AUTOMATIC regrouping only — manual POST /logs/regroup (full or
    # incremental) and the range-reingest endpoint still work.
    # Stage 2 incremental grouping: a TERMINAL transaction (has a RESPONSE / hard error) whose end is
    # older than this (relative to the newest log timestamp) is SEALED — never recomputed, so its id
    # is permanent and each cycle only reprocesses the recent "live tail". Must be >> the longest real
    # transaction (measured ≤2 min), to be safe against a late-arriving RESPONSE. 15 min default.
    log_seal_window_seconds: int = 900
    # An INCOMPLETE transaction (REQUEST seen, no RESPONSE yet) is kept unsealed far longer, so a
    # slow/late response can still join it; only after this long "abandon" window is it sealed as
    # permanently incomplete. Keeps the unsealed pool tiny without ever splitting a slow request.
    log_abandon_window_seconds: int = 3600
    # Stage 2 grouping staleness guard: an open transaction idle longer than this is ABANDONED
    # (flushed as incomplete) so a far-later RESPONSE — especially a user-less one matched by FIFO —
    # can't bind across a huge time gap and create a bloated multi-day transaction. Real
    # transactions are ≤2 min, so 5 min is safe.
    log_open_gap_seconds: int = 300
    # Scoped (windowed) regroup PAD: a regroup of time range [lo, hi] actually rebuilds
    # [lo - pad, hi + pad] so a transaction straddling the range boundary is never split. Must be
    # >= log_seal_window_seconds (the max a transaction can span); the code enforces that floor, so
    # this only ever widens the window. Same 15-min default as the seal window.
    log_regroup_pad_seconds: int = 900
    # finalize_pending caps how much wall-clock time a SINGLE regroup_window processes in one
    # transaction. A large backlog (e.g. a first backfill of weeks of continuously-logging data)
    # coalesces into one huge span; without a cap the whole span is deleted + rebuilt with every
    # entry loaded into ONE session, so the identity map grows unbounded. Splitting the span into
    # padded sub-windows (each its own committed transaction) keeps memory and transaction size
    # bounded and lets progress persist. Sub-windows overlap by the pad and rebuild with deterministic
    # ids, so the split stays lossless. 6 h default; steady-state poll windows (seconds of data) are
    # far below this and never split.
    log_regroup_max_window_seconds: int = 6 * 3600
    # Dead-letter cap: a stitch window (log_regroup_pending run) that FAILS this many finalize attempts
    # in a row is marked abandoned (abandoned_at set) and no longer retried, instead of being re-tried
    # forever. Prevents a poison window (e.g. one on a permanently-dead disk block) from burning the
    # statement_timeout every cycle. Its failure is recorded (attempts / last_error) and alerted loudly.
    log_regroup_max_attempts: int = 3
    # Backoff between Stage 2 retries, using the shared policy in app/services/queueing/retry_policy.
    # Previously a failing window was retried on the very next tick, so all three attempts were spent
    # within seconds — before a transient condition could clear, which made the retries useless.
    log_regroup_backoff_base_seconds: float = 30.0
    log_regroup_backoff_max_seconds: float = 1800.0

    # --- Stage 2 stitch worker: the consumer that owns draining log_regroup_pending ---
    # log_regroup_pending has always been a durable work queue (Stage 1 writes a ticket in the same
    # transaction as the entries), but it had no consumer — so every producer had to remember to call
    # finalize_pending itself: the SFTP transport, the directory watcher, the parse worker. This
    # worker owns it, and the producers now only write tickets.
    # ON by default: unlike the old count(*)-polling grouping worker this replaces, nothing else
    # drains the queue any more, so disabling it would leave data unstitched.
    log_stitch_worker_enabled: bool = True
    log_stitch_poll_seconds: float = 1.0
    # Cap on tenants stitched per drain, so one very busy tenant set cannot monopolise a tick.
    log_stitch_max_customers_per_tick: int = 25

    # --- Ingest queue: decoupling SSH fetching from Stage 1 parsing (log_source_objects) ---
    # Master switch, ON since 2026-08-05. The fetcher downloads bytes, saves them, and commits a
    # log_source_objects ticket together with the file checkpoint in ONE transaction; log_parse_worker
    # then drains that queue and runs Stage 1. The fetcher no longer holds the SSH connection and the
    # per-host advisory lock across the database work.
    #
    # Turning it OFF returns the fetcher to parsing inline, exactly as before. That is safe at any
    # time: the queue is simply drained to empty and stays empty (both workers also start when
    # unfinished rows exist, regardless of this flag, so a rollback cannot strand queued work).
    #
    # It must be read by BOTH processes - the poller lives in the worker, but the web tier serves
    # on-demand "fetch now" - so it belongs here rather than in a single systemd unit.
    #
    # See docs/plan/2026-08-02_15-47_log-source-objects-fetch-parse-decoupling.md.
    log_parse_worker_enabled: bool = True
    # Dead-letter cap for ONE downloaded byte-range. Deliberately the same number as
    # log_regroup_max_attempts so an operator has a single retry budget to reason about across both
    # stages. Only TRANSIENT failures (bad sector, statement timeout, dropped connection) consume it;
    # a permanent failure (corrupt bytes, missing storage key) abandons on the first attempt because
    # retrying cannot help.
    log_parse_max_attempts: int = 3
    # Backoff base. Attempt N waits base * 2^(N-1) plus up to 25% jitter, so 30s / 60s / 120s by
    # default. Stage 2 has no backoff at all and retries a failing window on every finalize tick,
    # which hammers a degraded disk; this avoids repeating that.
    log_parse_backoff_base_seconds: float = 30.0
    log_parse_backoff_max_seconds: float = 1800.0
    # How long a claimed row stays leased before another worker may reclaim it. Bounds how long a
    # crashed worker can strand its row.
    log_parse_lease_seconds: float = 120.0
    # Backpressure. Today the inline `await` makes it impossible for the fetcher to outrun the
    # database; once decoupled it can, filling ./uploads. A tenant with more than this many rows
    # awaiting parse is skipped for the tick.
    log_parse_queue_max_pending: int = 500
    # Parse-worker loop cadence and per-drain claim cap.
    log_parse_poll_seconds: float = 2.0
    log_parse_batch_size: int = 20
    # Delete the stored file once its row is `ingested` — the first time anything has been able to
    # clean ./uploads, because nothing previously tracked whether a file was successfully ingested.
    log_parse_delete_ingested_files: bool = True

    # Background loops (embedding, watcher, Stage 2 grouping, SSH poll supervisor, notifications,
    # log-space cleanup) must run in EXACTLY ONE process. Under gunicorn -w N the FastAPI lifespan runs
    # per worker, so set RUN_BACKGROUND_WORKERS=false on the web service and run the dedicated
    # `python -m app.worker` process (which always runs them, guarded by a singleton advisory lock).
    # Default true so single-process / dev / `uvicorn main:app` deployments are unchanged.
    # See app/background.py and docs/background-workers-web-worker-split.md.
    run_background_workers: bool = True

    # Per-statement Postgres timeout (ms) applied to every connection, 0 = disabled (default, so no
    # behavior change on existing deployments). A safety net: with the regroup window now bounded
    # (log_regroup_max_window_seconds) every statement is small, so a non-zero value here — e.g.
    # 300000 (5 min) in production — kills a genuine runaway and surfaces it instead of spinning
    # silently. Enable via DB_STATEMENT_TIMEOUT_MS in the environment.
    db_statement_timeout_ms: int = 0

    # Per-statement timeout (ms) the WORKER applies to its own heavy DB ops (log-entry inserts and
    # stitch windows) via `SET LOCAL statement_timeout` — overriding the web-tier db_statement_timeout_ms
    # for those transactions only. On a slow/degraded disk a legitimate insert or window-rebuild can run
    # well past the web guard (30s), so this must be generous; but it is FINITE (not 0) so a pathological
    # bad-sector stall is bounded and skipped rather than hanging the worker indefinitely. 120 s default.
    log_worker_statement_timeout_ms: int = 120000

    # Idempotency-Key store retention (hours). A key row can be replayed until it expires; after that
    # a retry with the same key is treated as a fresh request. 24 h is generous for user double-submit
    # / network-retry windows. The expired rows are swept opportunistically.
    idempotency_ttl_hours: int = 24

    # --- Remote SSH log source (pull-ingestion from the Windows Server) ---
    # Background poller: a supervisor that runs one loop per customer with >= 1 ENABLED source. It is
    # ON by default and idle when no source is enabled (a cheap "any enabled sources?" query per
    # reconcile tick) — so auto-poll is controlled entirely from the frontend via each source's
    # `enabled` flag, with no env to set. This flag is only a global kill-switch: set False to stop
    # ALL background polling regardless of source flags. The on-demand POST /logs/fetch-remote trigger
    # works regardless of this flag.
    ssh_log_fetcher_enabled: bool = True
    ssh_log_fetcher_poll_seconds: float = 60.0
    ssh_connect_timeout_seconds: float = 20.0
    ssh_max_file_size: int = 200 * 1024 * 1024  # 200 MB — mirror the upload cap
    # Fernet key (urlsafe-base64, 32 bytes) used to encrypt inline private-key material / passphrase
    # at rest. Empty ⇒ inline secrets are refused and a private_key_path (file on the backend host)
    # must be used instead.
    ssh_secret_key: str = ""

    # --- SSH hardening (see docs/ssh-log-fetch-hardening-and-per-customer-poller.md) ---
    ssh_operation_timeout_seconds: float = 60.0   # per-SFTP-op (glob/stat/open/read/close) hard ceiling
    ssh_keepalive_interval_seconds: float = 15.0  # asyncssh keepalive probe cadence
    ssh_keepalive_count_max: int = 3              # drop the connection after this many missed probes
    ssh_fingerprint_bytes: int = 4096             # head bytes hashed to detect log rotation (per file/poll)
    ssh_checkpoint_retention_days: int = 30       # prune checkpoints for vanished paths older than this
    ssh_fetch_lock_wait_seconds: float = 30.0     # on-demand: max wait to acquire the per-host fetch lock
    ssh_poll_max_concurrent: int = 8              # global cap on concurrent per-customer fetches (DB pool guard)
    ssh_poll_reconcile_seconds: float = 30.0      # how often the poller supervisor re-scans customers
    ssh_auto_disable_after_failures: int = 10     # consecutive failed poller fetches before auto-disable (0 = off)

    # --- Log debugging agent (Phase 2) ---
    anthropic_api_key: str = ""
    log_agent_model: str = "claude-opus-4-8"
    log_agent_max_tokens: int = 8000
    log_agent_max_iterations: int = 12  # safety cap on the tool-use loop

    # --- Notifications (rules → in-process bus → channels: Teams/Slack/WhatsApp) ---
    # Background worker: evaluates rules over recently-finalized transactions, publishes events to
    # the in-process bus, and the dispatcher fans them out to each customer's enabled channels.
    # Delivery is durable store-and-forward (Postgres outbox + per-channel delivery rows), so a
    # channel/internet outage never drops an alert — it is retried when connectivity returns.
    # OFF by default.
    notifications_enabled: bool = False
    notification_poll_seconds: float = 10.0
    # Streaming rules only consider transactions whose start is within this window — a flood guard so
    # enabling the worker on an existing DB doesn't replay the entire error history at once.
    notification_lookback_seconds: int = 3600
    # Exponential-ish backoff schedule (seconds) indexed by attempt count; the last value is reused
    # for every further attempt. Keeps retrying a failed delivery until it succeeds or hits the cap.
    notification_retry_backoff_seconds: list[int] = [30, 60, 300, 900, 3600]
    # After this many failed attempts a delivery is dead-lettered (status=dead) instead of retried
    # forever. Kept high so long outages (overnight, multi-hour) still recover on their own.
    notification_max_attempts: int = 50
    # Optional public base URL of the app/frontend; when set, alert cards include a deep link to the
    # transaction view. Empty ⇒ cards just show the transaction id/fields.
    app_public_base_url: str = ""

    # --- Log-space cleanup (disposable auto-expiry + presence sweep) ---
    # Background worker: hard-purges disposable log spaces whose expires_at is due (same purge as
    # DELETE /customers/{code}) and sweeps stale presence rows. OFF by default; the DELETE endpoint
    # works regardless of this flag.
    logspace_cleanup_worker_enabled: bool = False
    logspace_cleanup_poll_seconds: float = 3600.0  # hourly — expiry is a slow, day-scale process
    # Default TTL for a newly-created disposable: expires_at = created + this. 30 days. Used only at
    # create time to stamp expires_at; the worker then acts on the stored expires_at.
    logspace_disposable_ttl_seconds: int = 2592000  # 30 days
    # Presence rows not refreshed within this window are considered stale — filtered out on read and
    # bulk-swept by the worker. 12 h, so a closed/crashed client stops showing as "present".
    logspace_presence_ttl_seconds: int = 43200  # 12 hours


settings = Settings()
