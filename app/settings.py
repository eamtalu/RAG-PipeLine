from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


# https://docs.pydantic.dev/latest/concepts/pydantic_settings/
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- App ---
    app_name: str = "RAG Backend"
    debug: bool = False

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
    log_grouping_worker_enabled: bool = False
    log_grouping_poll_seconds: float = 5.0
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

    # --- Remote SSH log source (pull-ingestion from the Windows Server) ---
    # Background poller: connects to each enabled per-customer LogSshSource over SFTP, pulls the new
    # tail of its remote log files, ingests, then finalizes. OFF by default — the on-demand
    # POST /logs/fetch-remote trigger works regardless of this flag.
    ssh_log_fetcher_enabled: bool = False
    ssh_log_fetcher_poll_seconds: float = 60.0
    ssh_connect_timeout_seconds: float = 20.0
    ssh_max_file_size: int = 200 * 1024 * 1024  # 200 MB — mirror the upload cap
    # Fernet key (urlsafe-base64, 32 bytes) used to encrypt inline private-key material / passphrase
    # at rest. Empty ⇒ inline secrets are refused and a private_key_path (file on the backend host)
    # must be used instead.
    ssh_secret_key: str = ""

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


settings = Settings()
