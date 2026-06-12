from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


# https://docs.pydantic.dev/latest/concepts/pydantic_settings/
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- App ---
    app_name: str = "RAG Backend"
    debug: bool = False

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

    # --- Log debugging agent (Phase 2) ---
    anthropic_api_key: str = ""
    log_agent_model: str = "claude-opus-4-8"
    log_agent_max_tokens: int = 8000
    log_agent_max_iterations: int = 12  # safety cap on the tool-use loop


settings = Settings()
