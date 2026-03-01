# RAG Backend — Project Structure

## Overview

A 7-stage RAG (Retrieval-Augmented Generation) ingestion pipeline built with FastAPI, PostgreSQL, and pluggable vector storage.

---

## Directory Tree

```
RAG FAST API/
├── main.py                              # uvicorn entry point
├── requirements.txt                     # Python dependencies
├── docker-compose.yml                   # Postgres + pgvector
├── alembic.ini                          # Alembic config
├── .env.example                         # Environment variable template
├── .gitignore
│
├── app/
│   ├── __init__.py
│   ├── main.py                          # FastAPI app + lifespan (starts embedding worker)
│   ├── config.py                        # Pydantic Settings (.env driven)
│   │
│   ├── models/                          # SQLAlchemy ORM + Pydantic schemas
│   │   ├── __init__.py
│   │   ├── database.py                  # Async engine + session factory
│   │   ├── job.py                       # Job table (status tracking)
│   │   ├── chunk.py                     # Chunk table (text + metadata)
│   │   ├── embedding_queue.py           # Embedding queue table
│   │   └── document.py                  # ParsedDocument Pydantic model
│   │
│   ├── api/                             # HTTP layer
│   │   ├── __init__.py
│   │   ├── router.py                    # /api/v1 prefix router
│   │   └── upload.py                    # Upload endpoint + job status
│   │
│   ├── storage/                         # Object storage abstraction
│   │   ├── __init__.py
│   │   ├── base.py                      # Abstract ObjectStorage interface
│   │   └── local.py                     # Local filesystem (dev) — swap for S3 in prod
│   │
│   ├── pipeline/                        # Ingestion stages 2–5
│   │   ├── __init__.py
│   │   ├── detector.py                  # MIME detection + strategy routing
│   │   ├── normalizer.py                # Convenience wrapper: detect → parse → ParsedDocument
│   │   ├── chunker.py                   # Heading-aware text chunking
│   │   ├── orchestrator.py              # Runs stages 2→5, enqueues for embedding
│   │   └── parsers/                     # Format-specific parsers
│   │       ├── __init__.py
│   │       ├── base.py                  # Abstract BaseParser interface
│   │       ├── pdf.py                   # PDF parser (pdfplumber)
│   │       ├── docx.py                  # DOCX parser (python-docx)
│   │       ├── markdown.py              # Markdown parser (mistune)
│   │       └── html.py                  # HTML parser (BeautifulSoup4)
│   │
│   ├── workers/                         # Background workers
│   │   ├── __init__.py
│   │   └── embedding_worker.py          # Async batch embedding worker
│   │
│   └── vectorstore/                     # Pluggable vector DB layer
│       ├── __init__.py
│       ├── base.py                      # Abstract VectorStore interface
│       ├── factory.py                   # Backend selector from config
│       ├── pgvector.py                  # pgvector implementation (default)
│       ├── qdrant.py                    # Qdrant implementation
│       └── pinecone.py                  # Pinecone implementation
│
├── alembic/                             # Database migrations
│   ├── env.py                           # Async migration runner
│   ├── script.py.mako                   # Migration template
│   └── versions/                        # Auto-generated migration files
│
└── tests/
    └── __init__.py
```

---

## The 7-Stage Pipeline

| Stage | File(s) | Description |
|-------|---------|-------------|
| **1. Upload** | `app/api/upload.py` | Validates file, persists to object storage, creates a job record in PostgreSQL |
| **2. Type Detection** | `app/pipeline/detector.py` | Uses `python-magic` for true MIME sniffing (not extensions), routes to the correct parser via strategy pattern |
| **3. Parsing** | `app/pipeline/parsers/pdf.py`, `docx.py`, `markdown.py`, `html.py` | Format-specific extraction — pdfplumber (PDF), python-docx (DOCX), mistune (Markdown), BeautifulSoup4 (HTML) |
| **4. Normalization** | `app/pipeline/normalizer.py`, `app/models/document.py` | All parsers converge to a common `ParsedDocument` Pydantic model with unified fields |
| **5. Chunking** | `app/pipeline/chunker.py` | LangChain `RecursiveCharacterTextSplitter` + `tiktoken`, heading-aware with breadcrumb metadata |
| **6. Embedding Queue** | `app/workers/embedding_worker.py` | Async background worker polls `embedding_queue` table, generates embeddings in batches via OpenAI |
| **7. Dual Storage** | `app/vectorstore/pgvector.py`, `qdrant.py`, `pinecone.py` | PostgreSQL for metadata/jobs, pluggable vector DB for embeddings (pgvector, Qdrant, or Pinecone) |

---

## Data Flow

```
                 ┌──────────┐
  File Upload ──▶│ Stage 1  │──▶ Object Storage + Job Record
                 │ (upload) │
                 └────┬─────┘
                      │
                 ┌────▼─────┐
                 │ Stage 2  │──▶ MIME detection (python-magic)
                 │(detector)│──▶ Route to parser
                 └────┬─────┘
                      │
                 ┌────▼─────┐
                 │ Stage 3  │──▶ PDF | DOCX | Markdown | HTML
                 │(parsers) │──▶ Extract text + headings
                 └────┬─────┘
                      │
                 ┌────▼──────┐
                 │ Stage 4   │──▶ Uniform ParsedDocument
                 │(normalize)│
                 └────┬──────┘
                      │
                 ┌────▼─────┐
                 │ Stage 5  │──▶ Token-counted chunks
                 │(chunker) │──▶ Heading breadcrumbs
                 └────┬─────┘
                      │
              ────────▼──────────
              embedding_queue table       ◀── decoupling boundary
              ───────────────────
                      │
                 ┌────▼──────┐
                 │ Stage 6   │──▶ OpenAI embedding batches
                 │ (worker)  │
                 └────┬──────┘
                      │
                 ┌────▼──────┐
                 │ Stage 7   │──▶ pgvector / Qdrant / Pinecone
                 │(vectordb) │──▶ PostgreSQL (metadata)
                 └───────────┘
```

---

## Key Models

### `Job` (PostgreSQL)
Tracks the lifecycle of an uploaded document:
`pending → detecting → parsing → chunking → embedding → completed | failed`

### `Chunk` (PostgreSQL)
Stores each text chunk with token count, heading breadcrumb, and JSON metadata.

### `EmbeddingQueueItem` (PostgreSQL)
Decouples ingestion from embedding. Worker atomically claims batches with `SELECT ... FOR UPDATE SKIP LOCKED`.

### `ParsedDocument` (Pydantic)
Normalized in-memory representation every parser must produce:
- `raw_text` — full extracted plain text
- `headings` — heading tree for breadcrumb-aware chunking
- `title`, `page_count`, `metadata`

---

## Design Patterns

- **Strategy Pattern** — `detector.py` maps MIME types to parser classes. Adding a new format = one parser class + one registry entry.
- **Abstract Interfaces** — `ObjectStorage`, `BaseParser`, and `VectorStore` are all ABCs. Swap implementations without touching business logic.
- **Decoupled Embedding** — uploads return instantly. Chunks are queued in PostgreSQL and processed asynchronously by a background worker.
- **Factory Pattern** — `vectorstore/factory.py` selects the backend from a single `VECTOR_STORE_BACKEND` env var.

---

## Quick Start

```bash
# 1. Copy env and set your OpenAI key
cp .env.example .env

# 2. Start Postgres with pgvector
docker compose up -d

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run first migration
alembic revision --autogenerate -m "initial"
alembic upgrade head

# 5. Start the server (embedding worker starts automatically)
uvicorn main:app --reload
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/documents/upload` | Upload a document (multipart/form-data) |
| `GET`  | `/api/v1/documents/jobs/{job_id}` | Check job status |
| `GET`  | `/health` | Health check |

---

## Configuration

All settings are driven by environment variables (see `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://rag:rag@localhost:5432/rag` | PostgreSQL connection |
| `UPLOAD_DIR` | `./uploads` | Local file storage path |
| `OPENAI_API_KEY` | — | Required for embedding generation |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | OpenAI embedding model |
| `EMBEDDING_DIMENSIONS` | `1536` | Vector dimensions |
| `CHUNK_SIZE` | `512` | Max tokens per chunk |
| `CHUNK_OVERLAP` | `64` | Token overlap between chunks |
| `VECTOR_STORE_BACKEND` | `pgvector` | `pgvector`, `qdrant`, or `pinecone` |
| `WORKER_POLL_SECONDS` | `2.0` | Embedding worker poll interval |

---

## Dependencies

### Core

| Package | Purpose |
|---------|---------|
| `fastapi` | Web framework — async API endpoints, validation, OpenAPI docs |
| `uvicorn[standard]` | ASGI server that runs FastAPI (extras add lifespan, reload, websockets) |
| `python-multipart` | Parses `multipart/form-data` — required by FastAPI for `UploadFile` |
| `pydantic` | Data validation & serialization (FastAPI uses it for request/response models) |
| `pydantic-settings` | Loads `.env` files into a typed `Settings` class — all config in one place |

### Database

| Package | Purpose |
|---------|---------|
| `sqlalchemy[asyncio]` | ORM + async database toolkit — defines Job, Chunk, EmbeddingQueue tables |
| `asyncpg` | PostgreSQL async driver — SQLAlchemy uses this under the hood for `postgresql+asyncpg://` |
| `alembic` | Database migration tool — generates and runs schema changes (`CREATE TABLE`, `ALTER`, etc.) |

### Object Storage

| Package | Purpose |
|---------|---------|
| `aiofiles` | Async file I/O — non-blocking reads/writes for uploaded files |

### Stage 2 — Detection

| Package | Purpose |
|---------|---------|
| `python-magic` | Wraps `libmagic` — sniffs true MIME type from file bytes (not just file extension) |

### Stage 3 — Parsers

| Package | Purpose |
|---------|---------|
| `pdfplumber` | Extracts text + layout from PDFs, page by page |
| `python-docx` | Reads `.docx` files — extracts paragraphs, heading styles |
| `mistune` | Fast Markdown to HTML renderer — we parse headings from raw MD then strip HTML for plain text |
| `beautifulsoup4` | HTML parser — extracts text, headings, title from HTML documents |

### Stage 5 — Chunking

| Package | Purpose |
|---------|---------|
| `langchain-text-splitters` | `RecursiveCharacterTextSplitter` — splits text at natural boundaries (paragraphs, sentences, words) |
| `tiktoken` | OpenAI's tokenizer — counts tokens per chunk so they fit within embedding model limits |

### Stage 6 — Embeddings

| Package | Purpose |
|---------|---------|
| `openai` | Official OpenAI SDK — calls the embeddings API to turn text chunks into vectors |

### Stage 7 — Vector Stores

| Package | Purpose |
|---------|---------|
| `pgvector` | pgvector support for SQLAlchemy — stores/queries vectors directly in PostgreSQL |
| `qdrant-client` | *(optional)* Qdrant vector DB client — uncomment in `requirements.txt` if using Qdrant |
| `pinecone` | *(optional)* Pinecone managed vector DB client — uncomment in `requirements.txt` if using Pinecone |
