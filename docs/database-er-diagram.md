# Database ER Diagram

A visual map of the database so you can understand the data model at a glance.
This document is generated from the SQLAlchemy models under `app/persistence/models/` and the raw-SQL vector table in `app/persistence/vectorstore/pgvector.py`.
It is documentation only, so nothing here changes the running system.

## How to read this (beginner primer)

If you are new to database diagrams, read this short section first.

- **Table (entity):** a spreadsheet-like collection of rows.
  Each box in a diagram is one table, and the lines inside the box are its columns.
- **Primary Key (PK):** the column that uniquely identifies each row in a table.
  Here almost every table uses a `uuid` column named `id` as its PK.
- **Foreign Key (FK):** a column that points at the PK of another table, creating a link between them.
  Example: `chunks.job_id` points at `jobs.id`, so every chunk "belongs to" one job.
- **Cardinality (the crow's foot):** the shape at the end of a line tells you "how many".
  `||` means exactly one, and `o{` (the crow's foot) means zero-or-many.
  So `jobs ||--o{ chunks` reads as "one job has zero or many chunks".

### Solid vs dashed lines

This is the most important convention in this document.

- **Solid line (`--`) = a real, database-enforced foreign key.**
  The database itself guarantees the link and will cascade or null it on delete.
- **Dashed line (`..`) = a logical "soft" reference with no database constraint.**
  The link exists in the application's logic only.
  The biggest example is `customer_code`, the multi-tenant partition key that appears on almost every table but is a real foreign key in only one place.

## Big picture

The schema is built around two "spines" that hold everything together.

1. **The ingestion spine: `jobs.id`.**
   A `job` is one uploaded/ingested file.
   Deleting a job cascades and deletes all of its derived rows (chunks, entities, embedding queue items, log entries, log transactions).
2. **The tenant spine: `customer_code`.**
   Nearly every table carries a `customer_code` string so data is partitioned per customer.
   It is enforced as a foreign key only from `customer_display_names` and `logspace_presence` into `customers`; everywhere else it is a soft convention.

The tables fall into seven subsystems: RAG documents/embeddings, WMS log ingestion, remote SSH log fetching, the customer registry, saved views, notifications, and idempotency keys.

### Master overview (enforced foreign keys only)

This diagram shows only the real, database-enforced foreign keys, so you can see the true structural skeleton without clutter.

```mermaid
erDiagram
    jobs ||--o{ chunks : "has"
    jobs ||--o{ chunks_entity : "has"
    jobs ||--o{ embedding_queue : "has"
    jobs ||--o{ log_entries : "has"
    jobs ||--o{ log_transactions : "has"

    chunks ||--o{ chunks : "parent of"
    chunks ||--o{ embedding_queue : "queued as"
    chunks_entity ||--o{ embedding_queue : "queued as"

    log_transactions ||--o{ log_entries : "groups"

    log_ssh_sources ||--o{ log_ssh_file_checkpoints : "tracks"

    customers ||--o{ customer_display_names : "aliases"
    customers ||--o{ logspace_presence : "presence"

    notification_events ||--o{ notification_deliveries : "fans out to"
    customer_notification_channels ||--o{ notification_deliveries : "sent via"
```

### Tenant partitioning (`customer_code`, soft links)

`customer_code` is the multi-tenant key.
It is a real foreign key only from `customer_display_names` and `logspace_presence` to `customers` (solid lines below).
On every other table it is a soft convention with no constraint (dashed lines below).

```mermaid
erDiagram
    customers ||--o{ customer_display_names : "FK (enforced)"
    customers ||--o{ logspace_presence : "FK (enforced)"

    customers ||..o{ jobs : "tenant key (soft)"
    customers ||..o{ log_entries : "tenant key (soft)"
    customers ||..o{ log_transactions : "tenant key (soft)"
    customers ||..o{ log_regroup_pending : "tenant key (soft)"
    customers ||..o{ log_regroup_runs : "tenant key (soft)"
    customers ||..o{ log_ssh_sources : "tenant key (soft)"
    customers ||..o{ log_ssh_file_checkpoints : "tenant key (soft)"
    customers ||..o{ log_ssh_fetch_runs : "tenant key (soft)"
    customers ||..o{ log_source_objects : "tenant key (soft)"
    customers ||..o{ saved_views : "tenant key (soft)"
    customers ||..o{ customer_notification_channels : "tenant key (soft)"
    customers ||..o{ notification_rules : "tenant key (soft)"
    customers ||..o{ notification_events : "tenant key (soft)"
```

## Subsystem 1: RAG documents and embeddings

This is the core retrieval-augmented-generation pipeline.
A file becomes a `job`, the job is split into `chunks` (and richer `chunks_entity` rows), and each piece is queued in `embedding_queue` to be turned into a vector.
The vectors themselves live in a separate raw-SQL `embeddings` table (pgvector), keyed by an application-level string id rather than a foreign key.

```mermaid
erDiagram
    jobs {
        uuid id PK
        string customer_code "soft tenant key"
        string filename
        string mime_type
        string storage_key
        string document_type
        enum status "pending..completed/failed"
        int chunk_count
        text error
        datetime created_at
        datetime updated_at
    }

    chunks {
        uuid id PK
        uuid job_id FK "-> jobs.id (CASCADE)"
        uuid parent_id FK "-> chunks.id (self, CASCADE)"
        string chunk_type
        int index
        text text
        int token_count
        jsonb metadata
        string heading_breadcrumb
        datetime created_at
    }

    chunks_entity {
        uuid id PK
        uuid job_id FK "-> jobs.id (CASCADE)"
        string source_file
        int chunk_index
        text text
        text context_header
        text full_text
        array context_path
        int context_depth
        array page_numbers
        string chunk_type
        string profile
        int token_estimate
        datetime created_at
    }

    embedding_queue {
        uuid id PK
        uuid chunk_id FK "-> chunks.id (CASCADE, nullable)"
        uuid chunk_entity_id FK "-> chunks_entity.id (CASCADE, nullable)"
        uuid job_id FK "-> jobs.id (CASCADE)"
        enum status "pending/processing/done/failed"
        datetime created_at
    }

    embeddings {
        text id PK "app-level string key, no FK"
        vector embedding "pgvector, HNSW index"
        text text
        jsonb metadata
    }

    jobs ||--o{ chunks : "split into"
    jobs ||--o{ chunks_entity : "split into"
    jobs ||--o{ embedding_queue : "produces"
    chunks ||--o{ chunks : "parent of (hierarchical)"
    chunks ||--o{ embedding_queue : "queued as"
    chunks_entity ||--o{ embedding_queue : "queued as"
    chunks ||..o| embeddings : "vector (soft, by id)"
    chunks_entity ||..o| embeddings : "vector (soft, by id)"
```

Note: `embeddings` is created by raw SQL (not an ORM model) and holds the vector plus an HNSW cosine index.
Its `id` is a string that the application sets to the chunk or entity id, so the link is logical, not a database foreign key.

## Subsystem 2: M3 WMS log ingestion

This is the log-analysis pipeline for the M3 warehouse system.
Raw log lines are stored losslessly as `log_entries` (Stage 1), then derived into one row per API request/response cycle as `log_transactions` (Stage 2).
All three tables here are **range-partitioned by UTC day** (migration `a1f6d70b3e92`) - see "Daily partitioning" below.
One transaction groups many entries.
`log_regroup_pending` and `log_regroup_runs` coordinate the async re-derivation of "dirty" time windows.

```mermaid
erDiagram
    log_transactions {
        uuid id "deterministic uuid5; unique w/ started_at, NOT a PK"
        uuid job_id FK "-> jobs.id (CASCADE)"
        string customer_code "soft tenant key"
        bool sealed
        uuid flow_id "soft, future log_flow"
        datetime started_at
        datetime ended_at
        date date
        int duration_ms
        string user_name
        string warehouse
        string reqid
        string method
        string transaction_name
        string item_number
        string delivery_number
        string order_number
        enum status "success/soft/error/incomplete"
        text error_text
        int entry_count
        jsonb attributes "long-tail params"
        datetime created_at
    }

    log_entry_assignment {
        uuid entry_id "soft -> log_entries.id (NO FK)"
        datetime entry_ts "copied from the entry; PARTITION KEY (UTC day)"
        uuid transaction_id "soft -> log_transactions.id (NO FK)"
        int seq "position within the transaction"
        string customer_code "soft tenant key"
        datetime assigned_at
    }

    log_entries {
        uuid id "unique w/ timestamp, NOT a PK"

        uuid job_id FK "-> jobs.id (CASCADE)"
        string customer_code "soft tenant key"
        string entry_hash "dedup, unique w/ customer_code + timestamp"
        string source_file
        int line_number
        datetime timestamp
        string level
        string thread
        string user_ctx
        enum entry_type "request/mi_call/sql/response/..."
        string mi_program
        string mi_transaction
        text message
        text raw_body
        jsonb fields
        datetime created_at
    }

    log_regroup_pending {
        uuid id PK
        string customer_code "soft tenant key"
        uuid job_id "soft -> jobs.id (no FK)"
        datetime range_start
        datetime range_end
        datetime consumed_at
        int attempts "retry budget"
        text last_error
        datetime last_attempt_at
        datetime abandoned_at "dead-lettered at max attempts"
        datetime available_at "backoff gate; DB clock"
        datetime created_at
    }

    log_regroup_runs {
        uuid id PK
        string customer_code "soft tenant key"
        enum status "running/completed/failed"
        int windows
        int pending_consumed
        text error
        jsonb result
        datetime created_at
        datetime finished_at
    }

    log_transactions ||..o{ log_entry_assignment : "groups (soft, no FK)"
    log_entries ||..o| log_entry_assignment : "currently assigned (soft, no FK)"
```

### `log_entry_assignment` — why `log_entries` is now append-only

Stage 2 used to write the grouping result back onto `log_entries` (`transaction_id` / `seq`) and clear it again through an `ON DELETE SET NULL` cascade.
`transaction_id` is indexed, so every rewrite touched the heap *and* the index, and the unsealed tail is regrouped repeatedly before it seals.
Measured on production 2026-08-05: **105,838,123 updates at 0.0% HOT** on 1.9M rows — roughly 55 rewrites per row.
That was the write amplification, dead-tuple churn and vacuum pressure behind the outage.

Separating the current interpretation from the raw evidence fixes it.
**Both foreign keys were removed** (migration `f04b7c29ae13`) so the table can be partitioned by day.
A foreign key makes the referenced table's partitions impossible to remove - `DETACH PARTITION` and `DROP TABLE` both refuse while one exists - and retention *is* dropping partitions.
Removing them also made the hottest write path ~4x faster: 200k assignment inserts went from 1,060 ms (two FK triggers, 200,000 calls each) to 249 ms.

The cost is that deletes no longer cascade, so every path that removes entries or transactions clears the assignments explicitly: `logspace_cleanup.purge_logspace`, the full wipe and date-range delete in `logs.py`, and `regroup_all` / `regroup_incremental` / `regroup_window` in `derive_transactions.py`.

"At most one current assignment per entry" is now a `UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)` rather than a primary key.
Not a primary key, because PostgreSQL forces PK columns to `NOT NULL` and `entry_ts` must stay nullable (`log_entries.timestamp` is).
`NULLS NOT DISTINCT` is what preserves the guarantee for timestamp-less entries - a plain `UNIQUE` treats two NULLs as different.

The column order is load-bearing: `entry_id` FIRST. Measured on 300k rows, looking up by `entry_id` alone takes 0.046 ms with `(entry_id, entry_ts)` and **10.8 ms** with `(entry_ts, entry_id)` - a sequential scan, because `entry_id` is no longer seekable.
Three hot paths filter on `entry_id` with no time bound.

The `log_entries.transaction_id` / `seq` columns and their index are **gone** (migrations `d5b830e14f72`, `e93c47a15b08`).
`DROP COLUMN` is a catalog operation in PostgreSQL, not a rewrite - measured at 0.1s on a 48 MB table with the relfilenode unchanged - so there was no reason to defer it.
`log_entries` is now strictly insert-only: five consecutive regroups of the same window perform **0** row updates, against ~55 rewrites per row before.

Note: `log_transactions.flow_id` is a nullable hook for a future `log_flow` table and has no foreign key today.

### Daily partitioning

`log_entries`, `log_transactions` and `log_entry_assignment` are range-partitioned by UTC day (migration `a1f6d70b3e92`).
The keys are `timestamp`, `started_at` and `entry_ts` respectively; `app/persistence/partitioning.py` is the single source of truth for which table is cut on which column, and what a day's partition is called.

Retention was `DELETE` + `VACUUM`, both of which read the whole table - on a heap that reached 40 GB, on a disk with bad sectors.
It is now `DROP TABLE <partition>`: a file unlink, no row scan, nothing left to vacuum.
`log_entry_assignment` is co-partitioned with `log_entries` on the same grain deliberately, so a day's entries and that day's assignments are dropped together and retention can never strand one without the other.

Three consequences show up in the schema above.

**No table has a PRIMARY KEY any more.**
PostgreSQL requires a unique constraint on a partitioned table to contain every partition-key column, and silently forces PK columns to `NOT NULL`.
All three partition keys are nullable - the parser genuinely emits entries whose timestamp will not parse - so a PK would make those rows un-insertable.
Identity is a `UNIQUE NULLS NOT DISTINCT` instead: `(id, timestamp)`, `(id, started_at)`, `(entry_id, entry_ts)`.
The key is present but never first, for the same measured reason as `log_entry_assignment` above.
The ORM models still declare `primary_key=True` on the id column, but that is row identity for SQLAlchemy only - the DDL it implies is invalid here and is never used, because Alembic is the sole schema builder.

**`log_entries` dedup grew a column**: `(customer_code, entry_hash)` became `(customer_code, entry_hash, timestamp)`.
This stays a correct dedup key because `entry_hash` is a sha256 over the raw line *including* its millisecond timestamp text, so an identical replay parses to the same instant and routes to the same partition.
`parse_insert.py`'s `ON CONFLICT` must name all three columns or the insert fails outright.
The one way the two can disagree is a customer's display timezone being changed between ingests, which re-parses the same text to a different UTC instant.
That is now blocked: `PATCH /customers/{code}` refuses a timezone change with **409** once the tenant has entries, unless `allow_mixed_timezones=true` (see `app/services/timezone_change_guard.py`).

**Every table has a `DEFAULT` partition** catching NULL-key rows, which is what makes timestamp-less entries insertable at all.

Partitions are maintained by `app/services/workers/log_partition_worker.py`, hourly.
It provisions `log_partition_precreate_days` (14) of runway ahead of today and drops days past `log_partition_retention_days` (60).
Creation is the dangerous half - an insert into a day with no partition fails outright - so a creation failure logs CRITICAL and the remaining runway is reported on every tick.

Partition health is exposed on `GET /api/v1/logs/regroup/status` as an additive `partitions` block (`days_ahead`, `oldest_day`, `newest_day`, `retention_days`, `default_partition_rows`, `healthy`).
It is global rather than tenant-scoped, and rides on that endpoint only because the AUTO-POLL card already polls it.

Dropping sits behind three gates: the day is past retention, no OPEN `log_regroup_pending` window overlaps it, and **entries lag transactions by one day**.
That last one is the midnight rule.
A transaction spans at most the seal window, so one starting at 23:58 owns entries until ~00:13 the next day; dropping day N's entries while a day N-1 transaction still referenced them would leave that transaction rendering half-empty.
So `log_transactions` is dropped for day D and `log_entries` + `log_entry_assignment` only for day D-1.
The cost is one extra day of entry storage and the bound is exact, not a safety guess.

## Subsystem 3: Remote SSH log fetching

This subsystem pulls raw log files from customer servers over SSH.
A `log_ssh_source` is one remote server/directory to poll.
`log_ssh_file_checkpoints` remembers how far each remote file has been read (so re-fetches are incremental), and `log_ssh_fetch_runs` records each fetch attempt.

```mermaid
erDiagram
    log_ssh_sources {
        uuid id PK
        string customer_code "soft tenant key"
        string name "unique w/ customer_code"
        string host
        int port
        string username
        string private_key_path
        text private_key_enc
        string remote_log_dir
        string file_glob
        bool enabled
        float poll_interval_seconds
        datetime last_ok_at
        text last_error
        datetime last_attempt_at "last fetch attempt (success or fail)"
        int consecutive_failures "circuit-breaker counter"
        datetime auto_disabled_at "set when breaker auto-disabled"
        datetime created_at
        datetime updated_at
    }

    log_ssh_file_checkpoints {
        uuid id PK
        uuid source_id FK "-> log_ssh_sources.id (CASCADE)"
        string customer_code "soft tenant key"
        string remote_path "unique w/ source_id"
        bigint last_size
        float last_mtime
        bigint last_offset
        datetime last_fetched_at
        string head_fingerprint "sha256 of file head; rotation guard"
    }

    log_ssh_fetch_runs {
        uuid id PK
        string customer_code "soft tenant key"
        uuid source_id "soft -> log_ssh_sources.id (no FK)"
        enum mode "incremental/timestamp/full"
        datetime requested_from
        enum status "running/completed/failed"
        enum phase "listing/fetching/regrouping/done"
        jsonb progress
        int files_considered
        int files_fetched
        bigint bytes_fetched
        int entries_ingested
        text error
        jsonb result
        datetime created_at
        datetime finished_at
    }

    log_ssh_sources ||--o{ log_ssh_file_checkpoints : "tracks per file"
    log_ssh_sources ||..o{ log_ssh_fetch_runs : "fetched by (soft)"
    log_ssh_sources ||..o{ log_source_objects : "downloaded from (soft)"
```

Note: `log_ssh_fetch_runs.source_id` is nullable and has no foreign key.
A null value means the run covered all enabled sources; otherwise it points at one source.

### `log_source_objects` — the fetch/parse handoff

One row is one contiguous byte range downloaded from a remote log file and saved to object storage, plus whether it has been parsed yet.
It exists so the SSH fetcher can stop parsing inline: the fetcher inserts this row and advances `log_ssh_file_checkpoints.last_offset` in **one transaction**, then releases the SSH connection and the per-host lock, and a separate worker (`app/services/workers/log_parse_worker.py`) leases the row and runs Stage 1.

The single transaction is load-bearing.
The checkpoint is a promise that everything behind it is handled; advancing it without this row would skip those bytes forever, and writing this row without advancing it would re-download them.

It also carries the retry budget that the fetch path previously lacked (`attempts` / `max_attempts` / `available_at` / `last_error`), giving it the same dead-letter semantics `log_regroup_pending` already has for Stage 2.

`source_id` deliberately has **no** foreign key: deleting an SSH source must never delete ingestion evidence, the same rule `log_ssh_fetch_runs.source_id` follows.
`job_id` is likewise a soft reference and is transitional — it disappears when `jobs` is retired from the log path.

```mermaid
erDiagram
    log_source_objects {
        uuid id PK
        string customer_code "soft tenant key"
        uuid source_id "soft -> log_ssh_sources.id (no FK)"
        string source_name "denormalized; survives a source rename/delete"
        string remote_path
        bigint start_offset
        bigint end_offset
        bigint observed_size
        float observed_mtime
        string head_fingerprint
        string content_sha256
        string storage_key "where the downloaded bytes live"
        string status "pending/leased/ingested/abandoned"
        int attempts
        int max_attempts
        datetime available_at "retry backoff gate"
        string lease_owner
        datetime lease_expires_at "crashed-worker recovery"
        text last_error
        uuid job_id "soft -> jobs.id (no FK), transitional"
        int entries_inserted
        datetime created_at
        datetime ingested_at
        datetime file_deleted_at
    }

    log_ssh_sources ||..o{ log_source_objects : "downloaded from (soft)"
    log_source_objects ||..o| jobs : "parsed into (soft, transitional)"
```

Deletion: `log_source_objects` has no job or tenant foreign key, so a tenant purge deletes it explicitly and unlinks the referenced files first (`app/services/logspace_cleanup.py`).
A **date-range** log delete deliberately leaves it alone — these rows describe byte ranges, not log dates.

## Subsystem 4: Customer registry

The tenant directory.
`customers` is the canonical list of tenants (each a "log space" of one `kind`: `permanent` or `disposable`), `customer_display_names` holds alternate display names per tenant, and `logspace_presence` records who is currently in a space.
A disposable owns a brand-new `customer_code` (1:1) and carries `owner_name` + `expires_at` (auto-purged when due); a permanent carries admin-set `name`/`description`/`environment` (`live`|`test`).
`inactive` is derived from `active=false`, not an `environment` value.
This is the one subsystem where `customer_code` is a real, enforced foreign key (into `customers`).

```mermaid
erDiagram
    customers {
        uuid id PK
        string customer_code UK "unique"
        string display_name
        string timezone "IANA tz"
        enum kind "permanent | disposable"
        string name "permanent-only"
        text description "permanent-only"
        enum environment "permanent-only: live | test"
        string owner_name "disposable-only"
        datetime expires_at "disposable-only: auto-purge"
        bool active
        datetime created_at
        datetime updated_at
    }

    customer_display_names {
        uuid id PK
        string customer_code FK "-> customers.customer_code (CASCADE)"
        string display_name "unique w/ customer_code"
        bool active
        datetime created_at
        datetime updated_at
    }

    logspace_presence {
        uuid id PK
        string customer_code FK "-> customers.customer_code (CASCADE)"
        string name "who is present; unique w/ customer_code"
        string note "optional"
        datetime since "server-set; TTL-swept"
    }

    customers ||--o{ customer_display_names : "has aliases"
    customers ||--o{ logspace_presence : "has presence"
```

## Subsystem 5: Saved views

A saved view is a stored analysis or filter snapshot that a user can name, assign, comment on, and close.
It is self-contained: the analysis state, the review comment thread, and the closure record all live in JSONB columns on the single `saved_views` table, so it has no foreign keys.

```mermaid
erDiagram
    saved_views {
        uuid id PK
        string customer_code "soft tenant key"
        text name
        string title
        text notes
        string saved_by
        string assignee
        string status "open/... "
        string due_date
        jsonb comments "append-only review thread"
        jsonb closure
        jsonb state "opaque analysis snapshot"
        datetime created_at
        datetime updated_at
    }
```

## Subsystem 6: Notifications and alerting

A data-driven alerting pipeline with a durable outbox so no alert is lost during an outage.
`notification_rules` decide WHEN to alert, `customer_notification_channels` decide WHERE alerts go, `notification_events` is the durable outbox of published alerts, and `notification_deliveries` tracks each (event x channel) send attempt with retries and backoff.

Streaming rules read `log_transactions` **incrementally**, via `notification_rules.cursor_at` (migration `c7a02f68b1d4`).
The cursor is a `log_transactions.created_at` - the WRITE time, not `started_at` which is when the log line happened.
That distinction is load-bearing: a week-old file backfilled today produces rows with an old `started_at` but a new `created_at`, so a cursor on `started_at` would silently never see them.
`log_transactions.created_at` is indexed for exactly this (migration `b3d914c7ea52`); because the table is partitioned, that index had to be built per-partition and attached, since PostgreSQL refuses `CREATE INDEX CONCURRENTLY` on a partitioned parent.

The engine never reads closer to the present than `notification_cursor_lag_seconds`.
`created_at` is stamped when Python builds the row rather than when Postgres commits it, so a long Stage 2 transaction can commit a row whose timestamp already sits behind the cursor; without the lag that row is never read, and dedupe cannot recover something never seen.
Dedupe by `dedup_key` remains as the safety net - the cursor prevents re-reading, dedupe prevents re-sending.

Digest (window) rules do not use the cursor at all; they summarise a completed interval and keep their own `rule:{id}:window:{n}` dedup key.

```mermaid
erDiagram
    customer_notification_channels {
        uuid id PK
        string customer_code "soft tenant key"
        string channel_type "teams/slack/whatsapp"
        string name "unique w/ customer_code+type"
        jsonb config "webhook url, secrets"
        bool enabled
        datetime created_at
        datetime updated_at
    }

    notification_rules {
        uuid id PK
        string customer_code "soft tenant key"
        string name
        text description
        string rule_type "status_match/text_match/digest"
        jsonb match "evaluator params"
        string severity
        jsonb target_channel_ids
        string status "draft/active/inactive"
        datetime cursor_at "how far this rule has read (log_transactions.created_at); NULL = never run"
        datetime created_at
        datetime updated_at
    }

    notification_events {
        uuid id PK
        string dedup_key UK "idempotency, unique"
        string customer_code "soft tenant key"
        uuid rule_id "soft -> notification_rules.id (no FK)"
        string event_type
        string severity
        string title
        text summary
        jsonb payload "rendered on every retry"
        jsonb target_channel_ids
        datetime created_at
    }

    notification_deliveries {
        uuid id PK
        uuid event_id FK "-> notification_events.id (CASCADE)"
        uuid channel_id FK "-> customer_notification_channels.id (SET NULL)"
        string channel_type "denormalized"
        string status "pending/delivered/failed/dead"
        int attempts
        datetime next_attempt_at
        text last_error
        datetime delivered_at
        datetime created_at
    }

    notification_events ||--o{ notification_deliveries : "fans out to"
    customer_notification_channels ||--o{ notification_deliveries : "sent via"
    notification_rules ||..o{ notification_events : "raised by (soft)"
```

Note: `notification_events.rule_id` records which rule raised the event (provenance) but has no foreign key, so an event survives even if its rule is later deleted.

## Subsystem 7: Idempotency keys

Server-side de-duplication for mutating POSTs (see `app/middleware/idempotency.py`).
A client sends an `Idempotency-Key` header; the first request is recorded here and its JSON response cached, so a retry or double-submit with the same key replays the stored response instead of duplicating the side effect.
Self-contained (no foreign keys); tenant-scoped by `customer_code`, with `UNIQUE(customer_code, idem_key)` as the atomic de-dup guard and `expires_at` for TTL cleanup.

```mermaid
erDiagram
    idempotency_keys {
        uuid id PK
        string customer_code "soft tenant key"
        string idem_key "client Idempotency-Key (unique per tenant)"
        string method
        string path
        string request_fingerprint "sha256(method|path|body)"
        string status "in_progress/completed"
        int response_status "cached response (null until completed)"
        jsonb response_body
        datetime created_at
        datetime completed_at
        datetime expires_at "TTL"
    }
```

## Full relationship reference

### Enforced foreign keys (solid lines)

| Parent | Child column | On delete |
| --- | --- | --- |
| `jobs.id` | `chunks.job_id` | CASCADE |
| `jobs.id` | `chunks_entity.job_id` | CASCADE |
| `jobs.id` | `embedding_queue.job_id` | CASCADE |
| `jobs.id` | `log_entries.job_id` | CASCADE (re-added by `a1f6d70b3e92`; `LIKE` does not copy foreign keys) |
| `jobs.id` | `log_transactions.job_id` | CASCADE (re-added by `a1f6d70b3e92`) |
| `chunks.id` | `chunks.parent_id` (self) | CASCADE |
| `chunks.id` | `embedding_queue.chunk_id` | CASCADE |
| `chunks_entity.id` | `embedding_queue.chunk_entity_id` | CASCADE |

| `log_ssh_sources.id` | `log_ssh_file_checkpoints.source_id` | CASCADE |
| `customers.customer_code` | `customer_display_names.customer_code` | CASCADE |
| `customers.customer_code` | `logspace_presence.customer_code` | CASCADE |
| `notification_events.id` | `notification_deliveries.event_id` | CASCADE |
| `customer_notification_channels.id` | `notification_deliveries.channel_id` | SET NULL |

### Soft references (dashed lines, no database constraint)

| Logical parent | Referencing column | Meaning |
| --- | --- | --- |
| `customers.customer_code` | `customer_code` on jobs, log_entries, log_transactions, log_regroup_pending, log_regroup_runs, log_ssh_sources, log_ssh_file_checkpoints, log_ssh_fetch_runs, log_source_objects, saved_views, idempotency_keys, and the notification tables | tenant partition key |
| `jobs.id` | `log_regroup_pending.job_id` | nullable, no FK |
| `log_ssh_sources.id` | `log_ssh_fetch_runs.source_id` | nullable, no FK (null = all sources) |
| `log_ssh_sources.id` | `log_source_objects.source_id` | nullable, no FK — deleting a source must not delete ingestion evidence |
| `jobs.id` | `log_source_objects.job_id` | nullable, no FK; transitional, set by the parse worker |
| `notification_rules.id` | `notification_events.rule_id` | provenance, no FK |
| future `log_flow` | `log_transactions.flow_id` | Phase-3 hook, no FK |
| `log_entries.id` | `log_entry_assignment.entry_id` | FK removed in `f04b7c29ae13` so daily partitions can be dropped; cleared explicitly by every delete path |
| `log_transactions.id` | `log_entry_assignment.transaction_id` | same |

| `chunks.id` / `chunks_entity.id` | `embeddings.id` (string) | app-level key into the pgvector table |
