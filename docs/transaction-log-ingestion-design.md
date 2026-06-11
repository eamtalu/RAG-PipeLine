# Transaction Log Ingestion + Agentic Debugging — Design Reference

> Status: **design agreed** · ready for step-by-step implementation
> Grain = **Option 1 (one transaction = one API call) + nullable `flow_id` hook**
> Pipeline = **two-stage: parse→insert raw entries, then derive transactions**
> Agent LLM = **Claude (Anthropic)**. Embeddings stay OpenAI.
>
> Scope: a second ingestion family beside the existing document pipeline, for ingesting
> Infor M3 WMS server logs into a relational store the LLM agent can query for debugging.

---

## 1. What the log actually is

A **.NET (log4net-style) WMS server log** that wraps Infor **M3 API** calls. Two facts drive
the parser design:

### 1.1 Line anatomy

```
2026-05-19 13:42:33,362 (BECWHLO) [94] INFO  M3WebServiceClassLib.Managers.M3ItemManager LookupAltUoMConversionFactorAndForm - Calling MMS200 - LstItmAltUnitMs
└── timestamp ──────┘ └─user─┘ └thr┘ └level┘ └──────── logger / class ─────────────┘ └──── method ────┘   └──── message ────┘
```

| Field | Notes |
|---|---|
| `timestamp` | `YYYY-MM-DD HH:MM:SS,mmm` (comma before millis) |
| `user` | `(BECWHLO)`, `((null))`, or `()` — **normalize all three; null/empty ⇒ no user** |
| `thread` | `[94]` — **NOT a reliable correlation key** (async handler hops threads: `MoveNext` → `<SendAsync>b__0` → `<SendAsync>b__1` land on different threads in one request) |
| `level` | `INFO `, `DEBUG`, `ERROR` (note padding) |
| `logger` | e.g. `M3WebServiceClassLib.Managers.M3ItemManager`, `Server.CommonCode.ApiLogHandler` |
| `method` | e.g. `LogAPICall`, `LogAPIResult`, `MoveNext`, `<SendAsync>b__1` |
| `message` | after ` - ` |

### 1.2 Most entries are **multi-line**

A single logical entry (`LogAPICall`, `LogAPIResult`, a stored-proc execution, a `REQUEST BODY`)
continues across many physical lines until the **next timestamped line**. Parsing is **two-pass**:

1. Split the file on timestamp-prefixed line starts.
2. Everything until the next timestamp is that entry's multi-line **body** (kept raw + parsed into fields).

So: **one record per timestamped entry**, not one per physical line.

---

## 2. What a "transaction" maps to

The natural boundary is an **API request/response cycle**, bracketed by `Server.CommonCode.ApiLogHandler`:

- **open** → `... MoveNext - REQUEST: <url>` (+ optional `<SendAsync>b__0 - REQUEST BODY: {...}`)
- **middle** → internal `M3...LogAPICall` (MI program + Transaction + URL + Inputs), matching
  `LogAPIResult` (Result + Records), and SQL stored-proc lines
- **close** → `... <SendAsync>b__1 - RESPONSE: <body>`

Two notions of "transaction" exist in the data — **both are promoted to columns**:
- **API endpoint** = `MethodName=` in the URL/body (e.g. `ConfirmPickLine`)
- **business transaction** = `TransactionName` / `TransactionType` (e.g. "Brighton Stock Pick" / `002001`)

**Correlation id** = `ReqID=3091-2025-11-20_…` (device-timestamp-rand) — present on REQUEST / REQUEST
BODY only, **not** on internal MI lines or the RESPONSE line. Internal calls are correlated by
**position in the ordered stream** within the open request (see §3) — not by id matching.

### Entry sub-types

`request`, `request_body`, `mi_call`, `mi_result`, `sql`, `response`, `info`, `error`

### Error model — **three-valued**, so the agent doesn't cry wolf

| status | meaning | example |
|---|---|---|
| ✅ `success` | request finished, no errors | `ConfirmPickLine` |
| ⚠️ `soft` | M3 returned "not found / needs value" but the app coped | `Location  does not exist`, `requires a value for field WHSL`, `No packages exist for delivery 10655` |
| ❌ `error` | a real `ERROR`-level failure | `Printer Error Code = 1801` |

Soft results live inside `DEBUG` `LogAPIResult` lines and are usually expected/handled. A real
`error` is an `ERROR`-level line. Transaction `status` rolls up from its entries.

---

## 3. Pipeline — two stages (parse→insert, then derive)

The pipeline is **split**, which is what makes cross-file transactions and deferred grouping work.

```
STAGE 1 — parse & insert  (per file, parallel-safe, idempotent)
   file N  ─┐
   file N+1 ├─►  parse each file  ─►  INSERT raw rows into  log_entry
   file N+2 ─┘   (timestamp, level, logger, type, mi_program, fields JSONB,
                  raw_body, source_file, line_number)        ← no grouping yet

STAGE 2 — derive transactions  (runs over the DB, not over a file)
   SELECT * FROM log_entry ORDER BY timestamp                ← all files merged by time
        │  REQUEST → RESPONSE state machine (+ extract promoted fields)
        ▼
   upsert  log_transaction  rows  ·  set  log_entry.transaction_id
```

### Why two stages

- **`log_entry` is the lossless source of truth.** Transactions are a **derived view** that can be
  (re)computed anytime without re-reading files.
- **Cross-file transactions "just work":** Stage 2 reads the whole table **ordered by timestamp**, so
  a transaction whose REQUEST is in file N and RESPONSE in file N+1 reassembles automatically — no
  carry-over buffer, no filename-ordering puzzle, no id-matching needed (which is essential since the
  RESPONSE line carries no `ReqID`).
- **Grouping is deferrable, re-runnable, revisable:** ship Stage 1 first (every line queryable), build
  Stage 2 later; change boundary rules / soft-vs-error classification / add `flow_id` roll-up by simply
  re-deriving. `log_entry.transaction_id` stays `NULL` until grouping runs.
- **Incremental & idempotent:** a REQUEST may be ingested before its RESPONSE's file arrives → the
  transaction is marked `incomplete`; a later Stage-2 pass finds the RESPONSE and closes it. Stage 2 is
  "upsert transactions from currently-available entries," safe to run repeatedly.

> Until Stage 2 exists, only **line-level** questions are answerable ("every `MMS200MI` call for item
> 101970", "all `ERROR` lines today"). The **transaction view** (the canonical render in §6) is what
> makes debugging easy — purely additive on top of already-captured data.

---

## 4. The grain decision (recap)

**Option 1 — one transaction = one API call** ✅, built with a nullable `flow_id` column from day one
(unused until Phase 3). Rejected alternatives: Option 2 (business-job grain) collapses the 41 M3 calls
and loses the debugging signal; Option 3 (both) is Option 1 + an additive `log_flow` roll-up we defer.

```
   Build now                    Add later with ZERO rework
   ┌──────────────────┐         ┌──────────────────────────────────┐
   │ log_transaction  │         │  + log_flow table                │
   │   (+ flow_id ❍)  │  ──────▶│  + grouping pass fills flow_id   │  = Option 3
   │ log_entry        │         └──────────────────────────────────┘
   └──────────────────┘
```

---

## 5. Relational schema

### 5.1 `log_transaction` — promoted (indexed, groupable) columns + JSONB catch-all

Design principle: **promote the common high-value dimensions to real columns** (fast filter / group /
aggregate); **keep every other extracted request param in `attributes` JSONB** (the long tail, nothing lost).

| group | columns | example |
|---|---|---|
| **pk / lineage** | `id`, `job_id`, `flow_id` (nullable hook), `source_file_start`, `source_file_end` | |
| **time** | `started_at`, `ended_at`, `date` (derived), `duration_ms` | 2026-05-19 · 231 ms |
| **who** | `user`, `user_id`, `employee_name` | BECWHLO / 1 / BEC |
| **where (org)** | `company`, `warehouse`, `warehouse_id`, `division`, `facility` | 911 / BRI / TMP |
| **where (device)** | `device_id`, `device_name`, `reqid` | 3091 / 25171E0100 |
| **what** | `method` (endpoint), `http_method`, `endpoint_url`, `transaction_name`, `transaction_type` | ConfirmPickLine / "Brighton Stock Pick" / 002001 |
| **business keys** | `route`, `item_number`, `delivery_number`, `picklist_suffix`, `order_number`, `reporting_number` | BRI05 / 104399 / 10655 |
| **outcome** | `status` (success/soft/error/incomplete), `error_text`, `entry_count`, `mi_program_count` | error / "Printer 1801" |
| **summaries** | `request_summary`, `response_summary` | |
| **catch-all** | `attributes` JSONB — every other request param | `{WebServiceMode, Retry, DepartureDate, FromZone, …}` |
| | `created_at` | |

Indexes: `reqid`, `user`, `date`/`started_at`, `method`, `transaction_name`, `transaction_type`,
`status`, `warehouse`, `delivery_number`, `item_number`, `order_number`.

### 5.2 `log_entry` — one row per timestamped entry (drill-down timeline)

| column | type | notes |
|---|---|---|
| `id` | PK | |
| `transaction_id` | FK → log_transaction, **nullable**, indexed | set by Stage 2; NULL before grouping / for orphans |
| `job_id` | FK → job | ingestion job |
| `source_file` | text | the file this line came from (a txn may span several) |
| `line_number` | int | position in file |
| `seq` | int | order within the transaction (set by Stage 2) |
| `timestamp` | timestamptz, indexed | the stream-ordering key |
| `level` | text | INFO / DEBUG / ERROR |
| `logger` | text | class |
| `method` | text | e.g. LogAPICall |
| `entry_type` | enum, indexed | request / request_body / mi_call / mi_result / sql / response / info / error |
| `mi_program` | text, indexed | e.g. MMS200MI (mi_call/mi_result) |
| `mi_transaction` | text, indexed | e.g. LstItmAltUnitMs |
| `result_status` | text | OK / soft-error text |
| `record_count` | int | records returned |
| `message` | text | one-line summary |
| `raw_body` | text | full multi-line raw entry |
| `fields` | JSONB | parsed Inputs / Outputs / Records |
| `created_at` | timestamptz | |

### 5.3 `log_flow` — **Phase 3 only (not built now)**
`id, name (TransactionName), user, device_id, started_at, ended_at, status, transaction_count`.
Phase 3 adds this table + a backfill grouper that fills `log_transaction.flow_id`.

**Phase 1 migration** adds `log_transaction` + `log_entry` only.

---

## 6. Canonical "Transaction Detail View" 🔒

This is the **locked** render returned by `get_transaction(id)` (REST + agent). Every element is
reconstructable from the two tables — see the field mapping in §6.1.

### Success example — `ListPickLinesByUser` (✅, 41 steps)

```
TRANSACTION #51   /api/picking/ListPickLinesByUser   ✅ SUCCESS
user BECWHLO · reqid …-500 · 15:06:07.746 → 15:06:09.608 · 1.86 s · 41 steps

  ▶ REQUEST   Warehouse BRI · Route BRI05 · Picker BECWHLO · LineStatusFilter 30,40
   1  📞 MWS420MI/LstPickersPL    ROUT=BRI05 PICK=BECWHLO        ✅ 5 recs
   2  📞 MWS422MI/LstPickDetail   DLIX=10655 PLSX=1              ✅ 5 recs  (items 102981,101998,101970,102534,104501)
   3  ⚙  filter pick lines on status 30,40                      ✅
   4  📞 OIS100MI/LstLine         ORNO=1000000817               ✅ 5 recs
   5  📞 MMS200MI/LstItmAltUnitMs item 102981                   ✅ 2 recs  (CS=2.0 EA=1.0)
   6  📞 MMS200MI/LstItmAltUnitMs item 102981 (re-check)        ✅ 2 recs
   7  📞 CUSEXTMI/GetFieldValue   item 102981                   ✅ "KETCHUP HEINZ [EA=TUB 4LTR]"
   8  📞 MMS200MI/LstItmAltUnitMs item 101998                   ✅ 3 recs  (CS=20 EA=3.333 KG=1.0)
   …  (101998,101970,102534,104501 follow same MMS200×2 + CUSEXT pattern)
  ◀ RESPONSE ✅ 200  → 5 pick lines (KETCHUP HEINZ, MELON WATER, GRAPEFRUIT PINK, CRESS MUSTARD, BROCCOLI TENDERSTEM)
```

### Failure example — `NewDeliveryPackage` (❌), surfacing both error flavors

```
TRANSACTION #53   /api/picking/NewDeliveryPackage   ❌ ERROR
reqid …-9769 · 0.50 s · 8 steps

  ▶ REQUEST BODY  Delivery 10655 · CARBOX · printer "TMP LL" · PrintLabel true
   1  📞 MWS423MI/LstPackages  DLIX=10655   ⚠ SOFT  "No packages exist for delivery 10655"  (expected)
   2  📞 MWS423MI/AddPackage   PANR=10655/1-1  ✅ OK  (package created)
   3  ❌ ERROR  RawPrinterHelper.SendBytesToPrinter  "Printer Error Code = 1801"  (the real failure)
  ◀ RESPONSE "10655/1-1"  (HTTP 200, but no label printed)
```

### 6.1 View → field mapping (keeps renderer and schema in sync)

```
Header  ← log_transaction
  TRANSACTION #51   /api/picking/ListPickLinesByUser   ✅ SUCCESS
            id              method / endpoint_url           status
  user BECWHLO · reqid …-500 · started_at → ended_at · duration_ms · entry_count

▶ REQUEST line  ← request_summary  (built from promoted cols + attributes JSONB)
  Warehouse=warehouse · Route=route · Picker=user · LineStatusFilter=attributes->>'LineStatusFilter'

Each numbered step  ← one log_entry row
  📞 mi_program/mi_transaction   fields->'Inputs'   result_status  record_count   (entry_type)
  ⚙ info step                    ← entry_type='info', message
  "KETCHUP HEINZ […]"            ← fields->'Records'

◀ RESPONSE line  ← the response entry + response_summary

Error flavors  ← log_entry
  ⚠ SOFT   ← entry_type='mi_result', result_status ≠ 'OK'
  ❌ ERROR ← level='ERROR'  → rolls up to transaction.status='error' + error_text
```

Aggregate questions ("count for user/date", "status of reqid X") come off the **promoted columns**
without touching `log_entry`:

| Ask | Resolves to |
|---|---|
| "how many transactions for BECWHLO on 2026-05-19?" | `SELECT count(*) FROM log_transaction WHERE user='BECWHLO' AND date='2026-05-19'` |
| "status of transaction …-4642?" | `SELECT status, error_text FROM log_transaction WHERE reqid='…-4642'` |
| "show everything about delivery 10655" | `WHERE delivery_number='10655'` → drill into `log_entry` |
| "which M3 programs errored today?" | `log_entry WHERE result_status<>'OK'` grouped by `mi_program` |
| "slowest endpoints for warehouse BRI" | `GROUP BY method ORDER BY avg(duration_ms)` |

---

## 7. Component layout (mirrors existing patterns)

```
app/services/log_ingestion/
  LogIngestion.py                 # mirrors DataIngestion.py; get_log_ingestion() DI provider
  pipeline/
    parse_insert.py               # STAGE 1: file bytes → log_entry rows (per file, idempotent)
    derive_transactions.py        # STAGE 2: ordered entries → log_transaction (cross-file, re-runnable)
  parsers/
    base.py                       # BaseLogParser(ABC): parse(text) -> list[LogRecord]
    m3_dotnet_parser.py           # default: two-pass timestamp-block parser
    LogParserFactory.py           # registry {"m3_dotnet": ...}, get_log_parser(fmt)
    data_class/log_record.py      # LogRecord (Pydantic)
  grouping/
    base.py                       # BaseTransactionGrouper(ABC): group(ordered_entries) -> transactions
    request_cycle_grouper.py      # default: REQUEST→RESPONSE state machine + field promotion
    GrouperFactory.py
app/services/workers/
  log_watcher.py                  # polls log_incoming_dir → Stage 1; mirrors embedding_worker
  log_grouping_worker.py          # runs Stage 2 incrementally; lifecycle in main.py
app/persistence/
  models/log_transaction.py, log_entry.py
  repositories/log_transaction_repository.py, log_entry_repository.py
app/api/v1/logs.py                # POST /logs/scan, POST /logs/ingest, POST /logs/regroup,
                                  # GET /logs/transactions (filter/aggregate),
                                  # GET /logs/transactions/{id} (canonical detail view),
                                  # GET /logs/entries (line-level), POST /logs/debug/ask
app/services/log_agent/           # Phase 2
  tools.py                        # search_transactions, aggregate_transactions, get_transaction,
                                  #   find_errors, search_entries
  agent.py                        # Claude tool-use loop → answer + cited transaction ids
```

### Triggers (both)
- **Watcher worker** — polls `log_incoming_dir`, runs Stage 1, moves files to `processed/` on success,
  `failed/` on failure (idempotency + replay). S3 event version later swaps the source behind
  `LogIngestion.ingest`.
- **Event endpoints** — `POST /logs/scan` (ingest current incoming dir), `POST /logs/ingest` (push one
  file), `POST /logs/regroup` (re-run Stage 2).

### Settings additions
`log_incoming_dir`, `log_processed_dir`, `log_failed_dir`, `log_watcher_poll_seconds`,
`log_grouping_poll_seconds`, `log_format` (default `m3_dotnet`), plus Anthropic config (Phase 2).

---

## 8. Phasing (step-by-step implementation order)

- **Phase 1a** — models + Alembic migration (`log_transaction`, `log_entry`) + parser + **Stage 1**
  parse-and-insert + watcher + `POST /logs/scan|ingest` + `GET /logs/entries`.
  *Smallest correct shippable unit: every line queryable.*
- **Phase 1b** — **Stage 2** grouping → `log_transaction` (cross-file, incremental, re-runnable) +
  grouping worker + `POST /logs/regroup` + `GET /logs/transactions` + `GET /logs/transactions/{id}`
  (canonical detail view §6).
- **Phase 2** — Claude tool-use debugging agent over the relational store (answers cite transaction ids).
- **Phase 3 (optional)** — `log_flow` table + backfill grouper to populate `flow_id` (Option-3 upgrade).

---

## 9. Real-log facts (confirmed from `mnp-logs/`)

- **Rotation = numbered suffix, size-based ~5 MB**: `eSmartServerLog.txt` (active/newest), `.1`…`.5`
  (older = higher number; log4net RollingFileAppender style). Size-based roll cuts mid-transaction →
  the cross-file case the two-stage design handles via in-content timestamp ordering.
- **Volume**: ~5 MB / ~110k–160k physical lines per file; ~6 files ≈ 30 MB / ~680k lines. Fine for PG.
  One real 54,925-line file parsed to **4,130 logical entries**, 0 header/logger misses.
- **A `WARN` level exists** (besides INFO/DEBUG/ERROR). `level` column preserves it. **Stage-2 TODO:**
  decide whether a WARN makes a transaction `soft`.
- **Safety decision — the move-based watcher must NEVER point at `mnp-logs`** (live rotating logs). It
  only drains a dedicated staging dir (`log_incoming_dir`). In-place ingestion of the rotating logs is
  **read-only** via `POST /logs/scan` over `log_source_dir`, with a dedup ledger (Step 4b) so re-scans
  don't duplicate. The growing active `.txt` (offset-based tailing) is a deferred ops concern.

### Still open
- **Multi-server feeds** — if a source dir is fed by >1 server, add a `log_source` identity so Stage 2
  doesn't stitch different servers' streams.
- ~~**Concurrent/overlapping requests** in one stream~~ — **RESOLVED 2026-06-11** by the thread-aware
  grouper (see §10). Confirmed bug: the single-stack state machine merged interleaved requests across
  users (17 mixed-user transactions in one real file). Fixed by demultiplexing on the `thread` column;
  verified 0 mixed-user transactions across 6 multi-user files (3,423 txns, up to 11 concurrent users).
- **Orphan entries** before the first REQUEST — synthetic "unknown" transaction vs. `transaction_id` NULL.

---

## 10. Build progress

- **Phase 1a — DONE** (verified against real `eSmartServerLog.txt`):
  - models `log_transactions` + `log_entries` + migration `f6a0c4d18e25` (applied)
  - parser package (`m3_dotnet`) + `LogRecord` + factory
  - Stage 1 `parse_insert` + `LogIngestion` service + `get_log_ingestion` DI
  - `log_watcher` (staging-only, move-based) wired into `main.py` lifespan
  - API: `POST /logs/ingest`, `GET /logs/jobs/{id}`, `GET /logs/entries`; router wired
  - settings: `log_format`, `log_incoming_dir`/`processed`/`failed`, poll intervals
- **Phase 1b — DONE** (verified on real data: 4,130 entries → **253 transactions**; 194 success / 51
  soft / 7 error / 1 incomplete; 33 orphan entries; max dur 19.7s, avg 673ms):
  - Stage 2 `derive_transactions.regroup_all` — REQUEST→RESPONSE state machine, full rebuild,
    promotes WMS dims to columns + `attributes` JSONB, 3-valued status rollup (WARN counts as soft).
  - `log_grouping_worker` (regroups when entry count changes) wired into `main.py` lifespan.
  - API: `POST /logs/regroup`, `GET /logs/transactions` (filters + `total` count), `GET
    /logs/transactions/{id}` (canonical detail view §6 — verified against real NewDeliveryPackage error).
  - Postman collection updated (30 requests, folder "Logs - Transactions (Stage 2)").
  - Known edges (acceptable / documented): a request with no RESPONSE absorbs entries until the next
    REQUEST (overlap/missing-response); orphan entries before the first REQUEST stay `transaction_id`
    NULL. Full-rebuild grouping is fine at current volume; go incremental if entries grow large.
- **Step 4b — DONE**: content-level dedup + read-only scan.
  - `log_entries.entry_hash = sha256(raw_body)` + UNIQUE index (migration `a1b2c3d4e5f6`, applied);
    Stage 1 inserts via `ON CONFLICT (entry_hash) DO NOTHING`. Guarantees the same log line is never
    stored twice — across re-ingestion, the growing active file, and overlap between rotated files.
    (Multi-server feeds would key the hash on `source + raw_body`; single-source for now.)
  - `POST /logs/scan?directory=...` (default `settings.log_source_dir`) ingests every file **read-only**
    — never moves/deletes. Verified on `mnp-logs`: scan #1 = 76,690 new across 6 files; scan #2 = 0 new
    (idempotent). Full corpus regrouped to **3,975 transactions** (3,181 ok / 615 soft / 135 error /
    44 incomplete), 112 orphans.
  - Postman updated (31 requests): "Scan directory (read-only, deduped)" + `mnpLogsDir` var.
- **Phase 2 — DONE**: Claude tool-use debugging agent over the relational store.
  - `app/services/log_agent/tools.py` — five **read-only** SQL-backed tools the model can call:
    `search_transactions`, `count_transactions`, `find_errors`, `get_transaction` (canonical detail
    view §6), `search_entries` (line-level). All SELECT-only; every result carries transaction ids so
    answers can cite them. Filters reuse the promoted columns (same vocabulary as the REST API);
    results are compact JSON (null fields dropped) to save tokens.
  - `app/services/log_agent/agent.py::LogDebugAgent` — manual async agentic loop on `AsyncAnthropic`
    (model `claude-opus-4-8`, `thinking: adaptive`, per claude-api skill defaults). System prompt
    teaches the M3 log model + status semantics (soft ≠ error). Manual loop keeps the request-scoped
    `AsyncSession` explicit per tool call; iteration cap = `log_agent_max_iterations` (default 12) with
    a tools-off final answer if the cap is hit. Preserves assistant `thinking`+`tool_use` blocks
    verbatim across turns.
  - API: `POST /logs/debug/ask` (`{"question": "..."}`) → `{answer, stop_reason, tool_calls, iterations}`.
    503 if `anthropic_api_key` unset.
  - Settings: `anthropic_api_key`, `log_agent_model` (default `claude-opus-4-8`), `log_agent_max_tokens`
    (8000), `log_agent_max_iterations` (12). `anthropic>=0.69` added to requirements.
  - Verified: all five tools exercised against the live DB (11,068 txns) — count/breakdown, error
    triage, get_transaction drill-down, line search for the "Index was out of range" .NET bug, and
    bad-id error handling. **LLM loop not yet run end-to-end** (no `ANTHROPIC_API_KEY` in `.env` yet —
    set it to use the endpoint). Postman: "Logs - Agent (Phase 2)" folder, 5 question variations (36 total).
- **Concurrency-safe Stage 2 grouping — DONE** (2026-06-11): fixed a confirmed mis-stitching bug where
  the single-stack REQUEST→RESPONSE state machine merged *interleaved* requests from concurrent users
  into one transaction (the M3 server processes multiple users at once; the timestamp-ordered stream
  interleaves them). Evidence: 17 transactions containing 2+ users in one real file.
  - Persist the log `thread` (migration `b8c2d7e91a04` adds `log_entries.thread`, backfilled from
    `raw_body`; Stage 1 now stores it). Measured correlation: a request's internal MI work stays on one
    thread (~98%), and a POST's REQUEST BODY shares that thread (~99%); the async REQUEST/RESPONSE
    bracket lines hop threads and the RESPONSE carries no id.
  - `derive_transactions._group` rewritten thread-aware: open transactions keyed by thread; REQUEST
    paired by ReqID (GET) / preceding body (POST) / User (GET work); RESPONSE matched best-effort FIFO
    (oldest open request) — user-safe because responses carry no user. Entries re-sorted chronologically
    before `seq` assignment.
  - Verified: **0 mixed-user transactions** across all 6 multi-user files (3,423 txns, ≤11 concurrent
    users) and the live DB. Orphan entries 33→0. Trade-off: a few % more `incomplete` (requests whose
    response can't be confidently matched — honest, vs. the old blind next-response attachment).
- **User-aware Stage 2 grouping (thread+user) — DONE** (2026-06-11): hardened the thread-only grouper
  after two residual leaks surfaced on the full 28-file corpus (193,864 entries): (1) trailing `info`
  narration lines of the *next* request leaked into the previous transaction because the user-change
  split only fired on `mi_call` m3user, not on narration; (2) genuine **.NET async thread reuse** —
  a thread runs user A's call, is reused mid-await for user B's continuation, then resumes A — which
  thread-only keying mixed (seen as A→B→A on one thread).
  - Root signal: **every** log line carries the log4net context user in its header (`(CPRICE)`), incl.
    the async RESPONSE line (no payload user / no ReqID, but a header user). Migration `c9d3e8f02b15`
    adds `log_entries.user_ctx` (backfilled from `raw_body`); Stage 1 now persists it (already parsed
    as `LogRecord.user`, previously dropped).
  - `_group` re-keyed from `thread` → **`(thread, user)`**: a line *with* a user routes to its
    `(thread,user)` builder and marks that stream current on the thread; a line with **no** user
    (16–54% of internal lines log as `(null)`) **inherits** the thread's current stream. This
    de-interleaves async reuse (A→B→A re-merges A correctly, B gets its own txn) with no fragmentation.
    RESPONSE now closes the oldest open request **for the response's own `user_ctx`** (FIFO within user),
    so a response can never cross users either.
  - Verified on the live DB (11,012 txns): **cross-user (user_ctx) = 0, response-user mismatch = 0,
    multi-request = 0, multi-response = 0**; 9,606 txns have request-thread ≠ response-thread all
    correctly stitched. (One `m3user`-differs case is a *legitimate* single `/loading/Load` POST whose
    internal MI calls act on different data owners — not a mis-stitch: one body, one response, one thread.)
- **§6 renderer — DONE** (2026-06-10): `app/services/mnp_log_ingestion/render.py::render_transaction`
  turns a transaction + its ordered entries into the locked §6 text view (header · sub-header ·
  `▶ REQUEST` dims · numbered steps with `mi_call`+`mi_result` folded into one `📞 PROG/TXN inputs ✅ N recs`
  line, `⚙ SQL`, `⚠️ SOFT`, `❌ ERROR` · `◀ RESPONSE`). Default collapses plain INFO narration; `verbose=true`
  renders it. Wired to `GET /logs/transactions/{id}/view` (text/plain, `?verbose=`) and added as a
  `rendered` field on the JSON `GET /logs/transactions/{id}`. Verified on real success / error / soft txns.
- **Phase 3 — NEXT (optional): `log_flow`** table + backfill grouper to populate the nullable `flow_id`
  hook, rolling transactions up into business flows (Option 3).

---

## 11. Resume here (next session) ⏸️

**Where we paused (2026-06-09):** Phase 1a, Phase 1b, Step 4b, and Phase 2 are all DONE and verified.
The ingestion pipeline + Stage-2 grouping + read-only scan/dedup + the Claude debugging agent are all
built and wired. See `docs/LOG_ANALYSIS_GUIDE.md` for how to run the app and do log analysis.

**One open to-do (not code — config):**
- [ ] Add `ANTHROPIC_API_KEY=sk-ant-...` to `.env` to actually use `POST /logs/debug/ask`. Until then the
      endpoint returns 503. The agent's five SQL tools are already verified against the live DB; only the
      live LLM loop is unrun. (It must be an Anthropic **API key**, not a Claude.ai subscription login.)

**What's left to build — Phase 3 (optional, purely additive):** roll transactions up into business flows.
Tell Claude: *"Implement Phase 3 from `docs/transaction-log-ingestion-design.md` — the `log_flow` table
+ backfill grouper that populates the existing nullable `flow_id` hook on `log_transaction`."* Scope:
  1. New `log_flow` model + Alembic migration (a flow = an ordered set of related transactions — e.g. a
     full pick/pack/ship business operation spanning many API calls). The `flow_id` column + index
     already exist on `log_transaction` (added in Phase 1a as a no-FK hook) — Phase 3 just fills it.
  2. A flow grouper (mirror `derive_transactions.py`) that reads transactions in time order and links
     them into flows by a business key (delivery_number / order_number / picklist, + user + a time
     window). Make it re-runnable like Stage 2.
  3. API: `GET /logs/flows`, `GET /logs/flows/{id}` (flow header + its ordered transactions).
  4. Optionally a 6th agent tool `get_flow` so the debugging agent can reason at the flow level.
Nothing in Phase 3 requires schema churn on existing tables — the hook was designed for exactly this.

**Key files to know when resuming:**
- Ingestion/parsing: `app/services/mnp_log_ingestion/` (parser, `pipeline/parse_insert.py` Stage 1,
  `pipeline/derive_transactions.py` Stage 2 `regroup_all`).
- Models: `app/persistence/models/log_entry.py`, `log_transaction.py` (`flow_id` hook here).
- Agent: `app/services/log_agent/tools.py` (5 read-only tools), `agent.py` (`LogDebugAgent`).
- API: `app/api/v1/logs.py`. Settings: `app/settings.py`. Workers: `app/main.py` lifespan.
- Postman: `postman/RAG_FAST_API.postman_collection.json` (7 folders, 36 requests).
