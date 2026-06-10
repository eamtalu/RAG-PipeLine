# Log Analysis — How to Run It

A practical, copy-paste guide to start the app and analyze Infor M3 WMS server logs: ingest logs →
let them become queryable transactions → query them over REST → ask the Claude debugging agent in
plain English.

> For the full design/rationale see `transaction-log-ingestion-design.md`. This file is just the
> "how do I use it" runbook.

---

## 0. The mental model (10-second version)

```
 log file(s)  ──► Stage 1: parse ──►  log_entries     (raw, lossless, deduped)
                                          │
                                          ▼
                       Stage 2: group ──► log_transactions  (one row = one API request/response)
                                          │
                        ┌─────────────────┴───────────────────┐
                        ▼                                       ▼
              REST queries (filter/count)            Claude agent ("ask in English")
              GET /logs/transactions                 POST /logs/debug/ask
```

- **Stage 1** runs automatically when you ingest a file. It writes `log_entries` and never stores the
  same log line twice (content hash).
- **Stage 2** runs automatically in the background (a worker regroups whenever the entry count changes),
  and can be triggered manually with `POST /logs/regroup`. It builds `log_transactions`.
- A **transaction** = one API request→response cycle, with an ordered timeline of what happened inside
  (M3 MI calls, SQL, errors). Status: `success` / `soft` / `error` / `incomplete`.

---

## 1. Prerequisites (one-time)

1. **Postgres** (the repo ships a docker-compose with pgvector):
   ```bash
   docker compose up -d postgres
   ```
   This serves Postgres on `localhost:5432` (user/pass/db all `rag`).

2. **Python deps:**
   ```bash
   pip install -r requirements.txt
   ```

3. **`.env`** (in the repo root). The log feature needs the database URL and — only for the Claude
   agent — an Anthropic API key:
   ```ini
   DATABASE_URL=postgresql+asyncpg://rag:rag@localhost:5432/rag

   # Required ONLY to use the debugging agent (POST /logs/debug/ask).
   # Must be an Anthropic API key (sk-ant-...), NOT a Claude.ai login.
   ANTHROPIC_API_KEY=sk-ant-...
   ```
   > Ingestion, grouping, and all REST queries work **without** an Anthropic key. The key is only
   > needed for the natural-language agent endpoint.

4. **Run database migrations** (creates the `log_entries` / `log_transactions` tables):
   ```bash
   PYTHONPATH=. alembic upgrade head
   ```
   (The `PYTHONPATH=.` prefix is required or alembic can't import `app`.)

---

## 2. Start the app

```bash
uvicorn main:app --reload
```

On startup it launches three background workers (you'll see them in the log):
- **embedding worker** — for the document/RAG side (unrelated to logs).
- **log watcher** — drains the staging dir `./logs/incoming` (move-based; only touches that dir).
- **log grouping worker** — runs Stage 2 automatically whenever the entry count changes.

App is now at `http://localhost:8000`. Health check: `GET http://localhost:8000/health`.

---

## 3. Ingest logs

You have **two** ways to get logs in. Both are safe to re-run — content-level dedup means already-seen
lines are skipped, so you never get duplicates.

### Option A — Scan a directory in place (recommended for live rotating logs) ✅

Reads every file in a directory **read-only** — never moves or deletes anything. This is how you point
at your real M3 logs.

```bash
curl -X POST "http://localhost:8000/api/v1/logs/scan?directory=/Users/amintalukder/myworkspace/personal/python work/mnp-logs"
```

Response tells you how many genuinely-new entries were ingested per file. Re-run anytime — a second scan
of the same (unchanged) folder adds 0; if the active log grew, only the new tail lines are added.

> ⚠️ The scan endpoint is the **only** safe way to ingest the live `mnp-logs` directory. Never point the
> move-based watcher (`./logs/incoming`) at your real logs — it would relocate them.

### Option B — Upload a single file

```bash
curl -X POST "http://localhost:8000/api/v1/logs/ingest" -F "file=@/path/to/eSmartServerLog.txt"
```

### Then let Stage 2 build transactions

The grouping worker picks up new entries automatically within a few seconds. To force it now:

```bash
curl -X POST "http://localhost:8000/api/v1/logs/regroup"
```

Response is the grouping stats: `{entries_scanned, transactions_created, orphan_entries, by_status}`.

---

## 4. Analyze — REST queries (no API key needed)

### Count / aggregate ("how many…")
`GET /logs/transactions` returns a `total` plus matching rows. Filter by any promoted dimension:

```bash
# How many transactions did a user run on a date?
curl "http://localhost:8000/api/v1/logs/transactions?user=SGIAMPORCA&date=2026-06-09"
# -> {"total": N, ...}

# Just the errors in a warehouse
curl "http://localhost:8000/api/v1/logs/transactions?status=error&warehouse=BRI"
```
Filters: `user`, `date`, `status`, `method`, `transaction_name`, `transaction_type`, `warehouse`,
`delivery_number`, `item_number`, `order_number`, `reqid`, `time_from`, `time_to`, `limit`, `offset`.

### Drill into one transaction (the canonical detail view)
```bash
curl "http://localhost:8000/api/v1/logs/transactions/<transaction_id>"
```
Returns the header (who/where/what/outcome) + the ordered step-by-step timeline (REQUEST → M3 MI calls
and their results → SQL → errors → RESPONSE).

### Line-level search
```bash
curl "http://localhost:8000/api/v1/logs/entries?q=Index%20was%20out%20of%20range&level=ERROR"
```

---

## 5. Analyze — ask the Claude agent in plain English (needs `ANTHROPIC_API_KEY`)

`POST /logs/debug/ask` — the agent picks the right read-only SQL tools, runs them, and answers with
**cited transaction ids**.

```bash
curl -X POST "http://localhost:8000/api/v1/logs/debug/ask" \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the most recent errors and what caused them?"}'
```

Response:
```json
{
  "answer": "…explanation citing transaction ids…",
  "stop_reason": "end_turn",
  "tool_calls": [ {"tool": "find_errors", "input": {...}}, {"tool": "get_transaction", "input": {...}} ],
  "iterations": 2
}
```

**Good questions to ask:**
- "How many transactions did user SGIAMPORCA run on 2026-06-09, and how many errored?"
- "Explain transaction `<id>` step by step and whether it succeeded."
- "Is there a transaction failing with 'Index was out of range'? Which API and user?"
- "Did any pick confirmations fail for delivery 15936? Why?"

> If you get **503 "anthropic_api_key is not configured"**, set `ANTHROPIC_API_KEY` in `.env` and restart.

---

## 6. Use Postman instead of curl

Import `postman/RAG_FAST_API.postman_collection.json`. Folders relevant to log analysis:
- **Logs - Ingestion** — upload / scan a directory.
- **Logs - Query** — line-level entry queries + job status.
- **Logs - Transactions (Stage 2)** — regroup, list/filter transactions, get one detail view.
- **Logs - Agent (Phase 2)** — natural-language questions to the debugging agent.

Set the collection variable `baseUrl` (default `http://localhost:8000`). `mnpLogsDir` is pre-filled with
your real logs path for the scan request. After listing transactions, copy an id into the `txnId`
variable to use the "explain a specific transaction" requests.

---

## 7. Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `log_entries` has rows but `log_transactions` is empty | Stage 2 hasn't run yet. Wait a few seconds for the grouping worker, or `POST /logs/regroup`. |
| Scan reports `inserted_new: 0` on re-run | Correct — dedup working. Nothing new in those files. |
| `alembic` → `ModuleNotFoundError: No module named 'app'` | Prefix the command with `PYTHONPATH=.` |
| `/logs/debug/ask` → 503 | `ANTHROPIC_API_KEY` not set in `.env`. Add it and restart. |
| Agent answer seems thin | It only uses what's in the DB. Make sure you've scanned + regrouped the relevant logs first. |
| A transaction shows `incomplete` | Its REQUEST was seen but the RESPONSE wasn't ingested yet (e.g. split across a not-yet-scanned rotated file). Scan the rest, then regroup. |

---

## 8. Typical end-to-end session (cheat sheet)

```bash
# 1. infra + app
docker compose up -d postgres
PYTHONPATH=. alembic upgrade head
uvicorn main:app --reload

# 2. ingest your real logs (read-only) and build transactions
curl -X POST "http://localhost:8000/api/v1/logs/scan?directory=/Users/amintalukder/myworkspace/personal/python work/mnp-logs"
curl -X POST "http://localhost:8000/api/v1/logs/regroup"

# 3a. analyze via REST
curl "http://localhost:8000/api/v1/logs/transactions?status=error&limit=10"

# 3b. or just ask (needs ANTHROPIC_API_KEY)
curl -X POST "http://localhost:8000/api/v1/logs/debug/ask" \
  -H "Content-Type: application/json" \
  -d '{"question": "Summarize today’s failures and the most affected user."}'
```
