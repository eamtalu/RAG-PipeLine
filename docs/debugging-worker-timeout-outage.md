# Debugging the `WORKER TIMEOUT` Outage: A Step-by-Step Walkthrough

A first-principles postmortem of why the log-explorer site (`https://192.168.0.142`) was intermittently "not responding".
It is written to be readable months from now, starting from zero knowledge of the box and working one layer at a time, from the outside in.

The short version of the conclusion: the site was not down.
A single backend endpoint (`GET /api/v1/logs/transactions/view`) loaded an unbounded set of log rows and rendered them synchronously, which blocked the async event loop past gunicorn's 30-second worker timeout, so gunicorn kept killing workers and in-flight requests failed with `ECONNRESET`.

---

## How to run these commands yourself

The Mac's SSH config for this host is broken (it points at a Windows path that does not exist), so always add two flags to bypass it:

```bash
ssh -o RemoteCommand=none -o RequestTTY=no amin@192.168.0.142
```

Once you are in, you are on the Ubuntu server and can run everything below directly.

---

## The mental model first (what this system even is)

When you open `https://192.168.0.142` in a browser, the request passes through a chain of programs.
Each one hands off to the next:

```text
Your browser
   |  HTTPS (port 443)
   v
nginx            <- "reverse proxy": the front door, does HTTPS, forwards traffic
   |  plain HTTP (port 3000)
   v
Next.js          <- the frontend app ("matrix / log explorer"), run by pm2
   |  HTTP (port 8000)   <- it calls the backend for data
   v
FastAPI (gunicorn + uvicorn)   <- the Python backend API, run by systemd
   |
   v
PostgreSQL       <- the database (log_entries, log_transactions, ...)

(separately)  a background worker  <- SSH-polls the Windows log server, ingests logs
```

Key terms:

- **Port**: a numbered "door" on a machine. Web = 443 (HTTPS) / 80 (HTTP). This app uses 3000 (Next.js) and 8000 (FastAPI) internally.
- **Reverse proxy (nginx)**: takes the public HTTPS request and forwards it to an internal app. Handles the certificate.
- **Process manager**: keeps a program running and restarts it. Here `pm2` runs the frontend and `systemd` runs the backend.
- **gunicorn / uvicorn / workers**: gunicorn is a supervisor that runs several copies ("workers") of the Python app so it can handle many requests at once. Here it runs 4 workers. uvicorn is the piece that actually speaks HTTP inside each worker.

The whole investigation is one question: which link in this chain is broken?
You test them one at a time, from the outside in.

---

## Step 1 - Is the machine even reachable? (network layer)

**Concept.** Before blaming the app, prove the box is alive and the network path works.
Can I reach it at all, and are the doors (ports) open?

```bash
# From your Mac:
ping -c 3 192.168.0.142                 # is the machine up + reachable?
nc -z -v 192.168.0.142 22               # is the SSH door open?
nc -z -v 192.168.0.142 80               # HTTP door?
nc -z -v 192.168.0.142 443              # HTTPS door?
```

- `ping` sends tiny "are you there?" packets. Replies mean the box is up.
- `nc -z` just knocks on a port without sending data. "succeeded" / "open" means something is listening.

**What I saw.** ping replied, and 22 / 80 / 443 were all open.

**Conclusion.** This is not a network, firewall, or box-down problem.
The door to 443 is open, so "not responding" is happening deeper, inside the app.
This is the single most important early result: it steered the whole investigation away from "server down" and toward "app misbehaving".

---

## Step 2 - Is HTTPS / TLS the problem? (encryption layer)

**Concept.** Port 443 being open only means a TCP connection works.
HTTPS also needs a successful TLS handshake (the encryption negotiation using a certificate).
A broken or expired cert would look like "not responding" in a browser.

```bash
# From your Mac. -v = verbose (show the handshake), -k = don't reject a self-signed cert
curl -vk --max-time 15 https://192.168.0.142/ | head -40
```

Look for these lines:

- `TLS handshake ... Finished` means encryption succeeded.
- `Server certificate: subject: CN=192.168.0.142 ... expire date: ...` shows the cert and its validity.
- `< HTTP/1.1 200 OK` and `< Server: nginx/1.24.0 (Ubuntu)` mean the server did answer.

**What I saw.** The TLS handshake succeeded (self-signed cert, valid until 2036), and the server returned `200 OK` from nginx.
But the response took 4 to 14 seconds to arrive (watch curl's progress spinner).

**Conclusion.** TLS is fine, nginx is fine, and the server does respond `200`, just slowly and intermittently.
So "not responding" really means "the page loads but is broken or too slow".

---

## Step 3 - What is the app actually sending back?

**Concept.** A `200 OK` can still be a broken page.
Read the HTML body and time it precisely.

```bash
# Precise timing breakdown:
curl -k -s -o /dev/null \
  -w 'connect=%{time_connect}s tls=%{time_appconnect}s ttfb=%{time_starttransfer}s total=%{time_total}s http=%{http_code}\n' \
  https://192.168.0.142/

# See the actual HTML:
curl -ks https://192.168.0.142/ | head -c 2000
```

- **TTFB** ("time to first byte") is how long until the server starts answering. If `connect` and `tls` are tiny but `ttfb` is huge, the app is slow, not the network.

**What I saw.** `connect` / `tls` were about 0.06s but `ttfb` was about 4s. The HTML body contained:

```html
<div class="ls-gate"><div class="ls-loading">resolving logspace...</div></div>
```

**Concept - "client-side gate".** The frontend sends a skeleton page that says "resolving logspace...", then JavaScript in the browser calls the backend API to load data and replace that message.
If those API calls fail, the page is stuck on "resolving logspace..." forever, which is exactly the "not responding" symptom.

**Conclusion.** The problem is the API calls the frontend makes after the page loads.
Time to get on the box.

---

## Step 4 - Map the stack on the server (what is running where)

**Concept.** Find every program in the chain and which port each listens on.

```bash
# On the box:
pm2 list                 # the frontend process(es) pm2 manages
ss -tlnp                 # every listening TCP port + which program owns it
```

- `ss -tlnp`: `-t` TCP, `-l` listening, `-n` numbers not names, `-p` show the program.

**What I saw.**

- `pm2 list` showed one app `matrix-log-explorer`, status `online`, but with 22 restarts (a warning sign).
- `ss -tlnp` showed `:443` and `:80` = nginx, `:3000` = `next-server` (frontend), `:8000` = `gunicorn` (the FastAPI backend, 4 worker processes).

**Conclusion.** The chain is confirmed. The frontend (3000) calls the backend (8000).
Next question: is the frontend or the backend the problem?

---

## Step 5 - Read the frontend's logs (first real clue)

**Concept.** A program's logs tell you what it is complaining about. pm2 keeps the frontend's logs.

```bash
# On the box:
pm2 logs matrix-log-explorer --nostream --lines 50
```

`--nostream` prints recent lines and exits (does not follow live).

**What I saw.** The error log was full of:

```text
Failed to proxy http://192.168.0.142:8000/api/v1/customers/timezones  Error: socket hang up
    ... code: 'ECONNRESET'
```

**Concept - `ECONNRESET` / "socket hang up".** The frontend opened a connection to the backend (`:8000`) to fetch data, but the backend slammed the connection shut mid-reply.
That is not "backend is slow", it is "backend died while answering".

**Conclusion.** The frontend is fine; it is the victim.
The backend (FastAPI on `:8000`) is dying mid-request, which is why "resolving logspace..." never finishes.

Quick confirmation that the frontend is healthy and the backend is the troubled one:

```bash
# On the box - hit each directly, bypassing nginx:
curl -s -o /dev/null -w 'next:3000 ttfb=%{time_starttransfer}s http=%{http_code}\n' http://127.0.0.1:3000/
curl -s -o /dev/null -w 'fastapi:8000 ttfb=%{time_starttransfer}s http=%{http_code}\n' http://127.0.0.1:8000/
```

Next.js answered in about 50ms; the backend was the troubled one.

---

## Step 6 - The smoking gun: the backend's system log

**Concept.** The backend is run by systemd (Linux's service manager). Its logs live in the journal.
`journalctl -u <service>` shows them.

```bash
# On the box - find the service name:
systemctl list-units --type=service | grep -iE "fastapi|gunicorn|rag"
#   -> fastapirag.service (the web API) and fastapirag-worker.service (background worker)

# Read the API's recent log:
journalctl -u fastapirag --since "1 hour ago" --no-pager | grep -E "WORKER TIMEOUT|SIGABRT|SIGKILL|Booting"
```

**What I saw.**

```text
[CRITICAL] WORKER TIMEOUT (pid:1806214)
[WARNING] Worker (pid:1806214) was sent SIGABRT!
...
[ERROR] Worker (pid:1806197) was sent SIGKILL! Perhaps out of memory?
```

**Concept - what "WORKER TIMEOUT" means (the heart of it).**

- gunicorn (the supervisor) expects each worker to check in periodically. This is the heartbeat.
- If a worker does not check in for 30 seconds (the default), gunicorn assumes it is hung and kills it (`SIGABRT` = "abort"; if that does not work, `SIGKILL` = "die now").
- While a worker is dead and a replacement is booting, it cannot answer requests, so the connections it was handling get `ECONNRESET`, which matches Step 5 exactly.

`SIGKILL! Perhaps out of memory?` is just gunicorn's guess when it has to force-kill.
It does not prove the machine ran out of memory (we disprove that next).

Count how often this happens:

```bash
journalctl -u fastapirag --since "7 days ago" --no-pager | grep -c "WORKER TIMEOUT"
```

**What I saw.** 18 kills in 7 days, in bursts.

**Conclusion.** The site "goes down" whenever gunicorn kills its workers for missing the 30-second heartbeat.
The real question is now: why does a worker stop checking in for 30 seconds?

---

## Step 7 - Rule out the "obvious" causes (so we don't chase ghosts)

**Concept.** Before theorizing, eliminate the boring explanations: out of memory, disk full, CPU maxed.

```bash
# On the box:
uptime                 # "load average" - roughly how many CPUs' worth of work is queued
nproc                  # how many CPU cores
free -h                # memory: look at "available"
df -h /                # disk space
```

**What I saw.** 8 cores, load about 3 (plenty of headroom), 43 GB RAM free, disk 13% used.

**Conclusion.** Not memory, not disk, not CPU starvation.
So `Perhaps out of memory?` was a red herring: the machine had lots of free RAM.
The worker is not dying from lack of resources; it is dying because it is stuck.

I also checked the database for stuck connections (a common cause of hangs):

```bash
# On the box - password is in the app's .env:
cd /opt/RAG-Pipeline/RAG-PipeLine
PW=$(grep -oP 'postgresql\+asyncpg://rag:\K[^@]+' .env); export PGPASSWORD="$PW"
psql -h localhost -U rag -d rag -c "select state, count(*) from pg_stat_activity where datname='rag' group by state;"
```

This showed a few `idle in transaction` connections (one about 13 days old).
It looked alarming, but I traced it to the code (`app/worker.py`) and it is a deliberate singleton lock that the background worker holds on purpose, not a leak.
Lesson: verify a scary-looking symptom against the code before blaming it.

---

## Step 8 - The key insight: why does a worker stop checking in? (async event loop)

This is the concept everything hinges on.

**Concept - the event loop.** This backend is async (Python asyncio).
Each worker runs a single event loop, which juggles many requests by switching between them whenever one is waiting.
The magic word is `await`:

- When code does `await database_query(...)`, it means "I am waiting on the database, go do other work meanwhile". The loop is free, and during that free time the worker also sends its heartbeat to gunicorn. So a slow database query does NOT stop the heartbeat.
- But if code does heavy synchronous work (a big loop, building a giant string, processing 100k objects) with no `await`, the loop is stuck on that one thing. It cannot switch tasks and it cannot send the heartbeat. After 30 seconds, gunicorn kills it.

So the hypothesis became: a worker is doing heavy synchronous (non-`await`) work for more than 30 seconds.
I needed to prove that slow DB is NOT the cause and that heavy synchronous work IS.

**The experiment - force a slow DB query and see if the worker survives.**

```bash
# On the box. This asks for row 4,000,000 of a 4-million-row table = a deliberately slow query.
# Terminal 1 - watch the service log:
journalctl -u fastapirag -f | grep "WORKER TIMEOUT"
# Terminal 2 - fire the slow request:
curl -s -o /dev/null -w 'http=%{http_code} total=%{time_total}s\n' --max-time 85 \
  -H "X-Customer-Code: tmp-live" \
  "http://127.0.0.1:8000/api/v1/logs/entries?limit=1&offset=4000000"
```

**What I saw.** The request ran a full 85 seconds and no `WORKER TIMEOUT` appeared.
The worker survived a request that took nearly 3 times the timeout.

**Conclusion (the big one).** A slow awaited database query does NOT kill a worker.
So the outages are not caused by slow queries.
The killer must be synchronous, CPU or processing work that blocks the event loop.
Now I just had to find which endpoint does that.

---

## Step 9 - Find and reproduce the killer

**Concept.** Look in the code for handlers that fetch a lot of data and then process or format it synchronously (no `await` during the heavy part).

```bash
# On the box - list the API endpoints and hunt for unbounded fetches:
cd /opt/RAG-Pipeline/RAG-PipeLine
grep -rnE "@router\.(get|post)" app/api/v1/logs.py
grep -rn ".scalars().all()" app/api/v1/       # ".all()" = load EVERYTHING - a red flag when unbounded
```

**What I found.** `GET /api/v1/logs/transactions/view` (in `app/api/v1/logs.py`, around line 521):

1. loads up to 500 transactions,
2. then loads every log entry for all of them with no limit (`WHERE transaction_id IN (...)` -> `.scalars().all()`),
3. then builds one giant text document from them, all synchronous.

On the biggest logspace (`tmp-live` = 4 million log entries), that is a huge amount of Python work with no `await` in the middle.
Perfect candidate.

**Reproduce it.**

```bash
# Terminal 1 - watch the log:
journalctl -u fastapirag -f | grep -E "WORKER TIMEOUT|SIGKILL"
# Terminal 2 - fire the heavy view:
curl -s -o /dev/null -w 'http=%{http_code} total=%{time_total}s\n' --max-time 65 \
  -H "X-Customer-Code: tmp-live" \
  "http://127.0.0.1:8000/api/v1/logs/transactions/view?limit=500"
```

**What I saw.** Within about 50 seconds: `WORKER TIMEOUT` -> `SIGKILL! Perhaps out of memory?`, and curl got `http=000` (connection killed).
This is the exact production signature, reproduced on demand (I ran it twice to be sure).

---

## Step 10 - Prove it is the Python, not the database

**Concept.** The request took about 50 seconds.
Was that 50 seconds waiting on the DB (which we said is harmless), or 50 seconds of synchronous Python (the killer)?
To tell them apart, watch the database's currently-running queries every half-second during the request.

```bash
# On the box - repeatedly check the longest active DB query:
PW=$(grep -oP 'postgresql\+asyncpg://rag:\K[^@]+' .env); export PGPASSWORD="$PW"
watch -n1 "psql -h localhost -U rag -d rag -tAc \"select coalesce(round(max(extract(epoch from now()-query_start))),0) as max_query_seconds from pg_stat_activity where datname='rag' and state='active'\""
# then fire the transactions/view request again in another terminal
```

**What I saw.** During the whole 50-second request, the longest active database query never exceeded about 4 seconds.
So the database was not busy for 50 seconds.
The remaining 46 seconds were spent inside the Python worker formatting the data, which is exactly the synchronous, event-loop-blocking work that stops the heartbeat.

**Conclusion - final, proven root cause.**

> The frontend's core feature (viewing transaction logs) calls `GET /api/v1/logs/transactions/view`, which loads an unbounded number of log entries and renders them into a big text blob synchronously.
> On a large logspace this blocks a worker's event loop for 40 or more seconds.
> gunicorn's default 30-second worker timeout then kills the worker (`WORKER TIMEOUT` -> `SIGKILL`).
> While the worker is dead or rebooting (about 30 to 50 seconds), API calls fail with `ECONNRESET` / 500, so the frontend hangs forever on "resolving logspace...", which is what you experience as "not responding".
> It is intermittent because it only happens when a heavy view is requested; the rest of the time the API answers in about 70ms.

Everything else (slow DB, out of memory, the 13-day database connection, CPU starvation) was tested and ruled out.

---

## The fix (for reference)

The durable fix is in the application code, not in the infrastructure:

- **Bound the entry loads** in `transactions/view` and the transaction-detail view. Cap or paginate the `WHERE transaction_id IN (...)` `.scalars().all()` so it can never materialize hundreds of thousands of rows.
- **Move the synchronous rendering off the event loop** with `await loop.run_in_executor(None, render_fn, ...)` (or make the handler a plain `def` so Starlette runs it in a threadpool). This keeps the loop free to send its heartbeat even during heavy work.
- **Guardrails** (defense in depth, not the root fix): add a gunicorn `--timeout 120 --graceful-timeout 30` and a Postgres `statement_timeout` so a single slow operation degrades gracefully instead of killing the worker.

---

## One-page command cheat-sheet

```bash
# get in (bypass the broken ssh config):
ssh -o RemoteCommand=none -o RequestTTY=no amin@192.168.0.142

# 1. reachable? ports open?           (from Mac)
ping -c3 192.168.0.142 ; nc -zv 192.168.0.142 443
# 2. TLS + HTTP OK? how slow?         (from Mac)
curl -vk https://192.168.0.142/ | head
# 4. what's running / listening?
pm2 list ; ss -tlnp
# 5. frontend errors?
pm2 logs matrix-log-explorer --nostream --lines 50
# 6. THE smoking gun - worker kills:
journalctl -u fastapirag --since "2 days ago" | grep -E "WORKER TIMEOUT|SIGKILL"
# 7. rule out resources:
uptime ; nproc ; free -h ; df -h /
# 8/10. watch live during a slow page load:
journalctl -u fastapirag -f | grep "WORKER TIMEOUT"

# processes = the workers (each is a PID):
pgrep -P <master_pid> ; ps -o pid,rss,cmd --ppid <master_pid>
# threads INSIDE one worker (loop thread + pool threads):
ps -T -p <worker_pid>          # or:  top -H -p <worker_pid>
```

---

## The one-line takeaway

`WORKER TIMEOUT` on a uvicorn/gunicorn worker almost always means synchronous work blocked the event loop, not that the database was slow.
Prove it by checking whether the time was spent in an active DB query (harmless `await`) or inside Python (the real culprit), then bound the data and push blocking work off the loop.

---

## Fix action plan (where to fix, and in what order)

### Where does the fix live?

The root cause is in the **backend code**, not the operating system.
The OS (Ubuntu) is healthy: 8 cores, 43 GB free RAM, low load.
The `Perhaps out of memory?` message was a red herring.
The OS-level items below are service and deployment configuration plus guardrails (systemd unit, gunicorn flags, Postgres and `.env` settings); they are defense in depth, not the actual bug.

### 1. Backend code - the real fix (repo: `app/api/v1/logs.py`)

- **Bound the entry load in `transactions/view`** (around line 521). The `WHERE transaction_id IN (...)` -> `.scalars().all()` currently loads every entry for up to 500 transactions with no cap. Add a hard limit or pagination so it can never pull hundreds of thousands of rows.
- **Do the same for the transaction-detail view** (`/transactions/{id}/view`). It loads all entries of one transaction unbounded.
- **Move the synchronous rendering off the event loop.** Wrap `render_transaction(...)` plus the big string-build in `await loop.run_in_executor(None, ...)`, or make the handler a plain `def` so Starlette runs it in a threadpool. This keeps the loop free to send its heartbeat even during heavy work.
- **Audit the other unbounded `.scalars().all()`** (logs.py lines around 558, 579, 598; `saved_views.py:342`) for the same pattern.

### 2. Service config - guardrails (OS-level, on the server)

- **`/etc/systemd/system/fastapirag.service`**:
    - Add gunicorn flags: `--timeout 120 --graceful-timeout 30 --max-requests 1000 --max-requests-jitter 100`.
    - Add `Environment=RUN_BACKGROUND_WORKERS=false` under `[Service]`.
    - Clean the corrupted lines 14-17 (pasted shell commands that systemd is ignoring).
    - Then run: `sudo systemctl daemon-reload && sudo systemctl restart fastapirag`.
- **`.env`** (`/opt/RAG-Pipeline/RAG-PipeLine/.env`): set `db_statement_timeout_ms=30000` (the safety net is already wired into `app/config/database.py`).
  - NOTE (added 2026-07-24): this 30 s cap is a **web-tier** guardrail. The log-ingestion transaction now **relaxes** it (`SET LOCAL statement_timeout = 0` in `parse_insert.py`) because index-heavy `log_entries` inserts on the failing/slow production disk legitimately exceed 30 s and were being cancelled (`QueryCanceledError`). See `docs/disk-io-resilience.html`.
- **Optional**: enable gunicorn access logging (`--access-logfile`) so a future blocking request is visible in the logs.

### 3. Immediate recovery (do first, restores service now)

- `sudo systemctl restart fastapirag` and `sudo systemctl restart fastapirag-worker` to clear any currently-wedged workers.

### Execution order

1. **Restart** the services for instant relief.
2. **Apply the code fix** (section 1); this is the actual cure. Deploy it.
3. **Apply the config guardrails** (section 2) so a future heavy request degrades gracefully instead of killing workers.

Priority: section 1 is essential.
Without it, more workers just means more workers to kill.
Sections 2 and 3 are protective.

---

## What was actually shipped (2026-07-22)

Investigation went beyond the original render bug and found the latency was mostly a missing index.
Three changes were deployed to `main` + the server:

1. **Composite index `ix_log_transactions_customer_started` on `log_transactions (customer_code, started_at DESC NULLS LAST)`** - migration `b9d4f2a7c318`. This was the real latency fix: the transaction list + text feed run `WHERE customer_code=? ORDER BY started_at DESC LIMIT n`, which previously did a ~143k-row scan + ~60k-row sort on tmp-live (223k rows). After the index: `EXPLAIN` shows an index scan (no sort), and `GET /transactions/view?limit=100` went from seconds/at-risk to ~0.7s, `limit=500` from ~46s (+worker kill) to ~3.4s.
2. **`app/worker.py`: commit after acquiring the singleton advisory lock** (`4af69f7`). The lock connection used to sit `idle in transaction` for the whole process life (observed 13 days), pinning the DB-wide vacuum horizon and blocking `CREATE INDEX CONCURRENTLY`. The lock is session-scoped so the commit is safe. See "Deferred" note below on why this matters for future online DDL.
3. **`app/api/v1/logs.py`: bound + `asyncio.to_thread` for the text-view renders** (`7984eb3`) - the availability guard (Axis A). `MAX_RENDER_ENTRIES = 50_000` is a runaway guard (not a normal limit; the max feed is ~14.5k). Kept because the index does not shrink a pathological render.

## Deferred / future optimizations (NOT done - pick up here if latency or load regresses)

- **`GET /api/v1/logs/transactions` (plural JSON list) computes `total = count(*)`** with the same filters on every call - a full scan of all matching rows (~143k on tmp-live), so that endpoint is still ~3-5s even with the index. It was left alone because **the frontend (matrix-log-explorer) does not call it** - the feed uses `/transactions/view`, and detail uses `/transactions/{id}[/view]`. If a client (API consumer, the debug agent, a future UI) starts using the JSON list and needs it fast: make `total` opt-in (`?with_total=false`) or use an estimate (`reltuples` / a capped count), since the `LIMIT n` slice itself is already fast via the composite index.
- **Per-filter composite indexes for hot saved views**, e.g. `(customer_code, status, started_at DESC)` or `(customer_code, user_name, started_at DESC)` - only if profiling shows a specific saved-view filter+sort is slow. The base `(customer_code, started_at DESC)` index covers the unfiltered/most-recent case.
- **Background-worker catch-up throttling (the second availability trigger).** After any downtime, `fastapirag-worker` floods into Stage-2 catch-up stitching that saturates Postgres CPU (observed load 5-7), which starves the web tier's heartbeats and briefly degrades even `/health`. The composite index makes catch-up cheaper, but if this still bites, throttle the worker's regroup batch cadence or `nice`/`ionice` the worker process (`fastapirag-worker.service`). Immediately after a deploy/restart, expect a few minutes of elevated latency until catch-up drains.
- **Config guardrails from section 2 above** (gunicorn `--timeout`, `db_statement_timeout_ms`, systemd unit cleanup) - still worth doing as defense in depth; not yet applied.
