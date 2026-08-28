# Warehouse analytics and ML platform: final architecture

**This is the single canonical document for this work.**
Everything else on this subject is superseded and should not be implemented from.

Last revised 2026-08-27.
The document has two parts:

- **PART I - The system as built.**
  A plain-English, fact-checked guide to how the software works today, from the SSH pull to the ML pipeline.
  Start here.
- **PART II - The design history.**
  The original architecture, the iteration-2 plan, and every as-built record and incident (sections 1 to 18v), preserved verbatim because code comments and tests cite these section numbers.

# PART I. The system as built: a plain-English guide

Written 2026-08-27, verified against the code and the running server on the same day.
This part is the source of truth for how the software works TODAY.
Part II below it is the design history: why each decision was made, in the order it was made, with its section numbers preserved because code comments and tests cite them.
If Part I and Part II disagree, Part I wins; if either disagrees with the code, the code wins and this document must be fixed.

Every claim here was checked in the source files named beside it.
Nothing is recalled from memory.

## 1. What this system is

Warehouse scanner devices talk to an M3 WMS server, and those servers write plain text log files.
This system turns those log files into three products:

1. A searchable, replayable record of every conversation between a device and the server (a **transaction**).
2. Live analytics: how many units were picked, counted or moved, per hour, day and month, sliced by item, user, warehouse or any approved field, on a dashboard that updates within about a minute of the physical scan.
3. Reproducible machine-learning training sets, buildable months later and provably identical.

The hard part is that log files are an unreliable narrator.
One business action spans many lines; a response can arrive seconds after its request; files rotate and get re-read; two app servers interleave the same user's work; lines arrive late or out of order.
Most of the design below exists to turn that mess into numbers that can be trusted, and to make every failure loud instead of silent.

## 2. The big picture

Five pipelines, connected by durable queues (never by memory), each with its own background worker:

```
 WMS app servers (TMP-AZ-BEC01, TMP-AZ-BEC02, ...) write log files
      |
      |  1. COLLECTION - pull the files            worker: ssh_log_fetcher (every ~60s/tenant)
      v
 [log_source_objects]  one downloaded byte range each, with a retry budget
      |
      |  2. STAGE 1 - parse lines into rows        worker: log_parse_worker (every 2s)
      v
 log_entries  (append-only; one row per log entry; deduped; partitioned daily)
      |
      |  ticket: [log_regroup_pending]  "this time range has new data"
      |
      |  3. STAGE 2 - stitch lines into transactions   worker: log_stitch_worker (every 1s)
      v
 log_transactions + log_entry_assignment  (the derived, updatable projection)
      |
      |  ticket: [analytics_pending_windows]  "transactions in this range changed"
      |
      |  4. ANALYTICS - fold transactions into facts   worker: analytics_worker (every 2s)
      v
 analytics_facts (current) + analytics_fact_ledger (every version, forever)
      |                       |
      v                       v
 rollups (hour/day/month)   5. ML - training sets pinned to an instant of the ledger
      |
      v
 FastAPI /api/v1/...  ->  the dashboard (a separate Next.js app, polls /status every 2s per tab)
```

Two rules hold everywhere:

- **Every hop is a database queue.**
  A ticket is written in the same database transaction as the data it describes, so a crash can never lose the "there is work to do" note.
  Failed work retries with backoff and, after a cap, parks in a visible dead-letter state with its error message.
  Nothing is ever silently dropped.
- **Every derived layer can be rebuilt from the layer above it.**
  Entries rebuild transactions; transactions rebuild facts; facts rebuild rollups.
  Rebuilding is idempotent: doing it twice writes nothing the second time.

## 3. Collection: getting the files

Components: `app/services/workers/ssh_log_fetcher.py` (the supervisor), `app/services/mnp_log_ingestion/remote/remote_fetcher.py` (all the logic).

Each tenant configures one row per WMS server in `log_ssh_sources`: host, credentials, a directory and a filename pattern.
A supervisor keeps one polling loop per tenant (re-checked every 30 s), each pulling over SFTP roughly every 60 s.

**How it avoids re-downloading**: `log_ssh_file_checkpoints` remembers, per file, how many bytes were already read (`last_offset`), plus the file's size, modified time and a fingerprint of its first 4 KB.
On each poll a small decision table (`_plan_incremental`, remote_fetcher.py:354) chooses one of: unchanged (skip), append (read only the new bytes), rotated-and-already-consumed (skip - the same content was read under its old name), rotated-or-truncated (re-read from zero; Stage 1's dedupe drops the overlap), or new file.

**The safety property**: the checkpoint is only an optimisation.
If it is ever wrong the worst case is a re-download, because true dedupe happens in Stage 1.
The downloaded byte range and its checkpoint advance are committed in ONE transaction, so there is no window where bytes are skipped forever or fetched twice.

Each downloaded range becomes a row in `log_source_objects`: the durable handoff to Stage 1, with a lease, an attempt counter, exponential backoff (30/60/120 s), and an `abandoned` dead-letter state after 3 tries (re-armable via `POST /logs/ingest-queue/reset-abandoned`).

### 3.1 Turning auto-poll on: the three start points

Enabling auto-poll for a server asks one question: where should history begin?
The three choices map to three fetch modes (frontend `SshSourcesPanel.tsx:355-385`, backend `remote_fetcher.py`):

```
 from now (recommended)   -> one "seed" fetch: stamps every current file as
                             already-read, ingests NOTHING, then enables.
                             Polling continues with only NEW bytes.

 from a specific date     -> one "timestamp" fetch from the chosen instant:
 & hour                      reads qualifying files, parses, dedupes, tickets;
                             auto-poll turns on ONLY if that backfill completes.

 all existing history     -> enables immediately; the first poll reads every
                             file from byte zero - the full backfill.
```

**A backfill cannot conflict with a rebuild**, verified mechanism by mechanism:
fetching locks per HOST while stitching locks per TENANT (disjoint namespaces, nothing can deadlock);
during a running rebuild the maintenance flag pauses only stitching and analytics, so the backfill's downloads and parsing continue and their tickets simply queue until the rebuild finishes;
and duplicates are impossible by construction - lines dedupe on their content hash, transactions on their fingerprints, facts through the diff - so backfill and rebuild converge to the identical result in either order.
A backfill also never NEEDS a rebuild afterwards: tickets position the stitching windows at the data's own timestamps (section 11.4).

**Three sharp edges of "from a specific date & hour"** (all in `remote_fetcher.py`):

1. It silently does nothing if the chosen date is not older than the tenant's oldest existing entry - the arming fetch short-circuits as `already_local` (`:648-652`) and just switches polling on.
   Forcing a re-pull over an existing range is the manual Fetch with mode `full`.
2. Files are selected by their remote last-modified time, and a file modified BEFORE the chosen instant is stamped fully-read without ingesting (`:449-452`) - permanently, unless a later full fetch re-pulls it.
3. Backdating beyond the oldest existing day-partition drops the rows into the DEFAULT partition: readable, but retention can never reclaim them and the partition health counter goes amber.
   Beyond the 60-day retention it is doubly pointless - those days would be dropped anyway.

Tables owned here: `log_ssh_sources`, `log_ssh_file_checkpoints`, `log_ssh_fetch_runs` (on-demand run tracking), `log_source_objects`.

## 4. Stage 1: lines become rows

Component: `app/services/mnp_log_ingestion/pipeline/parse_insert.py`, driven by `log_parse_worker` (claims queue rows with `FOR UPDATE SKIP LOCKED`, so one poison file can never block the queue).

The parser (`parsers/m3_dotnet_parser.py`) groups a timestamped header line plus its continuation lines into one logical entry, then classifies it into one of eight types:
`request`, `request_body`, `response`, `mi_call`, `mi_result`, `sql`, `error`, `info`.
Timestamps are parsed as the tenant's local wall clock and converted to UTC at this single choke point.

Each entry becomes one row in `log_entries` - the system's append-only ground truth.
A row is never updated; corrections happen downstream.

**Dedupe**: every row carries `entry_hash = sha256(the full raw text)`, and inserts use `ON CONFLICT DO NOTHING` on `(customer_code, entry_hash, timestamp)`.
This is what makes file rotation and re-fetching safe: the same line can arrive five times and lands once.

**The ticket to Stage 2**: after inserting, Stage 1 writes one row into `log_regroup_pending` saying "the range [oldest, newest] of what I just inserted is dirty".
That ticket is written in the same transaction as the entries: if the entries commit, the ticket exists.

Tables owned here: `log_entries` (partitioned by day), `jobs` (per-file status), `log_regroup_pending` (the ticket queue to Stage 2).

## 5. Stage 2: rows become transactions

This is the heart of the system, in `app/services/mnp_log_ingestion/pipeline/derive_transactions.py`, driven by `log_stitch_worker`.

### 5.1 What a transaction is

One conversation between a device and the server: a request, its body, the work it caused (info/mi_call/mi_result/sql lines) and its response.
Stage 2's job is to decide which lines belong together and to summarise them into one row of `log_transactions` (who, what, when, status, duration, item, quantity and so on), plus one `log_entry_assignment` row per member line recording "this entry belongs to that transaction, at this position".

A transaction's id is **deterministic**: a UUID derived from the content hash of its request line (`_anchor`, derive_transactions.py:469).
Rebuild the same conversation tomorrow and it gets the same id, so saved links and citations never break.

### 5.2 How one worker tick runs (the ticket walk)

```
 every second, per tenant with open tickets (log_stitch_worker._tick):

 1. SEAL   mark transactions final if they have been quiet long enough (sealer.py)
 2. REAP   delete expired grouper state (stream_state.reap, TTL 24h)
 3. DRAIN  finalize_pending:
      claim   open tickets past their backoff (clock-based, dead-letter respected)
      merge   tickets whose ranges are within 2x pad of each other -> one run
      split   a run longer than 6 hours -> consecutive 6-hour windows
      rebuild each window through regroup_window (below), one DB transaction each,
              under a per-tenant advisory lock
      stamp   the run's tickets consumed only after EVERY window committed
      on failure: attempts+1, exponential backoff, dead-letter after 3 tries
                  (permanent errors dead-letter immediately), always with last_error
```

### 5.3 The padded window: why rebuilds never cut a conversation in half

A ticket says "minute X changed", but a conversation near minute X may have started before it or end after it.
So `regroup_window` widens every rebuild by 15 minutes on each side (the **pad**, which is always at least the seal window).

```
 ticket range:              [13:00 ---------- 14:00]
 what actually rebuilds: [12:45 ---------------- 14:15]

 every transaction STARTING in [12:45, 14:00] is freed (sealed ones included),
 every entry in [12:45, 14:15] that is new or belongs to a freed transaction
 is re-grouped from scratch, and the results are written back.
```

**The cross-pad extension (18q/18r era)**: if a conversation ended just before 12:45 and a brand-new line inside the window could still belong to it, the floor moves back to that conversation's start (bounded at pad + gap = 20 minutes) so it rebuilds whole.
If a joinable conversation starts beyond even that bound, the window refuses loudly (`CrossPadSpanExceeded`) and the ticket dead-letters, because rebuilding it partially would split it silently.
Governed by `stage2_cross_pad` = off / shadow / on.

### 5.4 The grouping logic: which lines belong together

`_group` (derive_transactions.py) walks the window's entries in time order and maintains open "streams".
Since chunk 67 a stream is keyed by **(server, thread, user)** - all three, because:

- The **server** is the leading folder of the file path (`TMP-AZ-BEC01/...`).
  Thread numbers are small integers reused by every server process, and one picker's two operations can hit both app servers within milliseconds.
  No pairing rule may ever cross servers (that was the chimera-transaction bug, section 18r).
- The **thread** is the server's processing thread for the request.
- The **user** disambiguates, because .NET reuses a thread mid-request for another user's work.

The pairing rules, all scoped inside one server:

```
 request        -> waits in a pending pool until its work appears
 request_body   -> opens a stream; claims its request by ReqID (GET),
                   or the most recent id-less pending request (POST)
 info/mi_*/sql  -> joins its (server, thread, user) stream; a user-less line
                   inherits whatever stream is live on that thread;
                   the stream claims a pending GET request once the user is known
 response       -> closes the OLDEST still-open stream of the SAME USER on the
                   SAME SERVER (first-in-first-out), because responses carry
                   no request id (verified: 0 of 18,090 live responses do)
 quiet gap      -> a stream idle for more than 300s is closed as-is
                   (log_open_gap_seconds; the longest real conversation
                    measured is 363.7s TOTAL, with entries well inside 300s
                    of each other)
```

### 5.5 Persisting: how update and insert actually happen

`_persist` (derive_transactions.py:720) compares what the rebuild produced against what is stored, row by row, using two SHA-256 fingerprints per transaction:

- `row_fingerprint`: what the row's columns say (status, times, item, quantity, ...).
- `members_fingerprint`: which entries belong to it, in order.

```
 rebuild produced a transaction; is its id already stored?

 NO  -> INSERT the row and its assignments                      "created"
 YES, both fingerprints match     -> write NOTHING              "unchanged"  (the ~98.7% case)
 YES, row differs, members same   -> UPDATE the row in place    "row_only"
 YES, members differ              -> UPDATE row + rewrite its
                                      assignment rows           "rewritten"
 stored in this window but not
 reproduced by the rebuild        -> DELETE row + assignments   "vanished"
                                     (a merge or split absorbed it)
 id exists but is OUTSIDE the
 window's rebuild set             -> SKIP, warn, leave entries  "clash"
                                     (out-of-order ingest; repair = the
                                      server-side full-rebuild runbook)
```

This verdict table is what took write volume from 22.4 writes per surviving row (the old delete-everything-and-reinsert design) to about 1.
The price of the skip-if-unchanged optimisation is a discipline: any change to the grouping or the computed columns must bump `_DERIVE_VERSION` (fingerprints.py), or stored rows would keep matching their own stale fingerprints and never receive the change.
A pinned source-digest test fails loudly if someone forgets.

### 5.6 Sealing: when a transaction becomes final

A transaction with a response is **sealed** 15 minutes after its last entry; an incomplete one (no response yet) waits an hour before being sealed as permanently incomplete.
Both cutoffs are measured against the tenant's newest LOG timestamp, not the wall clock, so backfilling old files seals correctly.
Sealed means "this row will not change again" - the promise the dashboard's Provisional/Settled badge is built on.
A dedicated sealer tick does this with an UPDATE (it used to happen only as a side effect of rebuilds, which left 2,516 rows unsealed forever - section 18f).

### 5.7 The rebuild lane and the head lane

Everything described above is the **rebuild lane**: any change, however small, is handled by re-deriving a padded window from scratch and writing only the difference.
It is the only lane that exists today, and since S3 it is cheap (unchanged rows cost no writes).

The **head lane** is BUILT and shipping in SHADOW (chunk 72):
a fast path processing only brand-new entries at the head of the stream against saved open-stream state (`log_open_stream`) and the per-tenant stitch checkpoint (`log_stitch_checkpoint`), planning one update per continued conversation and one insert per new one, with every surprise (a window behind the checkpoint, disordered state, a would-be merge of parked conversations, an id clash, an anonymous open stream, a parked stream that is already closed, a parked stream whose entries cannot be reloaded) routed back to the rebuild lane by name, and each declined window writes one journal line naming its reason (chunk 75).
Governed by `stage2_head_lane` = off / shadow / on, shipped as shadow: the plan is built for every eligible window, the rebuild executes as the authority, and the two are compared - a DIVERGED line in the journal is what stops `on`.
The comparison is HORIZON-AWARE (chunk 73, section 18s): the rebuild legitimately sees more than the plan (its padded read reaches 900 seconds past the window's high edge, and Stage 1 keeps committing lines between the plan and the rebuild), so the shadow asks two questions that are well-defined across that difference.
First, ownership, always: every line the plan assigned must sit in the same transaction the authority put it in - the question promotion actually hangs on.
Second, fingerprints, only on a shared horizon: byte-identical digests are demanded exactly where the authority's final member set equals the planned set; a transaction the rebuild extended past the plan's horizon is checked by ownership alone, because its digests describe a different entry set by construction.
The equivalence bar for writing stays the strictest available: after a head-lane apply, a rebuild of the same window must report every transaction unchanged (byte-identical fingerprints), certified by the authority itself.
Its benefit is read cost and latency, not correctness; the rebuild lane remains the authority.
Section 11.5 walks both lanes panel by panel, with a worked example - read that if this paragraph is not enough.

Related: the S4 shadow (`stage2_stream_lookup = shadow`) already saves grouper state each window and measures whether a state-seeded regroup would agree with the from-scratch one.
It changes nothing and is slated for replacement by the head lane's own shadow (P5).

Tables owned here: `log_transactions` (partitioned by day), `log_entry_assignment` (partitioned by day, co-partitioned with entries), `log_open_stream` + `log_pending_request` (saved grouper state), `log_regroup_runs` (manual run tracking).

## 6. Ticketing from Stage 2 to analytics

Whenever Stage 2 frees and rebuilds a window, it writes a ticket into `analytics_pending_windows` covering the padded window, in the SAME database transaction as the rebuild (derive_transactions.py:1185 area).
Five publish sites exist, one per code path that can change or delete transactions, and a test census asserts no sixth path can appear unnoticed.
The contract (invariant 2): no transaction changes without a committed ticket whose range contains its start time.

The analytics ticket queue mirrors the Stage 2 one: attempts, backoff (5 s base, 15 min cap), dead-letter after 5 tries, and a `last_error` on every failure.

## 7. Analytics: transactions become facts, facts become charts

Components: `app/services/analytics/` - `consume.py` (the fold), `normalizer.py`, `payload.py`, `capture.py`, `diff.py`, `rollups.py`, `read.py`, `reconcile.py`; worker `analytics_worker` (2 s poll).

### 7.1 One fold cycle

```
 claim the tenant's due tickets
 merge tickets that genuinely OVERLAP (gap=0)   } correctness: a boundary-crossing
 split the merged range into 6-hour slices      } rebuild reverses+inserts in ONE diff,
 for each slice (own transaction, 120s timeout):  but no job is ever unbounded
   read source transactions in range   (no LIMIT - a truncated read would
                                        look like mass deletion to the diff)
   read stored facts in range
   skip reading response entries for any transaction whose Stage 2
     row_fingerprint is unchanged     (96% of the read cost, measured)
   extract response fields -> register unknown names -> read approvals
   normalise: one FACT per transaction (24 contract fields + attributes)
   diff stored vs new     -> insert / update / reverse / unchanged
   apply + append EVERY change as a new version in the LEDGER
   expand records (R4) for transactions with the expand switch on
   quarantine rows that cannot be normalised (never halt the tenant)
   recompute exactly the dirty rollup buckets, from the facts
   update the tenant state row (watermarks, freshness, counts, revision+1)
   stamp the tickets consumed - same transaction, last
```

### 7.2 The two fact tables

- `analytics_facts` holds the CURRENT version of each fact - one wide row per transaction, keyed `(customer, source_transaction_id, event_time)`, kept forever.
- `analytics_fact_ledger` holds EVERY version, append-only, with a `reason` (insert / update / reverse) and a shared `recorded_at` per fold.
  A reversal is a ledger row too, so "what did we believe at time T" is always answerable.
  This table exists from day one specifically so ML training sets are reproducible.

### 7.3 What one fact contains

The normaliser is a pure function: 24 contract fields (ids, times, method, transaction name, status, item, locations, user, device, quantity, classification...) plus an `attributes` bag.
Quantity comes only from an allow-list of quantity-carrying methods (`ConfirmPickLine -> QuantityPicked`, `ReportCount -> CountedQuantity`, `AddStockCountLine -> CountedQuantity`); an unreadable quantity quarantines the row rather than counting zero.
`business_date` is the tenant's LOCAL day.
Response payload scalars arrive namespaced (`resp.ItemNumber`, `mi.record_count`); string values are capped to their column width BEFORE fingerprinting (a production truncation outage taught that, section 18q).

### 7.4 The registries: your on/off switches

- `analytics_transaction_registry`: per transaction name, three independent switches.
  `capture` (default on) gates whether facts exist at all; turning it off stops new history but deliberately does not delete old.
  `show` (default on) gates the rollups (the charts).
  `expand` (default off) turns on per-record capture into `analytics_record_facts`.
- `analytics_field_registry`: every payload field name ever seen is recorded (name only, NEVER a value); a field's values are captured only after approval.
  34 safe names are seeded in code; credential-shaped names (token, password, apikey, ...) are never auto-approved.

One shared predicate (`capture.py`) is used by the source read, the stored read and the auditor, so the three can never disagree about what "captured" means.

### 7.5 Rollups: the pre-computed charts

Three grains: hourly (kept 90 days), daily (tenant-local, kept forever), monthly (kept forever).
Rows store only additive ingredients (sum, count, sum of squares, min, max, histogram) in four dimension slots; averages and rates are finished at read time, so partial aggregates always combine correctly.
Only DIRTY buckets are recomputed, from scratch, and a bucket whose facts vanish is deleted (a stale chart total is the bug class this prevents).
Weekly charts derive from daily at read time.

### 7.6 The status card and freshness

`analytics_tenant_state` is one denormalised row per tenant - everything `GET /analytics/status` shows, readable in one indexed lookup because the dashboard polls it every 2 seconds per tab.
Freshness has two separate meanings, deliberately:

- `lag_seconds` / `stale`: is analytics BEHIND the source? (folded watermark vs source watermark, warn over 300 s)
- `unsealed_share` / `provisional`: will the newest numbers still MOVE? (share of the last window's transactions not yet sealed - by design nonzero on a live system)

### 7.7 The auditor

A report-only reconcile worker re-checks a settled 48-hour window every hour: every transaction has a fact or a recorded reason; every rollup bucket equals a fresh fold of its facts (comparing only buckets the window covers WHOLE - chunk 66); no entry is left assigned to nothing.
It never repairs on its own; `POST /analytics/reconcile` with `repair=true` publishes ordinary tickets / refolds instead of writing totals directly.

Tables owned here: `analytics_pending_windows`, `analytics_facts`, `analytics_fact_ledger`, `analytics_metrics` (metric definitions as data), the three rollup tables, `analytics_tenant_state`, `analytics_quality_issues` (quarantine), `analytics_transaction_registry`, `analytics_field_registry`, `analytics_record_facts`.

## 8. ML: reproducible training sets

Component: `app/services/analytics_ml/features.py`; tables `analytics_feature_sets`, `analytics_predictions`.

A training set is defined by two coordinates: an INSTANT (`pinned_at`) and a code version.
Building one reads the LEDGER as it stood at that instant (newest version per transaction at or before the pin, reversals excluded), orders deterministically, and stores only the pin, the version, the row count and a SHA-256 content hash - never the rows, which are a pure function of the pin.
`verify()` rebuilds at the same pin and compares hashes, making the reproducibility promise testable in production.
Exceeding 500,000 rows raises instead of truncating: a model trained on an unchosen subset is worse than a build that refused.

Honest status: the machinery is built and tested, but nothing in the application calls `build()` yet and nothing writes `analytics_predictions`; the ML consumer cursor therefore does not yet appear at runtime.

## 9. Housekeeping that keeps it all alive

- **Partitioning** (`app/persistence/partitioning.py`): nine tables are partitioned (entries/transactions/assignments daily; facts/ledger/records/quality monthly; hourly rollups daily; daily rollups yearly).
  A worker pre-creates 14 days of runway every hour and alarms CRITICAL below 3 days.
- **Retention**: log tables keep 60 days; facts, ledger, daily rollups and record facts keep forever; hourly rollups 90 days; quarantine 1 year.
  A partition is dropped only when FOUR gates agree: past retention, no open stitch window overlaps it, every live consumer cursor has read past it, and analytics is healthy (or its hold has been capped at 14 days).
- **Consumer cursors** (`consumer_cursors`): each incremental reader (analytics, notifications, ML when live) publishes "I have consumed everything before T"; retention respects the minimum.
  A cursor silent for 24 h is excluded from the minimum and logged CRITICAL - losing one consumer's tail is survivable, filling the disk is not.
- **Notifications**: rules read `log_transactions.updated_at` through per-rule cursors, publish deduped events (key: rule + transaction + status) into an outbox, and a delivery worker sends them to Teams/Slack/WhatsApp channels with rate limits, backoff, and a 50-attempt dead letter.

## 10. The component map, with every table in its place

```
+--- COLLECTION ------------------------------------------------------------+
| ssh_log_fetcher -> remote_fetcher -> object storage                       |
|   log_ssh_sources          config: one row per WMS server                 |
|   log_ssh_file_checkpoints per-file byte cursor (optimisation only)       |
|   log_ssh_fetch_runs       on-demand run status                           |
|   log_source_objects       QUEUE -> Stage 1 (lease, retries, dead letter) |
+---------------------------------------------------------------------------+
                                   |
+--- STAGE 1: PARSE --------------------------------------------------------+
| log_parse_worker -> parse_insert -> m3_dotnet_parser                      |
|   jobs                per-file status                                     |
|   log_entries         GROUND TRUTH (append-only, deduped, daily parts)    |
|   log_regroup_pending QUEUE -> Stage 2 (attempts, backoff, dead letter)   |
+---------------------------------------------------------------------------+
                                   |
+--- STAGE 2: STITCH -------------------------------------------------------+
| log_stitch_worker -> finalize_pending -> regroup_window -> _group/_persist|
| + sealer (finality)  + stream_state (S4 shadow)  + cross-pad extension    |
|   log_transactions      the PROJECTION (update-in-place, daily parts)     |
|   log_entry_assignment  entry -> transaction membership (daily parts)     |
|   log_open_stream       saved open-stream state (S4 / future head lane)   |
|   log_pending_request   saved unmatched requests                          |
|   log_regroup_runs      manual run status                                 |
|   analytics_pending_windows  QUEUE -> analytics (backoff, dead letter)    |
+---------------------------------------------------------------------------+
                                   |
+--- ANALYTICS -------------------------------------------------------------+
| analytics_worker -> consume (fold) -> normalizer/payload/capture/diff     |
| -> rollups -> tenant state       + reconcile worker (report-only audit)   |
|   analytics_facts           CURRENT fact per transaction (monthly parts)  |
|   analytics_fact_ledger     EVERY version, append-only (monthly parts)    |
|   analytics_hourly_rollups  charts, hourly (daily parts, 90d)             |
|   analytics_daily_rollups   charts, tenant-local day (yearly parts)       |
|   analytics_monthly_rollups charts, monthly (unpartitioned)               |
|   analytics_metrics         metric definitions as data                    |
|   analytics_tenant_state    ONE status row per tenant                     |
|   analytics_quality_issues  quarantine (monthly parts, 1y)                |
|   analytics_transaction_registry  capture/show/expand switches            |
|   analytics_field_registry  field allowlist (names only, never values)    |
|   analytics_record_facts    per-record capture, R4 (monthly parts)        |
+---------------------------------------------------------------------------+
                                   |
+--- ML --------------------------------------------------------------------+
| analytics_ml/features (no producer wired yet)                             |
|   analytics_feature_sets   pin + code version + content hash              |
|   analytics_predictions    model outputs (no writer yet)                  |
+---------------------------------------------------------------------------+

+--- PLATFORM (crosses all pipelines) --------------------------------------+
| log_partition_worker (runway + retention, 4 gates)                        |
| notification_worker (rules -> outbox -> deliveries)                       |
|   customers, customer_display_names, saved_views, logspace_presence,      |
|   idempotency_keys, consumer_cursors,                                     |
|   notification_rules/events/deliveries, customer_notification_channels   |
+---------------------------------------------------------------------------+
| Separate RAG/document pipeline (not the log path):                        |
|   chunks, chunks_entity, embedding_queue, embeddings (pgvector)           |
+---------------------------------------------------------------------------+
```

## 11. Deep dives: the questions this design answers, with pictures

These are the questions that came up while reviewing the system, answered with the diagrams that made them click.
Keep them: each one is a design decision someone will question again.

### 11.1 Why a transaction row is UPDATED in place, yet no history is ever lost

`log_transactions` cannot be append-only, because a transaction row is an AGGREGATE over its entries: its end time is the max, its status comes from the response, its duration is the difference.
Adding one late entry necessarily changes the row.
But "not append-only" never required losing history, because history lives one layer down, in the ledger:

```
 log_transactions (the PROJECTION - one row, edited in place)

   txn 123:  status=incomplete  --later-->  status=success
                                            (the old value is GONE from this table)

 analytics_fact_ledger (the DIARY - a new row per version, never edited)

   v1  txn 123  status=incomplete   recorded 14:03
   v2  txn 123  status=success      recorded 14:05
       both kept forever -> "rebuild the data exactly as it was at 14:04" works
```

And the deeper truth: mutability is not WHY recent data changes.
The cause is physical - when a picker scans a request, its response simply has not happened yet.
Even a fully append-only store would have to revise its answer five minutes later; it would just record the revision as a new row, which is exactly what the ledger does.

### 11.2 Provisional is not slow: the sealing timeline

The dashboard's yellow "Provisional" badge is about FINALITY, not speed.
The data path is fast (a scan reaches the chart in about a minute); the badge says the newest numbers may still move, because their transactions are still inside the seal window:

```
 14:02:11 request -- 14:02:14 response ---- silence ----> 14:17:14 SEALED
                                  |<-------- 900 s ------->|
 before sealing: counted, charted, but PROVISIONAL (could still change)
 after sealing:  frozen forever -> the badge flips to green Settled
```

There were two honest designs, and this system deliberately picked the fast one:

| | numbers appear | risk |
|---|---|---|
| only count sealed rows | 15 minutes late | none |
| count immediately + badge (CHOSEN) | ~1 minute | recent figures may shift, and the badge says so |

The share of unsealed contributors is measured over the LAST FOLDED WINDOW only (the live tail), so roughly "the last 15 minutes of activity"; older bars on the chart never move.

### 11.3 How a late line rejoins its conversation: move the window floor, never attach by id

When a line arrives whose conversation started just before a rebuild window's padded floor, the fix is NOT to look up the exact transaction and attach the line to it.
The fix is one line of geometry: move the floor.

```
 today:   window floor is FIXED at  ticket_start - 900s
          conversation started at floor - 50s -> invisible -> its late
          response becomes an orphan, and the fingerprint skip makes
          that permanent

 with the cross-pad extension (built, ships in shadow):
          one query asks "does an open conversation end just before my
          floor, AND is there a brand-new line that could join it?"
          if yes -> floor moves back to that conversation's start
                    (bounded at pad + gap = 20 minutes)
          -> the SAME rebuild now sees the whole conversation -> joins it
          beyond the bound -> the window REFUSES loudly (dead letter),
                    never a silent partial rebuild
```

Why the tempting alternative - "find the exact transaction id and attach the line" - was rejected, three facts deep:

1. **An attach is a re-derivation, not an append.**
   The row's columns are aggregates over ALL its entries, so attaching one line correctly means loading every prior entry anyway; the imagined read savings do not exist.
2. **The id cannot tell you whether the line actually belongs to it.**
   Ownership is decided by the grouping rules (FIFO per user, thread flips, gap limits), and only running the grouper over the combined lines answers correctly.
   Two implementations of grouping is exactly what produced the measured shadow divergence (one from-scratch group became seventeen seeded ones).
3. **Everything downstream comes free through the rebuild.**
   Update by partition key, fingerprint recompute, membership rewrite, seal recompute, the analytics ticket - the rebuild path already does all of it, tested; a targeted attach would re-implement each as a second write authority that must agree with the first forever.

### 11.4 "What if the line belongs to a window six hours older?"

Two very different cases, both already handled - neither needs the extension to reach six hours back:

```
 CASE 1: the line's TIMESTAMP is 6h after the conversation's last entry
   -> it can NEVER join it. The 300s stream gap rule refuses everywhere -
      in the extension, in a full rebuild, in any design. Six hours of
      silence means a new conversation, by the system's own definition.
      (That is why the extension bound is 20 minutes and not more:
       past pad + gap a join is arithmetically impossible.)

 CASE 2: a BACKFILL - a file arrives NOW containing lines stamped 6h ago
   -> no cross-pad involved at all, because WINDOWS FOLLOW ENTRY
      TIMESTAMPS, not arrival time:

      file arrives 20:00 containing lines stamped 14:02
        -> Stage 1's ticket says "the range around 14:02 changed"
        -> the rebuild window positions itself 6 hours back
        -> the old conversation is INSIDE that window -> freed -> rebuilt
        -> the late line joins through today's normal path
      (this already works; it is why rebuilds free sealed rows too)
```

### 11.5 The rebuild lane and the head lane: the full picture

Two words, defined once:

- The **rebuild lane** is how Stage 2 works TODAY: whenever anything changes, re-derive a whole padded window from scratch and write only the difference.
- The **head lane** is the fast path (chunk 72, shipped in SHADOW): remember where processing got to, and handle only the brand-new lines at the head of the stream.

**Panel 1 - what the rebuild lane does today, and what it wastes.**
Every worker tick re-reads a window of recent history just to discover that almost none of it changed:

```
 the entry stream (time ->)
 ────■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■□□□
     older lines, already stitched                        new  NOW

 tick 1:      [------- re-read ~30-45 min -------]   writes: only the diff
 tick 2 (+1s):   [------- re-read again -------]     writes: ~0
 tick 3 (+2s):     [------ re-read again ------]     writes: ~0

 98.7% of what each tick re-reads comes out UNCHANGED.
 Since S3, the WRITES are already minimal (fingerprints skip them).
 The re-READS are the remaining waste - that is all the head lane removes.
```

**Panel 2 - what the head lane remembers instead.**
Instead of re-deriving the recent past, it keeps two pieces of durable memory:

```
 the CHECKPOINT (table: log_stitch_checkpoint): "stitched through here"
 ────■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■□□□
                                                     ▲    new  NOW
                                                CHECKPOINT

 the PARKED OPEN STREAMS (table: log_open_stream):
   (BEC01, thread 45, user amin) -> transaction A, still open, last line 14:53:07
   (BEC02, thread 33, user sara) -> transaction B, still open, last line 14:53:09

 for each NEW line after the checkpoint:

   continues a parked stream?   YES -> append ONE membership row,
                                       UPDATE that one open transaction
                                NO  -> INSERT a new transaction
   anything surprising?         -> STOP GUESSING, hand the range to the
      (older timestamp than the      REBUILD LANE - never improvise
       checkpoint, gap exceeded,
       anonymous stream, any of
       the six known miss modes)
```

**Panel 3 - the router: how the two lanes will share the work.**

```
                        new work arrives
                              |
                 is it brand-new lines AT THE HEAD,
                 and do ALL the safety guards pass?
                   /                          \
                 YES                           NO
                  |                             |  (a backfilled file, a late
             HEAD LANE                          |   line, a guard tripping,
        append + update in place                |   a manual repair)
        cheap, near-instant                     v
                  |                        REBUILD LANE
                  |                   pad +-15 min, free every
                  |                   transaction in the window,
                  |                   regroup from scratch,
                  |                   write only the difference
                   \                          /
                    +-----------------------------------------------+
                    | both lanes MUST produce IDENTICAL results,    |
                    | proven by a shadow phase (run both, compare)  |
                    | before the head lane is ever allowed to       |
                    | write. the REBUILD LANE is always the referee:|
                    | any disagreement means the head lane is wrong.|
                    +-----------------------------------------------+
```

**Panel 4 - one worked example through both lanes.**
Picker amin scans; three lines arrive over two fetches:

```
 14:53:07  request  (BEC01, thread 45, amin)      arrives in fetch 1
 14:53:08  body     (BEC01, thread 45, amin)      arrives in fetch 1
 14:53:12  response (BEC01, thread 40, amin)      arrives in fetch 2 (late)

 REBUILD LANE (today):
   fetch 1 ticket -> re-derive [14:38..15:08]: builds txn A = [request, body],
                     status incomplete. Plus re-reads ~40 min of neighbours
                     to conclude they are all unchanged.
   fetch 2 ticket -> re-derive the window AGAIN: txn A = [request, body,
                     response], status success; one UPDATE. Neighbours:
                     unchanged again.

 HEAD LANE (planned):
   fetch 1 -> two lines after the frontier; no parked stream matches ->
              INSERT txn A [request, body]; park stream (BEC01, 45, amin);
              advance the checkpoint. Nothing else read.
   fetch 2 -> one line; parked stream matches (same server, same user,
              within the 300s gap) -> append membership, UPDATE txn A to
              success; close and unpark the stream; advance the checkpoint.

 SAME final transaction, byte for byte. Different cost:
   rebuild lane read  ~thousands of lines per tick to get there;
   head lane read     exactly three.
```

Summary table:

| | rebuild lane (today) | head lane (built, in shadow) |
|---|---|---|
| trigger | a ticket: "this time range changed" | a new line at the stream head |
| reads | the whole padded window, every tick | only the new lines |
| writes | only the difference (~1/row, S3) | one membership + one update per line |
| handles | everything: backfills, repairs, late lines | only the clean common case |
| on surprise | it IS the fallback | hands the range to the rebuild lane |
| correctness | the authority, always | must match the rebuild lane, gated by its own shadow phase |
| status | built, running | built (chunk 72), running in SHADOW; `on` is the manual flip |

The one-sentence takeaway: the head lane is a bookmark plus parked conversations, so the common case stops re-reading the past; the rebuild lane keeps existing untouched underneath it as the referee and the repair tool.

### 11.6 Case study: the chimera transaction, or why grouping is server-scoped

Reconstructed from live forensics (2026-08-27 12:09:35, one picker, two app servers, twenty milliseconds).
This single case produced three symptoms that looked unrelated: a slow leak of orphaned lines (~300/day), hourly "skipped an already-sealed id" warnings, and an analytics undercount.

```
 what the two servers actually logged (one user, two operations):

 BEC01:  request .113 --- body .134 --- work (thread 45) --- response .330
 BEC02:  request .115 --- body .144 --- work (thread 33) --- response .338

 what the grouper used to build (matching pools ignored the server):

   CHIMERA txn:  [BEC01 request .113] + [BEC02 body .144 + BEC02 work]
                 + [BEC01 response .330]        <- sealed, "success", WRONG
   leftovers:    BEC02 request .115, BEC01 body .134
                 -> minted the SAME deterministic id as the chimera
                 -> skipped as a duplicate -> stranded UNASSIGNED forever

 what it builds now (every key and pool carries the server):

   txn A: [BEC01 request .113, body .134, work, response .330]   correct
   txn B: [BEC02 request .115, body .144, work, response .338]   correct
```

The rule that fixed it is one sentence: a thread only exists inside one server process, so no pairing rule - stream keys, request pools, the response FIFO - may ever match lines across two servers.
The repair for the historical damage is the ordinary full rebuild: with everything freed, no id can clash, the chimeras dissolve, and every stranded line finds its home.

## 12. The workers: who actually runs each pipeline

Verified on the live server (192.168.0.142) on 2026-08-27.

### 12.1 Two operating-system services, one of which does all the background work

```
 +--------------------------------------------------------------------------+
 |  SERVICE 1: fastapirag  (the WEB tier)                                   |
 |  gunicorn -w 4  ->  four identical FastAPI processes                     |
 |  serves every /api/v1/* request the dashboard makes                      |
 |  runs NO background loops in this deployment                             |
 +--------------------------------------------------------------------------+
 +--------------------------------------------------------------------------+
 |  SERVICE 2: fastapirag-worker  (the BACKGROUND tier)                     |
 |  python -m app.worker  ->  ONE process holding a singleton advisory      |
 |  lock in Postgres, so a second copy started by mistake refuses to run    |
 |  hosts ALL TEN worker loops below as tasks inside this one process       |
 +--------------------------------------------------------------------------+
```

Every loop must run in exactly one process: the loops assume they are the only writer of their queue, and the per-tenant advisory locks serialise the rest.
Stopping `fastapirag-worker` pauses the whole factory (collection, stitching, analytics, notifications) while the web tier and the dashboard stay up, which is exactly what a full-history repair needs.

### 12.2 The ten loops inside the worker process, mapped to the pipelines

| # | Worker loop | Pipeline | Cadence | On this server | What it does |
|---|---|---|---|---|---|
| 1 | `ssh_log_fetcher` | collection | ~60 s per tenant | on (default) | pulls new log bytes from every enabled WMS server over SFTP, checkpointed per file |
| 2 | `log_watcher` | collection | 5 s | on (always) | ingests files dropped by hand into the staging directory |
| 3 | `log_parse_worker` | **Stage 1** | 2 s | on (default) | leases downloaded byte ranges and turns lines into `log_entries` rows |
| 4 | `log_stitch_worker` | **Stage 2** | 1 s | on (default) | three phases per tick: seal due transactions, reap expired stream state, drain the stitch tickets through the rebuild lane |
| 5 | `analytics_worker` | **analytics** | 2 s | **on (.env override; default off)** | drains analytics tickets: fold transactions into facts, ledger, rollups, tenant state |
| 6 | `analytics_reconcile_worker` | analytics (audit) | 1 h | **on (.env override; default off)** | report-only audit of a settled 48-hour window; never repairs |
| 7 | `log_partition_worker` | platform | 1 h | on (default) | pre-creates 14 days of partition runway; drops partitions past retention behind four gates |
| 8 | `notification_worker` | notifications | 10 s | on (per-tenant switch) | runs rules, publishes deduped events, delivers to Teams/Slack/WhatsApp with backoff |
| 9 | `embedding_worker` | RAG documents | 2 s | on (always) | embeds document chunks; not part of the log path at all |
| 10 | `logspace_cleanup_worker` | platform | 1 h | off (default) | purges expired disposable log spaces |

**There is no ML worker.**
The ML pipeline (feature sets, predictions) is library code waiting for a caller; when it gets one, it will follow the same pattern: a loop in this process, a queue or a pin as its input, and a consumer cursor so retention respects it.

### 12.3 One picture: which worker touches what

```
   WMS servers                     staging directory
        |                                 |
   [1 ssh_log_fetcher]              [2 log_watcher]
        \_______________  ________________/
                        \/
              log_source_objects (queue)
                        |
              [3 log_parse_worker]  ......................... STAGE 1
                        |
                   log_entries
                        |
              log_regroup_pending (queue)
                        |
              [4 log_stitch_worker] ........................ STAGE 2
                 |seal |reap |drain
                        |
        log_transactions + log_entry_assignment
                        |
              analytics_pending_windows (queue)
                        |
              [5 analytics_worker] ......................... ANALYTICS
                        |
        facts + ledger + rollups + tenant state
                        |                  \
              (dashboard reads)       [6 analytics_reconcile_worker]
                                      hourly report-only audit

   cross-cutting, on their own clocks:
     [7 log_partition_worker]   runway + retention for all 9 partitioned tables
     [8 notification_worker]    rules -> outbox -> channel deliveries
     [9 embedding_worker]       the separate RAG document pipeline
     [10 logspace_cleanup]      disabled here
```

### 12.4 How to read the server's state at a glance

```
 systemctl is-active fastapirag          -> the dashboard and API
 systemctl is-active fastapirag-worker   -> the entire background factory
 journalctl -u fastapirag-worker -f      -> every loop logs here, one process
```

The only two .env overrides on this server beyond the defaults are `ANALYTICS_WORKER_ENABLED=true` and `ANALYTICS_RECONCILE_WORKER_ENABLED=true`; everything else runs on the defaults listed above.

### 12.5 The maintenance screen and the rebuild lifecycle (chunks 69-71)

Manage -> Maintenance in the frontend drives the repair operations that used to need SSH:
a tracked rebuild - full history, or aimed at one date range - one-click re-arms for both dead-letter queues, and the consistency checker with optional repair.

The rebuild's whole lifecycle, from click to notification:

```
 operator clicks "Rebuild..."  (whole history, or From/To for one range)
      |
      v
 POST /logs/regroup/full[?start&end]  -> 202 + run id   (409 if one is running)
      |
      |            the RUN ROW (log_regroup_runs, kind='full', status=running)
      |            is simultaneously three things:
      |              1. the poll target the screen watches
      |              2. the MAINTENANCE FLAG - stitch + analytics workers
      |                 skip THIS tenant while it is fresh (other tenants
      |                 and this tenant's collection/parsing keep flowing)
      |              3. the crash detector - stale past 6h -> resume + CRITICAL
      v
 a SEPARATE PROCESS runs the rebuild      (an event loop would freeze under
   whole history -> regroup_all            tens of minutes of grouping CPU)
   date range    -> the same 6h-sliced
                    windowed rebuilds the live worker uses
      |
      v
 outcome recorded on the run row  (completed + stats | failed + error;
      |                            a hard subprocess crash is caught by an
      |                            exit watcher and recorded as failed too)
      v
 NOTIFICATION published through the ordinary pipeline (chunk 71):
   completed -> info, failed -> error; deduped per (run, outcome);
   delivered to the tenant's Teams/Slack/WhatsApp channels by the
   notification worker, honouring the tenant gate and rate pacing
      |
      v
 workers notice the flag is gone on their next tick (1-2s) and drain
 the queued work; analytics restates the rebuilt range via the ordinary
 tickets the rebuild published (union of the transactions + entries spans)
```

Nobody stops or starts anything, and nobody owes the browser tab their attention: the pause, the resume, the re-fold and the announcement are all mechanisms, not procedures.

## 13. Known gaps register (verified 2026-08-27)

Honest imperfections found while fact-checking this part; none is currently causing damage, each is a candidate work item.

1. `analytics_tenant_state.last_error` is only ever written as NULL, so the retention gate's "last cycle failed" branch cannot fire; per-run errors live on the ticket rows instead.
2. Nothing calls `analytics_ml.features.build` outside tests, so the `ml:features-v1` cursor never appears at runtime.
3. The rollup read path serves only sum and count, so stats/extent/percentile aggregations fall back to fact scans even though the columns exist.
4. The `show` (hidden) gate applies only where rollups are written; a live-tail or ad-hoc fact scan can still include hidden transactions.
5. `GET /analytics/status` emits an ETag but the server never handles `If-None-Match` (no 304s); conditional requests are left to clients and proxies.
6. A few docstrings still describe superseded behaviour (`capture.observe_names` says show defaults off; the transaction-registry model says R4 is not built), and the root CLAUDE.md still claims Stage 1 relaxes its statement timeout to 0 where the code uses a finite 120 s.
7. RESOLVED (18v, chunk 76): the stored stream state now carries a `server` column in its unique key, so two servers' same-numbered threads park side by side instead of newest-wins evicting one.


---

# PART II. The design history

Everything below is the historical record: iteration 1 (sections 1 to 17), the iteration-2 plan and its as-built sections (18 to 18v).
It explains WHY the system is shaped the way Part I describes.
Where the two disagree, Part I is current and the section here records what was believed at the time.

## Read this first: which iteration you are looking at

**ITERATION 1 is everything already built and running.**
Sections 1 to 17 describe it.
In the component map in section 5 that is every **green** box (existed before this work and was reused) and every **blue** box (built by this work: N1 to N7, the analytics platform).
All of it is shipped, deployed and under test - 1,065 tests pass.
If a section does not say otherwise, it is describing iteration 1.

**ITERATION 2 is everything being built now, and none of it exists yet.**
Sections 18 to 18e describe it, and it has its own component map.
It is the eight stages `S1`, `R1`, `R1b`, `R3`, `R2`, `S2-S4`, `R4`, `M1`, plus six new tables.
Everything belonging to it is drawn in **violet** in the HTML twin, which in this document has always meant "a later phase".

**The single rule: violet means not built.**
If you are reviewing what is about to change, read only section 18 onward and only the violet.

| | Iteration 1 | Iteration 2 |
|---|---|---|
| Status | **shipped and running** | **nothing exists yet** |
| Sections | 1 to 17 | 18 to 18e |
| Components | E1-E8 (reused), N1-N7 (built by this work) | S1-S4, R1-R4, M1 |
| Tables | 34 exist | + 6 new, + 1 undecided |
| Colour in the maps | green and blue | violet |

**Revision note, 2026-08-21.**
Several changes, all from re-reading the code and measuring the live server. **None alters the
architecture.**

> **Every change is tabulated in [section 17, the correction log](#17-correction-log), with what the
> document said before.** Go there first if a downstream component is behaving unexpectedly and you
> need to know whether the document moved under it.

Summarised below; the log is authoritative.

**1. A factual error corrected throughout.**
`regroup_incremental` was described as the live path running every 70 seconds, and it is neither.
The automated Stage 2 path is `log_stitch_worker` -> `finalize_pending` -> `regroup_window`.
Marked inline everywhere it changes a conclusion, in section 3 and in F1.
It moves the ledger sizing down and F1's priority down.
Source of the error: three stale comments in `derive_transactions.py`, tabulated in section 3.

**2. Two more ticket publish sites found (F12).**
N1 listed three; there are five.
Both halves of `DELETE /logs/data` remove `log_transactions` rows, and one of them does so through a `jobs` cascade with no statement to hook.
Recorded in N1, F12, invariant 2, Phase 2 and verification item 5.
This is the only defect in the design that erases its own evidence, so it is the one item worth treating as blocking for Phase 2.

**3. A third removal path, needing the opposite fix (F13).**
The tenant purge also removes `log_transactions`, through the same `jobs` cascade.
Section 6 previously asserted that it does not; that sentence is corrected.
Because the tenant itself is going away, the fix is to delete its `analytics_*` rows rather than publish a ticket.
Recorded in section 6, F13, invariant 15, Phase 1 and verification item 12b.

**4. The partition manager (E4) was extended so Phase 1 can register its tables at all.**
`partitioning.py` was daily in every function and retention was one global setting, so a monthly,
retained-forever table could not be expressed: it would have got daily partitions and been dropped at
60 days.
It now carries a per-table `Grain` and a `KEEP_FOREVER` / `RETENTION_DAYS` policy, and three
grain-only bugs were fixed with it (early expiry, day-keyed retention gates, and runway measured to the
wrong end of a period).
Recorded as a new E4 component-detail section, in the component map, Phase 1 and invariant 16.
Behaviour for the three log tables is unchanged and tested.

**The general lesson from 2 and 3**, worth more than either finding: the list of places to hook was built by asking "where does Stage 2 rebuild".
The question that produces a complete list is **"where does a row leave `log_transactions`"**, answered by grep rather than by reasoning.
There are seven answers: three rebuild paths, two API deletes, one tenant purge, and retention's partition drop, which is the one deliberate exception.

## What this replaces

| Document | Status |
|---|---|
| `docs/plan/2026-08-17_22-32_merged-warehouse-analytics-platform-plan.md` and `.html` | merged into this document |
| `docs/warehouse-analytics-architecture.md` and `.html` | merged into this document |
| `docs/plan/2026-08-11_00-11_real-time-warehouse-consumption-analytics.md` and `.html` | superseded; core mechanism adopted here |
| `docs/data-architecture-scale-ml.md` and `.html` | earlier sketch; the Parquet and DuckDB proposal is not adopted |

Alembic migrations for this work should cite **`docs/analytics-ml-architecture/final_architecture.md`**.

## How to read this

1. **Context** and **Why the design is unusual** explain the one problem everything else exists to solve.
2. **Verified ground truth** is the evidence base.
   Do not re-derive it.
3. **Architecture** is the structure: components, which tables each owns, and the flows.
4. **Decisions** covers the thirteen corrections, the metric registry, and the grain cascade.
5. **Build** covers phases, verification and invariants.
6. **Open questions** lists what is still blocking.
7. **Correction log** records every post-final change with its previous value, so a decision taken on older wording can be traced and reversed.

---

# 1. Context

The goal is an analytics platform over the live WMS log data on the Matrix host, delivering three things.

A real-time running total per item, folded as each record arrives.
Easy daily, weekly and monthly aggregation across user-chosen metrics, scalable to millions of rows.
A second pipeline for machine learning and agentic AI on the same foundation.

**Migration base, checked 2026-08-18.** Deployed Alembic head is `e4b28f5c9107`, which is also the repo head across 43 revisions, so there are no pending migrations.
Phase 1 lands on a clean head using `deploy.sh`'s ordinary pull, migrate, restart order.

# 2. Why the design is unusual

`log_transactions` is not append-only and not a time series.
It is a **mutable derived projection**: each transaction is stitched from roughly 15 raw log lines, and when a late line arrives the row is deleted and rebuilt, possibly with different values.

**Superseded 2026-08-23 - see section 18.** The row is still mutable, but Stage 2 is moving from delete-and-reinsert to UPDATE in place, and `log_entry_assignment` is becoming append-only. The measured churn below describes the *content*, which is unchanged; it stops describing the *storage*.

Measured on the live server:

| Measurement | Value |
|---|---|
| Rows written more than 5 min after their own entries | **22,183 of 22,465 (98.7%)** |
| Sealed rows, average gap from newest entry to row write | **6,098 s (1.7 h)** |
| Sealed rows, worst gap | **19,312 s (5.4 h)** |
| Rows built promptly, within 60 s | 126 (0.6%) |

Two designs are therefore ruled out.

**Simply adding numbers up is wrong.**
A rebuilt row would be counted twice, producing plausible-looking wrong totals with no error.

**Waiting for rows to settle is also wrong.**
Sealing is computed against the newest log timestamp rather than the wall clock (`derive_transactions.py:535-538`), so the wait has no clock-based bound and stalls entirely when ingestion pauses.

**Consequence for implementation.**
The version fingerprint on each fact row is load-bearing, not an optimisation.
At a 98.7% rebuild rate almost every recheck must be absorbed as a no-op by a matching fingerprint, or the system produces a constant stream of pointless aggregate writes.

# 3. Verified ground truth

Read from the live server and from code.
Do not re-derive.

**Volume and shape.**
72,682 transactions, roughly 1,100,516 raw log entries, 1,474,393 parsed M3 records inside `log_entries.fields`.
Peak 68 derived transactions per minute, 13 pick transactions per minute.
Tenants `tmp-live` and `tmp-test`.

**Cardinality grows far more slowly than volume.**
692 distinct items with consumption, 395 on the busiest day, against 25,008 in the item master.
More picks does not mean more items, and this is what makes the grain cascade viable.

**Scan cost on this hardware is roughly 2.5 microseconds per row.**
1.04 M rows took 2,541 ms.
Planning cost tracks partition count rather than data volume: 7 ms against one partition versus about 85 ms against 21.
Prepared statements amortise it to about 9 ms, and asyncpg prepares by default.

**Filter on `method`, never `transaction_type`.**
`ConfirmPickLine` is 1:1 with `attributes->>'QuantityPicked'`.
`transaction_type` contains WMS-supplied placeholders, and `AddStockCountLine` carries real `CountedQuantity` under one, so a type filter silently drops real data.
**Corrected 2026-08-21:** the placeholder set is `xxxxxx`, `XXXXX`, `00xxxx`, `0050XX` **and `XXXXXX`**; the original list of four missed the last.
Because that list has now been wrong once, detection is a PATTERN rather than an enumeration: any value containing `x` or `X`. Empirically every such value on this data is a placeholder and no legitimate code contains one. See C2.

**Only 3 of 49 methods carry quantities.** Corrected 2026-08-21, was "2 of 49".

| Method | Field | Rows |
|---|---|---|
| `ConfirmPickLine` | `QuantityPicked` | 16,075 |
| `ReportCount` | `CountedQuantity` | 11,343 |
| `AddStockCountLine` | `CountedQuantity` | 83 |

`AddStockCountLine` is the third, and this document already named it one paragraph above as the trap
where a real quantity hides under a placeholder `transaction_type`: it was measured but not counted.
For the other 46 the meaningful measures are volume, duration, status and actor.
Encoded as the allow-list in `app/services/analytics/contract.py` (`QUANTITY_FIELD`); see C1 in the
correction log.

The row counts above are also higher than the original 14,654 and 9,076 simply because more data has
arrived since. That is growth, not a discrepancy.

**A JSONB key's presence is not evidence the method carries a quantity.** Added 2026-08-21.
3 of 7,307 `ListItemAlternateUnitsOfMeasure` rows carry a stray `CountedQuantity` (values 3.415, 18, 0, all under `transaction_type = xxxxxx`).
A listing call has no business reporting a stock count, so this is parser leakage.
Consequence for N2: read a quantity only from an allow-listed METHOD. Testing `attributes ? key` instead would fold three phantom stock counts into the totals. See C3.

**Quantity fields.**
`QuantityPicked` and `ExpectedQuantity` are 100% clean.
`QuantityToBePicked` is 91% empty strings and `attributes.Weight` is 100% empty.
Quantities are fractional, so `NUMERIC` throughout, never float.
`ExpectedQuantity` arrives as `30.000000` while `QuantityPicked` arrives as `30.0`, so comparisons must be numeric, never string.

**`ExpectedQuantity` is mutable per instruction, not an order-line total.**
Fill rate is not derivable from it.
Use `OIS100MI/LstLine.ORQA` from the M3 layer instead.

**Identity, re-verified 2026-08-18.**
A new group's id is `uuid5(fixed_namespace, anchor_entry_hash)` (`derive_transactions.py:445`).
A rebuilt group **inherits** its id via `continuity.assign(...)` in `_resolve_ids`.
The 1.3% figure in `continuity.py` is the measurement that motivated that fix, not a live exposure.

**But inheritance is wired into only one of three rebuild paths, and that one is the path that actually runs:**

| Path | Deletes | Inherits ids | Result | How it is reached |
|---|---|---|---|---|
| `regroup_window` (`:858`, `:895`) | all in the padded range, sealed included | **yes** | id preserved | **the automated path**: `log_stitch_worker` -> `finalize_pending` -> `regroup_window` |
| `regroup_incremental` (`:780`, `:801`) | unsealed only, no time bound | no | id recomputed, can change | manual only: `POST /logs/regroup?incremental=true` (`logs.py:358`) and the tail of the date-range delete (`logs.py:728`) |
| `regroup_all` (`:677`, `:698`) | everything | no | id recomputed, can change | manual only: `POST /logs/regroup` (the default) and two scripts |

**Corrected 2026-08-21.**
An earlier revision of this document described `regroup_incremental` as the live path running every 70 seconds.
That is wrong.
`app/background.py:84-164` registers eight background loops and `regroup_incremental` is in none of them; the only automated Stage 2 driver is `log_stitch_worker`, which imports `finalize_pending` alone (`log_stitch_worker.py:37`).
The error was inherited from three stale comments in the source, which are listed in the next section.
The practical effect of the correction is recorded under "Why an id change costs this design nothing" and in F1.

### A documentation bug worth reporting upstream

`_resolve_ids` takes an optional continuity map, defaulting to empty:

```python
async def _resolve_ids(db, builders, customer_code,
                       cont: continuity.Continuity = continuity.EMPTY):
```

Its docstring then says, at `derive_transactions.py:572-573`:

> `cont` defaults to no predecessors, which reduces this to the previous behaviour.
> Only `regroup_window` can supply one, **because only it frees transactions.**

"Frees transactions" means deletes rows from `log_transactions`.
**That claim is false.**
Two other functions delete from the same table:

```python
# regroup_all, line 677
del_stmt = delete(LogTransaction)                                           # everything

# regroup_incremental, line 780
free_stmt = delete(LogTransaction).where(LogTransaction.sealed.is_(False))   # all unsealed
```

**And the same file contains three further comments that wrongly present `regroup_incremental` as the automated path.**
These are what an earlier revision of this document believed, so they are worth reporting upstream together:

| Location | What it says | Reality |
|---|---|---|
| `derive_transactions.py:11-13` | "regroup_incremental(db) LIVE path ... This is what the worker runs" | no worker calls it |
| `derive_transactions.py:775-776` | "None processes every customer with unassigned entries (what the background worker runs)" | the background worker calls `finalize_pending`, never this |
| `derive_transactions.py:778` | "this runs on the live path every cycle" | it runs when someone calls the endpoint |

**Why it matters.**
Because `regroup_all` and `regroup_incremental` never pass `cont`, they receive `EMPTY` and recompute ids from the anchor entry.
The `_resolve_ids` docstring implies that is safe on the grounds that they do not free transactions, which is untrue: both delete from `log_transactions`.

This is a comment defect, not a code defect.
Recomputing ids for unsealed rows may be a deliberate scoping choice, since an unsealed record has arguably not established an identity worth preserving.
But the comment gives a false reason, which is worse than giving none.

The two defects compound in opposite directions, which is why both are listed.
The `_resolve_ids` docstring understates how many paths delete, so a reader thinks id stability is protected everywhere.
The three "live path" comments overstate which path runs, so a reader thinks the unprotected path is the hot one.
Together they produce exactly the wrong mental model: id churn everywhere, on the busiest path. The truth is the opposite.

### Why an id change costs this design nothing

This is the part worth internalising, because it is the strongest argument for the range diff.

Picture a shop where every sale gets a numbered receipt.
Numbers are normally stable, but occasionally, when the books are redone, a sale is renumbered: its old number vanishes and a new one appears.

If the method were "look up receipt 47 and update it", a renumbering is unrecoverable.
Receipt 47 no longer exists, so nothing corrects it, and its old amount stays in the total forever.

Our method is "take every receipt in the 10:00 to 10:30 window, compare against what the books now say for that window, and apply the difference".
Under that method a renumbering is invisible.
The old number is absent from the source so it is reversed, the new number is absent from our records so it is applied, and the net effect is zero without anyone writing special handling for it.

An id change is simply two rows of the diff table firing together:

| Situation | Action | On an id change |
|---|---|---|
| id in both, fingerprint equal | skip | |
| id in both, fingerprint differs | reverse old, apply new | |
| **id in our facts, absent from source** | **reverse** | the departed id |
| **id in source, absent from our facts** | **apply** | the arrived id |

Identical handling to a **merge**, where two records combine so an id disappears, and to a **split**, where one divides so an id appears.

**The failure mode this avoids.**
A per-id update, `UPDATE ... WHERE source_id = ?`, passes a test where a quantity changes and **fails silently** when an id vanishes.
Nothing looks for the departed id, so its contribution stays in the total permanently and no error is raised.
That is why the range diff is a hard requirement rather than an implementation preference, and why verification item 4 exists separately from item 3.

**The one real consequence, corrected 2026-08-21.**
`continuity.py` reads as though ids are now stable, and on the automated path they are.
The path that runs unattended is `regroup_window`, and it does pass `cont` (`:858-861`, `:895`), so ids are preserved through every ordinary rebuild.

An earlier revision of this section said the opposite: that ids were "stable on backfill only" and therefore `analytics_fact_ledger` would churn more than `continuity.py` suggests.
That followed from believing `regroup_incremental` was the live path, and it is wrong in the safe direction.
Expect **less** ledger churn than the earlier estimate, not more.

Id churn is still possible, but only when someone invokes `POST /logs/regroup` or the date-range delete, so it is operator-driven and occasional rather than continuous.
The range diff absorbs it either way, which is the whole point of the section above.
Nothing about the design changes; only the volume estimate moves, and it moves down.

If the deferred update-in-place change ever ships, DELETE and INSERT becomes UPDATE, ids stop churning, and most of this churn disappears.
That is the same upstream change that would break the retention cursor described in F6.
One decision, two effects on us, which is why it is tracked as a dependency rather than a footnote.

**It shipped as a decision on 2026-08-23 - see section 18.** Both effects are now live obligations: the churn estimate moves down, and `_FRONTIER_COLUMN` must move to `updated_at` before the frontier stalls.

**Source uniqueness is `UNIQUE NULLS NOT DISTINCT (id, started_at)`, not unique on id.**
Confirmed in `log_transaction.py:43-44` and migration `a1f6d70b3e92:126`.
`started_at` is the partition key and nullable, and a primary key would force it `NOT NULL`, making timestamp-less rows un-insertable.

**Live checks, 2026-08-17.**
NULL `started_at`: 0.
Duplicate id pairs: 0.
NULL `ended_at`: 0.
Unsealed at any moment: 1,638.
`log_regroup_pending`: 7,407 tickets with 0 pending, 0 abandoned, 0 retries, roughly one every 70 seconds.

**Real data loss, present today.**
847 to 1,079 `log_entries` rows have no row in `log_entry_assignment`, all past the abandon window, from two files (`TMP-AZ-BEC02/eSmartServerLog.txt`, `TMP-AZ-BEC01/eSmartServerLog.txt`).
Only 7 are pick-related.

**Infrastructure.**
Stock PostgreSQL 16, 48 available extensions, with **no TimescaleDB, Citus, columnar, hll, tdigest or pg_partman**.
`work_mem` is 4 MB, `shared_buffers` 8 GB.
`partitioning.py` supported daily bounds only.
**Resolved 2026-08-21**: it now carries a per-table `Grain` (daily, monthly, yearly), and
`log_partition_worker` gained `KEEP_FOREVER` and per-table `RETENTION_DAYS`.
Both were prerequisites for Phase 1 rather than part of it: registering a monthly, retained-forever
table under the old code produced daily partitions and had the retention worker drop them at 60 days.
See `tests/test_partition_grains_chunk36.py`.
The frontend has no chart library, and `next.config.mjs` needs a rewrite entry or `/api/v1/analytics/*` returns 404.

---

# 4. Naming

Final scheme, after two rejected attempts.

| Plan name | Final name |
|---|---|
| `warehouse_analytics_dirty_window` | `analytics_pending_windows` |
| `warehouse_consumption_contributions` | `analytics_facts` |
| `warehouse_consumption_contribution_history` | `analytics_fact_ledger` |
| `warehouse_item_consumption_hourly` | `analytics_hourly_rollups` |
| `warehouse_item_consumption_daily` | `analytics_daily_rollups` |
| `warehouse_item_consumption_monthly` | `analytics_monthly_rollups` |
| (new) | `analytics_metrics` |
| `warehouse_analytics_state` | `analytics_tenant_state` |
| `warehouse_analytics_quality_issues` | `analytics_quality_issues` |
| (new, ML) | `analytics_feature_sets`, `analytics_predictions` |

**The prefix is `analytics_`, not `warehouse_analytics_`.**
A 20-character prefix breaks PostgreSQL's 63-character identifier limit once `partitioning.py` appends a date and the concurrent-index recipe appends columns.
`partition_name` raises past the limit, so this fails at partition creation rather than quietly.

```
80  OVER  warehouse_analytics_rollup_hourly_2026_08_18_customer_code_bucket_start_item_idx
64  OVER  warehouse_analytics_rollup_hourly_2026_08_18_customer_bucket_idx
55  ok    analytics_hourly_rollups_2026_08_18_customer_bucket_idx
```

`analytics_` also matches the form of the existing `notification_` and `log_` prefixes, which use a single domain noun.

**Names are plural**, because the repo is predominantly plural: `log_entries`, `notification_rules`, `consumer_cursors`, `log_regroup_runs`, `saved_views`, `jobs`, `customers`, against only `log_entry_assignment`, `log_regroup_pending` and `logspace_presence`.

**`analytics_pending_windows`, not `dirty_window`.**
"Dirty window" is imported data-engineering jargon.
The repo's equivalent is `log_regroup_pending`, named for what is pending rather than for a term of art.

**`facts` and `fact_ledger`, not `contributions`.**
These are not synonyms and the distinction is load-bearing.

- `contributions` means "what this row contributed to a total", which is too narrow now that 47 of 49 methods carry no quantity.
- `ledger` implies entries and their reversals side by side, but `analytics_facts` holds one current row per transaction; the reversal is a computation, not a stored row.
- `facts` means one row per event with dimensions and measures, accurate for all 49 methods.

"Ledger" **is** right for the history table, which is genuinely append-only with one row per version.
Hence `analytics_fact_ledger`.

**Module names follow the tables.**
The repo pairs `services/notifications/` with `notification_*` tables and `notification_worker.py`.
The parallel is `services/analytics/` with `analytics_*` tables and `analytics_worker.py`.

**Still reversible.**
Renaming before Phase 1 costs nothing; renaming after the first migration costs a migration.

---

# 5. Component map (ITERATION 1)

**Everything in this section is built and running.**
For what is being added, see the iteration-2 map in section 18d.

Fifteen components.
Eight already exist and are reused unchanged, six are new, one is a later phase.
The document-RAG side of this codebase (embedding worker, `chunks`, `embedding_queue`) is out of scope and not shown.

| # | Component | Status | Module |
|---|---|---|---|
| E1 | SSH log fetcher | exists | `workers/ssh_log_fetcher.py`, `mnp_log_ingestion/remote/remote_fetcher.py` |
| E2 | Parse worker, Stage 1 | exists | `workers/log_parse_worker.py`, `pipeline/parse_insert.py` |
| E3 | Stitch worker, Stage 2 | exists | `workers/log_stitch_worker.py`, `pipeline/derive_transactions.py` |
| E4 | Partition manager | exists, **extended 2026-08-21** for per-table grain and keep-forever retention (see E4 detail) | `persistence/partitioning.py`, `workers/log_partition_worker.py` |
| E5 | Alerting engine | exists | `services/notifications/` |
| E6 | Logspace cleanup | exists | `workers/logspace_cleanup_worker.py`, `services/logspace_cleanup.py` |
| E7 | Retention cursor registry | exists | `services/consumer_cursors.py` |
| E8 | Log watcher, local-directory input | exists | `workers/log_watcher.py` |
| N1 | Ticket publisher | new | `services/analytics/pending_windows.py` |
| N2 | Fact normaliser | new | `services/analytics/normalizer.py` |
| N3 | Analytics worker | new | `services/workers/analytics_worker.py` |
| N4 | Metric registry | new | `services/analytics/registry.py` |
| N5 | Rollup folder | new | `services/analytics/fold.py` |
| N6 | Read layer | new | `persistence/repositories/analytics_repository.py` |
| N7 | API and agent tools | new | `api/v1/analytics.py`, additions to `services/log_agent/tools.py` |
| M1 | ML pipeline | later | `services/analytics_ml/` |

# 6. Table ownership

**For every analytics table the rule is absolute: exactly one component may write it.**
Two writers to one aggregate is how totals silently diverge, and it is not recoverable without a full rebuild.
The `Also written by` column is empty for all eleven analytics tables, and that is a constraint to enforce in review, not an observation.

**The existing log tables do not follow that rule, and pretending otherwise would be misleading.**
Each has a primary writer, but E6 logspace cleanup deletes from eight of them and the API offers purge endpoints.
The one-writer rule is what the new work commits to, not a property inherited from the pipeline.

**Corrected 2026-08-21.**
An earlier revision said "E6 deletes from every non-partitioned log table but touches neither `log_entries` nor `log_transactions`."
That is false.
`logspace_cleanup.py:114` deletes the tenant's `jobs`, and `log_entries` and `log_transactions` go with them via `ON DELETE CASCADE`.
The file's own cascade map states this at `logspace_cleanup.py:7-9`.
See F13.

E4 dropping whole partitions is how those two tables are reclaimed *on the retention path*, which is why retention differs between the two groups.
A tenant purge is a different thing and removes them outright.

| Table | Primary writer | Also written by | Read by | Partitioned | Retention |
|---|---|---|---|---|---|
| `log_ssh_sources` | E1 fetcher, status | log-sources API, E6 delete | E1 | no | until deleted |
| `log_ssh_file_checkpoints` | **E1 fetcher**, upsert | E6 delete | E1 | no | 30 days per `ssh_checkpoint_retention_days` |
| `log_source_objects` | **E1 fetcher**, insert | E2 lease and status, API and E6 delete | E1, E2 | no | until cleaned |
| `log_ssh_fetch_runs` | E1 fetcher, run status | E6 delete | API | no | until cleaned |
| `jobs` | E2 parse worker | document pipeline, embedding worker, API and E6 delete | API | no | until cleaned |
| `log_entries` | **E2 parse worker** | API purge, **and E6 tenant purge via the `jobs` cascade** (F13) | E3, N3 | daily on `timestamp` | 60 days, partition drop |
| `log_regroup_pending` | **E2 parse worker**, insert | E3 stamps `consumed_at`, E6 delete | E3 | no | until cleaned |
| `log_entry_assignment` | **E3 stitcher** | E6 delete | E3, N3 | daily on `entry_ts` | 60 days, partition drop |
| `log_transactions` | **E3 stitcher**, delete and insert | API purge, **and E6 tenant purge via the `jobs` cascade** (F13) | E3, N3, N6 | daily on `started_at` | 60 days, partition drop |
| `log_regroup_runs` | E3 stitcher, run status | E6 delete | API | no | until cleaned |
| `consumer_cursors` | E5, N3, M1, one row each | none | E4 | no | forever |
| `analytics_pending_windows` | **N1 only** | none | N3 | no | pruned after consume |
| `analytics_facts` | **N3 only** | none | N5, N6, M1 | monthly on `event_time` | **forever** |
| `analytics_fact_ledger` | **N3 only** | none | M1 | monthly on `recorded_at` | **forever** |
| `analytics_metrics` | **N7 only** | none | N3, N5, N6 | no | forever |
| `analytics_hourly_rollups` | **N5 only** | none | N5, N6 | daily on `bucket_start` | 90 days |
| `analytics_daily_rollups` | **N5 only** | none | N5, N6 | yearly on `business_date` | **forever** |
| `analytics_monthly_rollups` | **N5 only** | none | N6 | none needed | **forever** |
| `analytics_tenant_state` | **N3 only** | none | N6, N7 | no | forever |
| `analytics_quality_issues` | **N3 only** | none | N6, N7 | monthly | 1 year |
| `analytics_feature_sets` | **M1 only** | none | M1 | monthly | forever |
| `analytics_predictions` | **M1 only** | none | N6, M1 | monthly | forever |

`consumer_cursors` has three writers, but each owns a distinct row keyed by consumer name (`analytics:warehouse-v1`, `ml:features-v1`, `notifications`), so there is no contention.
That pattern is already established in the codebase.

**No component writes to any `log_*` table.**
The analytics platform is strictly a reader of the ingestion pipeline, with the single exception of N1 inserting a ticket inside E3's own transaction.

## Four position-tracking mechanisms, easily confused

The pipeline has **four** independent ways of remembering "how far have I got", and they answer different questions.
Assuming `consumer_cursors` covers ingestion is a natural mistake and a wrong one.

| Mechanism | Question it answers | Keyed by | Owner | If lost |
|---|---|---|---|---|
| `log_ssh_file_checkpoints` | how many **bytes** of this remote file have I pulled | `(source_id, remote_path)` | E1 | re-reads the file; costs bandwidth, not data |
| `log_source_objects` lease | who is **currently parsing** this downloaded byte range | `status`, `lease_owner`, `lease_expires_at` | E2 | the row returns to `pending` and is retried |
| `log_regroup_pending` | which **time windows** still need stitching | ticket rows, `consumed_at` | E2 writes, E3 consumes | that window is never re-stitched |
| `consumer_cursors` | how far each **reader of `log_transactions`** has got | consumer name | E5, N3, M1 | retention drops data the reader still needs |

Three properties worth carrying into the analytics design.

**The byte checkpoint is an optimisation, not a correctness mechanism.**
`log_ssh_file_checkpoint.py` says so directly: correctness comes from `entry_hash` content dedup in Stage 1 regardless.
It also trims each read to the last newline so a partial trailing line is never ingested, and resets to zero when a file shrinks, which is how rotation is handled.

**The checkpoint alone would lose data, and the fix is the pattern we reuse.**
From `log_source_object.py`: the fetcher inserts the source-object row and advances the checkpoint in **one transaction**, because "a crash between checkpoint advanced and entries inserted would skip those bytes forever".
That is the same reasoning as writing the analytics ticket inside the stitcher's transaction.
The problem has already been solved once here; N1 is applying the established pattern rather than inventing one.

**The checkpoint is overwritten as a file advances**, so it cannot answer which file version and byte range produced a given entry.
`log_source_objects` exists partly to preserve that provenance, with path, offsets, size, mtime and fingerprint.

The analytics module adds a **fifth** of its own, `analytics_pending_windows`, deliberately shaped like `log_regroup_pending` and explained under N1.

# 7. Component detail

## E4. Partition manager (exists, extended 2026-08-21)

Owned by `app/persistence/partitioning.py` (pure: registry, names, bounds, arithmetic) and
`app/services/workers/log_partition_worker.py` (the hourly loop and the retention policy).
Listed here rather than left implicit under "exists" because **every partitioned analytics table is
registered with it, and a mistake in that registration destroys data no other component can rebuild.**

It has two jobs per tick, both idempotent.
**Create** provisions today through today + `log_partition_precreate_days` (14).
This half must not fail quietly: an insert into a period with no partition fails outright, so an
exhausted runway stops Stage 1 dead.
**Drop** reclaims disk by unlinking a partition rather than `DELETE` + `VACUUM`, which is why these
tables were partitioned at all.

### What was extended, and why it had to be before Phase 1

The module was daily in every function: the name format, the FROM/TO bounds, the coverage arithmetic
and the expiry comparison.
Retention was a single setting applied to every registered table.
This plan partitions seven analytics tables: **five monthly** (`analytics_facts`, `analytics_fact_ledger`,
`analytics_quality_issues`, `analytics_feature_sets`, `analytics_predictions`), **one yearly**
(`analytics_daily_rollups`) and **one daily** (`analytics_hourly_rollups`).
Five of the seven are retained forever.
Registering them under the old code would have done two silent, destructive things: created daily partitions instead of
monthly ones, and had the worker drop the forever tables at 60 days, a whole month at a time.
Raw data is gone at 60 days, so a dropped fact partition cannot be rebuilt from anything.

Three bugs only exist once grains do, and all three were fixed with the extension:

| Bug | Effect if left | Fix |
|---|---|---|
| expiry compared the partition's FIRST day | January droppable on 2 March under 60-day retention, throwing away 30 in-policy days | `expired_days` compares `period_end` |
| the retention gates protected a DAY | a window or reader working on 15 August would not hold the August partition | gates keyed on `(table, period_start)` and compared against the period's span |
| runway measured to the partition's START | a monthly table reports 0 days of runway on the 1st, tripping the CRITICAL alarm all month | `_runway_for` measures to `period_end` |

### The interface Phase 1 registers against

```python
# app/persistence/partitioning.py
class Grain(str, enum.Enum):
    daily = "daily"; monthly = "monthly"; yearly = "yearly"

PartitionedTable(table="analytics_facts", key="event_time",
                 note="...", grain=Grain.monthly)

# app/services/workers/log_partition_worker.py
KEEP_FOREVER: frozenset[str]   # never dropped, whatever retention is configured
RETENTION_DAYS: dict[str, int] # per-table override in days, ignored if in KEEP_FOREVER
```

Both collections are **empty today**, which is what makes the extension provably behaviour-preserving
for the three log tables: their retention arithmetic is unchanged, and at a daily grain
`period_start` and `period_end` are the identity, so every new comparison collapses to the old one.
All three are explicitly `grain=Grain.daily` at their definition site, and a test asserts they still
are, so a future change to one of them fails loudly instead of reshaping live retention.

Verified by `tests/test_partition_grains_chunk36.py` (31 tests) plus the pre-existing
`test_partitioning_chunk23`, `test_partition_worker_chunk25`, `test_partition_status_chunk27` and
`test_consumer_cursors_chunk34`.
Every public signature the migration `a1f6d70b3e92`, `alembic/env.py` and the status endpoint depend on
is unchanged.

### What Phase 1 must decide per table

Grain, retention, and the DEFAULT partition.
The third is not optional: every partition key in this schema is nullable, and without a DEFAULT an
insert of a NULL key fails outright and takes the batch with it.

## N1. Ticket publisher

Records that a bounded event-time range of a tenant's transactions changed.
Runs in the **same database transaction as the change**, not as a separate process.

**Publishes from five sites, corrected 2026-08-21.**
An earlier revision listed three, all in `derive_transactions.py`.
Two more exist in the API, and both remove `log_transactions` rows.
The rule that decides the list is not "where does Stage 2 rebuild" but "where does a row leave `log_transactions`", and a grep for that (`delete(LogTransaction)`, plus the `jobs` cascade) returns five.

| # | Site | Bounds | Notes |
|---|---|---|---|
| 1 | `regroup_window` (`derive_transactions.py:864`) | the padded window it already computes at `:837-838` | the automated path; bounds are free |
| 2 | `regroup_incremental` (`:780`) | min and max `started_at` over the **freed unsealed set**, padded | the select at `:779` returns ids only, so add `started_at`; insert the ticket before the commit at `:787` (F1) |
| 3 | `regroup_all` (`:677`) | min and max over everything freed, padded, or one ticket per day of the span | insert before the commit at `:681` |
| 4 | **date-range delete** (`api/v1/logs.py:723`) | min and max `started_at` over the rows `txn_conds` selects, padded | see below |
| 5 | **full wipe, via cascade** (`api/v1/logs.py:681-682`) | the tenant's whole `started_at` span, or one ticket per day | see below; **this one has no delete statement to hook** |

### The two API sites, and why site 5 is the awkward one

Both live in `DELETE /logs/data`.

**Site 4, the date-range delete**, is an ordinary `delete(LogTransaction)` and needs nothing unusual: select the affected `started_at` range first, publish, then delete, all in the transaction that already exists there.
Note it does call `regroup_incremental` afterwards (`logs.py:728`), and that does **not** cover it: site 2's bounds come from the freed unsealed set, and these rows are already gone, so they are not in it.

**Site 5, the full wipe**, deletes `jobs` and lets the database remove the transactions:

```python
# api/v1/logs.py:681-682
res = await db.execute(delete(Job).where(
    Job.customer_code == customer, Job.document_type == DOCUMENT_TYPE))
```

They disappear because of `log_transaction.py:64`:

```python
job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
```

So there is **no `log_transactions` statement to attach a ticket to**: the removal happens inside PostgreSQL.
The ticket must therefore be published **before** the `delete(Job)`, while the rows are still readable, and it must cover the tenant's whole span rather than a computed window.
The endpoint already counts the transactions first (`logs.py:675-676`), so the span is one extra aggregate beside a query that is already being run.

**Why this cannot be deferred.**
`analytics_facts` is retained **forever** and raw data is dropped at 60 days.
A purge with no ticket leaves the purged contribution in every total permanently, with no error and, after 60 days, nothing left to recount against.
It is the only defect in this design that erases its own evidence.

Insert-only.
No foreign key, no unique constraint a retry could violate, no trigger, because a failure here fails ingestion.

Table shape mirrors `log_regroup_pending` field for field: `id`, `customer_code`, `range_start`, `range_end`, `created_at`, `consumed_at`, `attempts`, `last_error`, `last_attempt_at`, `abandoned_at`, `available_at`, with index `(customer_code, consumed_at)`.

It is a **separate table**, not a shared one, for two reasons.
`consumed_at` is single-consumer, so a second consumer stamping it means whichever runs second finds the window closed and skips work it never did.
And `log_regroup_pending` rows are written in Stage 1 per ingested file (`parse_insert.py:188-190`) from `log_entries.timestamp`, so they describe ingest ranges and never cover `regroup_incremental`.

## N2. Fact normaliser

One `log_transactions` row plus its `attributes` JSONB to one typed fact row, or a quarantine record with a reason.

**Pure: no database, no clock, no configuration.**
This is where correctness is won, which is why the module has no I/O.

- Reject placeholder `transaction_type` values.
- Cast quantities to `NUMERIC`, never float, treating empty string as **absent** rather than zero.
- Classify as `pick` (quantity above zero), `attempt` (quantity zero), or non-quantity.
- Compute `business_date` in the tenant timezone.
- Compute the version fingerprint over every field affecting a measure, **excluding the source row's `created_at`**, which Stage 2 refreshes on every rebuild by construction. Including it would make every fingerprint differ every cycle, so no recheck could ever be absorbed as a no-op and the 98.7% rebuild rate would become a write rate.
- A transaction with **no `method`** is a `non_quantity` fact, never a quarantine (added 2026-08-22, C7). 25 of 397 live transactions have none and they are real activity, so quarantining them would hide 6.3% of transactions from every volume, duration and status metric while the totals still looked plausible.

Quarantine is therefore narrow by design: the only reason is a quantity-bearing method whose quantity cannot be read.

**Absent is never zero.**
A missing field means "not supplied", a different fact from zero, and it must survive as such.

## N3. Analytics worker

Turns tickets into fact rows and aggregate deltas, exactly once in effect.
Runs inside the existing singleton `python -m app.worker`, never in the four web workers, which stay read-only for analytics.

One cycle, one transaction per RUN (corrected 2026-08-22, D6; this said "a single transaction per tenant"):

1. Claim available tickets: `consumed_at IS NULL AND abandoned_at IS NULL AND available_at <= clock_timestamp()`.
   **Not `now()`**, which is fixed at transaction start and would make a fresh row look permanently not-yet-due.
2. Coalesce ranges into disjoint runs.
3. Take `pg_advisory_xact_lock(hashtext('analytics:' || customer_code))`.
   **Distinct from the stitcher's `hashtext(customer_code)`**, or a slow fold would stall log stitching.
4. `SET LOCAL work_mem = '64MB'`.
5. Read current `log_transactions` in range, carrying `UtcWindow.covers(..., include_null=True)`.
6. Normalise via N2.
7. Read existing `analytics_facts` rows in the **same** range.
8. **Range diff**, never per-row upsert.
9. Apply outcomes, append changed versions to `analytics_fact_ledger`, hand deltas to N5, write quarantine rows.
10. Update `analytics_tenant_state`, publish the retention position, stamp tickets consumed.

The diff:

| Condition | Action | Total |
|---|---|---|
| in both, fingerprint equal | skip | unchanged |
| in both, fingerprint differs | reverse old, apply new | moves by the difference |
| **in facts, absent from source** | **reverse** | decreases |
| in source, absent from facts | apply | increases |

The third row is why the diff must span a range.
A merge makes one id disappear, and a per-id update would never look for it, leaving its contribution stranded permanently.
Splits are the mirror image.

**Failure policy.**
A poisoned row is quarantined and the tenant continues.
A failed run leaves its tickets open, bumps `attempts`, and dead-letters at the configured maximum.
One bad row never halts a tenant, following the precedent in `consumer_cursors.py`.

**Retention position.**
The maximum `created_at` observed among fully processed rows, stored **per tenant** on `analytics_tenant_state.source_write_frontier`, and published under `analytics:warehouse-v1` as the **MINIMUM across tenants** (corrected 2026-08-22, D5; this originally published the maximum directly).
Per tenant because `consumer_cursors` holds one row per consumer while retention is global: a tenant that is far ahead must not speak for one that is far behind.
A tenant that has processed nothing has a NULL frontier and suppresses publishing entirely.
See also D9, a unit mismatch in this mechanism that is being carried deliberately.
Held in a **single named constant**, because a deferred upstream change to update-in-place would require switching to `updated_at`.
**That change was approved on 2026-08-23 (section 18), so this switch is now required rather than hypothetical.**

## N4. Metric registry

Holds what is measured, as data rather than code.

Definition row: `id`, `customer_code`, `name`, `dimensions`, `measure`, `filter`, `grains`, `status` in `draft`/`active`/`inactive`, `created_by`, `backfilled_through`.
Each measure carries `name`, `aggregation`, `field`, a classification filter (`only`) and a **status filter** (`statuses`), added 2026-08-22 (C6) because an errored or incomplete pick carries a full quantity and would otherwise be counted.
Both filters are per MEASURE, not per definition, so one definition can hold a total and an error count that differ only by which rows they admit.
Consumption admits `success` only.
`soft` is excluded on the reading that a not-found response means the confirmation never registered in the system of record, and the choice is safe to make strictly because zero of the 69 live `soft` transactions carry a quantity-bearing method (C8).
Shape follows `NotificationRule`, which already proved the pattern here.
Dispatch must be a **registry, not an if-chain**, unlike `build_evaluator` today.

Validation the registry enforces:

- A quantity measure may only be registered where the methods actually carry quantities, and only 3 of 49 do (C1). Enforced against `contract.QUANTITY_FIELD` so the registry and the contract cannot disagree.
- Dimensions must exist on the fact row.
- A definition cannot go active until its backfill has run, or its chart shows a false start date.

## N5. Rollup folder

Maintains the grain cascade per active definition: `facts → hourly → daily → monthly`.
Hourly and daily are folded from the facts; monthly reads the level below (corrected 2026-08-22, D7; this said every level reads only the level below).
The fact table is still read only once per cycle: the dirty facts are read once and folded into both grains in a single pass.
Daily cannot come from hourly because hourly buckets are UTC hours while daily buckets are the tenant-local `business_date`, and at a half-hour UTC offset one hour per day straddles two local dates.

**Every write is recompute-and-replace, never increment.**
An additive upsert double-counts on the first retry.

**Additive components only.**
Sums and counts direct.
Averages as `sum` plus `count`.
Variance as `sum`, `sum_sq`, `count`.
Percentiles as a 20-bucket log histogram, because bucket counts add and percentiles do not.
Distinct counts are **not cascaded**; they are computed per period from the fact table, cheap because the fact table and the monthly grain share a partition boundary.

**Weekly has no table.**
ISO Monday weeks derive from daily at read time.

## N6. Read layer

Answers every question, and is the only component that does.

**Grain selection.**
The coarsest grain covering the window, targeting under 100,000 rows scanned.
A twelve-month request resolves to monthly, never daily.

**Two-tier read.**
Pre-aggregated rollups for settled ranges, unioned with a bounded live scan of the recent tail.
Both halves use **one boundary value read from the persisted cursor**, never a freshly computed one, or a lagging worker produces double counts or gaps.

**Ad-hoc fallback.**
A query no definition covers falls back to a bounded fact-table scan, and the response marks itself as such so the interface can show it rather than silently running slow.

All queries parameterised so asyncpg prepares them, worth roughly 100 ms per call.

## N7. API and agent tools

| Endpoint | Notes |
|---|---|
| `GET /analytics/status` | **exactly one row read** plus `ETag`, because the browser polls every 2 s per tab |
| `GET /analytics/metrics` | list and manage definitions |
| `POST /analytics/metrics` | create a definition, returns `202` with a backfill job |
| `GET /analytics/series` | one series per selected type, or one combined total, toggleable |
| `GET /analytics/breakdown` | top-N by dimension for a window |
| `POST /analytics/backfill`, `/reconcile` | `202 Accepted`, tracked |

Every endpoint uses `Depends(get_current_customer)` and `Depends(get_session)`.
Keyset pagination with default and hard maximum limits, no default `COUNT(*)`.

**Agent tools** added to `TOOLS` and `_DISPATCH` in `services/log_agent/tools.py`.
The agent already exists as a Claude tool-use loop at `POST /api/v1/logs/debug/ask`, with `customer_code` injected server-side and never model-exposed.
Because metrics are registry rows, the tools are generic: `list_metrics`, `query_metric`, `explain_freshness`.
No new tool per metric.

**Frontend** needs a `next.config.mjs` rewrite for `/api/v1/analytics/*`.
Charts are hand-built inline SVG; the repo has no chart library and adding one would be its first in two years.
`src/components/notifications/ActivityTab.tsx` is the only existing table-plus-tiles-plus-polling page and is the template.

## M1. ML pipeline

Reads `analytics_fact_ledger` at a **pinned revision**, not the current fact table.
That is what makes a training run repeatable months later, and it is why the ledger must exist from day one rather than being added when ML starts.

Writes `analytics_feature_sets` (features plus the pinned revision and a code version) and `analytics_predictions` (output keyed by subject, horizon and model version).
Registers its own cursor `ml:features-v1`.

Predictions are served by N6 through the same read layer, so a forecast and an actual are never fetched by two code paths that could disagree.
Anomaly detection reuses E5 rather than a parallel alerting path.

# 8. Data flows

## Flow A: change to ticket

```
log_ssh_sources ─► E1 fetcher
                     │   ONE transaction
                     ├─► log_ssh_file_checkpoints    byte offset advanced
                     └─► log_source_objects          durable handoff, work queue
                              │  E2 leases the row
                              ▼
                         E2 parse ──► log_entries
                                  ├─► jobs
                                  └─► log_regroup_pending      Stage 1 ticket
                                             │  E3 consumes
                                             ▼
     E8 watcher ──► log_entries          E3 stitch ──► log_transactions
     (staging dir, second                          ├─► log_entry_assignment
      Stage 1 path)                                └─► N1 ─► analytics_pending_windows
                                                        ONE transaction with the change
```

The ticket and the change commit together or neither commits.
That single property is what makes the coverage argument hold.

## Flow B: ticket to fact

```
pending_windows ─► N3 claim + coalesce + lock
                     ├─read─► log_transactions      (current truth for the range)
                     ├─read─► analytics_facts       (existing rows, same range)
                     ├─► N2 normalise
                     ├─► range diff
                     ├─write─► analytics_facts          (upsert)
                     ├─write─► analytics_fact_ledger    (append)
                     ├─write─► analytics_quality_issues
                     ├─► N5 apply deltas
                     ├─write─► analytics_tenant_state
                     ├─write─► consumer_cursors
                     └─write─► pending_windows.consumed_at
                     ────────── one transaction ──────────
```

## Flow C: fact to rollups

```
analytics_facts
        │  per active definition in analytics_metrics
        ▼
  hourly_rollups ──► daily_rollups ──► monthly_rollups
                          │
                     weekly derived at read time, no table
```

Any level can be deleted and rebuilt from the fact table, which is what makes rollups safe to treat as disposable.

## Flow D: read

```
request ─► N7 ─► N6
                  ├─ pick coarsest grain covering the window
                  ├─read─► monthly | daily | hourly rollups   (settled)
                  ├─read─► log_transactions tail              (live, bounded)
                  ├─ union on ONE boundary from consumer_cursors
                  └─read─► analytics_tenant_state             (freshness)
                                    │
                        ┌───────────┴───────────┐
                     Frontend                Agent tools
```

## Flow E: ML training

```
analytics_fact_ledger @ pinned revision
        ├─► M1 build features ─► analytics_feature_sets
        ├─► M1 train ─────────► model artifact on disk
        └─► M1 infer ─────────► analytics_predictions ─► N6 ─► N7
```

## Flow F: retention safety

```
N3 ─► consumer_cursors('analytics:warehouse-v1')  ┐
M1 ─► consumer_cursors('ml:features-v1')          ├─► E4 min position
E5 ─► consumer_cursors('notifications')           ┘        │
                                                           ▼
                                       drop partitions older than min
```

A component that stops reporting is excluded and logged critical rather than blocking retention forever.
That trade is already decided in `consumer_cursors.py`.

---

# 9. The thirteen corrections

F1 to F11 were defects found in the 2026-08-11 plan by code reading and live measurement.
F12 and F13 were found on 2026-08-21, after this document was written, by grepping for every statement that removes a `log_transactions` row rather than reasoning from the rebuild paths.
Both are coverage gaps of the same kind, but they need different fixes, which is why they are separate entries.

**F1. Ticket bounds must come from the delete, not the new entries.**
`regroup_incremental` deletes `WHERE sealed IS FALSE` with no time predicate (`:779-780`).
Bounds inferred from incoming entries would miss an older unsealed row caught in the same delete, which then drifts permanently.
Fix: compute bounds from the set actually freed, which is already selected before the delete.
Note the select at `:779` returns ids only, so it needs `started_at` added to derive the bounds, and the ticket must be inserted before the commit at `:787`.

**Priority corrected 2026-08-21.**
An earlier revision said "this path runs every 70 seconds", which is wrong: `regroup_incremental` is reachable only from `POST /logs/regroup?incremental=true` and the tail of the date-range delete.
It is an operator action, not a cadence, so F1 is no longer urgent.
It still has to be done, because the endpoint exists and deletes without a time bound, but it can follow the automated path rather than lead it.
The `regroup_window` publish site is the one that matters for continuous correctness, and its bounds are already computed at `:837-838`.

**F2. Reconciliation must also check completeness against `log_entries`.**
Recomputing from `log_transactions` cannot detect the roughly 1,000 orphaned entries, because both sides read the same incomplete projection and agree.
Fix: two scheduled checks.
Totals against a recount, plus a count of `log_entries` past the abandon window with no assignment row, grouped by source file and program.
**Immediate action:** run a scoped regroup over 2026-08-15 to 2026-08-17 for the two named files, and find the cause first, because whatever caused it will recur.

**F3. Ledger identity must mirror the source constraint.**
Source uniqueness is `(id, started_at)`, and two rows can share an id in different partitions silently.
Fix: key on `(customer_code, source_transaction_id, source_started_at)`.
Zero duplicate pairs exist today; the extra column costs nothing.

**F4. Freshness needs two numbers.**
The 5-second target measures analytics lag behind the projection, but records are not final for 1.7 h on average.
The screen could truthfully say "updated 2 seconds ago" about a number still due to move.
Fix: **copy freshness** (analytics watermark versus source watermark) and **settledness** (share of contributing records still unsealed, and the age of the oldest).
A window with unsealed contributors reads as *provisional*, not *stale*.

**F5. The status endpoint must read exactly one row.**
The browser polls every 2 s per tab across four web workers, and the response includes counts over other tables.
Fix: the worker writes every status field into the single `analytics_tenant_state` row each cycle.
A test asserts the endpoint issues exactly one query.

**F6. The retention cursor must publish a write-time position.**
Retention gating uses a write-time cursor, but the worker is driven by event-time ranges.
Fix: track the maximum `created_at` among fully processed rows and publish the minimum across tenants.
**Watch:** a deferred change to update-in-place would stop `created_at` moving and break this silently, so keep the field a single named constant.
**This watch has fired - see section 18.** Until the constant moves to `updated_at`, the frontier stalls and source partitions are held indefinitely.
That change would also collapse the 98.7% figure this plan is sized against.

**F7. Copy the existing ticket table, do not share it.**
Resolved in favour of a separate table with an identical shape.
See N1 for the two reasons.

**F8. Zero-quantity picks are attempts, not consumption.**
**Corrected 2026-08-21**, was a single figure of 9.2%. Measured now, and the denominator matters:

- **8.29%** of `ConfirmPickLine` confirmations alone (1,333 of 16,075) record zero units and still say success, typically an empty location.
- **7.47%** across all three quantity-carrying methods (2,056 of 27,511), which is what the consumption metric reports because its filter spans all three.

Quote whichever matches the denominator on screen; the two are not interchangeable. See C4.
Fix: three counters in every rollup.
`quantity` (sum of units), `pick_count` (confirmations above zero), `attempt_count` (all confirmations).
The zero-pick rate derives from the last two and is a first-class metric, because it names specific empty locations and replaces the fill rate this data cannot support.

**F9. Add daily and monthly grains, derive weekly.**
Hourly and lifetime only means a month-to-date question sums roughly 1.08 M rows.
See N5 and the sizing table below.

**F10. Keep the fact ledger, or machine learning is impossible later.**
A rebuild overwrites the previous value, so a training set is not reproducible and discarded versions cannot be recovered.
Fix: `analytics_facts` for the latest value, `analytics_fact_ledger` append-only for every version, retained longer than raw data.

**F11. Load testing needs a synthetic tenant.**
The exit criterion requires 100 times the measured rate, roughly 78,000 records an hour, with no described way to generate it.
Fix: a dedicated `synthetic-load` tenant whose generator drives Stage 2 normally, so tickets and rebuilds are genuinely exercised.
Excluded from every production read path and from alerting.
It also produces the defect fixtures, so correctness and load share one harness.

**F12. Two API delete paths also remove transactions, and both must publish a ticket.**
Added 2026-08-21.
N1 originally listed three publish sites, all in `derive_transactions.py`, derived from "where does Stage 2 rebuild".
The correct question is "where does a row leave `log_transactions`", and that has five answers: the three rebuild paths plus both halves of `DELETE /logs/data`.

- **Date-range delete** (`api/v1/logs.py:723`) issues an ordinary `delete(LogTransaction)`. The `regroup_incremental` call that follows at `:728` does not cover it, because those bounds come from the freed unsealed set and these rows are already gone.
- **Full wipe** (`api/v1/logs.py:681-682`) deletes `jobs` and the rows go via `ON DELETE CASCADE` (`log_transaction.py:64`). There is no `log_transactions` statement to hook, so the ticket must be published before the `delete(Job)`, covering the tenant's whole span.

Fix: publish from both, per the table in N1.
**Severity: this is the only defect in the design that erases its own evidence.**
A purge with no ticket leaves the purged contribution in every total permanently; `analytics_facts` is kept forever while raw data is dropped at 60 days, so after 60 days there is nothing left to recount against and no error was ever raised.
Retention's own partition drops (`log_partition_worker.py:146`) are deliberately NOT in this list: analytics keeps its history after raw data expires, which is the point of verification item 9.

**F13. A tenant purge must remove that tenant's analytics rows, not correct them.**
Added 2026-08-21.
`logspace_cleanup.purge_logspace` deletes the tenant's `jobs` (`logspace_cleanup.py:114`), and `log_entries` and `log_transactions` go with them via `ON DELETE CASCADE`.
It is reached two ways: `DELETE /api/v1/customers/{code}`, always available, and the E6 auto-expiry worker, which is **off by default** (`logspace_cleanup_worker_enabled: bool = False`).

**The fix is different in kind from F12, which is why it is a separate entry.**
F12 is about a window whose contents changed, so the answer is a ticket and a range diff.
Here the tenant itself is going away, so correcting its totals is meaningless: the right action is to delete its analytics rows outright.
Publishing a ticket would be actively wrong, since the worker would try to fold a tenant that no longer exists.

Fix: add every `analytics_*` table to the purge's cascade map.
That file already maintains the map explicitly and says why, at `logspace_cleanup.py:7-19`: "so this stays correct as the schema grows".
Note none of the analytics tables can rely on a cascade: they key on `customer_code`, which is a soft tenant key with no foreign key, exactly like `log_entry_assignment`, which that file already deletes explicitly at `:110-111` for the same reason.

**Severity: lower than F12, but it is a leak that grows.**
Nothing produces a wrong number; a purged tenant simply leaves its facts, ledger, rollups, tickets, state and quality rows behind forever, since every one of those is retained forever or pruned only on consume.
Disposable log spaces are created per trial, so the orphan count grows with tenant churn rather than with data volume.
It is also the one place where forgetting is invisible: the tenant is gone, so nobody looks at its rows again.

# 10. Additional hardening

| # | Item | Why |
|---|---|---|
| A1 | Quarantine must never halt a tenant | Halting on one bad row freezes every metric until a human intervenes |
| A2 | Do not reuse the stitcher's lock key | Sharing `hashtext(customer_code)` means a slow fold **stalls log stitching**. Note `pg_try_advisory_lock(0x7A9B, 1)` in `app/worker.py` is a two-argument lock in a separate space and cannot collide |
| A3 | Ticket table provably constraint-free | It is written inside the ingestion transaction, so a failed insert fails ingestion |
| A4 | Routine reconciliation must be windowed | Full recount grows with all retained history, becoming a job nobody runs |
| A5 | One authoritative revision | The tenant revision governs cache validation and must bump in the same commit |
| A6 | Normalise the missing-warehouse value once | Mapping null to a sentinel in two code paths lets one item's total split across two keys |
| A7 | Every source read carries the window predicate | Including `include_null=True`, because a range test is false for null |
| A8 | Align the worker cadence | Polling every second against 68 records per minute mostly finds nothing |
| A9 | `SET LOCAL work_mem = '64MB'` | The global 4 MB spills grouping to disk, and is shared with ingestion |
| A10 | Enable `pg_stat_statements` | Available, not installed. Without it, slow queries are guesswork |

# 11. Metric registry and grains

**The registry is the centre of the design, not an extension.**
The user chooses what is measured and how it is sliced, from the interface, and the measure list is not fixed now.
So nothing about dimensions or measures may be hardcoded into a rollup schema.

## The consequence that cannot be deferred

If measures are chosen later, the fact row must capture **every potentially useful field now**.
Raw data is dropped at 60 days, so a measure invented next year can only be backfilled across history if its fields were already being written.

**So the fact row is wide:**

| Group | Fields |
|---|---|
| Identity | `source_transaction_id`, `source_started_at`, `source_version_hash`, `revision` |
| Time | `event_time`, `business_date` (tenant-local), `duration_ms` |
| Operation | `method` (49 values), `transaction_name` (22), `transaction_type`, `status` |
| Subject | `item_number`, `lot_number`, `order_number`, `delivery_number` |
| Place | `warehouse`, `warehouse_id`, `from_location`, `to_location` |
| Actor | `user_name`, `device_id`, `device_name` |
| Measures | `quantity` where present, attempt and pick classification, plus a typed slot per registered measure |

Anything not written here is lost at 60 days.

## Dimensions

Aggregate by **both** `method` (49 values, API level) and `transaction_name` (22 values, the operator's screen).
Both are low cardinality, so together they stay under roughly 1,700 hourly rows per day.

## How a user-defined metric works

1. The user picks a dimension set, a measure, a filter and a grain in the interface.
2. That writes a definition row with a `draft`/`active`/`inactive` lifecycle.
3. The worker begins maintaining its rollups on the next cycle.
4. A backfill populates its history from the fact table, possible only because the fact row is wide and retained.

Rollups are stored generically: a definition identifier, a fixed number of dimension slots and additive measure slots, rather than a bespoke table per metric.
Adding a metric is a row plus a backfill, never a migration.

**The honest limit.**
Fully arbitrary slicing over years cannot be pre-aggregated, because the combinations explode.
Registered definitions are fast at any range; genuinely ad-hoc exploration falls back to a bounded fact-table scan, and the interface must show that distinction.
A newly defined metric has **no history until its backfill runs**, which should be visible in the interface.

## Grains

Grains are hourly, daily, weekly and monthly.
Weekly and monthly group on the **tenant-local business date with ISO Monday-start weeks**, because `date` is computed as `to_display(started).date()` (`derive_transactions.py:172`) while `started_at` is UTC, and for a UK warehouse those diverge by an hour for half the year.
Every query still carries a `started_at` predicate for partition pruning, because the local date is not the partition key (`logs.py:711-713`).

Sizing at 50,000 picks per day and 5,000 active items:

| Grain | Retention | Rows at 5 years, per measure | Rows at 5 years, consumption (3 measures) |
|---|---|---|---|
| Hourly | 90 days | 3.2 M | 9.6 M |
| Daily | forever | 9.1 M | 27.3 M |
| Monthly | forever | 300 K | 900 K |

**Revised 2026-08-21 (C5).** The original figures assumed one rollup row per definition per bucket.
Measure slots are now named for their additive ROLE (`sum_value`, `count_value`, `sum_sq`, `min_value`, `max_value`, `histogram`) rather than numbered, which makes invariant 8 structural: there is no column a finished answer could be written into.
The cost is that a definition needing one sum and two counts cannot share one set of role columns, so a rollup row is keyed per **(definition, measure)** and consumption emits three rows per bucket instead of one.
Multiply by the number of measures a definition carries.

Rows scanned for "top items this month": 1,500,000 raw, 1,080,000 hourly, 150,000 daily, **5,000 monthly**.

**Selection is toggleable.**
A chosen set of types renders either as one series per type or as one combined total, both valid from the same rollup because counts and sums are additive.
Percentiles across a selection need merged histograms, never averaged percentiles.

## Additivity is a schema rule

A rollup stores **components, never finished answers**.
Averaging twelve monthly averages is not the yearly average.

| Measure | Composes | Stored as |
|---|---|---|
| Quantity, pick count, attempt count | yes | directly |
| First and last event | yes | `min`, `max` |
| Average, rate | no | `sum` + `count` |
| Std dev, coefficient of variation | no | `sum`, `sum_sq`, `count` |
| Median, p95 | no | 20-bucket log histogram |
| Distinct items, operators, orders | no | computed per period from the fact table |
| Top-N | no | the full item set for the window |

No `hll` or `tdigest` is available, so distinct counts have no approximation to fall back on.

---

# 12. Delivery sequence

**Phase 0. Contract and fixtures.**
The consumption definition with the three counters.
Fixtures for zero pick, short pick, error, incomplete, late backfill, rebuild, **merge**, **split**, and a multi-confirmation line whose `ExpectedQuantity` changes.
Build the synthetic tenant generator here so it serves both correctness and load.

**Phase 1. Schema, models, ER diagram.**
Wide fact table with the F3 key, the ledger, generic per-definition grains, the definition table, quality, state, and the ticket table.
Register each partitioned table in `partitioning.PARTITIONED` with an **explicit `grain`**, and give it a retention policy in `log_partition_worker`.
`KEEP_FOREVER` takes the five partitioned tables retained forever: `analytics_facts`, `analytics_fact_ledger`, `analytics_daily_rollups`, `analytics_feature_sets`, `analytics_predictions`.
`RETENTION_DAYS` takes the two with a finite number: `analytics_hourly_rollups` at 90 days and `analytics_quality_issues` at a year.
Neither applies to `analytics_monthly_rollups`, `analytics_metrics`, `analytics_pending_windows` or `analytics_tenant_state`, which are not partitioned at all.
Both are new as of 2026-08-21; see the E4 component detail.
A table registered without a grain silently gets daily partitions, and one registered without a retention decision silently inherits the 60-day log retention.
**Add every new table to the purge cascade map in `logspace_cleanup.py` in the same change** (F13); a table created without deciding how it gets purged is a table that never does.
**Update `docs/database-er-diagram.md` in the same change**, per repo `CLAUDE.md`: the per-subsystem `erDiagram` block, the master overview, the tenant-partitioning diagram, and the relationship reference tables.
The width of the fact row is the load-bearing decision here, since omissions cannot be backfilled after 60 days.

**Phase 2. Ticket publication.**
From every path that creates, deletes or rebuilds, with bounds from the freed set.
**All five sites in the N1 table, including both halves of `DELETE /logs/data`** (F12), not only the three in `derive_transactions.py`.
Deployed with the worker disabled, so coverage is proven against real traffic while the only cost is a growing table.
Prove coverage by grepping for every statement that removes a `log_transactions` row and checking each against the N1 table, rather than by reasoning about which paths rebuild.

**Phase 3. Normaliser, diff, worker.**
Range diff, never per-record update.
Quarantine without halting, own lock namespace, write-time cursor, `work_mem` per transaction.

**Phase 4. Checks.** ~~Backfill and checks.~~
**There is no backfill** (decided 2026-08-22, D8): analytics counts from switch-on, and earlier history is not folded.
Windowed routine reconciliation plus explicit full runs, and the completeness check against `log_entries`.
Gate source retention on healthy state.
Every run provisions its own destination partitions before reading (C9), which is required regardless of D8 because `regroup_all` and rotated log files also produce facts with an old `event_time`.

**Phase 5. Read APIs.**
Grain selection, both freshness numbers, single-query status.

**Phase 6. Frontend.**
Provisional versus stale, hand-built charts, and the `next.config.mjs` rewrite.

**Phase 7. Rollout.**
Schema, then tickets with the worker off, then report-only reconciliation (~~then backfill and~~ - D8), then one tenant, then the interface behind a feature flag.

# 13. Invariants

Each fails **silently**, producing a plausible wrong number rather than an error.

1. Exactly one component writes each table.
2. No transaction is deleted by any path without a committed ticket whose range contains its `started_at`. **Including deletes the code does not issue directly**, such as the `jobs` cascade behind the full wipe (F12); retention's partition drops are the one deliberate exception.
3. Ticket and change commit in the same transaction.
4. A ticket is stamped consumed only after its entire range is diffed.
5. The diff spans a range, so a vanished id is reversed.
6. A matching fingerprint writes nothing, which is what makes retries free.
7. Rollup writes are recompute-and-replace, never increment.
8. Rollups store additive components, never finished answers.
9. Both halves of a read use one boundary value from the persisted cursor.
10. A missing source field is recorded as absent, never as zero.
11. The fact row is written wide, because omissions cannot be backfilled after 60 days.
12. Analytics uses its own advisory lock namespace, so it can never stall ingestion.
13. Quarantine never halts a tenant.
14. The analytics cursor field is one named constant.
15. A tenant purge removes that tenant's `analytics_*` rows outright, and does not publish a ticket (F13). Every analytics table keys on `customer_code` with no foreign key, so nothing cascades on its behalf.
16. Every partitioned table declares an explicit `grain`, and a table whose raw source expires before it does is registered `KEEP_FOREVER`. Both default silently to the log tables' policy, and a wrongly-dropped fact partition is the one loss in this design that nothing can rebuild.

# 14. Verification

**Correctness of the total**

1. Reconciliation: ledger totals equal a direct recount for the window, scheduled not one-off.
2. **Completeness**: entries past the abandon window with no assignment row, by file and program.
   Currently non-zero, so **this test starts red and that is correct**.
3. Restatement: rebuild an already-folded window and assert totals unchanged; change a quantity and assert the old contribution reverses exactly once.
4. **Merge and split**: a merged record's vanished id reverses, a split's new id applies.
   A per-record update passes test 3 and fails this one.
5. Ticket coverage: rebuild an old unsealed row through the automated path (`finalize_pending` -> `regroup_window`) and assert a committed ticket contains it.
   Repeat through `regroup_incremental`, since that path derives its bounds differently (F1).
   Then **once per site in the N1 table, all five** (F12): fold a window, remove its transactions by that route, and assert the total returns to zero rather than staying at its pre-delete value.
   The two API routes are the ones that fail today, and the full wipe is the one a per-statement hook cannot catch.
6. Identity: the key includes `source_started_at`, and two rows sharing an id produce two facts.
7. Idempotence: the same fold twice leaves values unchanged.
8. Crash safety: kill the worker mid-batch; no gap, no duplicate.

**Data that cannot be recovered**

9. Retention independence: drop a 61-day-old raw partition; facts, ledger and grains survive.
10. Cursor gating: the position blocks retention while behind; a stalled worker is logged critical then stops blocking.
11. Reproducibility: build a training set at a revision, restate a fact, rebuild at the same revision, assert identical output.
12. Rebuildability: delete a grain entirely and rebuild it from the fact table to identical values.
12b. Tenant purge completeness (F13): fold a window for a throwaway tenant, purge the tenant, and assert every `analytics_*` table has zero rows for it. Assert no ticket was published, since the fold target no longer exists.

**Traps specific to this data**

13. Attempts versus picks: zeros excluded from `pick_count`, included in `attempt_count` and the zero-pick rate.
14. Additivity: monthly-grain metrics equal fact-computed ones, including distinct count, p95 and average.
15. Top-N is not composable.
16. Local business date at 00:30 during BST lands on the right local day and ISO week.
17. Numeric fidelity end to end; comparisons numeric, never string.
18. Placeholder rejection: quarantined with a reason, never dropped silently.

**Registry and user-defined metrics**

19. **Fact-row completeness**: every listed field is populated wherever the source has it.
    This guards the one irreversible schema decision.
20. **A newly defined metric backfills correctly**: define one for a combination never registered before, backfill, and assert it equals a direct aggregate.
21. Selection is additive both ways, and percentiles come from merged histograms.
22. Quantity measures are refused where the field does not exist, since 47 of 49 methods have none.

**Performance and end to end**

23. Status is one query.
    Queries are prepared, worth roughly 100 ms each.
24. A 12-month request resolves to monthly, never daily.
    Grain reads plan as index-only scans, with `EXPLAIN (ANALYZE, BUFFERS)` proof.
25. Load: synthetic tenant at 100 times the measured rate, worker lag inside target.
26. In the real interface: a live pick updates within target; a window with unsealed contributors reads as provisional; empty and loading states, fractional formatting, light and dark all checked.

# 15. Open questions

**Blocking correct sizing**

1. **Expected production volume**: picks per day at full rollout, and distinct active items per day.
   Every grain, retention and index above is sized from an assumption of 50,000 picks and 5,000 items, against today's roughly 1,300 and 692.
   An order of magnitude either way means re-cutting the cascade before building.
2. **Tenants at full rollout.**
   Everything keys on `customer_code` and there are two.
   Ten busy tenants multiplies every row count tenfold and may justify per-tenant partitioning.

**Blocking Phase 1**

3. **Confirm the table naming scheme**, since renaming after the first migration costs a migration.

**Blocking N7**

4. **Definition scope**: are metric definitions per tenant, or global templates a tenant enables?
   Per tenant is assumed above.
5. **Who may create metrics.**
   There is no authentication in this codebase yet; `api/deps.py:34-42` is a permit-all placeholder with a `TODO(auth)`.
   A self-service metric builder is exactly the surface where that starts to matter.

**Can run in parallel**

6. **Cause of the two orphaned files**, before treating a regroup as the fix.
7. **Chart approach**: hand-rolled SVG consistent with the repo's zero-dependency history, or accept the first chart dependency.

# 16. Out of scope, but should be fixed

Two live defects found during this work, unrelated to analytics.

`PgVectorStore.ensure_collection()` runs `CREATE EXTENSION IF NOT EXISTS vector` from an always-on worker (`app/background.py:82-86` to `embedding_worker.py:173-180`).
The server has no pgvector, so it retries forever roughly every 4 seconds on both the web and worker processes, masking real errors.

`PgVectorStore.query()` lacks the `text_match` parameter that `search_service.py:167-172` passes, so hybrid search raises `TypeError` on the default backend.

---

# 17. Correction log

Every change made to this document after it was declared final, with **what it said before**, so a
decision taken on the old wording can be traced and reversed.

Kept as a table rather than in git history alone for one reason: a reader who finds a downstream
component behaving unexpectedly needs to know whether the document changed under it, and needs the old
value to compare against. Each entry is also marked inline where the fact lives.

**Almost nothing here changes the architecture.** Most entries correct a measured figure, add a fact,
or record a sizing consequence. The components, flows, invariants and delivery sequence are unchanged
except where an entry says otherwise: F12, F13, E4, and **D8, which cancels the Phase 4 backfill
outright**. D8 is the only entry that removes planned work and the only one that makes history
permanently unrecoverable, so it is stated in full rather than summarised.

## Ground truth corrections, from Phase 0 (2026-08-21)

| # | Section | It said | It is | Evidence |
|---|---|---|---|---|
| C1 | 3, and N4's validation | "Only **2** of 49 methods carry quantities. `ConfirmPickLine` (14,654 rows) and `ReportCount` (9,076)." | **3**: `ConfirmPickLine`/`QuantityPicked` 16,075, `ReportCount`/`CountedQuantity` 11,343, `AddStockCountLine`/`CountedQuantity` 83 | Live query grouping by `method` on the presence of each quantity key. `AddStockCountLine` was already named in this section as a trap, so it was measured but not counted. |
| C2 | 3 | Placeholder `transaction_type` values are "`xxxxxx`, `XXXXX`, `00xxxx`, `0050XX`" | Those **plus `XXXXXX`**, and detection is now a pattern (`[xX]`) rather than a list | `SELECT DISTINCT transaction_type WHERE transaction_type ~ '[xX]'` returned five values. |
| C3 | 3 (added) | nothing | 3 of 7,307 `ListItemAlternateUnitsOfMeasure` rows carry a stray `CountedQuantity`, so a quantity must be read from an allow-listed METHOD and never from the presence of a JSONB key | Values 3.415, 18, 0, all under `transaction_type = xxxxxx`. |
| C4 | F8 | "**9.2%** of pick confirmations record zero units" | **8.29%** of `ConfirmPickLine` alone (1,333 / 16,075); **7.47%** across all three quantity methods (2,056 / 27,511) | Two different denominators. The original single figure did not say which, and they are not interchangeable. |
| C5 | 11, grains sizing | Hourly 3.2 M / Daily 9.1 M / Monthly 300 K rows at 5 years, assuming one rollup row per definition per bucket | Same **per measure**; consumption carries three, so 9.6 M / 27.3 M / 900 K | Consequence of naming measure slots for their additive role instead of numbering them, which makes invariant 8 structural. A definition needing one sum and two counts cannot share one set of role columns. |

**If C5 turns out to be the wrong trade**, the fallback is the original design: anonymous
`measure1..measure8` slots and one rollup row per definition. That restores the smaller row counts and
costs the structural guarantee, so additivity goes back to being a review convention. The decision is
reversible until the first Phase 1 migration.

## Design corrections, earlier the same day

| # | Section | It said | It is |
|---|---|---|---|
| D1 | 3, F1 | `regroup_incremental` is the live path, running every 70 seconds | It is reachable only from two HTTP endpoints. The automated path is `log_stitch_worker` -> `finalize_pending` -> `regroup_window`. Inherited from three stale comments in `derive_transactions.py`. Moves ledger churn DOWN and F1's priority down. |
| D2 | N1, F12 | Tickets publish from three sites, all in `derive_transactions.py` | **Five.** Both halves of `DELETE /logs/data` also remove `log_transactions`, and one does so through a `jobs` cascade with no statement to hook. |
| D3 | 6, F13 | "E6 deletes from every non-partitioned log table but touches neither `log_entries` nor `log_transactions`" | It deletes the tenant's `jobs`, and both tables go with them via `ON DELETE CASCADE`. Needs the opposite fix from F12: delete the tenant's `analytics_*` rows rather than publish a ticket. |
| D4 | 5, and new E4 detail | E4 "exists", with no detail section | Extended: `partitioning.py` gained a per-table `Grain` (daily/monthly/yearly) and `log_partition_worker` gained `KEEP_FOREVER` / `RETENTION_DAYS`. Was a prerequisite for Phase 1, not part of it. |

## Ground truth corrections, from Phase 3 (2026-08-22)

Every one of these was found by running the pipeline end to end against real data.
None of them could have been found by re-reading the fixtures, and C6 is a case where the fixtures were right about the intent and wrong about the data.

| # | Section | It said | It is | Evidence |
|---|---|---|---|---|
| C6 | Phase 0 fixtures, N4, F8 | The `error` fixture: "A hard failure carries no units. It must not appear in the quantity total, and must not be silently folded in as a zero-unit attempt either." It modelled such a row as `quantity = None`. | An errored `ConfirmPickLine` carries a **full** `QuantityPicked`, and so does an `incomplete` one. The quantity is stated on the REQUEST line; whether the pick failed is decided by what arrives afterwards. So both summed into consumption and nothing could express the rule. | Five synthetic scenarios through the real parser, stitcher and worker: the facts total **30.333333** across all statuses and **10.333333** across completed ones. Per-row: `success/10.0`, `success/0.0`, `incomplete/10.0`, `error/10.0`, all classified `pick`. |
| C7 | N2 (added) | nothing about transactions with no `method` | **25 of 397** live transactions (6.3%) have no method, and they are real stitched activity, not fragments: `entry_count` 2 to 28, durations to 172,523 ms, `mi_program_count = 0` on 24 of 25, and `status = incomplete` on 9. They normalise as `non_quantity` facts. Quarantining them - the first implementation - would have hidden 6.3% of transactions from every volume, duration and status metric while the totals still looked plausible. | Live query over `log_transactions WHERE method IS NULL`, inspecting entry counts, durations and statuses. |
| C8 | N4, and C6's status set | nothing about the `soft` status | **69** live `soft` transactions exist and **zero** carry a quantity-bearing method. `soft` means "M3 returned not-found/needs-value but the app coped", so if the ERP returned not-found the confirmation did not register in the system of record. It is excluded from consumption on that reading, and the measurement is what makes the choice safe: it moves no current number. | Live query crossing `status` with the three quantity-bearing methods. |
| C9 | E4, Phase 4 | nothing about the analytics partition runway being forward-only | The runway is built forward only (`coverage_days(today, ahead=14)`), because ingestion only ever writes new data. Measured 2026-08-22: `analytics_facts` partitions began **2026-07-01** while the 60-day source floor was **2026-06-23**, an 8-day window with nowhere to go; `analytics_hourly_rollups` began 2026-07-22, a 29-day window; `analytics_daily_rollups` was fully covered. Worse, a filled DEFAULT partition is a **one-way door**: PostgreSQL then refuses to create that period's real partition. | Partition bounds read from `pg_class.relpartbound`. The one-way door was verified directly: `CREATE TABLE ... PARTITION OF` failed with "updated partition constraint for default partition would be violated by some row". |

**On C6, what was NOT changed.** The fact rows still record `status = error, quantity = 10.0`, because that is what happened.
A fact is a record of an event; whether it counts toward a metric is a registry decision.
The fix is therefore a new `Measure.statuses` filter, held per measure for the same reason `only` is per measure - one definition can then carry both a total and an error count.
If the exclusion of `incomplete` turns out to be wrong, note that it is self-correcting: a later Stage 2 pass closes the transaction and it counts under its real status.

## Design corrections, from Phase 3 (2026-08-22)

| # | Section | It said | It is |
|---|---|---|---|
| D5 | N3, retention position | "The maximum `created_at` observed among fully processed rows, published under `analytics:warehouse-v1`." | That value is stored **per tenant**, on a new `analytics_tenant_state.source_write_frontier` column (migration `d5e83c1a6f97`), and the value published is the **MINIMUM across tenants**. `consumer_cursors` holds one row per consumer while retention is global, so publishing whatever the worker had just processed would let a tenant that is far ahead advance the position past one that is far behind - and `log_partition_worker` would then drop source partitions the lagging tenant had never read, with its cursor moving past the gap unaware. A tenant with a NULL frontier has processed nothing and suppresses publishing entirely, because SQL's `MIN` would otherwise skip it and publish a claim that is too far ahead. |
| D6 | Phase 3, N3 | "One cycle, all in a single transaction per tenant" | One transaction per **run**. N1 splits a wide range into per-day tickets precisely so each unit of work stays bounded and a poison day fails in isolation, which is only true if a day is also a transaction boundary. A single transaction spanning a `regroup_all` ticket set would read 60 days of transactions at once and let one bad day roll back 59 good ones. Invariant 4 is unaffected: each run's tickets are stamped consumed in the same transaction as that run's changes. |
| D7 | N5, grain cascade | "Each level reads only the level below, so the fact table is read once per cycle." | Hourly **and** daily are both folded from the facts; only monthly reads the level below. Hourly buckets are UTC hours while daily buckets are the tenant-LOCAL `business_date`, so folding daily from hourly is exact only when the tenant's UTC offset is a whole number of hours - at +05:30 one UTC hour per day straddles two local dates and would be attributed entirely to one, wrong by up to half a day's traffic, silently, and only for some tenants. The stated concern is preserved exactly: the dirty facts are read ONCE and folded into both grains in a single pass. |
| D8 | **Phase 4, Phase 7** | "Phase 4. Backfill and checks." and Phase 7's "then backfill and report-only reconciliation" | **There is no backfill.** Decision taken 2026-08-22 after C9 was measured. Analytics counts from the day the worker is switched on; history before that is not folded, and becomes unrecoverable once its raw `log_entries` pass 60-day retention. Phase 4 keeps its other three items - windowed reconciliation, the completeness check against `log_entries`, and gating source retention on healthy state. Phase 6 gains an obligation: the interface must show "no history before &lt;switch-on date&gt;" rather than draw an empty chart, which would read as zero activity. `analytics_metrics.backfilled_through` stays NULL, which is the field that signals it. |
| D9 | N3, F6 (recorded, not corrected) | - | A unit mismatch that predates this design and is being carried deliberately. F6 specifies the retention position as a `log_transactions.created_at`, a WRITE time, matching what every other consumer publishes (`NotificationRule.cursor_at` says so in its own comment). But `log_partition_worker.periods_blocked_by_consumers` compares that position against a partition's **event-time** upper bound. Write times run ahead of event times, so partitions are released slightly earlier than a strict reading allows, and a transaction written long after the event it describes is where the gap bites. Deviating for one consumer would make the MIN across consumers a comparison between two different units, which is worse. Implemented as specified. |

**D8 is a deliberate loss of data, not a deferral.**
The alternative - provisioning partitions for the historical range and folding it - was costed and rejected: it needed one call to the already-tested `pt.migration_days`, roughly three extra monthly partitions on the fact table and sixty daily ones on the hourly rollups.
It remains available for as long as the raw entries survive, which is 60 days from each day's ingestion.
After that the decision cannot be revisited, because there is nothing left to fold.

**The runway gap C9 identified is closed regardless of D8**, because skipping the backfill removes only one of the three paths that write facts with an old `event_time`.
The others are `regroup_all` re-deriving a tenant's whole history, and an ingested rotated log file (`eSmartServerLog.txt.40`) carrying weeks-old lines.
Every run therefore calls `pt.ensure_coverage` for its own destinations before reading anything, in its own short transaction so the `ACCESS EXCLUSIVE` lock is not held for the length of the run.
The alternative of skipping rows with no destination was rejected: filtering the source read makes it narrower than the stored read, and the range diff reads that as *reverse it*, so any fact already in a DEFAULT partition would be deleted and its contribution stripped from every rollup.

## How to read an entry

Each inline correction is dated and states what it replaced, so the section reads correctly on its own.
This table exists so the set is enumerable without re-reading the document.

Corrections are numbered and never renumbered. A future correction to a corrected figure gets a new
number and cites the old one, so the chain stays traceable.

# 18. ITERATION 2 begins here. Stage 2 redesign: the deferred update-in-place change, approved 2026-08-23

**Everything from this point to the end of the document is ITERATION 2: planned, approved, and NOT YET BUILT.**
Sections 1 to 17 above are iteration 1 and are running in production.

This section records a change to the **upstream** pipeline, not to the analytics platform.
It is in this document because the analytics design rests on premises that this change removes, and because sections 2, N3, F6 and Flow F all already track it as a pending dependency.

Nothing in sections 1-17 is retracted.
Everything in them describes the platform **as built**, against a Stage 2 that delete-and-reinserts.
This section describes what Stage 2 becomes, and exactly which of those premises stop being true.

## What was implemented before

Stage 2 is a **window re-deriver**.
A ticket arrives, `regroup_window` deletes every transaction anchored in `[lo - 900 s, hi]` regardless of `sealed`, re-reads the entries that the delete just made unassigned, re-groups them from scratch, and re-inserts everything.

That shape is deliberate and it earned three properties the analytics platform was designed around:

- **Back-filling into a sealed region is lossless**, because sealed rows are re-derived too (`derive_transactions.py:846-848`).
- **A crashed worker needs no recovery.** All grouper state is derived per cycle, so an interrupted window simply leaves its ticket open.
- **A wrong grouping self-heals.** Any mis-grouped transaction is re-derived on the next of ~22 passes, which is why no split-that-should-have-merged has ever been observed in production.

## Why it is changing

Measured on the deployed database, 2026-08-23:

| Measurement | Value |
|---|---|
| WAL generated | **9.6 GB/day** against a 4.5 GB database (2.05 bn records in 73 days) |
| Inserts per surviving row, `log_transactions` | **22.4** on one day partition |
| Inserts per surviving row, `log_entry_assignment` | 18.1 - 52.7 M inserts to hold 2.6 M rows |
| In-place updates | **0** anywhere - always DELETE + INSERT |
| Autovacuums on the two tables | 7,493; 278 on a single day partition |
| Real content versions per transaction | **1.005** (measured from `analytics_fact_ledger`) |

The last two lines are the argument: the content is stable, the storage is rewritten ~22 times.

**And the amplification is arithmetic, not inefficiency.**
`pad = max(log_regroup_pad_seconds, log_seal_window_seconds)` = 900 s, and the mean interval between stitch tickets is 77.2 s, so consecutive padded windows overlap 96%: `1800 / 77 ≈ 23`.
Storage layout does not appear in that expression, which is why making `log_transactions` append-only was **considered and rejected** - it would keep 22 rows instead of writing 22 rows, and every read would then pay version resolution on the hot feed path.

## A live bug this investigation surfaced

`sealed` is written in exactly one place, `_write_transaction` (`derive_transactions.py:611`), from `_is_sealed`.
**No `UPDATE` sets it anywhere.**
So sealing is a *side effect of re-insertion*: once `lo_p` advances past a row's `started_at` (~960 s), nothing re-derives it and it never seals.

| Day | Total | Unsealed | of which incomplete |
|---|---|---|---|
| 2026-08-18 | 9,437 | **733** | 71 |
| 2026-08-19 | 10,642 | 292 | 81 |
| 2026-08-20 | 7,679 | 172 | 20 |

**2,516 rows are permanently unsealed**, the oldest dating to 2026-08-06.
Two silent consequences inside this platform:

- **F4's settledness signal is wrong, though not in the way first written here.** `_settledness` (`consume.py:383-395`) computes the share over THIS WINDOW's rows only, and `analytics_tenant_state` holds one row per tenant, overwritten each run - so unsealed rows outside the newest folded window contribute nothing, and sludge cannot accumulate into the number. What actually breaks is subtler: a row that passes its seal window without a rebuild stays unsealed forever, so a window folded while holding one records a share that will never improve, and a tenant that stops ingesting freezes a permanently *provisional*-looking number. The correction was found by reading the code during the 18c/18d verification pass, not when this line was written.
- **E5's most useful alert may never fire.** `stability.py` gates on `incomplete + sealed`; if nothing ever seals an incomplete row, that path is unreachable.

This is fixed first, independently of the rest.

## What Stage 2 becomes

An **incremental state machine**, with the re-deriver retained as a verified fallback.

```
BEFORE                                  AFTER
ticket arrives                          entry arrives
  -> DELETE every txn in +/-900 s         -> look up open stream (thread, user_ctx)
  -> entries become unassigned            -> found: append 1 assignment,
  -> re-read + re-group ALL of them                UPDATE the open transaction
  -> re-INSERT ALL of them                -> miss:  FALL BACK to the window re-derive
                                          -> closed transaction: never touched again
22.4 writes/row                         ~1.0 writes/row
```

Two shape changes matter more than the mechanism:

- **`log_entry_assignment` becomes append-only.** An entry is assigned once. 52.7 M writes drop to 2.6 M.
- **`log_transactions` is UPDATEd in place while open, then never touched.** It cannot be append-only: the row is an *aggregate* over its entries (`started_at` = min, `ended_at` = max, `status`, `entry_count`, `response_summary`), so adding an entry necessarily changes it. But "not append-only" never required delete-and-reinsert.

Five stages, each independently shippable:

| # | Stage | Writes/row | Note |
|---|---|---|---|
| S1 | Explicit sealer | 22.4 | fixes the bug above; prerequisite for everything |
| S2 | Durable stream position | 22.4 | no gain alone; `open_pos` is a batch index and `req_pos` keys on a CPython object address, so neither survives a process boundary |
| S3 | Fingerprint skip + UPDATE in place | **~1.05** | the big win, and the oracle S4 is verified against |
| S4a | State machine in shadow | ~1.05 | runs both paths, compares fingerprints, promotes nothing |
| S4b | State machine enabled | **~1.00** | removes the re-derive from the hot path |

## What this changes for the analytics platform

Four premises in sections 1-17 stop being true.
Each is a required follow-up, not an optional one.

| Where | Said | Becomes |
|---|---|---|
| **Section 2** | "`log_transactions` ... when a late line arrives the row is deleted and rebuilt" | The row is **updated in place**. The 98.7%-rewritten figure stays true of the *content* but stops being true of the *storage*. |
| **N3, F6** | Retention position is `max(log_transactions.created_at)`, "held in a single named constant because a deferred upstream change to update-in-place would require switching to `updated_at`" | **That change is now happening.** `consume.py:85` `_FRONTIER_COLUMN` must move to `updated_at`. The constant existed for exactly this, and it is the whole of the edit. |
| **Flow F** | "**Watch:** a deferred change to update-in-place would stop `created_at` moving and break this silently" | The watch has fired. Until `_FRONTIER_COLUMN` moves, the analytics retention frontier stalls and `periods_blocked_by_consumers` holds source partitions forever. |
| **E5 / notifications** | The cursor reads `log_transactions.created_at`, and `stability.py` exists because "every Stage 2 rebuild refreshes `created_at`, so an in-flight transaction re-enters the cursor's feed on every rebuild until it seals" | `cursor.py:153-157` must move to `updated_at`. The churn `stability.py` was written to absorb largely disappears, so that filter becomes a much weaker load-bearer. |

`created_at` also changes meaning: today it is "last rebuilt", afterwards it is genuinely "first written".

**`_DERIVE_VERSION` is not optional, and it is easy to leave out.**
S3 skips a rewrite when the stored fingerprint matches the recomputed one.
So if `_group`, `compute`, `_is_sealed`, `_anchor` or `_entry_stream_order` is ever edited without bumping a version constant that feeds the hash, the stored rows keep matching their own stale fingerprint and the edited derivation never reaches them.
The projection is then quietly wrong forever, with no failing test and no alert - the worst shape a bug can take in a system whose whole job is to be the trusted projection.
Pin it with a source-digest test; the repository already uses `inspect.getsource` assertions elsewhere.
This was specified in the plan and had not been carried into this document until the verification pass.

**One free improvement it unlocks.** `evaluators.py:64`'s dedup key is `(rule_id, transaction_id)` and version-blind, so a transaction whose status changed from `incomplete` to `error` is deduped away and never re-alerts - an accepted residual risk in `stability.py`. With a row fingerprint available, the key becomes `(rule_id, transaction_id, status)`.

**What does not change.** The range diff, the ledger, the rollup cascade, the metric registry and every invariant in section 13 are unaffected. The diff already treats a changed transaction and a vanished transaction identically, which is precisely why it absorbs this upstream change without modification - the property section 8 claimed for it, now tested against a real one.

**S1 causes no analytics fact churn at all, which was not obvious.**
`sealed` is NOT one of the 24 `contract.FACT_FIELDS` and the normaliser never reads it - `consume.py` loads the column only for F4's settledness figure.
So sealing a row cannot change its fact fingerprint, the range diff reports `unchanged`, and no fact or ledger row is written.
S1 therefore needs no analytics ticket and has no re-fold cost, which is worth stating because the opposite was assumed while planning it.

## Impact on the position-tracking tables

The document already warns that four position-tracking mechanisms are easily confused (section 7).
This change touches them very unevenly, so each is stated separately.

| Table | Tracks | Impact | If the follow-up is forgotten |
|---|---|---|---|
| `log_ssh_file_checkpoints` | bytes pulled per remote file | **none** - Stage 1, upstream of this | - |
| `log_source_objects` lease | who is parsing which byte range | **none** - Stage 1 queue | - |
| `log_regroup_pending` | which windows still need stitching | **none**, in meaning or volume | - |
| `analytics_pending_windows` | which ranges N3 must re-diff | **volume falls ~95%** | see below |
| `consumer_cursors` | how far each reader of `log_transactions` has got | column moves to `updated_at` | **fails safe** |
| `notification_rules.cursor_at` | how far each alert rule has read | column moves to `updated_at` | **fails unsafe** |

**The SSH checkpoint and the Stage 2 queue are genuinely untouched.**
The checkpoint is a *byte offset* in a remote file and Stage 2 sits downstream of it.
`log_regroup_pending` keeps its meaning and its volume: it is still the durable "there is work here" signal written in the same transaction as the entries, which is the reason Stage 2 can fail completely and be retried.
One nuance only - after S3 a ticket can be consumed having written nothing.
That is correct: `consumed_at` means *examined*, not *changed*.

**The two `created_at` readers fail in opposite directions, which is why they need different urgency.**

- `consumer_cursors` (retention) computes `max(created_at)` over folded rows. After S3 a row created at 09:00 and updated at 10:00 reports 09:00, so the position **under-reports** progress. A lower position blocks *more* partitions from being dropped. The cost is disk, not data. Fix it for accuracy, but it cannot lose data.
- `notification_rules.cursor_at` reads `created_at >= lo AND < hi`. After S3 an updated row keeps its original stamp, falls behind the cursor, and is **never read again**. That breaks the one invariant `cursor.py` exists to hold - *"NO ROW IS EVER SKIPPED. A row MAY be read more than once. Dedupe absorbs a repeat. Nothing absorbs a skip."* It must move in the **same change** as S3, not after it.

**The analytics ticket reduction has a hidden cost.**
`regroup_window:901` publishes a ticket unconditionally, every cycle, for the whole padded window - roughly 22x more than needed. That waste is also a **brute-force safety net**: even if the diff logic were wrong, N3 would still re-examine the range. Narrowing to "publish only on change" removes the net, so two obligations become load-bearing rather than incidental:

- the **delete** branch must still publish, or a transaction the rebuild no longer produces keeps its contribution in every total;
- a ticket must span **old and new** `started_at`, because a transaction whose start moved is a reverse of the old analytics key plus an insert of the new one. Ticketing only the new instant leaves the old fact double-counted for good.

The replacement net already exists - N7's reconciler, whose `facts_vs_transactions` check detects exactly a missing fact.
But `analytics_reconcile_worker_enabled` is currently `False`.
**Turn it on before S3 ships.** Swapping an expensive always-on net for a cheap periodic one is a good trade only if the periodic one is running.

## Impact on notifications: measured, and there is no regression

Checked against the live rule set rather than reasoned about, because the seal-flip in S1 could in principle release a flood of overdue alerts.

**The two active rules, and what they match:**

| Tenant | Type | Matches | Transactions |
|---|---|---|---|
| `mnp` | `text_match` | `error_text` contains "printer error" | **0** |
| `tmp-live` | `status_match` | `{"statuses": ["error"]}` | 151,216 |

**The burst that does not happen.** `stability.py` gates `incomplete + unsealed` as "wait", so those rows are not alertable today. S1 seals them, which makes them alertable for the first time:

| Unsealed status | Count | On seal |
|---|---|---|
| `success` | 1,908 | already alertable - dedup blocks any repeat |
| `incomplete` | **577** | becomes alertable for the first time |
| `soft` | 151 | already alertable - dedup blocks |
| `error` | 3 | already alertable - dedup blocks |

Total alert history to date is 58 events / 58 deliveries, so 577 at once would be a tenfold flood.
It does not fire, because **neither active rule matches `incomplete`** - `tmp-live` matches only `error`, and `mnp` has no transactions at all.

That is a fact about the current configuration, not a property of the design.
**Before S1 runs, re-check the rule set**: any active rule matching `incomplete` would release one alert per previously-unsealed row, back to 2026-08-06.
If such a rule exists, seal the historical backlog with rule cursors advanced past it, then start the sealer.

**S1 is a notification fix, not just a risk.** `stability.py` calls `incomplete + SEALED` "the genuinely useful alert" - the request whose response is never coming. Today 577 rows can never reach that state, so a rule written to catch stuck requests silently would not work. After S1 it does.

**S3 with the cursor moved: no behaviour change.** A row alerts once, and re-enters the feed only when it genuinely changed - rather than on every one of ~22 rebuilds. `stability.py` keeps working and has less churn to filter, so it becomes a weaker load-bearer rather than a broken one.

**Deliberately NOT bundled.** `evaluators.py:64`'s dedup key is `(rule_id, transaction_id)` and version-blind, so a transaction going `incomplete` to `error` never re-alerts - an accepted residual risk in `stability.py`. A fingerprint makes the fix one line (`(rule_id, transaction_id, status)`), but it **changes alert volume**: one transaction could then alert more than once. That is an improvement, not a regression fix, so it stays out of this change and behind its own decision.

## Partitioning, indexes and the new tables

**Every existing partition rule prevails.** Grains are unchanged (`log_transactions` daily, `log_entry_assignment` daily), the co-partitioning of assignments with entries on `entry_ts` holds because append-only does not change `entry_ts`, retention stays at base for transactions and **base + 1** for entries and assignments, the one-day lag rationale still applies, all three drop gates are untouched, and the DEFAULT-partition health check is unaffected because the change adds no default rows. `chunk36`'s guard that the log tables are untouched by `KEEP_FOREVER` / `RETENTION_DAYS` still passes.

**The two new tables are deliberately NOT partitioned.** `log_open_stream` and `log_pending_request` are small self-cleaning working sets - a few hundred rows, deleted when a stream closes. Same reasoning that keeps `analytics_monthly_rollups` out of `PARTITIONED`: nothing worth pruning, and partitioning adds planning cost for no gain.

**But they need a reaper that derived state never needed.** `evict_stale` closes a stream *when an entry arrives*. A tenant that stops ingesting leaves its streams open forever, and the row leaks. Derived state cannot leak; persisted state can. A TTL sweep on both tables is required, not optional, and belongs in the sealer tick. Monitor `count(*)` on each - a number that only grows is the alarm, and it is the one failure mode with no upstream signal.

**The sealer must be bounded.** Its predicate would otherwise seal a 59-day-old row, bumping `updated_at`, which feeds the notification cursor - so an ancient transaction could alert **one day before its entries are dropped**, leaving a detail view with no entries. Add a horizon clause well inside `log_partition_retention_days`. Theoretical today (the oldest partition is 18 days old) and unavoidable once 60 days of history exists.

**Two mechanical notes on the new partial index.** `CREATE INDEX CONCURRENTLY` is not supported on a partitioned parent, so follow migration `b3d914c7ea52`: `CREATE INDEX ... ON ONLY parent`, then per-partition `CREATE INDEX CONCURRENTLY`, then `ALTER INDEX ... ATTACH PARTITION`; the parent index stays invalid until every partition attaches. And autogenerate is already safe - `alembic/env.py:44-46` excludes per-partition *indexes* as well as tables, deriving the pattern from `partition_name_pattern()`, so the 33 per-partition copies will not be proposed for dropping.

**The seal flip is not a cheap UPDATE.** `sealed` is indexed and `updated_at` will be, so HOT is impossible and PostgreSQL writes a new tuple with an entry in all 23 indexes, exactly like an insert - `log_entries` took 105.8M updates at **0.0% HOT** for the same reason. Already inside the ~1.05 writes/row estimate, but it means dropping `ix_log_transactions_sealed` will not buy HOT back, because `updated_at` must stay indexed for the cursor.

## Frontend impact: verified, no contract breakage

**No HTTP response exposes the changing fields.** `_txn_summary` (`logs.py:321-345`) emits 22 fields and none of `created_at`, `updated_at` or `sealed`; likewise the agent-tool twin and the text renderer. Every `created_at` in the API layer belongs to a different table. No frontend file reads a transaction-level `created_at`, `sealed` or either fingerprint.

That matters more than it sounds: `created_at` changing meaning from "last rebuilt" to "first written" is the riskiest part of the change, and it is **invisible to the frontend** because it was never serialised.

**"Append-only" for assignments means written ONCE, not versioned.** `UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts)` must stay in force. Retaining superseded rows for one `entry_id` would be breaking: `logs.py:1059` emits `timeline[].seq` straight from the assignment, so duplicates would render the same step twice and trip `MAX_RENDER_ENTRIES` sooner.

**Three display meanings change, none needing code:**

| Field | Rendered at | Change |
|---|---|---|
| `freshness.unsealed_share` / `provisional` | `AnalyticsPanel.tsx:225-235` | the sealer moves the number and can flip the badge from amber **Provisional** to green **Settled** |
| `queue.open_tickets` | `AnalyticsPanel.tsx:283` | publish-on-change makes the "Queue" tile read ~0 permanently, so the tile stops carrying signal |
| `last_regroup_at` | `PollingStatus.tsx:158` | a ticket consumed having written nothing still advances it, so "last updated" no longer implies data changed |

The `pending_regroup` banner is unaffected: it derives from local upload phase, and the ~95% ticket reduction is `analytics_pending_windows`, which `/logs/regroup/status` never reads.

## Risk this accepts

The re-deriver's third property - *a wrong grouping self-heals on the next pass* - is the one being given up, and it is given up by S3 rather than by S4: once an identical rebuild writes nothing, nothing revisits the row.

Six ways the stream lookup can miss are enumerated in the implementation plan, each producing a **split that should have been a merge**.
The mitigations are a guard (`last_entry_ts < lo` and `lo - last_entry_ts < log_open_gap_seconds`, otherwise fall back) and shadow mode: run both paths and compare `(id, row_fingerprint, members_fingerprint)` until a week of zero divergence on real traffic.

The single most important test in the plan is the narrowest: **a RESPONSE arriving in a later window than its REQUEST must still produce one transaction, not two.**

## 18a. Response capture: decided 2026-08-23

A separate change from S1-S4.
It touches no Stage 2 code, and is recorded here because it changes what a fact row can measure.

### The gap

Stage 1 parses the response fully and Stage 2 discards it.

| Layer | Parsed by Stage 1 | Reaches `analytics_facts` today |
|---|---|---|
| request params + flat request body | yes | **yes** - 34 keys via `attributes` |
| nested objects/arrays in the request body | yes | no - dropped by `_merged_attrs:90` |
| `response` payload (150,104 entries, all parsed) | yes | **no** - only `response_summary`, which is the string `"OK"` |
| `mi_result.records[]` (3,641,353 records) | yes | **no** |
| `mi_result.record_count` (424,632 values) | yes | **no** |

`_merged_attrs` (`derive_transactions.py:75-91`) loops the entries and reads only `request` and `request_body`.
A real response holds `QuantityOnHand`, `AllocatedQuantity`, `TotalNumberOfBalances`; a real `mi_result` holds per-record `STQT`, `ALQT`, `BANO`, `PRDT`, `ITDS`.
None of it is measurable today.

### Decisions

**Capture response scalars for all 49 methods.**
Measured over 400 live responses: 947 scalar values against 9 non-scalar, across 145 distinct keys.
Cost is `attributes` growing from 806 to roughly 2,000 bytes, about 1.8 MB/day, roughly 3 GB over five years - against a 4.5 GB database that already generates 9.6 GB of WAL per day.
The upside is unrecoverable: the same reasoning section 11 already uses for the fact row's width, since raw entries drop at 60 days.

**Per-record expansion stays selective.**
`records[]` is where the ~200k rows/day is, so it is opt-in per method, chosen in the interface.
The 60-day retention window is the safety net: you can always decide to expand a method you did not pick, provided the scalars were already being captured.

**Field capture is an allowlist that DISCOVERS.**
`_SENSITIVE` (`derive_transactions.py:45`) is a five-word denylist, and the two most frequent response keys across all 145 are `AccessToken` and `M3UserCredentials`.
A denylist guarding a `KEEP_FOREVER` table is the wrong shape: one renamed credential field becomes permanent.
But a static allowlist would silently drop new fields, losing the future-metric history the capture exists to buy.
So an unknown key is neither captured nor ignored - its **name only** is recorded, `captured = false`, and surfaced for review.
The allowlist becomes data, one row per `(method, field)`, self-populating rather than hand-maintained.
Storing only the name means the discovery record itself cannot leak a value.

**The payload goes to `analytics_facts`, NOT to `log_transactions`.**
Merging it into the projection would widen the exact hot table S3 exists to make cheaper to write.
Instead N3 reads the transaction's entries alongside it and passes them to N2, which stays pure because the reading happens in N3.
That keeps this change entirely analytics-side.

**Namespace, never flat-merge.**
Request and response both carry `ItemNumber`; a flat merge silently drops one.
Response fields are prefixed on capture (`resp.QuantityOnHand`, `mi.STQT`) so `fold`'s `row.get(field)` keeps working unchanged and a collision is structurally impossible.

### Open, not yet decided

Whether the per-record grain is a new table or a second row type in `analytics_facts`.
A separate table keeps the existing fact row's meaning clean but doubles the fold path; the answer depends on how `rollups.recompute` handles two grains, which has not been looked at yet.

## 18b. The registry, and how a provisioned transaction reaches analytics

Traced against the running code, not designed.
Everything in the first half is existing behaviour; the second half records what per-transaction insight configuration needs and does not yet have.

### There is no stream. There is a ticket.

```
raw log file
     |
     v  Stage 1   parse_insert.py            ->  log_entries          (append-only)
     |                                           publishes NOTHING
     v  Stage 2   derive_transactions.py      ->  log_transactions
     |                                           publishes the ticket, in the SAME COMMIT
     v
  analytics_pending_windows                   <-  a Postgres row: range_start, range_end
     |                                            NO transaction identity
     v  N3        consume.py                   poll 2.0s: claim -> lock -> range diff -> normalise
     v
  analytics_facts  ->  analytics_rollups  ->  revision++  ->  the chart reloads
```

Stage 1 never publishes a ticket, and that is correct: analytics measures transactions, so a ticket cannot exist until Stage 2 has decided what the transaction is.

The five publish sites:

| Site | Occasion |
|---|---|
| `derive_transactions.py:686` | full wipe and rebuild |
| `derive_transactions.py:809` | unsealed rows freed for restitch |
| `derive_transactions.py:901` | the padded regroup window |
| `logs.py:719`, `logs.py:777` | on-demand regroup or delete from the API |

The hand-off is a durable row rather than a push, because the worker is a separate systemd process and gunicorn recycles its workers.
A push would lose the hand-off on any restart; a row survives it.
`derive_transactions.py:805` states the property directly: *"Before the commit, so ticket and change are atomic (invariant 3)."*
So a rebuilt transaction with no ticket cannot occur.

**The consequence for the registry.**
Because a ticket carries a time range and no transaction identity, provisioning a transaction does not create a stream.
It changes what the consumer does with windows it was going to process anyway.
Flipping `Capture` on therefore needs no separate backfill machinery: write the registry row and publish tickets across the retention range in one commit, and the ordinary path does the rest.
The range diff reports every already-folded transaction as `unchanged` (fingerprint-absorbed) and inserts only the newly captured one, and `_coalesce` (`consume.py:194`) merges adjacent tickets so 60 days is a handful of runs rather than sixty.

That is why the queue must stay one ticket per `(customer, window)`.
Segregating it per transaction would cost a per-transaction cursor, lock and reaper to buy the ability to skip work that must not be skipped.

**The invariant a registry write must honour.**
The registry row and its tickets must be written in the SAME commit, exactly as Stage 2 does.
Row first, commit, then publish leaves a transaction marked captured with no tickets - silently absent until some unrelated rebuild happens to touch those windows.
The natural shape of an API handler is "save, then react", so this is easy to get wrong.

### Configuring insights per transaction

What the registry already supports, verified against the code:

| Requirement | Supported | Where |
|---|---|---|
| hourly, daily, monthly aggregation | **yes** | `GRAINS` (`definition.py:47`) also has `weekly`, derived from daily at read time |
| per item | **yes** | `item_number` is one of 24 eligible fact fields |
| several different metrics on one transaction | **yes** | many definitions, each with its own dimensions, measures and grains |
| a different set of metrics per transaction | **yes** | `method_filter` at `definition.py:261`, plus the `transactions` key R1 adds |
| written from the interface, no deploy | **yes** at the API (`POST /metrics`), **no** UI yet |
| up to 4 dimensions per definition | **yes, capped** | `DIMENSION_SLOTS = 4` (`analytics_rollup.py:48`) - `dim1..dim4` are columns, so 5 is a migration |

What it does NOT support, and this is a correction to section 18a:

**An `attributes` key can be neither a dimension nor a measure.**
`validate` rejects both - dimensions at `definition.py:216`, measure fields at `definition.py:226` - because each is checked against `contract.FACT_FIELDS`, and `attributes` is not in it.
Mechanically it would fail anyway: `rollups.py:125` reads `row.get(name)` off the flat fact dict, so `row.get("resp.BaseUoM")` is `None` when the value lives nested under `row["attributes"]`.

Section 18a claimed the namespacing meant *"`fold`'s `row.get(field)` keeps working unchanged"*.
That is true of `fold` and false of the path as a whole: `validate` refuses the definition long before `fold` sees a row.
So R3 as written captures response scalars that nothing can then read.
Capture and read must land together or the feature is storage with no product.

**There is no unit-of-measure field at all.**
None of the 24 eligible fields is a UoM, which is why `AnalyticsPanel` carries the `mixesUnitsOfMeasure` warning: pick confirmations do not record one, so a total silently adds kilograms to eaches.
Grouping by base UoM is therefore not a configuration change today - the value has to be captured first, and then be readable.

**A promotion mechanism already exists, but is a code constant.**
`_FROM_ATTRIBUTES` (`normalizer.py:47`) lifts `LotNumber`, `FromLocation` and `ToLocation` out of `attributes` into typed columns.
That is precedent for promoting a field, and also the reason promotion alone is not the answer: it is a dict in source, so every new field is a deploy, which defeats configuring from the interface.

### The open decision

How an attribute-backed field becomes groupable:

| | Mechanism | Deploy per field? | Indexable | Validation authority |
|---|---|---|---|---|
| **A** | promote to a typed fact column, extending `_FROM_ATTRIBUTES` | yes | yes | `FACT_FIELDS`, as now |
| **B** | a dimension may name an attribute path, `attr:resp.BaseUoM` | no | no, a JSONB extraction | the discovery registry |
| **C** | B to explore, then promote the ones that prove useful | only for promotion | after promotion | both |

**Decided: C.** Attribute paths to explore, promotion to typed columns for what proves useful.
B alone is what the stated goal requires, and the discovery registry from R1 is what makes it safe: `validate` checks the named field against the registry rather than a hardcoded tuple, so a typo is still refused, but a newly discovered field becomes usable without a release.
The registry stops being an allowlist and becomes the schema.

### Why C is cheaper than it looks, and the one thing it must not get wrong

Two existing mechanisms decide this, both verified rather than assumed.

**Promotion copies; it never moves.**
`normalizer.py:228-231` puts `attributes` on the fact first and only then reads out of it with `attributes.get(key)`.
The key therefore survives promotion, so a metric still written as `attr:resp.BaseUoM` keeps resolving after `base_uom` exists.
That retires the risk of one dimension splitting across two representations: the two paths read the same surviving value, they do not compete.

**The fingerprint makes promotion self-healing.**
`_fingerprint` (`normalizer.py:139`) hashes every field not in `_NOT_FINGERPRINTED`, which includes the promoted columns and the `attributes` blob itself.
So adding a promoted column changes the fingerprint of every fact.
The range diff then reports those transactions as CHANGED rather than `unchanged`, reinserts them with the new column populated, and the rollups recompute off the rebuilt facts.

That means promotion needs **no hand-written backfill**.
Without this, a promoted column would be NULL on every historical fact while the stored rollup still held the value, and `rollups_vs_facts` would report drift on every historical bucket - a permanently red auditor, which is worse than no auditor.
The docstring already anticipates the reasoning: the blob is fingerprinted *"precisely so a measure nobody has thought of yet can be built from it later, which makes a change inside it a change to a potential measure."*

**The cost this makes explicit.**
Promotion re-folds the retention window, and so does R3, because adding response scalars to `attributes` changes every fingerprint too.
Neither is free, and both need tickets published across the retention range exactly as a `Capture` flip does.
This is the correct behaviour rather than a problem - it is what populates the new column on old facts - but it should be a deliberate, announced operation, not a side effect of a save button.

**The invariant C must not get wrong.**
Both read paths must normalise identically.
The `attr:` path extracts text from JSONB; the promoted column goes through `_as_text`.
If those differ by so much as trimming or case, the same base UoM lands in two different rollup rows and one item's total splits in half.
One helper, used by both, asserted by a test that folds the same fact down each path and compares the stored dimension byte for byte.

**Still open, and not specific to C.**
Whether an ACTIVE definition's `dimensions` may be edited at all.
`dim1..dim4` are positional and interpreted through `definition.dimensions`, so changing that list changes what the existing rollup rows mean while their values stay as they were.
The safe rule is that dimensions and measures are immutable once active and an edit forks a new definition, which is what `status` and `backfilled_through` already exist to support.
Promotion does not need this resolved, since renaming `attr:resp.BaseUoM` to `base_uom` yields the identical dimension VALUE and so leaves stored rollups correct - but a genuine dimension change does.

Worth stating either way: four dimensions at hourly grain multiplied by item-number cardinality is where the rollup tables actually grow, and `DIMENSION_SLOTS` caps it at four for that reason rather than by accident.

## 18c. ML readiness, the component map, and the staging plan

Every claim below was checked against the running code and the live database before being written.
Where the document was wrong, the correction is stated rather than quietly patched.

### ML: the part that had to exist from day one already does

F10 said the fact ledger *"must exist from day one rather than being added when ML starts"*.
It does, and it is working.

| Claim | Verified how |
|---|---|
| `analytics_fact_ledger` exists, append-only | `models/analytics_fact.py:157`, written at `analytics/consume.py:325` |
| every version is retained, not just the latest | live: 524 distinct transactions produce 586 ledger rows, **62 of them at revision 2** |
| partitioned for long retention | monthly on `recorded_at` (`partitioning.py:114`), 5 partitions live |
| outlives raw data | in `KEEP_FOREVER` (`log_partition_worker.py:70-74`) while `log_entries` drops at 60 days |
| the ML cursor name is reserved | `ml:features-v1`, and `consumer_cursors.report` is already called by analytics (`consume.py:520`) and notifications (`notification_worker.py:32`) |

Those 62 second-revision rows are the whole point: a rebuild does not destroy the prior value, so a training set built at a revision can be rebuilt identically later.
That is the acceptance test the plan already states (Phase 1, test 11).

**Not built, and genuinely deferrable:** `analytics_feature_sets`, `analytics_predictions`, the `ml:features-v1` registration, and `services/analytics_ml/`.
None of it is on a clock.

**What is on a clock is feature CAPTURE, not the ML pipeline.**
The ledger guarantees a training set can be reproduced; it cannot invent features that were never captured.
Today a model would see 24 typed fields.
After R3 it would see those plus the response scalars - `OnHandQuantity`, `AllocatedQuantity`, `TotalNumberOfBalances`, and per-record `STQT`/`ALQT`.
Raw entries expire at 60 days, so a feature not captured today is not recoverable later.
This is the second independent reason to move R3 early, the first being section 18a's.

**A cost to state plainly.**
R3 changes every fingerprint, so it re-folds the retention window and writes a NEW ledger revision for every fact.
Each later promotion does the same.
The ledger is designed to grow this way, but it grows in steps of roughly one full copy per re-fold, so re-folds must be deliberate and counted rather than routine.

### Two errors found in the node table

Both were introduced when the plan was written and never reconciled against what shipped.

| Node | Document said | Actually |
|---|---|---|
| N5 Rollup folder | `services/analytics/fold.py` | **`services/analytics/rollups.py`** - no `fold.py` exists anywhere |
| N6 Read layer | `persistence/repositories/analytics_repository.py` | **`services/analytics/read.py`** - no repository file exists |

Four shipped modules have no node id at all: `diff.py` (the range diff), `consume.py` (N3's cycle), `contract.py`, `synthetic.py`, plus `reconcile.py` and `analytics_reconcile_worker.py` for the auditor.
The other 8 cited paths that appear to be missing are only the document's shorthand, writing `workers/x.py` for `app/services/workers/x.py`.

### The component map

34 parent tables exist.
All 34 appear below, plus the 2 planned ML tables, flagged as planned.
Every partition count, grain and retention figure here was read from the database and from `log_partition_worker.KEEP_FOREVER` / `RETENTION_DAYS`, not recalled.

```
                          WMS host  (SFTP / local directory)
                                        |
=== P1  INGESTION ==========================================================
   log_ssh_sources             which hosts, enabled per tenant     [config]
   log_ssh_file_checkpoints    byte offset per file              [position]
   log_ssh_fetch_runs          one row per fetch attempt            [audit]
                                        |
                                        v   bytes downloaded, NOT yet parsed
   log_source_objects  =========> QUEUE
=== P2  STAGE 1: PARSE =====================================================
   drained by log_parse_worker  ->  parse_insert.py
   parses request AND response in full
                                        |
                                        v
   log_entries        daily x 94 . 60 days . APPEND-ONLY
                      publishes NO analytics ticket: an entry is not a transaction
                                        |
   log_regroup_pending =========> QUEUE  (window needs stitching)
=== P3  STAGE 2: DERIVE ====================================================
   drained by log_stitch_worker  ->  derive_transactions.py
   groups entries into transactions, inherits ids, seals when settled
                                        |
        +-------------------------------+-------------------------------+
        v                               v                               v
   log_transactions              log_entry_assignment          log_regroup_runs
   daily x 95 . 60 days          daily x 16 . 60 days               [audit]
   MUTABLE projection            entry -> transaction
   (delete + reinsert)           co-partitioned with entries
        |
        |  publishes the ticket IN THE SAME COMMIT  (invariant 3)
        |  5 sites: derive_transactions.py:686/809/901, logs.py:719/777
        v
   analytics_pending_windows ====> QUEUE   range_start/range_end, NO txn identity
=== P4  ANALYTICS ==========================================================
   drained by analytics_worker (poll 2.0s)  ->  consume.py
   claim -> advisory lock -> range diff -> normalise
        |
        |   reads     analytics_metrics        the registry: what to measure
        |   reads     analytics_tenant_state   watermark, revision, frontier
        |
        +--> analytics_facts          monthly x 5 . KEEP FOREVER . latest version
        |        |                    the feature row: 24 typed fields + attributes
        |        |
        +--> analytics_fact_ledger    monthly x 5 . KEEP FOREVER . EVERY version
        |                             append-only. what makes ML reproducible.
        +--> analytics_quality_issues monthly x 5 . 365 days . quarantine
                 |
                 v   rollups.py folds facts into additive roles
        +--> analytics_hourly_rollups   daily x 67 .  90 days
        +--> analytics_daily_rollups    yearly x 2 . KEEP FOREVER
        +--> analytics_monthly_rollups  unpartitioned . KEEP FOREVER
                 |
                 v   read.py picks the coarsest grain that answers the question
              api/v1/analytics.py  ->  /status /metrics /series /breakdown /reconcile
                 |
                 v
              the chart                       audited by analytics_reconcile_worker
=== P5  NOTIFICATIONS ======================================================
   notification_rules  ->  evaluated against log_transactions
   notification_events  ->  notification_deliveries  ->  customer_notification_channels
=== P6  RAG / ASSISTANT ====================================================
   log_entries -> embedding_queue -> chunks / chunks_entity -> embeddings (pgvector)
=== P7  ML  (PLANNED, NOT BUILT) ===========================================
   analytics_fact_ledger  --pinned revision-->  analytics_feature_sets
                                                        |
                                                        v
                                                analytics_predictions
   registers its own cursor: ml:features-v1
=== CROSS-CUTTING ==========================================================
   log_partition_worker   creates the runway ahead, drops past retention
                          4 gates: retention . open window . live consumer . entry lag
   consumer_cursors       the live-consumer gate. analytics:warehouse-v1,
                          notifications, (ml:features-v1). Empty = nothing blocked.
   logspace_cleanup       per-tenant purge across every table above
   customers / customer_display_names / logspace_presence / saved_views /
   jobs / idempotency_keys / alembic_version          [platform + tenancy]
```

### Staging, from here to the end

Everything E1-E8 and N1-N7 is built and running.
What follows is what remains, in dependency order rather than in the order it was thought of.

| Stage | What | Depends on | On a clock? |
|---|---|---|---|
| **S1** | **BUILT 2026-08-24** - explicit sealer, `updated_at`, and the cursor moved onto it. See 18f. | nothing | done |
| **R1** | **BUILT 2026-08-25** - both registry tables, the shared capture predicate in all three readers, `transactions` in the fold filter. See 18g. | nothing | done |
| **R1b** | **BUILT 2026-08-25** - `attr:` paths for dimensions and measures, registry-gated, numeric coercion. See 18h. | R1 | done |
| **R3** | **BUILT 2026-08-25** - response scalars into `attributes`, namespaced, seeded allowlist with a credential veto. See 18i. | R1, R1b | done |
| **R2** | **BUILT 2026-08-25** - registry API, `/analytics/registry`, and `show` wired to the rollup gate. See 18j. | R1, R3 | done |
| **S2** | **BUILT 2026-08-25** - durable stream position; `req_pos` deleted rather than persisted. See 18k. | S1 | done |
| **S3** | **BUILT 2026-08-25** - fingerprint skip and UPDATE in place. 3 identical regroups now write 0 rows. See 18l. | S2 | done |
| **S4a** | **BUILT 2026-08-25, SHADOW** - state persisted and compared; re-derive still authoritative. See 18m. | S3 | done |
| **S4b** | SUPERSEDED (18q, 2026-08-27): the 7-day gate proved unmeasurable (`seeded_streams` = 0 in all 1,411 post-fix runs, structurally) and cross-pad joining shipped instead as a bounded backward window extension - no `_persist` change, no lookup promotion. `stage2_stream_lookup` stays `shadow`; the streaming end-state is re-planned as a head-lane fast path (18q, P4). | S4a | superseded |
| **R4** | **BUILT 2026-08-25, CAPTURE ONLY** - `analytics_record_facts`, opt-in per transaction. The record-grain FOLD is not built. See 18n. | R3 | capture done |
| **R4b** | The record-grain fold and read, so a record metric can be defined | R4 | no |
| **M1** | **BUILT 2026-08-25** - reproducible training sets pinned to the ledger, plus the predictions table and the reserved cursor. See 18o. | R3 | done |

**Why R3 before R2**, which inverts the obvious order: the allowlist starts empty, so R3 alone would capture nothing.
Seeding it with the 145 already-measured keys means capture begins accumulating history while the interface to manage it is still being built.
The alternative spends the 60-day window building a screen.

**Why the R-work precedes S1-S4**, which inverts the earlier plan: both touch `consume.py`, S1 moves the frontier column, and the registry predicate lands in the same query S1 rewrites.
Doing S1 first means doing that query twice.
Neither ordering is forced by correctness - this one is chosen because R3 is the only item with an expiry.

**S1 is independent of all of it** and is the cheapest thing on the list.

An earlier draft of this section listed a separate `S0` "sealer sweep" ahead of `S1`.
That was a duplicate: `S1` IS the sealer, and its first tick clears the backlog, so there was never a second piece of work.
The entry is removed rather than left as a synonym, because two names for one change is how a thing gets built twice.

## 18d. Component map, ITERATION 2: where every planned change lands

**Nothing in this section exists yet.**
Section 5's map is iteration 1 - built, deployed, tested.
This is iteration 2, and the HTML twin carries the same diagram drawn in the document's violet "later phase" colour so the two can be read side by side.

Section 18c's map is the system AS IT IS.
It named the 34 tables that exist plus the 2 planned ML tables, and it did not show the registry, the discovery table, the capture path, the per-record grain, or the two tables S1-S4 introduces.
This is that map: the same pipelines, with every change from S1 through M1 placed on the component it touches.

Verified before writing: all 34 existing tables appear, all 6 new tables appear and **none of them exists yet**, nothing is invented, every line citation resolves to a real file, and every figure matches the figure already in this document.

The six new tables:

| Table | Introduced by | Partitioned | Note |
|---|---|---|---|
| `log_open_stream` | S2 | **no**, deliberately | small self-cleaning working set; needs a TTL reaper |
| `log_pending_request` | S2 | **no**, deliberately | same |
| `analytics_transaction_registry` | R1 | no | **name proposed, not agreed**; one row per `transaction_name` |
| `analytics_field_registry` | R1 | no | **name proposed, not agreed**; one row per `(method, field)` |
| `analytics_feature_sets` | M1 | to decide | named in section 5's table list already |
| `analytics_predictions` | M1 | to decide | same |

Plus one undecided: R4's per-record grain is either a seventh table or a second row type in `analytics_facts`, and which it is depends on whether `rollups.recompute` folds two grains cleanly.
That code has not been read yet, so the map marks it `[OPEN]` rather than guessing.

```
  LEGEND   [NEW]  does not exist yet        [CHANGE] exists, this work modifies it
           [NEW?] proposed name, NOT agreed  [OPEN]  design decision not yet made

=== P1  INGESTION ============================== untouched by any of this work
   log_ssh_sources . log_ssh_file_checkpoints . log_ssh_fetch_runs
   log_source_objects ======> QUEUE
=== P2  STAGE 1: PARSE ========================= untouched
   parse_insert.py  ->  log_entries    daily x94 . 60 days . append-only
                        ALREADY parses response + mi_result in full.
                        Stage 2 discards it today; R3 stops discarding.
   log_regroup_pending ======> QUEUE
=== P3  STAGE 2: DERIVE ======================== S1 . S2 . S3 . S4
   derive_transactions.py
    S1 [CHANGE] explicit sealer, WITH A BOUNDED HORIZON (60 days)
                its first tick clears the 2,516-row backlog
                unbounded, it seals a 59-day-old row and alerts one day
                before that row's entries are dropped
    S2 [NEW]    log_open_stream         unpartitioned, self-cleaning
       [NEW]    log_pending_request     unpartitioned, self-cleaning
                ^^ both need a TTL REAPER in the sealer tick.
                   derived state cannot leak; persisted state can.
                   monitor count(*): a number that only grows is the alarm.
    S3 [CHANGE] log_transactions      UPDATE in place, not delete+reinsert
       [CHANGE] log_entry_assignment  APPEND-ONLY   52.7M -> 2.6M writes
       [NEW]    partial index on log_transactions
                CREATE INDEX ON ONLY parent -> per-partition CONCURRENTLY
                -> ATTACH PARTITION   (recipe from migration b3d914c7ea52)
    S4 [CHANGE] state machine replaces the window re-derive   22.4 -> ~1.0
   log_regroup_runs                 [audit] one row per regroup run, unchanged
        |
        v   ticket still published IN THE SAME COMMIT (invariant 3, unchanged)
   analytics_pending_windows ======> QUEUE     volume falls ~95% after S3
                                              STILL one ticket per
                                              (customer, window). NOT segregated.
=== P4  ANALYTICS ============================== R1 . R1b . R3 . R4
   consume.py     reads analytics_tenant_state (watermark, revision, frontier)
    S1 [CHANGE] _FRONTIER_COLUMN  created_at -> updated_at   consume.py:85
    R1 [NEW?]   analytics_transaction_registry     one row per transaction_name
                   [x] Capture   irreversible if unticked
                   [x] Show      free, reversible, retroactive
                   [ ] Expand    per-record rows, ~200k/day
                   transaction_name IS NULL -> no row: always capture, never show
    R1 [NEW?]   analytics_field_registry          one row per (method, field)
                   captured bool. unknown key -> NAME ONLY, captured=false.
                   never the value, because facts are KEEP FOREVER.
    R1 [CHANGE] ONE shared source predicate, read by BOTH:
                   consume.py:239       the fold
                   reconcile.py:125     facts_vs_transactions
                ^^ if these two diverge, the auditor alerts FOREVER
    R1 [CHANGE] analytics_metrics.filter gains "transactions" beside "methods"
                (a method is shared across transactions: ConfirmPickLine is in
                 both Brighton Stock Pick and JIT and Shorts Pick)
        |
        v   normalizer.py
    R3 [CHANGE] merge response scalars into attributes, NAMESPACED
                   resp.*  response payload   (947 scalars / 145 keys measured)
                   mi.*    mi_result + record_count
                   gated by analytics_field_registry
        |
        +--> analytics_facts         monthly x5 . KEEP FOREVER . latest version
        |      [CHANGE] attributes 806 -> ~2000 bytes, ~3 GB over 5 years
        |      [CHANGE] every fingerprint changes  =>  FULL RE-FOLD
        +--> analytics_fact_ledger   monthly x5 . KEEP FOREVER . EVERY version
        |      [CHANGE] +1 revision per fact per re-fold (one full copy)
        +--> analytics_quality_issues   monthly x5 . 365 days
        +--> [OPEN] R4 per-record grain: a NEW TABLE, or a 2nd row type in
                    analytics_facts? depends on whether rollups.recompute
                    folds two grains cleanly. NOT yet read.
        |
        v   rollups.py                                     DIMENSION_SLOTS = 4
   R1b [CHANGE] a dimension may name  attr:resp.BaseUoM
                validate() asks analytics_field_registry, NOT FACT_FIELDS
     C [CHANGE] promotion: attr: -> typed column via _FROM_ATTRIBUTES
                copies, never moves, so the attr: path keeps resolving.
                fingerprint change re-folds = the backfill, for free.
                BOTH paths must use _as_text or one UoM splits in two.
        |
        +--> analytics_hourly_rollups   daily x67 .  90 days
        +--> analytics_daily_rollups    yearly x2 . KEEP FOREVER
        +--> analytics_monthly_rollups  unpartitioned . KEEP FOREVER
        |
        v   read.py -> api/v1/analytics.py -> the chart
    R2 [NEW]    frontend registry screen: 7 transactions, 3 switches,
                the discovery list for newly seen fields
=== P5  NOTIFICATIONS ========================== S1 fixes one and moves two
    S1 [FIX]    stability.py's "incomplete AND sealed" alert starts working
                (577 rows can never reach that state today)
    S1 [CHANGE] notifications/cursor.py:153-157  created_at -> updated_at
                ^^ FAILS UNSAFE if forgotten
    S1 [CHANGE] evaluators.py:64 dedup key gains status  (free improvement)
   notification_rules . notification_events . notification_deliveries .
   customer_notification_channels
=== P6  RAG / ASSISTANT ======================== untouched
   log_entries -> embedding_queue -> chunks / chunks_entity -> embeddings
=== P7  ML ===================================== M1, and only after R3
   analytics_fact_ledger --pinned revision--> [NEW] analytics_feature_sets
                                                        |
                                                        v
                                              [NEW] analytics_predictions
   [NEW] cursor ml:features-v1        (the name is already reserved)
   the ledger makes a training set REPRODUCIBLE.
   it cannot invent features never captured -- which is why R3 gates M1.
=== CROSS-CUTTING ==============================
   log_partition_worker  [CHANGE] must NOT register log_open_stream or
                                  log_pending_request. Every existing grain,
                                  retention and drop gate is unchanged.
   consumer_cursors      [CHANGE] under-reports after S3. costs disk, not data.
   logspace_cleanup      [CHANGE] enumerates 9 analytics models explicitly at
                                  logspace_cleanup.py:133-135. EVERY new table
                                  must be added or a tenant delete orphans it.
   customers . customer_display_names . logspace_presence . saved_views .
   jobs . idempotency_keys . alembic_version              [platform + tenancy]

   TABLE COUNT   34 exist today  +  6 new  ( log_open_stream,
                 log_pending_request, analytics_transaction_registry,
                 analytics_field_registry, analytics_feature_sets,
                 analytics_predictions )  + 1 undecided (R4)  =  40 or 41
```

### Three obligations this map makes visible

**`logspace_cleanup.py:133-135` enumerates the nine analytics models by name.**
Every new table must be added there or a tenant delete orphans its rows.
This is a list in source, so nothing warns you - the test that a tenant delete leaves no rows behind has to name the new tables too.

**The two S2 tables need a reaper that no existing table needed.**
`evict_stale` closes a stream when an entry arrives, so a tenant that stops ingesting leaves its streams open forever.
Derived state cannot leak; persisted state can.
The TTL sweep belongs in the sealer tick, and `count(*)` on both tables is the only signal - a number that only grows is the alarm, and there is no upstream event to catch it.

**One shared source predicate, or the auditor is permanently red.**
`consume.py:239` decides what gets folded and `reconcile.py:125` decides what should exist.
If a capture-off transaction is skipped by one and expected by the other, `facts_vs_transactions` reports a discrepancy on every run forever.
A permanently red check is worse than no check, because it trains you to ignore the one thing that would catch a real divergence.

## 18e. Open decisions register

Verified 2026-08-23 by auditing every decision reached in design against this document: **34 of 34
present**. **Revised 2026-08-25**, because building S1-S4a, R1-R4 and M1 settled most of what was open -
and a register that still lists resolved items is worse than no register at all.

### Resolved by building

| Question | How it was settled |
|---|---|
| `transaction_name IS NULL` | As proposed: always captured, never shown, no registry row. `capture.is_captured` returns True for NULL and the registry holds no row for it. |
| a transaction seen for the first time | **CHANGED from the proposal.** `Capture` on and `Show` **ON**, not off. Show-off meant R1's discovery marked every existing transaction hidden and R2's rollup gate blanked every chart - 23 tests caught it. An under-counting total is the failure this architecture exists to prevent; the review is surfaced (`needs_review`) rather than enforced by hiding data. See 18j. |
| the two registry table names | Built as proposed: `analytics_transaction_registry`, `analytics_field_registry`. |
| R4's per-record grain | **A separate table**, and it was measured rather than argued. A second row type in `analytics_facts` inflated the seed definition's quantity from 10 to 40 - 4x, silently - because `_read_dirty_facts` has no grain predicate. See 18n. |
| `analytics_feature_sets` / `analytics_predictions` partitioning | Neither is partitioned. One row per training run and one per (subject, horizon, model, target) are small next to the fact tables, so there is nothing worth pruning. See 18o. |

### Still open

| Question | Recommendation | Blocks | Why it matters |
| may an ACTIVE definition's `dimensions` be edited | immutable once active; an edit forks a new definition | nothing shipped - R2 did not address it | `dim1..dim4` are positional, so changing the list changes what stored rollup rows MEAN while their values stay as they were. `status` and `backfilled_through` already exist to support forking. |

That is the only design question left. The two remaining STAGES are blocked on data rather than on
decisions:

| Stage | Blocked on |
|---|---|
| **S4b** promote the lookup | a week of `agreed: true` with `seeded_streams` non-zero on LIVE traffic. This development database cannot produce a stream that survives one window into the next - see 18m. |
| **R4b** the record-grain fold | nothing but effort. Safe to defer because record data is `KEEP_FOREVER` once captured, so the reader can be built at any time from stored rows. |

### What the audit corrected

Four defects were found in this document by the audit itself, all recorded in place above rather than silently patched:

| Defect | Correction |
|---|---|
| `S0` was listed as a stage ahead of `S1` | A duplicate - `S1` IS the sealer and its first tick clears the backlog. Removed. |
| `unsealed_share` described as "inflated by permanent sludge" | Wrong mechanism. `_settledness` reads only the current window and the value is stored one row per tenant, overwritten. Restated. |
| `_DERIVE_VERSION` specified in the plan, absent here | Added. Without it an edited derivation never reaches stored rows and the projection is quietly wrong forever. |
| S1 assumed to cause analytics re-folding | It does not. `sealed` is not one of the 24 `contract.FACT_FIELDS`, so sealing cannot change a fact fingerprint. |

Two of those four - the `S0` duplicate and the `unsealed_share` mechanism - were introduced by earlier passes of this document.
Recording that is the point: this section exists so the next audit starts from what the last one found.

## 18f. S1 as BUILT, 2026-08-24

**Shipped.** Migration `c4e17b9d5a83`, 22 tests in `tests/test_stage2_sealer_chunk53.py`, full suite 1,087 passing.
This is the first part of iteration 2 to exist.

### S1 turned out to be three things, not one

The plan described S1 as an UPDATE that sets `sealed = true`.
Implementing it surfaced that this fixes the flag and nothing else, because the notification cursor read `created_at`:

```
read_window: lo = the rule's cursor_at,  hi = now - lag      cursor.py:63-76
the cursor only ever moves FORWARD                            cursor.py:106+
the 3600 s lookback applies ONLY when cursor_at IS NULL
```

`alertable_predicate` names the mechanism it depends on in its own docstring: *"every Stage 2 rebuild refreshes `created_at`, so an in-flight transaction re-enters the cursor's feed on every rebuild until it seals."*
An UPDATE does not refresh `created_at`.
So a row sealed by the sealer would never re-enter the feed, `stability.py`'s `incomplete AND sealed` alert would still never fire for the 2,516 rows, and the sealer would have added a SECOND silent miss on top of the one it was written to remove.

Earlier passes of this document asserted repeatedly that S1 fixes the notification gap.
That was wrong as specified, and is why S1 shipped as:

| # | Part | Why it cannot be deferred |
|---|---|---|
| 1 | `updated_at`, backfilled to `created_at` | the cursor needs a column that moves on an UPDATE |
| 2 | the cursor reads `updated_at` (`cursor.py`, `engine.py`) | without it part 3 is invisible |
| 3 | the sealer, horizon 60 days | the actual fix |

Moving the cursor NOW rather than at S3 was deliberate: today the sealer is the only writer of an UPDATE, so the change is observable in isolation.
After S3 every rebuild is an UPDATE and the same edit would land under churn.

### Four things found by building it

**The sealer must enumerate its own tenants.**
The plan said to hang it off `log_stitch_worker`, which iterates `customers_with_due_work()` - tenants with an open `log_regroup_pending` row.
That would not have worked: the stuck rows are stuck *because* nothing tickets them any more, so enumerating by ticket leaves the sealer unable to reach the rows it exists to fix.
It enumerates by unsealed rows instead, which is what `ix_log_transactions_unsealed` is for.

**Two clocks, deliberately.**
The seal and abandon cutoffs use `_cutoffs`, measured against the tenant's newest entry, so back-dated ingestion seals correctly.
The horizon uses the DATABASE clock, because what it guards against is retention dropping the partition and retention uses `db_today`.
A tenant whose logs stopped 90 days ago would otherwise get a horizon 150 days back and the sealer would reach into partitions already gone.
Both are pinned by tests.

**The migration's first version silently did the wrong thing.**
`ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now()` does not leave existing rows NULL - PostgreSQL evaluates the default once and stores it as the column's missing value for every pre-existing row.
The backfill was written `WHERE updated_at IS NULL`, matched nothing, and all 397 local rows ended up holding one identical timestamp 62 days from their own `created_at`.
Since the cursor now reads that column, every notification rule would have seen its entire retained history as newly written and rescanned it.
Caught by a post-migration check that counted `updated_at <> created_at`, not by a test.
The column is now added with no default, backfilled unconditionally, constrained, and only then given a default.

**S1 causes no analytics fact churn at all.**
`sealed` is not one of the 24 `contract.FACT_FIELDS` and the normaliser never reads it, so sealing cannot change a fact fingerprint.
The range diff reports `unchanged` and no fact or ledger row is written.

### Two things left as they are

**The horizon is 60 days, equal to `log_partition_retention_days`.**
That leaves one day of residual risk at the boundary - a row sealed at 59 days bumps `updated_at`, is read by the cursor, and could alert one day before its entries are dropped.
Kept at 60 by decision; it is a setting (`log_seal_horizon_days`), so changing the trade-off is configuration.
The consequence is that rows already past the horizon when S1 first runs never seal.
Locally that is all 96 unsealed rows, whose newest is 74 days old; on the deployed database the oldest unsealed is 2026-08-06, so all 2,516 are inside the horizon and will seal.

**`ix_log_transactions_created_at` is retained** although the cursor no longer uses it.
The index redesign stays deferred pending `pg_stat_statements` with the analytics and reconcile workers ENABLED - the earlier "never scanned" measurement was taken with them off, which this document already records once as a mistake.

## 18g. R1 as BUILT, 2026-08-25

**Shipped.** Migration `d8f52c6a1b94`, 21 tests in `tests/test_analytics_capture_registry_chunk54.py`, full suite 1,108 passing twice consecutively.
Table names as proposed in 18e: `analytics_transaction_registry`, `analytics_field_registry`.
Neither is partitioned, for the reason 18d gave.

### The decision the switches did not settle

`capture` off had three possible scopes, and the difference is not cosmetic - the fold is a range diff, so anything present in stored and absent from source is REVERSED.

| Predicate applied to | Effect on facts that transaction already has |
|---|---|
| source only | diff sees stored-and-not-source, **reverses them, history deleted** |
| **source and stored** | **invisible to the diff: neither compared nor reversed, left alone** |
| neither | no gate at all |

The middle one shipped.
"Stop capturing" must not silently mean "destroy what you already have", and the words on the switch do not imply a delete.
Re-ticking brings the facts back into the comparison, where the fingerprint decides whether anything actually changed - measured below as 269 `unchanged`, 0 inserted.

### Three readers, one predicate

`capture.py` holds it once and three queries import it:

```
consume._read_source              what the fold reads from log_transactions
consume._read_stored              what it compares that against, in analytics_facts
reconcile.facts_vs_transactions   what the auditor expects to find a fact for
```

Any two disagreeing is permanent and loud: a transaction the fold skips but the auditor expects is reported as a missing fact on every run, forever.
A test asserts all three call it, checked on the source rather than by behaviour, because the failure mode is a MISSING call and a behavioural test only catches that once someone has written a fixture that happens to flip a switch and run the auditor together.

### Four things the implementation changed about the design

**The gate parameter has no default.**
It was first written `suppressed: frozenset[str] | None = None`, which meant a caller who forgot the argument silently read every transaction including the suppressed ones - the exact failure the module exists to prevent.
It is now required, so forgetting is a `TypeError`.
Found because two of my own tests forgot it and passed.

**The predicate returns EXCLUSIONS, not inclusions.**
An inclusion list would have to enumerate every name analytics has ever seen, so a brand-new transaction or a tenant with an empty registry would silently not be captured - and that is the one mistake retention makes permanent.
Exclusions mean unknown is captured and an empty registry captures everything.

**`suppressed` is read ONCE per run and passed to both halves of the diff.**
Reading it twice would be a race whose symptom is a fact silently reversed because somebody flipped a switch between the two queries.

**Discovery runs from the source rows already in hand**, after the read and inside the same transaction, so a name discovered on a run cannot suppress itself on the run that discovered it.
`ON CONFLICT DO NOTHING`, so observation never overwrites a decision - a transaction someone deliberately turned off must not come back on by itself the next tick, which is every second.

### E2E against the real projection

Tenant `mnp`, 269 transactions across 7 names, folded through `_consume_run`:

| Step | Result |
|---|---|
| empty registry | 269 facts, all 7 names including the 42 unnamed |
| discovery | **6** names registered `capture=on, show=off`; NULL correctly got no row |
| `capture` off for "Brighton Stock Pick" | source drops 269 to **73** rows |
| its existing facts | **196 kept**, `reversed: 0` |
| auditor | **0 findings** |
| re-enable | 269 `unchanged`, fact set byte-identical to the original |

### An unrelated flake fixed on the way

`test_the_position_is_the_minimum_across_tenants_not_the_maximum` failed once with no code change, and kept failing with R1 stashed - so it was neither R1 nor R1's E2E pollution.
The published retention position is the MINIMUM across every tenant, and a tenant with a NULL frontier blocks publication entirely, which the very next test asserts as intended behaviour.
Other modules commit `analytics_tenant_state` rows and never clean them up, so that file's cursor assertions passed on a fresh database and failed on every run after it.
The fixture now also clears state rows whose frontier is NULL, scoped so a genuinely processed tenant is untouched.

### Still open

`show` is expressible (`transaction_filter` on the definition, `transactions` beside `methods` in the stored filter) and is NOT yet wired to the registry's `show` column.
A metric must currently name its transactions explicitly.
Connecting the column to definition generation belongs with R2, the screen that sets it.

## 18h. R1b as BUILT, 2026-08-25

**Shipped.** No migration - R1b is pure code over R1's tables. 33 tests in
`tests/test_analytics_attr_dimensions_chunk55.py`, full suite 1,141 passing.

A metric may now name `attr:resp.BaseUoM` as a dimension or `attr:resp.QuantityOnHand` as a measure
field, resolved out of `attributes`, gated by the field registry.
That closes the gap 18b recorded: without it R3 captures response scalars that nothing can read.

### One resolution point, because there were two read points

```
definition.fold:297      value = row.get(m.field)          measures
rollups._dim_key:125     row.get(name) per dimension       dimensions
```

Both now go through `contract.resolve_field`, and both normalise through `contract.dimension_value`.
Writing it twice is exactly how the same value ends up in two rollup buckets and one item's total
silently halves - which is decision C's one hard requirement (18e), now enforced by a shared helper
rather than by discipline.

`read.py`'s ad-hoc live path needed no change: it builds facts as a flat column dict and routes through
`group_fold`, so it inherits the resolution.
The pre-aggregated path reads `dim1..dim4`, which already hold the resolved string.

### The find: a JSONB measure value can be a string

A typed measure field is already numeric - `quantity` is a `Decimal`, `duration_ms` an `int`.
A value out of JSONB is whatever the WMS logged, and the measured M3 records carry `"STQT": "624"`.
So `bucket[Role.sum_value] += value` would have raised `TypeError` on the first such row, inside the
fold, inside the worker's transaction, rolling back a whole window because one field was quoted.

`contract.numeric_or_none` coerces, and a value that cannot be coerced is SKIPPED under the existing
"absent is never zero" rule rather than counted as zero.
Skipping is the load-bearing half: a denominator drawn from rows that contributed no value makes every
rate wrong in a way that looks entirely plausible.
`bool` is rejected explicitly, because it subclasses `int` in Python and would otherwise fold a flag in
as 1.

### Purity kept, and it fails closed

`definition.py` still has no database access - a test asserts it, checked against the CODE with comments
and docstrings stripped, since `validate`'s docstring has to explain that the registry is the authority.
`validate` takes `known_attributes` as an argument and the callers supply it:
`registry.active_definitions` reads it once per tenant per fold, and `POST /metrics` reads it at save
time so a typo is refused with a message naming the field.

Omitting the argument refuses EVERY `attr:` path.
A caller who forgot it must not accidentally accept any attribute, because that would make the
allowlist optional for a table that is `KEEP_FOREVER`.

**Discovered is not approved.** A field appears in the registry with `captured = false`, and `validate`
reads only the rows where it is true. So discovery never authorises anything by itself, which is the
property the whole allowlist-that-discovers design rests on.

### E2E

| Step | Result |
|---|---|
| nothing approved | validate REFUSES, 2 problems, each naming its field |
| both fields discovered, `captured = false` | still refused |
| both approved | accepted |
| stored and reloaded via `active_definitions` | round-trips intact |
| folded, `"1974.0"` and `26` and `"not a number"` | KG and EA are SEPARATE buckets; the string sums; the non-numeric row is skipped, not zero |

Before R1b all three rows collapsed into one bucket keyed `None`.

### Not done

`show` is still not wired to the registry column - a metric names its transactions explicitly. That
belongs with R2, the screen that sets it. Promotion to a typed column (the second half of decision C)
is also not built; `attr:` is the explore half, and the shared `dimension_value` is what will make the
promoted column agree with it.

## 18i. R3 as BUILT, 2026-08-25

**Shipped.** No migration - R3 writes into `attributes`, which already exists. 40 tests in
`tests/test_analytics_response_capture_chunk56.py`, full suite 1,181 passing twice consecutively.
The item with the 60-day expiry is closed.

### What now reaches a fact

Measured shapes, not assumed ones:

```
response   {"response": {"StockZone": "A1", "QuantityOnHand": 1974.0, ...}}   nested ONE level
           {"response": ""}                                                   empty, and common
mi_result  {"result": "OK", "program": "MMS060MI",
            "transaction": "LstBalID", "records": [ ... ]}                     flat, plus ONE array
```

1,713 scalars against 20 non-scalars over 400 live entries, so a transaction-grain merge captures
nearly all of it. `records[]` is not expanded - only its length, as `mi.record_count`. That is R4, and
it is where the ~200k rows/day lives.

`resp.` and `mi.` prefixes are applied on the way in, so a response `ItemNumber` cannot displace the
request's - verified in the E2E, where the request-side value survives beside eight `resp.*` keys.

### The ordering bug, which was silent and would have been permanent

The first implementation read the approvals and then registered the discovered fields.
So on the run that DISCOVERED a seeded field, that field was not yet approved and the fact was written
without it.

That self-heals only if the window is folded again - and tickets are published on change, so a window
that never changes again never is.
The response data for it would have been lost when the raw entries expired at 60 days: exactly the loss
R3 exists to prevent, introduced by R3.

Found by an end-to-end run, not by a unit test. Discovery reported 12 fields and 9 approvals while the
fact carried nothing but its request-side attribute. The order is now
extract, register, read approvals, normalise, and a test asserts it on the source, because the defect
is an ORDER and no return value reveals it.

### The security shape

| Field | Outcome |
|---|---|
| approved in the registry | stored |
| unknown | NAME recorded, `captured = false`, value never stored |
| credential-shaped | never auto-approved, whatever any list says |

`SEED_FIELDS` exists because an empty registry captures nothing, and shipping "discovers everything,
captures nothing until somebody clicks" would have spent the whole 60-day window collecting field
names.
It lives in code rather than a table on purpose: it is a security boundary, so it should be reviewable
in a diff.

`never_auto_approve` sits underneath it as defence in depth. A hardcoded list is a thing someone will
edit, and `AccessToken` and `M3UserCredentials` are the two most frequent response keys of the 145
measured, flowing toward a table that is `KEEP_FOREVER`. So a credential-shaped name is vetoed no
matter what the list says - by PATTERN, not by exact name, because the risk is a field the WMS renames
or adds. `_SENSITIVE`'s five exact words (`derive_transactions.py:45`) are precisely why an exact
denylist was not enough.

A field can still be approved deliberately, by a person, one row at a time.

### E2E, with credentials planted on purpose

| Assertion | Result |
|---|---|
| 12 fields discovered | 9 auto-approved, 3 held for review |
| `resp.AccessToken`, `resp.M3UserCredentials` | discovered, **not** captured, values absent from the fact |
| `resp.SomeBrandNewField` | recorded by NAME, value `"review me"` absent |
| seeded warehouse fields | captured on the FIRST fold, `mi.record_count = 2` |
| request-side `ItemNumber` | intact beside eight `resp.*` keys |
| `STQT` / `BANO` from `records[]` | absent, as intended |
| R1b read | grouped by `attr:resp.BaseUoM`, summed `attr:resp.QuantityOnHand` = 1974.0 |

### A limitation found while testing, worth knowing before deploying

R3 joins entries to transactions through `log_entry_assignment`.
On this development database that table holds no rows for the `mnp` tenant at all - its 397
transactions predate the assignment table - so R3 captured nothing for it and the E2E had to be run on
planted data.

The degradation is graceful: no crash, no wrong numbers, simply no response attributes for
transactions with no assignments. But it means **R3 captures nothing for any transaction stitched
before assignments existed**, and that should be checked on the deployed database rather than assumed.

### The cost, as predicted

Every fingerprint changes, because `attributes` is fingerprinted. So the first fold after deploying
re-writes every fact in the retention window and appends one new ledger revision per fact. That is the
re-fold `18a` and `18c` both flagged; it is what populates the new keys on existing facts, and it
should be a deliberate, announced operation rather than a surprise.

### The flake, fixed properly this time

`test_the_position_is_the_minimum_across_tenants_not_the_maximum` failed again.
The earlier fix (18g) scoped the cleanup to NULL-frontier rows, which was wrong for the second reason:
a third tenant with an OLDER REAL frontier simply wins the global minimum.
The fixture now clears every tenant's analytics state, since `published == min(a, b)` is only true when
a and b are the only tenants with a row. Verified by two consecutive full runs.

## 18j. R2 as BUILT, 2026-08-25

**Shipped.** Backend: 4 endpoints, 21 tests, suite 1,203. Frontend: `/analytics/registry`, its menu
entry, `RegistryPanel`, 16 tests, suite 485, `tsc` clean, zero lint warnings.
No migration - the tables came with R1.

`show` is now wired to something, which 18h recorded as the gap R2 owed.

### The regression R2 surfaced, which was mine

`DEFAULT_SHOW` was `false`, reasoned in 18e as "an unreviewed transaction never surprises a reader".
That was backwards.
R1's discovery registers every transaction it sees, so the first fold after deploying would have marked
every EXISTING transaction hidden, and R2's rollup gate would then have blanked every existing chart.
Twenty-three tests caught it by folding to zero.

The two defaults are not symmetric, and neither is safe in the abstract:

| Default | Consequence |
|---|---|
| `capture` off | loses history IRREVERSIBLY, because entries expire at 60 days |
| `show` off | UNDER-COUNTS every chart, silently, until somebody reviews a row |

An under-counting total is the exact failure this architecture exists to prevent: it looks plausible
and nothing says it is wrong.
So both default ON, and the review is SURFACED (`needs_review`, from `reviewed_at IS NULL`) rather than
enforced by hiding data.
This reverses what 18e proposed, and the reversal is recorded rather than quietly applied.

### Where the `show` gate actually sits

`_read_dirty_facts` (`rollups.py`), not the chart.
A metric whose dimensions omit `transaction_name` could not be filtered from a pre-aggregated bucket at
read time - the information is gone by then. Gating the rollup read means:

- facts stay exactly where they are, so nothing is lost
- flipping it back on refills complete history on the next fold, which is the "one recompute" the switch
  advertises
- a NULL `transaction_name` always passes, because `x NOT IN (...)` is NULL for a NULL `x` and a row is
  kept only when the predicate is TRUE - without the explicit `IS NULL` the connectivity probes would be
  silently dropped the moment any transaction was hidden

### The ticket asymmetry

| Change | Ticket? | Why |
|---|---|---|
| `capture` ON | **yes** | the transaction has no facts for the range it was off |
| `capture` OFF | **no** | existing facts are deliberately left alone, so a fold would find nothing to change |
| `show` either way | **yes** | rollups genuinely have to be recomputed in both directions |
| `expand` | **no** | R4 does not exist, so it changes no stored data |

Publishing unconditionally would re-fold the whole retention window every time somebody ticked a switch
that does nothing yet.
The publish happens in the SAME transaction as the switch, which is invariant 3 applied to a registry
write.

### The screen

Three toggles are not presented as equals, because they are not equal.
Turning `capture` off is confirmed, and the dialog says to use `show` instead if the intent is only to
hide something. Nothing else is confirmed - a confirm on the safe direction too is how people learn to
dismiss them unread.

`PATCH` sends only the key that changed. A `PUT` would make "I toggled show" silently reassert the other
two from whatever that tab last read, which is how one stale tab undoes another person's decision.

An empty list says WHY it is empty. Rows are created by the fold, so empty means "nothing counted yet"
rather than "nothing configured", and on a screen those are indistinguishable. A read failure surfaces
as an alert rather than as an empty table, for the same reason.

### Still not built

R4 (`expand`) and the promotion half of decision C. `expand` is settable and inert, and the screen says
so rather than implying otherwise.

## 18k. S2 as BUILT, 2026-08-25

**Shipped.** No migration, no new table, and no efficiency gain: writes per row stay at 22.4, exactly as
the plan says. 18 tests in `tests/test_stage2_stream_position_chunk58.py`, full suite 1,221.

S2 is a prerequisite. S4's lookup asks "which open transaction does this entry belong to" against state
read back from a table, and two pieces of `_group`'s state could not survive a process boundary at all:

| Was | Why it cannot be persisted |
|---|---|
| `_TxnBuilder.open_pos` | an index within the CURRENT BATCH. Batch 2's index 0 is not batch 1's index 0, so the number means nothing once written down. |
| `req_pos: dict[int, int]` | keyed on `id(entry)`, a CPython object address. Not stable across processes, and not stable within one either, since CPython reuses addresses after collection. |

### The fix turned out to be a deletion

`req_pos` existed only to remember WHERE IN THE STREAM a request arrived.
That is a property of the entry, not of the loop reading it - so the dict was not made durable, it was
removed. `_stream_pos(e)` derives the same answer from the entry.

That also removed a leak nobody had noticed: `req_pos.pop(id(r), -1)` left an orphaned key behind
whenever a request was consumed by a path that did not pop it.

### The position, and the field the existing helper was missing

```
(timestamp is None, timestamp, source_file, line_number)
```

`source_file` is in it and is deliberately NOT in the pre-existing `_entry_stream_order`.
That helper orders entries WITHIN one transaction, where the file is effectively constant.
This one is a key across a whole window, which routinely spans several files - and without the filename,
two entries on line 5 of two different files compare EQUAL. That is exactly the collision a durable key
must not have.

### Three builders had no position at all

`open_pos` was never set on the three sites that create a builder for a matched pending request, an
orphan response, or a request with no response. It stayed at the `-1` default.

Harmless while it was a batch index, because those builders are appended immediately and never compared
again. Not harmless once S4 reads the field back and expects it to mean something. All three now carry a
real position, and the E2E asserts that NO builder is left at the sentinel - 0 of 22 on real data.

### The observable change

Two builders opening at the same instant used to break ties on batch index, which is arbitrary; they now
break on `(source_file, line_number)`, which is a property of the data. Strictly more deterministic.

### E2E on 1,200 real entries

| Check | Result |
|---|---|
| one batch | 22 transactions, all 1,200 entries accounted for |
| builders at the sentinel | **0 of 22** |
| position order vs read order | identical |
| entries lost or duplicated, at 7 different cut points | **none** |
| grouping the same input twice | identical |
| duplicate positions across the window | **0** |

Honest limitation: that sample spans a single `source_file`, so cross-file uniqueness is covered by the
unit test rather than by the live data.

### A docstring that was inverted

`_entry_stream_order` said "NULL timestamps first" and always did the opposite: `False` sorts before
`True`, so an entry that HAS a timestamp comes first and the unparsable ones trail. The behaviour is
right and is what the renderer wants; only the sentence was wrong. Corrected rather than left, because
the next person to touch ordering would have trusted it.

### A test assertion that took three attempts

"`_group` must not call `id()`" is a source assertion, since the failure is the PRESENCE of a construct
rather than a wrong answer. A plain substring search matched the COMMENT that explains the removal.
Stripping comments was still wrong, because `take_by_reqid(` and `_entry_reqid(` both contain the
characters `id(`. It is now an AST walk that asks whether the builtin is actually called.

Worth recording because chunks 29 and 55 each hit the first version of this separately: grepping source
text for a construct is nearly always weaker than parsing for it.

## 18l. S3 as BUILT, 2026-08-25

**Shipped.** Migration `e6b93a4d7f12` (two nullable columns, no backfill), 22 tests in
`tests/test_stage2_fingerprints_chunk59.py`, full suite 1,243.

Measured on 402 real transactions: **three consecutive regroups of the same window wrote zero rows.**
Before S3 that same window rewrote every row and every assignment on every pass.

### The delete was the trigger, not a storage decision

`assignments.is_unassigned()` was how a rebuild found work, and only the delete made entries eligible
again. So removing the delete meant replacing the trigger:

```
was    DELETE everything in the window -> re-read the entries that are now unassigned
is     read the stored digests -> read ALL entries in the window -> keep those whose owner is
       absent or is one of the transactions being rebuilt
```

Provably the same set: the delete removed exactly the rows in `freed`, which is exactly what made
their entries ownerless. Both inputs were already in memory, and it takes a `NOT EXISTS` off the hot
path.

### Two fingerprints, and the split that produces the gain

| | Covers | Changing it costs |
|---|---|---|
| `row_fingerprint` | the transaction's own columns | one UPDATE |
| `members_fingerprint` | an ORDERED digest of the entry ids | an UPDATE **plus** rewriting the assignments |

`entry_count` is not a substitute for the second. Swap one same-timestamped entry between two
transactions and both keep their counts, their timestamps and every derived column, while
`log_entry_assignment` holds the wrong mapping forever - because nothing recomputes a row whose
fingerprint matched.

The split is where the assignment gain comes from: sealing changes the ROW and not the MEMBERS.
Verified - corrupting a row digest produced one row update and **zero** assignment writes.

### Two bugs found by building it, both mine

**The early return left orphans.** `regroup_window` returns early when no entries are eligible. Before
S3 the delete had already happened above, so that path was safe. After S3 nothing had been deleted, so
a window whose entries had all disappeared left every one of its transactions alive pointing at
nothing - with the ticket published above describing a reversal that never happened.

Caught by `test_site_1_publishes_even_when_the_rebuild_finds_nothing`, whose premise is exactly "freed
and not rebuilt". That test existed before S3 and was written for a different reason; it earned its
keep here.

**`existing` stopped meaning what it meant.** `_persist` skipped any id already in the table as an
out-of-order clash. After S3 the row being compared against is in the table by design, so every single
transaction would have been skipped as a clash. The check is now "in the table AND not one of the rows
this window is rebuilding".

### S3's counters were computed and then dropped

`transactions_unchanged`, `_row_only`, `_rewritten`, `_deleted` were returned by `_persist` and not
propagated by `_merge_stats`, so every caller saw them as absent. The numbers that say whether the
skip is working were invisible one function short of anywhere they could be read. Found by the E2E
printing `unchanged=None`.

### E2E, both directions

Proving it writes nothing is only half the job; a no-op would pass that.

| Check | Result |
|---|---|
| three identical regroups | **0 writes**, 402 unchanged each time |
| corrupt a ROW digest | `row_only=1`, `updated_at` moved, **0 assignment writes** |
| corrupt a MEMBERS digest | `rewritten=1`, row updated, assignments rewritten |
| back to quiet | settles to 0 writes |
| `stage2_fingerprint_skip = False` | `created=402, unchanged=0` - full delete-and-reinsert, a real rollback |
| flag back on | settles to 0 writes |

### Two measurement mistakes worth recording

The first E2E read `pg_stat_user_tables` deltas and reported zero for work that had really happened -
those counters are updated asynchronously by the stats collector. And `LIKE
'log_entry_assignment_2026%'` silently missed `log_entry_assignment_default`, which is exactly where
these May/June rows live. It now counts actual rows and digests `updated_at`, so an in-place UPDATE is
visible even though the row count does not move.

### `_DERIVE_VERSION`, and what pins it

Currently 1. A test digests the source of `_group`, `compute`, `_merged_attrs`, `_is_sealed`, `_anchor`,
`_entry_stream_order` and `_stream_pos` and asserts it against a stored constant. Edit any of them and
the test fails with the new digest and instructions: bump the version if the change alters what a
transaction's columns SAY, update the constant alone if it was cosmetic.

Without it an edited derivation never reaches stored rows - they keep matching their own stale
fingerprint, with no failing test and no alert.

### Still deferred

`created_at` now genuinely means "first written" for rows written after this, but rows written before
S3 keep whatever their last rebuild stamped. The analytics frontier (`consume.py:_FRONTIER_COLUMN`)
still reads `created_at` and should move to `updated_at` - S1 already moved the NOTIFICATION cursor,
which was the one that failed unsafe. That remains open.

S4, the lookup, is next and is the only stage that removes the re-derive from the hot path.

## 18m. S4a as BUILT, 2026-08-25 - in SHADOW, promoting nothing

**Shipped.** Migration `f4c82e9b6d31` (`log_open_stream`, `log_pending_request`), 24 tests in
`tests/test_stage2_stream_lookup_chunk60.py`, full suite 1,268.

`stage2_stream_lookup` defaults to `shadow`. The state is written and read, a second grouping is seeded
from it and COMPARED, and the re-derive stays authoritative. Verified on real data: the shadow pass
changed the transaction count, the assignment count and the `updated_at` digest by exactly nothing.

### Why shadow rather than on

S3 made the six known miss modes PERMANENT. Nothing revisits a row whose fingerprint matched, so a
split that should have merged never heals - whereas before S3 it healed on the next of 22 rebuilds,
which is precisely why none has ever been observed in production. Promoting without measuring
divergence would make a silent split unrecoverable.

The mode is three-valued (`off` / `shadow` / `on`) and an unrecognised value falls back to **shadow,
not off**. That is deliberate: a typo falling through to `off` would look exactly like S4 working
perfectly and never diverging, which is the most misleading failure available.

### The honest limitation, and it is the main finding

**The seeded path could not be exercised on this development data at all.**

Re-running one window refuses every stream as `clock_went_backwards` - correctly, since state saved
from a window sits inside it and seeding from your own output would be circular. So `agreed: True` on
a single window is VACUOUSLY true: both runs were unseeded.

Splitting into two adjacent windows refuses all 30 as `quiet_gap`: window A's newest stream ends
2026-05-19 12:42 and window B starts 2026-05-30, an eleven-day gap. The guard is behaving exactly as
designed; this dataset simply contains no stream that survives from one window into the next.

So the mechanism is proven by a unit test - a REQUEST in one window and its RESPONSE in another become
ONE transaction when seeded, and remain two when not - and **not** by live measurement. That is the
whole argument for shadow mode: divergence can only be measured where windows are minutes apart, which
is production and not a dev snapshot.

| Run | stored | seeded | refusals | agreed |
|---|---|---|---|---|
| same window twice | 98 | 0 | `clock_went_backwards: 98` | vacuously |
| two adjacent windows | 30 | 0 | `quiet_gap: 30` | vacuously |

### Refusals are counted BY REASON

"The guard declined 900 times because the tenant was idle" and "declined 900 times because the clock
went backwards" are the same number and completely different problems. So the report carries
`{quiet_gap: n, clock_went_backwards: n, no_timestamp: n}` rather than a single tally.

### The bug the unique constraint caught

`_group`'s `open_by_key` is a dict, so at most ONE stream per `(thread, user_ctx)` can be open. But the
list S4 saves from holds FINISHED builders too, and a thread that flipped A to B and back contributes
two builders under the same key - which violated `uq_log_open_stream_key` on the first real run.

The save now keys rather than appends, newest wins. That is also the right answer rather than merely a
way to satisfy the constraint: the most recent activity on a key IS the stream a following entry would
join. `NULLS NOT DISTINCT` is what made this a loud failure instead of a silently duplicated stream.

### Replace, never mutate

Read state, seed, write the RESULT back - all inside `regroup_window`'s existing transaction. The state
can therefore never be AHEAD of the assignments: either both commit or neither does. A failure leaves
the ticket open, the retry reads the same pre-failure state, and it converges. An incremental mutation
would leave state describing entries that were never assigned, with nothing to notice.

State is saved from the AUTHORITATIVE grouping, not the seeded one, or the next window would seed from
a grouping nobody persisted.

### The reaper, which is required rather than optional

`evict_stale` closes a stream when an ENTRY ARRIVES, so a tenant that stops ingesting leaves its rows
forever. Derived state could not leak; this can. The sweep runs in the stitch tick, keyed on
`updated_at` rather than `last_entry_ts` - what matters is how long ago the GROUPER touched the row, not
how old the log line was, and a backfill of month-old data is actively in use.

`count(*)` on both tables is reported on every sweep, because it is the only health signal these tables
have and there is no upstream event to catch a leak.

### What promotion needs

A week of `agreed: true` on real traffic WITH `seeded_streams` non-zero. The second half is the part
this build cannot supply, and the first is meaningless without it.

`_DERIVE_VERSION` stayed at 1. Adding `_group`'s `seed` parameter changed the pinned source digest, and
the pin fired - working as intended. With no seed the added loop iterates an empty tuple, so every
existing row derives exactly as before and every stored fingerprint stays valid. Asserted by a test
rather than reasoned about, because "my change is a no-op" is the belief that makes an unbumped version
dangerous.

## 18n. R4 as BUILT, 2026-08-25 - capture only, the record fold is NOT built

**Shipped.** Migration `a2d5f81c93e7` (`analytics_record_facts`), 23 tests in
`tests/test_analytics_record_grain_chunk61.py`, full suite 1,294.

### 18a's open question, settled by measurement

18a left it open whether the record grain should be a new table or a second row type in
`analytics_facts`, pending a read of `rollups.recompute`. It is not a matter of taste.

`_read_dirty_facts` selects the whole table with no grain predicate, and `group_fold` has no notion of
grain. So record rows fold into the SAME buckets as their parent. Feeding the seed definition one
transaction plus three of its records inflated the quantity total from **10 to 40 - 4x, silently.**

Avoiding that would require EVERY definition to carry a grain filter, and forgetting one on any single
definition produces a plausible-looking wrong total. A separate table makes the mistake structurally
impossible: the existing fold names `AnalyticsFact` and cannot see these rows. Both the inflation and
the structural separation are now tests, so the question cannot be re-opened by accident.

### What R4 does, and the one thing it does NOT

**Does:** one `analytics_record_facts` row per `mi_result.records[]` entry, for transactions whose
`expand` is ticked, namespaced `rec.` and gated by the same field allowlist as the scalar grain.

**Does NOT: there is no record-grain fold or read.** A record metric cannot be defined or charted yet.
That is the cost 18a named for this option - "doubles the fold path" - and it is deferred rather than
done.

The split is deliberate and follows the same reasoning as R3: **capture has a 60-day deadline and read
does not.** `records[]` lives in `log_entries`, which drops at 60 days, so a record not captured today
is gone. Once captured the table is KEEP_FOREVER and a fold can be built over it at any time from
stored data. R1b's lesson - "capture without read is storage with no product" - applied to R3 because
`validate` REFUSED the metric outright, making the feature unreachable. Here the data simply
accumulates until the reader exists.

### `expand` is the one switch that inverts the pattern

| Switch | Direction | Default | Why |
|---|---|---|---|
| `capture` | exclusions | on | a name missing from the registry must still be captured |
| `show` | exclusions | on | a name missing must still be shown, or charts under-count |
| `expand` | **inclusions** | **off** | a name missing must NOT be expanded |

~200k records a day on the deployed database against ~1,400 transaction facts. Defaulting on would grow
a table by hundreds of thousands of rows a day that nobody asked for, so the volume is chosen rather
than inherited - and silence has to mean no.

**No record field is seeded either.** The seed list exists so the SCALAR grain produces history from day
one without a screen. A record field only exists because somebody ticked `expand`, so they are already
making a decision and can make this one too. Visible in the E2E: expansion writes the rows immediately
and their attributes stay empty until a field is approved.

### The measurement discrepancy, stated rather than smoothed over

The doc cites 3,641,353 records from the deployed database. **This development database holds 8,614 in
its entire retained window** - average 2.3 records per `mi_result` entry, maximum 26, 61 distinct keys,
and all 3,765 sampled field values scalar.

The smaller number is not evidence that the cost is small. It is evidence that this box holds far less
data, and the deployed figure is the one to plan capacity against.

`LOUD_EXPANSION = 500` logs a warning for any single transaction above it - far above anything measured,
so it only fires when `expand` is ticked on something that returns a catalogue. A WARNING and not a cap:
truncating would produce a record count that looks complete and is not.

### It does not undo S3

Expansion is driven by the DIFF's verdicts, not by the source rows. A transaction the diff called
`unchanged` has records that are also unchanged, so re-expanding it would put back exactly the write S3
removed. Verified: a settled window leaves the record-fact ids byte-identical.

Replace-per-transaction rather than upsert-per-record, because a re-expansion can produce FEWER records
than the last one and an upsert keyed on `(transaction, index)` would leave the previous tail behind
forever. Verified: three records became one, with no orphan.

### Three registrations that are silent if forgotten

Partitioning (monthly on `event_time`), `KEEP_FOREVER`, and the tenant purge list. All three were caught
by existing guards the moment the table was added - the partitioning set, the retention classification
and the nullability split each failed and had to be updated deliberately. `event_time` is nullable
because it is COPIED from the parent's, which is parsed from a log line, so a record whose transaction
has no parsable timestamp stays insertable.

### E2E on real shapes

| Check | Result |
|---|---|
| `expand` off | **0** record facts; `mi.record_count = 3` still on the transaction fact |
| `expand` on | **3** record facts, one per record |
| approval | `rec.STQT` and `rec.ITNO` stored, `rec.BANO` absent |
| settled window | record-fact ids **byte-identical** - S3's skip survives |
| 3 records become 1 | **1** row, no orphan tail |

## 18o. M1 as BUILT, 2026-08-25

**Shipped.** Migration `b7e34c9a2f58` (`analytics_feature_sets`, `analytics_predictions`), 20 tests in
`tests/test_analytics_ml_features_chunk62.py`, full suite 1,315.

This is the chunk F10 was written for. Its claim was that the fact ledger "must exist from day one
rather than being added when ML starts", and it is now proven on real data rather than asserted:
**restate a fact, rebuild the training set at the same pin, and the content hash is identical.**

### The plan said "pinned revision". There is no such coordinate.

`analytics_fact_ledger.revision` is PER FACT - measured 1..2 per transaction on live data - and the
ledger carries no tenant-level revision at all. A per-fact counter cannot identify a moment in time.

The coordinate is an INSTANT, `pinned_at`, resolved against `recorded_at`. That works for a specific
reason: a fold stamps every ledger row it writes with the same instant, so a fold is atomic in those
terms and any pin is a clean cut BETWEEN folds rather than through one. Ties break on `revision`
descending, so even a same-microsecond collision resolves deterministically instead of by whichever row
the planner happened to return first.

`latest_pin` exists because the natural mistake is to pin to `now()`, which can select a moment a fold
is part-way through writing.

### What makes it reproducible, and what makes that checkable

A feature set stores three things and no rows:

| | |
|---|---|
| `pinned_at` | WHICH versions of the facts |
| `code_version` | WHICH transformation - the same facts through different code are a different set |
| `content_hash` | WHAT the answer was |

The rows are a pure function of the first two over the ledger, so storing them would be a second copy
that can disagree with the first, and it would be the largest table in the system.

The third is what turns "reproducible" from a claim into a test. `verify` rebuilds at the stored pin and
compares, and it is a production function rather than only a test helper - it is the one check that can
notice the guarantee has quietly stopped holding, which would otherwise surface as two models that
disagree for no visible reason.

### Three decisions that each prevent a plausible wrong answer

**A reversed fact is excluded.** The ledger records a reversal as a version like any other, so a
transaction whose newest version at the pin is a reversal did not exist then. Including it would train a
model on rows the system had already retracted.

**One row per transaction, not one per version.** A row per version would weight a frequently-restated
transaction more heavily than a stable one - a bias introduced by Stage 2's write pattern rather than by
anything in the warehouse.

**An oversized set RAISES rather than truncating.** CLAUDE.md rule 3 applies here more than anywhere: a
silently truncated training set trains a model on a subset nobody chose, and it looks fine until it is
wrong in production.

### The cursor, and why registering it matters

`ml:features-v1`, the name reserved in this document from the start. Registering it is not bookkeeping:
`consumer_cursors` is what stops the partition worker dropping source data a lagging reader has not
seen. A pipeline that read the ledger WITHOUT registering would have its history dropped from under it,
and its cursor would move past the gap without noticing.

The position is the PIN, not `now()` - everything strictly before it has been consumed, and anything
after it has not been looked at.

### A test that was inverted rather than deleted

`test_the_ml_tables_are_not_in_phase_1` asserted these tables did NOT exist, on the grounds that
"building them now would freeze a feature-set shape before the ML work has said what it needs". M1 has
now said, so the reason no longer holds. It is now a SHAPE guard on the columns reproducibility
requires, which is worth more than an absence one.

### E2E on the real ledger

| Step | Result |
|---|---|
| build at the ledger high-water mark | 269 rows, 10 features, hash `9a7b6e58...` |
| cursor | `ml:features-v1` registered at the pin |
| restate a fact with quantity 999999 | done |
| **rebuild at the SAME pin** | **hash identical - reproducible** |
| build at a LATER pin | different hash, and the restated row is present |

The last row matters as much as the one above it: if every pin gave the same answer, reproducibility
would be trivially satisfied by a function that ignores its argument.

### What M1 is not

It builds training SETS and records model OUTPUTS. It does not train a model - there is no estimator,
no fitting, no scoring. That is downstream work, and it is now unblocked in the only way that mattered:
a training set built today can be rebuilt identically in a year, so a model's inputs are recoverable
when somebody asks why it said what it said.

## 18p. The S4 shadow divergence: diagnosed and fixed, 2026-08-25. Still in shadow.

18m predicted the gate; the first night of live traffic failed it: **8 of 8 runs where a stream
actually seeded DIVERGED**, always with the same signature - one extra group in the seeded run, 7-8
groupings shifted. Diagnosed on the live data by replaying real windows read-only, then dissecting the
one that diverged.

### The mechanism, measured not theorised

Every diverging seeded stream's transaction was **outside the window's rebuild set** (`in_freed=False`
for all 11 in the dissected window). Two consequences compound:

**A phantom open stream steals FIFO responses.** An out-of-scope stream describes a persisted
transaction whose entries the authoritative run cannot even see. But its seeded builder sits in
`open_by_key`, so when a user-scoped RESPONSE arrives with no thread match, the FIFO rule - "oldest
open request for that user" - hands it to the phantom instead of the stream both runs see. Every later
same-user grouping then shifts by one, which is exactly the 7-8-groups signature.

**A carried entry replayed is a duplicated transaction.** An in-scope stream's entries are also
eligible window rows. Re-processing the stream's own REQUEST closes the seeded builder as "a prior
cycle", splitting one transaction into two groups.

One measured window went from **1 cold group to 17 seeded ones** through these two together.

### The fix, verified against the live data before it was written into the code

| | |
|---|---|
| scope | `_shadow_compare` seeds only streams whose transaction is in `freed`, and reports the excluded count as `out_of_scope` so the exclusion is visible rather than silent |
| dedupe | `_group` skips rows a seeded builder already carries |
| result on the diverging window | `fixed == cold`, exactly |

`_DERIVE_VERSION` stays unbumped a second time: the dedupe is behind `if seeded_ids and ...`, so the
unseeded persisting path is byte-identical, and the same test that proved that for S4a proves it now.

### What the exclusion means, honestly

Joining a late response across the pad boundary to a NOT-rebuilt transaction is S4b's genuinely new
capability - the plan calls it failure mode 1 and "the single most important test". It is now
deliberately **out of the shadow measurement**, for a reason stronger than comparison hygiene: under
`mode=on`, `_persist` would skip such a builder's id as an out-of-order clash, so the capability cannot
even be persisted without its own `_persist` design. Measuring what cannot be persisted was
manufacturing divergence.

So shadow now measures the question it can answer - *does the seeded path reproduce the re-derive on
identical inputs?* - and `out_of_scope` counts how often the bigger question is being deferred.

### The gate, restated - and then measured to death (see 18q)

As written on 2026-08-25: seven consecutive days of `agreed: true` with `seeded_streams > 0`.
Measured on 2026-08-27 after ~24 hours of live traffic: 1,411 shadow runs, zero divergences, and `seeded_streams = 0` in EVERY run - the agreements were vacuous.
The reason is structural, not bad luck: a stream may only seed when its transaction is in the rebuild set AND its last entry precedes the window floor, and a rebuilt transaction's entries are inside the window, so the two conditions are near mutually exclusive.
The ~1 usable stream per run (1,401 in 24 h) is exactly the cross-pad population the fix correctly excludes as `out_of_scope`.
This gate can therefore never accumulate evidence and is SUPERSEDED by 18q.
`stage2_stream_lookup` remains `shadow` and is never promoted to `on`.

## Also corrected while measuring

`settings.py:78-79` and `:110-112` claim "real transactions are ≤2 min".
Measured over 151,757 live transactions: avg 1.7 s, p99 28.1 s, **max 363.7 s (6.1 min)** - longer than `log_open_gap_seconds` = 300 s.
Both windows should be re-derived from measurement rather than from that note.
(Done in 18q: both comments now carry the measured numbers, and the gap measurement itself - zero attributable splits over 7 days - is recorded there.)

Full implementation plan, staging and verification gates: `~/.claude/plans/2026-08-23_22-06_stage2-incremental-state-machine.md`.

## 18q. Cross-pad healing as BUILT, the S4b gate superseded, and the streaming end-state re-planned. 2026-08-27.

Prompted by the owner's question "when can stream lookup go on?", answered by measurement, and redirected by design review.
The full plan (gap analysis of the streaming goal per pipeline stage, two rejected designs, live measurements) is `~/.claude/plans/2026-08-27_01-30_stage2-streaming-endstate-and-crosspad.md`; this section records what shipped and what it supersedes.

### The measurements that changed the plan (all live, 2026-08-27)

1. **The S4b gate was unmeasurable.**
1,411 shadow runs after the 18p fix, zero divergences, `seeded_streams = 0` in every single one - structural, as restated in 18p above.
2. **The cross-pad case is structurally impossible at current settings.**
A cross-pad join needs the old transaction to span more than pad - gap = 600 s; the measured maximum span is 363.7 s (and 354.5 s over the most recent 7 days).
3. **Responses carry no identity.**
0 of 18,090 response entries have any reqid-like field (their `fields` hold exactly one key, the payload), so an identity-based targeted lookup - the tempting alternative - cannot exist for the one entry type that arrives late.
Requests and POST bodies DO carry `ReqID`; ask the WMS team whether M3 can log it on response lines, because a yes reopens identity joins from a position of strength.
4. **The gap rule is not splitting anything.**
7 days, ~54,000 transactions: 4 orphan-response fragments, none preceded by an incomplete transaction of the same user within 30 minutes.
`log_open_gap_seconds` stays 300; the orphan-response-per-day count joins the health sweep as the standing alarm, and raising the gap ever requires a `_DERIVE_VERSION` bump.

### What shipped instead of "cross-pad _persist" (chunk 65)

A **bounded backward window extension** in `regroup_window` (`_cross_pad_floor`, `CrossPadSpanExceeded`), governed by `stage2_cross_pad` = off / shadow / **on** (ships as shadow).
When a conversation ends within `gap` of the window's padded floor AND an ownerless entry could actually join it, the floor moves back to the conversation's start - bounded at pad + gap = 1200 s - and the EXISTING cold rebuild sees the conversation whole and joins it through the machinery already trusted: grouping, fingerprints, update-in-place, ticketing.
`_persist`, `_group` and `_shadow_compare` are untouched; the chunk-59 derivation digest and the chunk-42 delete census pass unmodified, which was a design constraint, not an accident.
A joinable conversation starting beyond even that bound RAISES, so the ticket retries and dead-letters loudly rather than the rebuild splitting it silently.

Why not the original "attach to the exact transaction id in `_persist`" (design A): its own span guard refuses every genuine attachment by definition (the target starts before the floor, so the merged span always exceeds the pad), and persisting the seeded grouping re-opens the 18p phantom cascade.
Why not "just find the record by identity": measurement 3.

The three review amendments that made the extension shippable, each pinned by a chunk-65 test:
the floor is pad + gap not pad (or healthy 600-900 s spans dead-letter);
the ownerless-entry precondition is mandatory (or nearly every live tick would extend and re-free the freshly-sealed band, one pointless rewrite + notification re-entry per transaction fleet-wide);
and the moved floor threads through every window-derived read via the single `lo_p` local (or an inherited id falls into the vanished DELETE - the citation-breaking churn `continuity` exists to prevent).

### Also fixed, chunk 64: the two S3 follow-ups the doc mandated

- `consume.py` `_FRONTIER_COLUMN` moved from `created_at` to `updated_at` - the constant had quietly become DEAD (the fold read the literal string), so the test pins both the binding and that the fold reads through it.
This was a LIVE latent defect from the moment S3 deployed: Flow F's watch, fired.
- The notification dedup key gained the status: `(rule, txn, status)`, so a status change - the correction the alert exists for - re-alerts, while a re-polled unchanged transaction still dedupes to one event.

### What remains of the streaming end-state (the owner's goal)

The goal ("update in place, insert if new, skip if same, append-only collection") is ~90% built and live; what is NOT streaming is the read side - every tick still re-reads the padded window and regroups it in memory.
The remaining roadmap, in the plan file above:

- **P4, head-lane streaming** (the true S4b, on the correct axis): a per-tenant durable frontier; entries at/after it processed incrementally against the `log_open_stream` state (continue → append one assignment + one UPDATE per open transaction per batch; new → INSERT); every one of the six miss modes routes to the rebuild lane; gated behind its OWN correctly-axised shadow comparing against the rebuild lane on the same new entries; promotion by manual review.
Benefit: reads drop to new-entries-only and stitch latency drops from cadence-bound to near-arrival; writes are already ~1/row.
- **P5**: once P4's shadow lands, retire the mis-axised S4a window-replay comparison and the `stage2_stream_lookup` setting (the state tables stay - P4 is their intended purpose).

### Found during the post-deploy verification, fixed as chunk 66

The reconcile worker had been reporting 127-463 `rollups_vs_facts` findings EVERY hourly pass since it was first enabled on 2026-08-25 - oscillating with traffic, never converging, and unread.
Diagnosed from live finding samples: every one was a bucket the rolling window only PARTIALLY covered - the stored rollup was folded from the whole bucket, the recount only from the window's slice, so they disagree by construction ("differs" = bucket straddling an edge; "missing" = stored fetch clipped the bucket out; "orphaned" = the bucket's facts all in the clipped part).
Fix: compare only buckets the window covers WHOLE (UTC hour for hourly, tenant-LOCAL day for daily), and widen `analytics_reconcile_window_hours` 24 → 48 because a 24 h window can never fully contain a local day - the daily grain was one fix away from being silently unauditable.
The two guard tests pin the other direction: a real drift or a genuinely orphaned rollup in a fully covered bucket still reports.

The constant `entries_vs_assignments = 6` findings are REAL and correct: ten entries from 2026-08-05 in two truncated files (`TMP-AZ-BEC01/eSmartServerLog.txt.12`, `TMP-AZ-BEC02/eSmartServerLog.txt.62`), never stitched, reported by design because the orphan nobody notices is precisely the old one.
Repairable at will with a windowed regroup over 2026-08-05 05:30-05:45 UTC; until then they are the known baseline.

### Standing health additions

(See also 18r below - the orphan-leak diagnosis that the clean reconciler view made possible.)

- Orphan-response transactions per day (entry_count = 1, sole entry a response): expected ~0-1; a rise is the signal to revisit the gap rule or ingestion truncation.
- `journalctl -u fastapirag-worker | grep "cross-pad"`: the shadow phase's review surface (candidates, would-extend seconds, would-raise counts).
- Any `CrossPadSpanExceeded` dead letter is a genuine span-over-pad conversation: repair with a manual full regroup over its span, and investigate what produced it.

## 18r. The orphaned-entry leak: diagnosed and fixed. Server-scoped grouping, 2026-08-27. Chunk 67.

The reconciler noise fix (chunk 66) left one real signal standing: 5,353 unassigned entries, growing ~300/day since ingestion began on 2026-08-10, plus hourly "skipped builder(s) with an already-sealed id" warnings.
Diagnosed on live data, mechanism reconstructed line by line from the 2026-08-27 12:09:35 case (user OPRACHASUK).

### The mechanism

The tenant runs two app servers (TMP-AZ-BEC01, TMP-AZ-BEC02), each writing its own log file, and one picker's operations can hit both within milliseconds.
The grouper keyed streams by (thread, user) and matched requests to work through user-scoped pools - but thread ids are small integers reused by every server process, and every one of the matching pools (POST body takes the most recent id-less pending request, GET work takes the oldest request of its user, a response FIFO-matches its user's open work) ignored which FILE the line came from.
So the two servers' interleaved lines cross-bound into one CHIMERA transaction: the persisted, sealed, "success" row held BEC01's request, BEC02's body and work, and BEC01's response - two real operations counted as one, with mixed attributes.

The strand then followed from identity: the fetcher delivers one server's file a tick before the other's, so the first stitch builds a transaction from the partial view (anchored at, say, BEC02's request - the id is minted from the request line's content hash).
When the other file's lines arrive, the regroup re-deals the requests: the builder holding most of the old members INHERITS the old id by continuity plurality, while the displaced request MINTS the very same id from the same line - and `_persist` skips the collision, leaving the loser's entries unassigned.
S3's fingerprint permanence means nothing ever revisits them: the leak.

Three symptoms, one cause: the orphan growth, the clash warnings, and a silent analytics undercount.

### The fix (chunk 67)

Grouping is scoped to the SERVER - the leading path segment of `source_file` (`_entry_server`).
Builder keys become (server, thread, user), the thread-inheritance map is keyed (server, thread), and all three pending-request pops plus the response FIFO match only within one server.
Within one server nothing changes, which the chunk-67 guard tests pin; files with no directory are their own server, so single-server tenants and every existing fixture derive identically.
`_DERIVE_VERSION` is bumped 1 -> 2 - the first real bump - because chimera groupings genuinely change what stored rows say; the chunk-59 digest is regenerated with it.
The stored S4a stream state has no server column; seeded streams derive their server from their own reloaded entries, and a same-(thread,user) collision across servers in the state table stays newest-wins (shadow-only, and S4a is scheduled for retirement in P5).

The flagship end-to-end test reproduces the exact live strand - two-phase per-file ingestion, two overlapping stitches - and asserts zero skips, zero unassigned entries, and two single-server transactions.

### Backlog repair

After deploying, run a FULL regroup for the tenant (`POST /logs/regroup`): every transaction is freed, so no clash is possible, the chimeras dissolve into their real per-server operations, all 5,353 orphans are stitched, and the per-tenant analytics ticket restates every affected fact through the ordinary diff (the ledger keeps the history, ML pins stay reproducible).
Expect the version bump to rewrite each surviving row once, and the facts for affected days to shift SLIGHTLY UPWARD (operations that were mashed into one are now counted as two).
The standing alarm afterwards: the unassigned-entry count and the clash warning should both flatline at zero; any recurrence is a new mechanism, not this one.

## 18s. The head-lane shadow's false alarms: horizon-aware comparison, 2026-08-28. Chunk 73.

The chunk-72 head lane deployed in shadow mode and immediately scored 4 DIVERGED out of 5 windows on live traffic, with both lanes demonstrably healthy (queues empty, zero orphans, facts flowing).
Forensics on the diverged transactions found the comparison at fault, not the lanes - the same window-boundary artifact class chunk 66 removed from the reconciler.

### The mechanism

The shadow compared the plan's fingerprints per transaction id against the rows the rebuild persisted for the same window.
But the two lanes never see the same world on a live tenant, for three structural reasons:

1. The rebuild's padded read reaches 900 seconds PAST the window's high edge, so it folds in lines that belong to the NEXT window.
   Live proof: transaction `6dfc0f04` persisted with 321 entries, of which 192 lay beyond the window hi the plan was built for.
2. The rebuild frees and re-derives history below the window floor, folding freed below-floor entries into transactions the plan could only continue from parked state.
3. Stage 1 keeps committing: in-window lines can land between `build_plan` and `regroup_window`, visible to the second and not the first.

The plan is a claim about horizon `hi` over the entries that existed at plan time; the persisted row describes whatever the rebuild saw milliseconds-to-seconds later, up to `hi + 900s`.
Fingerprints across those two horizons differ for perfectly healthy windows, so the shadow could never earn the promotion it exists to gate.
Worse, the old comparison was simultaneously BLIND to the failure it should catch: it never looked at which entries a transaction held, so a plan that grouped an entry into the wrong conversation - the one divergence that would actually change the system's output - passed as long as the fingerprints it computed were self-consistent.

### The fix (chunk 73)

`shadow_compare` now asks two questions that are well-defined across the horizon difference:

- **Ownership, always.** Every entry the plan assigned must sit in the SAME transaction the authority put it in (checked against `log_entry_assignment`).
  This is the question promotion hangs on: would the head lane have grouped differently?
- **Fingerprints, only on a shared horizon.** Byte-identical row/members digests are demanded exactly where the authority's final member set equals the planned set.
  A transaction the rebuild extended past the plan's horizon is checked by ownership alone, and the AGREED line reports how many were extended so the coverage is visible rather than silent.

The DIVERGED line now names the evidence (which entry went where, which digest differed on identical members) instead of an opaque id list.
The `rebuilt ids missing from the plan` check was retired: on a live tenant the rebuild always creates transactions from entries the plan never saw (late-ingested, pad-folded, freed history), so the check was pure noise, and its intent - the plan must not miss work - is already guaranteed by construction (the plan reads EVERY unassigned entry in its window, and each one lands in a planned transaction, so the ownership check covers them all).

Four tests pin it (`test_head_lane_shadow_chunk73.py`): the beyond-horizon fold and the late-arrival race must AGREE; a wrongly-grouped entry and a digest that differs on identical members must DIVERGE.
The apply-path equivalence bar (a rebuild after a head-lane apply must report all-unchanged) is untouched.

## 18t. The stale parked stream: finished conversations saved as open, 2026-08-28. Chunk 74.

The horizon-aware shadow (18s) earned its keep on its very first live window: a DIVERGED line whose evidence showed the head-lane plan binding two brand-new responses to conversations from ~3 minutes earlier that were already complete (success, sealed pending).
The authority was right; the plan's SEED was wrong.

### The mechanism, three facts long

1. `regroup_window`'s save loop parked every builder still inside the quiet gap - including FINISHED ones.
   The comment above it claimed "an OPEN stream is one whose transaction is still receiving entries", but the code checked only the gap, never closure, so a responded conversation was written to `log_open_stream` as open.
2. `_group` seeds every parked stream into `open_by_key` - which is, by definition, open work.
3. A RESPONSE binds to its user's OLDEST open work (FIFO by `open_pos`).
   A wrongly-parked finished conversation is always older than the fresh request sitting beside the new response, so it stole every new response for that user.

The chapter-13 gaps register had flagged the state table as imprecise and called it "harmless while the state is shadow-only measurement".
The head lane is the first consumer that depends on the state being precise, and the improved shadow caught the imprecision on its first window - which is the shadow working, not failing.

### The fix (chunk 74)

Both sides of the contract:

- **Save** (`derive_transactions.py`, the stream-state save loop): a builder whose entries contain a response is closed - `_group` closes it the moment the response binds and never lets it receive another entry - so it is no longer saved as an open stream.
  The misleading comment is corrected to state the real rule.
- **Plan** (`head_lane.build_plan`): the head lane never TRUSTS state it can cheaply disprove.
  A parked stream whose reloaded entries already contain a response is stale by definition (legacy rows written before this fix, or any future save regression), and the window routes to the rebuild lane with the new named fallback `parked_closed` - never guess, fall back.

Legacy stale rows on the server self-heal: the very next rebuild of any window replaces the tenant's whole state (save is DELETE-then-INSERT per tenant), so `parked_closed` fallbacks fade within seconds of deployment.

Three tests pin it (`test_head_lane_seed_chunk74.py`): a finished conversation is not parked while a genuinely open one still is; the exact live strand end to end (window one completes conversation A, window two's new response for the same user must not be stolen by A - plan builds the new conversation whole and the shadow AGREES); and a hand-planted legacy stale row makes the plan fall back `parked_closed` instead of guessing.

## 18u. Declined windows became visible, 2026-08-28. Chunk 75.

During the chunk-74 shadow watch, two consecutive windows produced no verdict and the journal could not say why: `build_plan`'s fallbacks returned a named reason to the caller but logged nothing, so a quiet shadow was ambiguous between "the tenant is idle" and "every window trips a guard".
S4a solved the same problem with its refusal counts; the head lane now does the equivalent - one INFO line per declined window, from the head-lane module, naming tenant, window and reason (`fell back to the rebuild lane for <lo>..<hi> - <reason>`).
Volume matches the existing per-window Stage 2 stats line, so it cannot flood the journal.
Two tests pin it (`test_head_lane_fallback_logging_chunk75.py`): a declined window names its reason; an eligible window logs no fallback.

## 18v. The state wipe: a wider save than the window could speak for, 2026-08-28. Chunk 76.

The chunk-75 fallback logging and the 18s evidence format together made the next live DIVERGED pair fully diagnosable from the journal alone, and the diagnosis was reconstructed step by step:

1. 12:49:15 - a window ending 11:49:08.893 stitched a new conversation (request 11:49:08.688) and parked it as an open stream. Correct.
2. 12:49:23 - an OVERLAPPING, OLDER ticket (window ending 11:48:51.972, entirely behind the previous one; merged tickets overlap routinely) rebuilt next.
   Its state save was a whole-tenant DELETE-then-INSERT, and since the step-1 conversation was outside its freed range, the save WIPED that parked stream.
3. 12:50:34 - the head-lane plan built with the hole in its seed.
   A response binds to its user's OLDEST open work, so with the front conversation missing every response for that user shifted one conversation over - the chained ownership mismatches in the DIVERGED evidence.

The rebuild lane is immune by design ("the state is a cache, not the truth" - it re-derives from raw lines); the head lane is the first consumer that needs the cache to be COMPLETE, not merely non-stale.

### The fix (chunk 76), three legs

- **Scoped save.** `stream_state.save` now has two explicit contracts.
  The head lane's apply still whole-replaces, because its plan re-parks every stream it still believes in - the plan IS the complete state.
  The windowed rebuild passes the set of transactions it actually touched (freed or rebuilt); deletes are scoped to those, to genuine key collisions with the rows being written, and to consumed pending requests - every other row SURVIVES.
- **The server joined the stream key** (migration `b5e19f7c3a84`).
  Thread ids are small integers reused by every app server process (18r), so a key of (customer, thread, user) forced newest-wins across servers and silently dropped one server's open conversation - the same hole in another form, live on this two-server tenant.
  Closes gaps-register item 7.
- **`parked_unreadable`.** A loaded stream whose transaction has no reloadable entries was silently dropped from the plan's seed; the head lane now falls back by name instead of planning around a hole.
  Alongside this, the rebuild-side save now derives each stream's transaction id from the SAME continuity assignment `_persist` uses (inherit-aware), instead of re-minting - so the stored pointer can no longer disagree with the persisted id.

Four tests pin it (`test_stream_state_scope_chunk76.py`): the overlapping-older-window wipe survives; the full live strand end to end (the response still finds its conversation and the shadow AGREES); both servers' same-(thread,user) conversations park side by side; a broken stream pointer makes the plan fall back.
