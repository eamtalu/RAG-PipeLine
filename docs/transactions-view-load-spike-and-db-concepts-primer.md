# transactions/view load spike (2026-07-25) - incident record + database concepts primer

This document has two jobs at once.
It is a **record of a specific incident** on 2026-07-25 (a `WORKER TIMEOUT` recurrence on the transactions feed), and it is a **beginner-friendly primer** on the database and Python concepts that explain the incident.
Read it front to back the first time; later, jump to the section you need.

All numbers here were measured on the live production database on 2026-07-25 (customer `tmp-live`).

---

## 1. TL;DR

The feed endpoint `GET /api/v1/logs/transactions/view` timed out again on 2026-07-25.
Heavy page requests (large `limit`, older `date`) blocked the backend workers past the 120-second limit, the workers were killed, and the machine's load average spiked to 17 before recovering on its own.

The investigation corrected two of its own early guesses along the way, and landed here:

- The endpoint is **not** drowning in hundreds of thousands of rows.
  A busy 500-transaction page really only touches about **8,400** log entries.
- The real cost centres are **stale table statistics** (causing wasted work), a table that is **~91% dead rows** (bloat) sitting on a slow disk, and an **unnecessary database sort**.
- The right fix is **refresh statistics + clear the bloat + sort in Python**.
- We explicitly **rejected adding a database index**, because this table's real pain is *writing*, and an index only adds more writing.

None of the fixes were implemented at the time this document was first written; it was the analysis and the plan.

**Update (2026-07-25 afternoon):** when we began executing the plan, remediation uncovered that the underlying disk is **physically failing** - it has unreadable bad sectors - which escalated the incident from database tuning to hardware recovery.
The stats fix (`ANALYZE`) was applied safely; `VACUUM FULL` could not run and was abandoned; the bad disk blocks were surgically recovered by hand; and the bloat-reclaim step shifted from `VACUUM` to `TRUNCATE` + re-ingest.
That whole story - what a bad sector is, why it breaks `VACUUM`, and the exact recovery commands - is recorded in **Section 9**, added at the end.

---

## 2. The incident

### What the user saw
The site appeared to "give timeout".

### What was actually happening
The backend runs 4 identical worker processes behind a master process.
A worker must periodically tell the master "I'm still alive".
If a worker is stuck on one request and goes silent for **120 seconds**, the master assumes it is hung, kills it, and starts a fresh one.
When a worker is killed mid-response, the half-sent answer is cut off, and the proxy in front reports `socket hang up` / `ECONNRESET`.

That is exactly the "timeout".
It is not a network problem; it is a worker being killed for taking too long.

### The evidence
```
backend log:
  10:53:48  [CRITICAL] WORKER TIMEOUT (pid:1979118)
  10:54:42  [CRITICAL] WORKER TIMEOUT (pid:1979115)

frontend proxy log:
  Failed to proxy .../transactions/view?limit=500&offset=500&date=2026-07-23  Error: socket hang up

nginx access log (five of these, 10:49-10:54):
  GET /api/v1/logs/transactions/view?limit=500&offset=500&date=2026-07-23   500

load average over the episode:
  during:  17.08  (1-min)   ->   after:  0.59
```

Every failing request was the same endpoint, on **older dates** (2026-07-17, 2026-07-23) with **large pages** (`limit=500`).
By the time the system was inspected, load had already fallen back to normal and the backend health check answered in 2-3 ms - the spike was real but transient.

---

## 3. How the feed endpoint actually works

A common first assumption is that this endpoint only reads the `log_transactions` table.
That is half right.
It reads **two** tables, in sequence, and the expensive part is the second one.

The data is nested: one transaction has many log entries.

```
   log_transactions (the summary)          log_entries (the actual content)
   +----------------------------+          +-----------------------------------+
   | id: T1                      |<---+     | transaction_id: T1  seq:1  REQUEST |
   | user: CPRICE                |    +-----| transaction_id: T1  seq:2  step..  |
   | status: success             | one txn  | transaction_id: T1  seq:3  step..  |
   | started_at: 09:15:22        | has many | transaction_id: T1  seq:4  RESPONSE|
   | date: 2026-07-23            | entries  |     ... ~17 entries for this txn   |
   +----------------------------+          +-----------------------------------+
```

The endpoint (`app/api/v1/logs.py`, `view_transactions`, around line 552) runs three queries:

| # | Table | Purpose | Cost |
|---|-------|---------|------|
| 1 | `log_transactions` | count, for the pager total | cheap (index-only) |
| 2 | `log_transactions` | fetch the page of up-to-500 transaction summaries | cheap |
| 3 | `log_entries` | fetch every log entry for those transactions, to render them | **the villain** |

A `log_transactions` row is only a *summary* (who, when, status).
The actual request/step/response text lives as individual rows in `log_entries`.
To render the readable view, query 3 must pull those entries - that is where nearly all the rows and all the sort cost live.

The relevant code:
```python
625    ids = [t.id for t in txns]                  # the up-to-500 transaction ids from query 2
627    entry_rows = (await db.execute(
628        select(LogEntry).where(LogEntry.transaction_id.in_(ids))
629        .order_by(LogEntry.seq.asc().nullslast(), LogEntry.line_number.asc())   # <-- the sort
630        .limit(MAX_RENDER_ENTRIES + 1)          # MAX_RENDER_ENTRIES = 50_000 (logs.py:47)
631    )).scalars().all()
```

---

## 4. Concepts primer (with visuals)

This section is the "teach me like a beginner" part.
Each concept is small and standalone.

### 4.1 Estimate vs actual, and stale statistics

When the database plans a query, it first **estimates** how many rows it will touch, using stored statistics about each table.
Those estimates can be wrong if the statistics are old.

For our page, the plan showed both numbers side by side:
```
   Index Scan on log_entries   (rows=692 ...)      (actual ... rows=17 ...)
                                    ^ ESTIMATE           ^ REALITY
```
The database **guessed** 692 entries per transaction (x 500 = ~346,000 total).
Reality was **17** per transaction (x 500 = ~8,400 total).
A 40x overestimate.

Why? The statistics for `log_entries` were 3 days stale, and the table had been modified 8 million times since:
```
   relname     | last stats refresh | live rows  | modifications since refresh
   ------------+--------------------+------------+----------------------------
   log_entries | 2026-07-22         | 4,492,522  | 8,074,183   (~180% of the table)
```

This matters for real, not just cosmetically:
the inflated estimate makes the database think the query is enormous, which triggers a wasteful optimisation step (see JIT below) and can push it toward worse plans.

Fixing it is one cheap command: `ANALYZE log_entries` recomputes the statistics.

> **JIT, in one line.** When a query's *estimated* cost crosses a threshold, Postgres spends time compiling optimised machine code for it (Just-In-Time compilation). Here that cost ~1.3 s and gave almost no benefit, because the inflated estimate crossed the threshold for a query that really only touches 8,400 rows. Correct statistics drop the estimate below the threshold, and JIT stops firing.

### 4.2 What an index is, and why it makes *writing* expensive

A table is an unordered pile of rows.
Finding "all rows for transaction T1" by scanning the whole pile is slow.

An **index** is a separate, always-sorted side-list of `value -> location`:
```
   The table (unordered pile)        An index on transaction_id
   +----------------------+          +-----------------------+
   | slot 900: T1          |         | T1 -> slots 900, 903   |
   | slot 901: T5          |         | T2 -> slot  911        |
   | slot 902: T1          |         | T3 -> slot  907        |
   | slot 903: T1          |         | T5 -> slots 901, 904   |   <- kept sorted
   +----------------------+          +-----------------------+
```
Reading becomes fast: jump straight to the T1 entry in the sorted side-list.

The hidden cost is on **writing**.
The side-list must stay perfectly in sync with the table, and a table can have several indexes (several side-lists):
```
   ONE change to a row  -+-> update the table
                         +-> update index side-list #1
                         +-> update index side-list #2
                         +-> update index side-list #3     ... every index = more work per write
```
Each of those is a little disk write.
On a server whose disk is the bottleneck, more indexes means more write pain.
That is why a recent migration (`e2a9c7b41d68`, 2026-07-24) *deleted* several indexes on this table - its title is literally "cut insert write-amplification", and it noted that dropping them "roughly halves per-insert index work".

### 4.3 The cheap-update shortcut (HOT), and why this table never gets it

Postgres has an optimisation called a **HOT update** (Heap-Only Tuple).
The rule:
```
   You change a row...
     ...and NO index side-list is sorted by the thing you changed?
          -> just edit the row in place, touch no index-lists.   (cheap = HOT)
     ...but you changed a value that an index-list IS sorted by?
          -> you must re-file it in that side-list.               (expensive)
```

This table constantly rewrites `transaction_id` (see 4.4 and section 3 of the write-path facts), and `transaction_id` is indexed.
So its updates can never take the cheap path.
The measured proof:
```
   log_entries updates so far:  63,111,268
   of those, cheap HOT ones:            162      <-  0.0%
```
Zero percent cheap.
Every one of 63 million updates paid the expensive "re-file in the index-lists" cost.

### 4.4 Dead tuples, bloat, and VACUUM

Another Postgres quirk: an "update" does not overwrite the old row.
It writes a **new version** and marks the old one **dead**:
```
   UPDATE entry: set transaction_id = T1

   before:  [ v1: transaction_id = blank ]   <- now marked DEAD (a "dead tuple")
   after:   [ v2: transaction_id = T1    ]   <- the live version

   the dead one is not removed immediately; it sits there taking space.
```

Do that 63 million times and the table fills with dead rows that readers must skip past:
```
   log_entries today:
     live rows:  4,492,522   #
     dead rows: 47,578,508   ##########   (~10 dead for every 1 live = ~91% junk)

   [####.#........#....#.........#....#........]  <- a reader hunts live rows among the dead
```

A janitor process called **VACUUM** clears the dead rows so the space can be reused.
Here it has fallen far behind (a stuck long-running transaction can "pin" the janitor and stop it reclaiming, which is a documented past problem on this box).
Consequences:
- the table is ~40 GB on disk but holds only ~4 GB of real (live) data,
- cold reads of old dates are slow, because they drag ~40 GB of mostly-dead pages off the slow disk.

This is very likely the **main** reason older dates time out - more than the sort ever was - and it is fixable with no code change by getting the janitor caught up.

### 4.5 Two ways to sort: one giant DB sort vs many tiny Python sorts

We need each transaction's entries in order (request -> step 1 -> step 2 -> response), where `seq` is the Stage-2 canonical order and `line_number` is the physical-file tiebreak.
But we do not need the *database* to produce that order.

**The database way (today) - sort everything, then throw the order away:**
```
   step 1: fetch all ~8,426 entries for the 500 transactions on the page
   step 2: dump ALL 8,426 into one pile and SORT the whole pile by (seq, line_number)
           (too big for the small sort workspace -> spills to the SLOW disk, ~6.8 MB)
   step 3: immediately split the sorted pile into 500 per-transaction groups,
           discarding the cross-transaction order we just paid for
```

**The Python way (the fix) - split first, then sort tiny groups:**
```
   step 1: fetch the ~8,426 entries with NO ordering asked of the database
   step 2: split into ~500 small buckets by transaction   (the code already does this)

              bucket T1: [ 3 entries ]
              bucket T2: [ 17 entries ]
              bucket T3: [ 5 entries ]
              ... ~500 buckets, avg 17 entries each ...

   step 3: sort each tiny bucket by (seq, line_number) in Python (microseconds)
```

Same final result, very different cost:

| | Database sort (today) | Python bucket-sort (fix) |
|---|---|---|
| Big pile sorted at once | 8,426 | never |
| Spills to slow disk | yes (~6.8 MB write) | no |
| New index needed | (the rejected idea did) | none |
| Extra writes to bottleneck disk | yes | none |
| Where the work happens | slow shared disk | fast in-memory Python |

The code already loops over the entries to group them, so sorting each little group is a one-line addition:
```python
by_txn = {}
for e in entry_rows:
    by_txn.setdefault(e.transaction_id, []).append(e)      # split into buckets (already here)
for bucket in by_txn.values():
    bucket.sort(key=lambda e: (e.seq is None, e.seq, e.line_number))   # seq NULLS LAST, then line_number
```

---

## 5. Root-cause analysis (ranked)

1. **Stale statistics** -> a 40x row overestimate -> ~1.3 s of wasted JIT compilation per heavy request, plus risk of poor plans.
2. **Table bloat (~91% dead rows) on a slow disk** -> cold reads of old dates drag ~40 GB of mostly-dead pages off a contended HDD. Likely the biggest single driver of the old-date timeouts.
3. **An unnecessary database sort** of ~8,400 wide rows that spills ~6.8 MB to the slow disk, and whose global ordering is discarded anyway.
4. **GIL-bound Python render** - building the text for up to 50,000 entries is single-threaded CPU work that can starve the worker's "still alive" heartbeat under concurrency.
5. **The single slow HDD** shared between live reads and the constant ingest/stitch writes - the structural floor the other items sit on.

Under a burst of heavy requests across all 4 workers, contending for that one disk, these combine into the load-17 spike and the worker kills.

---

## 6. The index idea we rejected (and why it is recorded here)

An early proposal was to add a composite index on `log_entries (transaction_id, seq, line_number)` and reorder the query so the sort would disappear.
It is written down here specifically so nobody re-proposes it without knowing the trap.

Why it was rejected:
- `transaction_id` and `seq` are written by Stage-2 stitching, and **rewritten repeatedly** - each entry in the recent live tail is updated twice per cycle (set to NULL by a cascading delete, then re-assigned), every ~5 seconds (`derive_transactions.py:521-522`).
- The table already runs **0.0% HOT updates** across **63 million** updates, so every update already re-files in every index.
- Adding a third index would put more work in the path of all those rewrites, and would **newly index `seq`** (indexed nowhere today), creating brand-new write cost on the exact disk that is the bottleneck.
- It directly contradicts the recent, deliberate decision in migration `e2a9c7b41d68` to *remove* indexes here to cut write-amplification.

Net: a tiny read benefit (removing one occasional sort) in exchange for a permanent write cost on the bottleneck.
Section 4.5's Python-sort achieves the same read benefit with zero new writes.

---

## 7. Recommended fix / action list (not yet implemented)

| Priority | Action | Why |
|---|---|---|
| 1 | `ANALYZE log_entries`, and tune autoanalyze to run more often on this fast-growing table | Fixes the 40x stale estimate; removes the ~1.3 s wasted JIT; better plans. Cheap and safe. |
| 2 | Investigate and clear the ~91% bloat: catch-up `VACUUM`, and find why autovacuum fell behind | Likely the biggest real win for old-date reads; a de-bloated ~4 GB of live data can mostly live in the 8 GB cache. |
| 3 | Move entry ordering out of SQL and into Python per-bucket (section 4.5) | Removes the DB sort and its disk spill with no index and no added writes. |
| 4 | Lower `MAX_RENDER_ENTRIES` from 50,000 to ~15,000 (`logs.py:47`) | Shrinks the worst-case Python render; normal pages (~1,700-8,400 entries) are unaffected. |
| 5 | Consider dropping any remaining low-value indexes on `log_entries` | Reduce write-amplification further - the direction the codebase is already moving. |
| 6 | (Structural, not a code change) faster disk (SSD), and revisit the 0%-HOT stitch-churn write pattern | The deeper floor everything else sits on. |

Optional guard for the render cascade (a judgement call, deferred): limit how many heavy renders run at once, or reduce the max page size from 500.

### How to verify a fix works
Re-run the heavy request shape under `EXPLAIN (ANALYZE, BUFFERS)` and confirm:
- no `Sort` / `Incremental Sort` node and no "external merge Disk" line (sort removed),
- no `JIT:` section (estimate back to normal after ANALYZE),
- for the bloat work, the drop in `n_dead_tup` and table size in `pg_stat_user_tables` / `pg_total_relation_size`.

---

## 8. Key facts and references

Measured on 2026-07-25 (`tmp-live`):
- entries on a `limit=500` page: ~8,426 total, avg 16.9 per transaction, max 212.
- `log_entries`: 4,492,522 live rows, 47,578,508 dead rows, 63,111,268 updates, 162 HOT (0.0%), ~40 GB on disk.
- statistics last refreshed 2026-07-22 with ~8 M modifications since.
- `work_mem` = 4 MB (Postgres default, not tuned; not the right lever here).

Code pointers:
- endpoint: `app/api/v1/logs.py` - `view_transactions` (~line 552), entry-fetch query (627-631), render grouping (654-660), `MAX_RENDER_ENTRIES` (47).
- render: `app/services/mnp_log_ingestion/render.py` - `render_transaction` (36).
- stitch write path: `app/services/mnp_log_ingestion/pipeline/derive_transactions.py` - entry assignment (521-522).
- index-drop rationale: migration `e2a9c7b41d68` ("cut insert write-amplification").

Related docs:
- `docs/ISSUE-worker-timeout-outage.md`, `docs/debugging-worker-timeout-outage.md` - the original 2026-07-22 outage and its fixes (the composite index on `log_transactions`, bounded/off-thread render, gunicorn and Postgres tuning).
- `docs/load-testing-and-dimensioning.md` - the load model and failure modes.
- `docs/transaction-log-ingestion-design.md` - the Stage 1 / Stage 2 pipeline design.

---

## 9. Escalation (2026-07-25, afternoon): the disk is physically failing - bad-sector diagnosis and recovery

This section records what happened when we started executing the fix plan above.
It is the most important operational lesson from the whole incident, so it is written as both a story and a reusable runbook.

### 9.1 What forced the escalation

While running the Phase 2 step (`VACUUM FULL` to reclaim the bloat), Postgres aborted with:
```
ERROR:  could not read block 4571733 in file "base/16388/16613.34": Input/output error
```
An `Input/output error` from Postgres is not a database problem.
It is the operating system telling Postgres that the physical disk could not read that spot.
The kernel log (`sudo dmesg -T`) confirmed it, and kept repeating it every ~60 seconds:
```
critical medium error, dev sda, sector 1117631824 op READ
sd 4:0:0:0: [sda] Sense Key : Medium Error [current]
sd 4:0:0:0: [sda] Add. Sense: Unrecovered read error
```
This reframed the whole task.
The machine is not merely slow because of tuning - it is running on a **physically failing disk** (the "failing/slow production disk" the older docs kept referring to).
Bloat and stale statistics were real problems, but the disk is the ground truth underneath them.

(The ~60-second repeat was autovacuum: `autovacuum_naptime` is 60 s, and the aggressive per-table setting we had just applied made autovacuum keep retrying `log_entries`, hitting the bad sector each time. We stopped that with `ALTER TABLE log_entries SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);`.)

### 9.2 Concept: a bad sector ("medium error")

A spinning hard disk stores data in physical 512-byte sectors on a magnetic platter.
When the surface of a sector degrades, the drive can no longer read it back, and reports a "Medium Error / Unrecovered read error".
This is **hardware damage, not software corruption** - no software can reconstruct the lost bytes.
It fails on **read**; whatever was stored there is gone.

### 9.3 Why this breaks VACUUM (and any full-table read) - the phase-specific lesson

Here is the key lesson, tied to the exact phase we were in.

`VACUUM FULL` reads **every** row in the table in order to rewrite it compactly.
On a healthy disk that is fine.
On a failing disk it is doomed: the full read eventually reaches the bad sector, hits the I/O error, and because `VACUUM FULL` is all-or-nothing, it **rolls back with zero progress** - having stressed the dying disk for nothing.

```
   VACUUM FULL on a failing disk:

   read block 0 ... block 1 ... ... block 4,571,732   [OK]
   read block 4,571,733                               [Input/output error]  <- bad sector
   -> the entire VACUUM FULL rolls back: 0 GB reclaimed, disk hammered for nothing
```

The same is true of plain `VACUUM`, `count(*)`, unfiltered `SELECT *`, and `REINDEX` - anything that scans the whole table will march into the bad sector and fail.
So on a failing disk you must **avoid full-table reads** and prefer operations that touch little or nothing:
- `ANALYZE` reads a small random **sample** (~30k pages), so it is low-risk (see 9.7).
- `TRUNCATE` reads **nothing** - it just drops the file - so it sidesteps every bad sector at once.
- A `ctid`-targeted query reads only the single page you name.

This is why the plan changed mid-flight: `VACUUM FULL` was struck off, and bloat reclaim moved to `TRUNCATE`.

### 9.4 Decoding the error: block number -> file -> exact page

Postgres tells you exactly where the damage is; you just decode it.
```
could not read block 4571733 in file "base/16388/16613.34"
```
- `16388` = the database's folder, `16613` = the relation's file (`log_entries`), `.34` = the 35th 1 GB segment.
- The block number `4571733` is **relation-wide**. Postgres splits a table into 1 GB segments of 131,072 blocks each (1 GB / 8 KB).
- Segment number = `4571733 / 131072` = 34 -> file `.34` (matches).
- Local page inside that segment file = `4571733 - (34 * 131072)` = `4571733 - 4456448` = **115285**.

That local page number is what we feed to `dd`.
And `ctid` (section 4-adjacent concept: a row's physical `(block, slot)` address) lets us poke the same page from SQL - `ctid >= '(4571733,0)' AND ctid < '(4571734,0)'` selects exactly the rows on that one block, without scanning the table.

### 9.5 How you "fix" a bad sector (remap), and why zeroing works

You cannot recover the lost bytes, but you can make the location usable again:
- Modern drives keep a hidden pool of **spare sectors**.
- A bad sector fails on **read**, but when you **write** to it, the drive detects the defect and transparently **remaps** that address to a fresh spare sector.
- So overwriting the damaged 8 KB page with zeros does two things at once: it forces the remap, and it makes future reads succeed (returning zeros).
- Postgres treats an **all-zero page as a valid, empty page**, so after zeroing it just sees "no rows here" and moves on - no error.
  The ~dozen rows that lived on that page are lost, which is acceptable here because `log_entries` is rebuildable.

### 9.6 The recovery runbook (the exact commands we ran)

Preconditions: a safe backup exists **off** the failing disk (see 9.8), and **Postgres is stopped** (never `dd` a file Postgres has open - you would corrupt it).

```bash
# 1. stop everything that touches the DB (dd requires Postgres down)
sudo systemctl stop fastapirag fastapirag-worker
sudo systemctl stop postgresql@16-main

# 2. PROVE the bad block before writing - this read MUST throw "Input/output error".
#    Safety gate: if it reads fine, the offset is wrong -> STOP, do not continue.
F=/var/lib/postgresql/16/main/base/16388/16613.34
sudo dd if="$F" bs=8192 skip=115285 count=1 of=/dev/null

# 3. zero that one 8 KB page -> forces the sector remap
sudo dd if=/dev/zero of="$F" bs=8192 seek=115285 count=1 conv=notrunc
sudo sync

# 4. verify it reads now (drop caches first so we test the DISK, not RAM)
sudo sysctl -w vm.drop_caches=3
sudo dd if="$F" bs=8192 skip=115285 count=1 of=/dev/null     # expect: "1+0 records", no error

# 5. start Postgres and confirm the page reads as empty from SQL
sudo systemctl start postgresql@16-main
PGPASSWORD=rag psql -h localhost -U rag -d rag -c \
  "SELECT count(*) FROM log_entries WHERE ctid >= '(4571733,0)' AND ctid < '(4571734,0)';"   # expect 0, no error

# 6. bring the app back
sudo systemctl start fastapirag fastapirag-worker
```

`dd` flag notes (worth understanding, not memorising):
- `bs=8192` = one Postgres page; `skip`/`seek` are counted in these 8 KB units, so they equal the local page number.
- `skip` = read offset (for input), `seek` = write offset (for output) - same page, different option name.
- `conv=notrunc` = overwrite in place; do **not** truncate the file down to the write size.
- dropping caches (`vm.drop_caches=3`) matters on the verify step, otherwise you might read a cached copy and think the disk is fine when it is not.

Worked examples of the blocks we actually recovered:

| Error block | Segment file | Local page = block - (segment * 131072) | dd skip/seek |
|---|---|---|---|
| 4571733 | 16613.34 | 4571733 - 34*131072 = 115285 | 115285 |
| 4636343 | 16613.35 | 4636343 - 35*131072 = 48823 | 48823 |

When bad blocks **cluster** (after fixing 4636343 the next read failed at 4636361, 18 pages later), you can zero a whole **range** in one pass instead of one page at a time (data loss tolerated):
```bash
# zero 512 contiguous pages (a ~4 MB band) covering the cluster in segment .35
sudo dd if=/dev/zero of="$F" bs=8192 seek=48800 count=512 conv=notrunc
```

### 9.7 How VERBOSE helped

We ran the maintenance commands with `VERBOSE`, and it directly shaped decisions.

`ANALYZE VERBOSE log_entries` printed:
```
INFO:  "log_entries": scanned 30000 of 5301336 pages, ... 5910283 estimated total rows
```
That one line **proved `ANALYZE` only samples** - 30,000 of 5.3 million pages, under 1% - rather than reading the whole table.
On a failing disk that was exactly what we needed to know: it told us `ANALYZE` was **safe to run** (tiny chance of touching a bad sector), while `VACUUM` was not.
Without `VERBOSE` we would have been guessing whether `ANALYZE` does a full scan.

More generally:
- On `VACUUM VERBOSE`, the progress lines show how far the scan got before it died, which helps confirm the failure is I/O (not logic) and roughly where.
- Rule of thumb: on a sick system, always add `VERBOSE` to maintenance commands.
  It turns an opaque "it failed" into "it scanned N of M pages and failed at block X" - which is what lets you locate and reason about the problem.

A refreshed-stats side note: before `ANALYZE`, `pg_stat_user_tables` showed stale figures (47.5M dead / 4.5M live from 2026-07-22).
After `ANALYZE`, the true figures were **~5.9M live / ~14.3M dead** - still heavily space-bloated (40 GB of file for ~6M live rows) but far less dead-tuple churn than the stale numbers implied.

### 9.8 Protect the data first - and store the backup OFF the failing disk

Before any `dd` surgery we took a backup, and deliberately wrote it to a **different machine** (the operator's laptop), never the server:
```bash
# run FROM the laptop; streams the dump over SSH to the laptop's disk,
# so it never lands on the dying server disk
ssh -o RemoteCommand=none amin@192.168.0.142 \
  "PGPASSWORD=rag pg_dump -h localhost -U rag -d rag \
     --exclude-table-data='public.log_entries' -Fc" \
  > ~/rag_backup_2026-07-25.dump
```
- `--exclude-table-data=log_entries` skips the huge, corrupt, **rebuildable** table's data (so the dump never reads the bad sector) while keeping its schema **and all other tables' data** - the small, non-rebuildable ones (customers, notifications, saved views, transactions).
- The dump must live **off** the failing disk: a backup stored on the disk you are protecting against is not a backup.
- Validity check: a pg_dump custom-format archive starts with the bytes `PGDMP` (`head -c 5 file | xxd`).
  `pg_restore` need not exist on the laptop - the restore happens on the server later; the laptop is just safe storage.

### 9.9 The whack-a-mole realisation, and the decision

Patching individual blocks works, but we hit bad sectors in **two** segments (`.34`, `.35`) and then a **cluster** within one - the damage is spreading.
Page-by-page patching becomes a losing race against the disk, and each patch loses more rows.
Given that (a) the disk cannot be replaced right now and (b) `log_entries` is rebuildable and data loss is acceptable, the decisive fix is:

- **`TRUNCATE TABLE log_entries;`** - it reads nothing, drops the entire 40 GB file (and every bad sector inside it) in one command, and reclaims all the bloat at once. Then rebuild `log_entries` by re-ingesting the source logs.

This simultaneously ends the bad-sector cycle, removes the bloat, and dissolves the original load-spike problem (a small table fits in cache and reads fast).
Its only cost is the `log_entries` history that the source logs can no longer supply.

The genuine long-term fix remains **replacing the disk** (an SSD), which every prior doc already recommended.
These bad sectors are that recommendation coming due.

### 9.10 Updated action list (supersedes Section 7 while on this disk)

1. **Done** - `ANALYZE log_entries` (safe, sampled; removed the stale-stats/JIT waste).
2. **Done** - surgically remapped the bad blocks in segments `.34`/`.35` (`dd`-zero -> drive remap).
3. **Do NOT run `VACUUM` / `VACUUM FULL` / `count(*)` / `REINDEX` on `log_entries` while on this disk** - any full scan hits the next bad sector and fails.
4. Keep autovacuum on `log_entries` **disabled** for now (`ALTER TABLE log_entries SET (autovacuum_enabled=false)`), so it does not scan into bad sectors every 60 s.
5. Reclaim the bloat via **`TRUNCATE` + re-ingest** (not `VACUUM FULL`), once the source-log availability is confirmed.
6. **Replace the disk (SSD)** as the real fix; then restore the off-disk dump and rebuild `log_entries`.
7. The code changes (Python bucket-sort in `logs.py`, lower `MAX_RENDER_ENTRIES`) still stand and are independent of the disk.

### 9.11 One-line takeaways

- `Input/output error` / "medium error" = failing hardware, not a DB bug; no software repairs the lost bytes.
- On a failing disk, avoid full-table reads: `ANALYZE` samples (safe), `TRUNCATE` reads nothing (safe), `VACUUM`/`count(*)` read everything (will fail).
- Decode `could not read block N in file X.seg`: segment = `N / 131072`, local page = `N - segment*131072`; that local page is the `dd` offset and `N` is the `ctid` block.
- Fix a bad sector by **writing** to it (forces the drive to remap to a spare); zeroing an 8 KB page makes Postgres see an empty page.
- Always back up **off** the affected disk before surgery, and add `VERBOSE` so failures tell you where they happened.
