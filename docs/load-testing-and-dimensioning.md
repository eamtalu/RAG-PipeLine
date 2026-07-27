# Load testing & dimensioning strategy

Goal: find the real limits of this deployment so server sizing and expansion are decided by numbers.
This revision is prioritised around the stated focus areas.

## Focus areas -> priorities

- **P0 (primary): concurrent viewers.** How many people can browse the transaction-log feed at once while it stays fast.
- **P0 (paramount): ingestion / disk-write throughput.** How much data per second the disk can physically absorb, and how much the ingestion pipeline actually achieves.
- **P1: aggregation (stitching).** How fast Stage-2 rebuilds transactions, and how it recovers after downtime.
- **P1: combined real-world limit.** Viewers + ingestion + stitching together (they share one disk).
- **P2: soak / stability**, and (secondary target) a **failure-mode map** of where it can slow, fail, or crash. The failure map is largely produced for free by pushing P0/P1 to failure.

## 0. Baseline facts (measured 2026-07-23/24)

- Host: 8x Intel Xeon X5650 @ 2.67GHz (aging, weak single-thread), 47 GB RAM (about 41 GB free/cache).
- Disk: **single 700 GB rotational HDD** (`sda`, `ROTA=1`) on VMware, about 55 ms write latency. The primary bottleneck.
- Stack: nginx (:443/:80) -> Next.js (:3000, pm2) -> gunicorn 4 uvicorn async workers (:8000) -> Postgres 16 (tuned). A separate `fastapirag-worker` does SSH log poll + Stage-2 stitch.
- Current ingest: about 170k entries/day, roughly 2 entries/sec average, bursty.
- From Phase-A micro-baselines: the feed's cost is the Python render (CPU, ~1 core per worker); the DB is well-indexed and fast (feed query about 31 ms); a 500-row feed renders in about 1.8 s, a 100-row feed in about 0.4 s; the feed page size is capped at 500.

## 1. Methodology

- Define a "viewer" as a concrete workload model before measuring (Section 3).
- Set pass/fail SLOs first (Section 4); a "limit" is the load at which an SLO first breaks.
- Change one variable at a time; ramp gradually; measure STEADY STATE (minutes per step); repeat for variance.
- Generate HTTP load from a SEPARATE machine (this Mac), never the server.
- Run `bench/monitor.sh` on the box for every test and correlate the knee with the resource that saturated (Section 7).
- Warm caches before measuring; discard the first ramp step.

## 2. Safety (live box, single HDD)

- Real clients use this server, so run the push-to-failure tests in a maintenance window with clients off (the chosen environment).
- Snapshot first: a VMware snapshot of the VM, or at least `pg_dump` before the destructive ingestion phases.
- Kill switch: stop the load generator (Mac) and `sudo systemctl restart fastapirag fastapirag-worker` (box).
- Use a throwaway tenant `bench` for ingestion/stitch tests; never write test data into `tmp-live`.
- Disk-ceiling tests write real bytes: bound the test file size and clean it up (do not fill the 700 GB disk).

## 3. Workload model (what is one "viewer"?)

One browser tab is about 6-9 requests/min from the access logs, dominated by polling. Model two archetypes and report both:

- **Watching tab (cheap):** mostly `GET /logs/regroup/status` on a timer plus an occasional light feed (`view?limit=100`). Nearly free, cache-served.
- **Active analyst (expensive):** frequent heavy feeds (`view?limit=500`, deep offset, filters) plus saved-view reads. CPU- and payload-heavy.

Background load that competes for the disk: **ingestion** (worker inserts entries) and **stitching** (Stage-2 delete-and-rebuild in padded windows).

## 4. SLOs (defaults, adjust to taste)

- Feed `view` (limit <= 100): p95 < 1.5 s, p99 < 3 s. Heavy `view` (limit=500): p95 < 3 s.
- Light endpoints (`regroup/status`, `ssh-sources`, `saved-views`): p95 < 300 ms.
- Error rate < 0.1% (no 5xx, no dropped connections). Zero gunicorn `WORKER TIMEOUT`.
- Ingestion keeps up: open `log_regroup_pending` returns toward 0 between polls (no unbounded growth).
- Stitching keeps up: a poll's stitch finishes before the next poll fires.
- Guardrails: sustained iowait < ~30%, CPU < ~80%, Postgres connections < ~70% of 100.

## 5. Tools

- HTTP crowd: **k6** (`bench/k6-read-load.js`), run from the Mac (`brew install k6`).
- Single-request timing: **curl**; DB query plans: **EXPLAIN (ANALYZE, BUFFERS)**.
- Server watcher: **bench/monitor.sh** (load, CPU us/sy/wa, disk sda %util, memory, PG connections) every 5 s.
- **Raw disk write ceiling: `fio`** (random + sequential write IOPS and MB/s) and **`pg_test_fsync`** (fsync ops/sec, which bounds commit rate); **`dd`** as a crude sequential sanity check. (`fio` may need `sudo apt install fio`; `pg_test_fsync` ships with Postgres.)
- DB write bench: **pgbench** (built-in) for a pure INSERT/commit rate.
- Ingestion driver (to build): generates realistic log files (a real seed file with shifted timestamps -> unique rows) and feeds tenant `bench`.

## 6. The tests, in priority order

### P0-A. Concurrent viewers (read-path ramp) — the primary number
Goal: the max concurrent viewers at SLO, per archetype.
Tool: k6 + monitor.sh. Bump the peak across runs until a threshold fails.
```
MON_VUS=150 ANA_VUS=0  k6 run bench/k6-read-load.js     # watching tabs, push high
MON_VUS=0   ANA_VUS=50 k6 run bench/k6-read-load.js     # active analysts, push
MON_VUS=80  ANA_VUS=20 k6 run bench/k6-read-load.js     # production-like mix
```
Determine: the tab count at first SLO breach, for watching vs analyst, and which resource capped first (expected: a single CPU core during heavy renders). Also test via nginx (`https://IP`) vs direct (`:8000`) to separate app cost from proxy cost.

### P0-B. Disk write throughput — how many bytes/sec (paramount)
This has two layers: the disk's physical ceiling, then what ingestion actually achieves against it.

B1. **Raw disk ceiling** (no app):
```
# random write (the pattern Postgres/fsync produces) — IOPS + MB/s, bypassing cache
fio --name=randw --filename=/var/lib/postgresql/bench.fio --size=2G --bs=8k \
    --rw=randwrite --direct=1 --numjobs=1 --runtime=60 --time_based --end_fsync=1
# sequential write (WAL-like)
fio --name=seqw --filename=/var/lib/postgresql/bench.fio --size=2G --bs=1M \
    --rw=write --direct=1 --runtime=60 --time_based
# fsync commit ceiling (bounds how many transactions/sec can durably commit)
/usr/lib/postgresql/16/bin/pg_test_fsync
rm -f /var/lib/postgresql/bench.fio
```
Determine: the hard physical limits — "about X MB/s sequential, Y random 8k IOPS, Z fsync/s." Ingestion can never beat these.

B2. **App-achieved ingestion** (the pipeline):
Feed `bench` at increasing rate via the ingestion driver, reads OFF first.
Determine: sustained entries/sec, the disk write MB/s it produces (iostat), and what % of the raw ceiling that is. Ramp until the worker falls behind (open pending grows) or `sda %util` pins ~100%. Then repeat WITH a baseline viewer load to measure read/write contention.
Compare to today's ~2 entries/sec to state real headroom and the entries/sec at which the disk becomes the wall.

### P1-C. Aggregation (stitching) throughput + catch-up
Throughput: after a B2 ingest, time a `finalize` for `bench`; compute entries stitched/sec and note write amplification (stitch does delete + rebuild, so it writes more than ingest).
Catch-up: `stop` the worker, accumulate a known backlog (shifted-timestamp files), `start` it, and time the drain to zero — while a light viewer load runs, to measure how much catch-up degrades live viewing. This is the downtime-recovery risk.

### P1-D. Combined steady-state — the real limit
Run a production-like viewer mix + a sustainable ingest + stitching all at once; ramp until an SLO breaks. Because they share the one HDD, this combined limit is lower than any isolated phase and is the number to dimension on.

### P2-E. Soak / stability
Hold the D "safe" level 1-4 h; watch for memory growth, iowait creep, worker recycles, latency drift.

## 7. What to measure, and the binding constraint

Capture per run and correlate at the knee: app req/s and p50/p95/p99 per endpoint, error rate, gunicorn timeouts/recycles; CPU total AND per-core (a single pegged core = render-bound); disk %util / w_await / MB/s; memory + swap; Postgres active/idle/idle-in-transaction connections, waiting locks, checkpoint frequency, temp-file spills.

Diagnosis map:
- Single core at 100% -> render/CPU bound (heavy feeds) -> more/faster CPU, smaller default page, caching.
- Disk %util ~100% -> I/O bound (ingest/stitch/uncached reads) -> SSD/NVMe, partitioning, retention.
- Postgres connections exhausted -> pool sizing (+ add `pool_pre_ping`).

## 8. Failure-mode map (secondary target)

Ranked by how likely they bite as load grows. Most are exercised by pushing P0/P1 to failure.

1. **Feed render = CPU** (the viewer wall): too many heavy feeds -> all 4 workers rendering -> queueing -> timeout -> 500s. Mitigated: page capped at 500, render off-thread, indexed. Lever: CPU, smaller page.
2. **4 workers + 120 s timeout**: 4 slow requests block everything; >120 s kills a worker and drops its in-flight requests. The original outage. Bounded now.
3. **Worker cold start ~19 s**: any recycle/deploy/crash removes 1 of 4 workers for ~19 s (heavy imports). Recycles now rare; deeper fix is lazy imports.
4. **Single HDD** (the hard floor): ingest/stitch writes saturate it and then everything (including viewing) waits on disk. Write-smoothing done; #1 upgrade is SSD.
5. **DB connection pool**: high concurrency + slow queries exhausts connections -> waits -> timeouts. Uses defaults; no stale-connection guard. Harden before pushing viewers high.
6. **Catch-up storm after downtime**: worker floods stitching on restart -> disk pins -> live viewing degrades for minutes. Lever: SSD + worker throttle.
7. **No redundancy**: one VM, one disk, one DB, one proxy, one worker. Any one dying = outage; HDD failure risks data loss. Check backups; longer term: replicas + backups.
8. **`count(*)` plural list endpoint**: full-scan count can tie up a worker; your UI avoids it. Make opt-in if exposed.

## 9. Dimensioning output & levers

State the answer as a tuple at SLO, e.g. "<= N watching tabs AND <= M active analysts AND <= R entries/sec (= ~M MB/s to disk), concurrently."
Ranked levers for this box: (1) SSD/NVMe disk, (2) newer/faster CPU, (3) Postgres read replica, (4) partition + retention on `log_entries`, (5) lower frontend poll frequency / payloads, (6) more workers if CPU-bound.
Re-measure the per-viewer cost and the binding resource after every lever.

## 10. Artifacts

Built and ready: `bench/monitor.sh`, `bench/k6-read-load.js`, `bench/ingest_driver.py` (dry-run-verified against the `bench` tenant), `bench/RUNBOOK.md`.
Phase A (done) and the raw disk-ceiling test (Run 2a, needs `fio`) can run first; the viewer ramp and ingestion/stitch/combined phases run in the maintenance window.
