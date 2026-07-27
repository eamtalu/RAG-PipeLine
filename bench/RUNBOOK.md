# Benchmark runbook (production, off-hours)

Execution guide for the load tests defined in `docs/load-testing-and-dimensioning.md`.
Chosen environment: production, in a maintenance window with real clients off.
Focus (all three): concurrent tabs (Phase B), tenants/ingest (Phase C), and recover-after-downtime (Phase D catch-up).

Files:
- `bench/monitor.sh` runs ON the server for the whole window.
- `bench/k6-read-load.js` runs FROM the Mac (install: `brew install k6`).
- Server access + all the ad-hoc recipes are in the `/ssh-matrix` command.

## Pre-flight (do every time, in order)

1. Announce the window; confirm the two real clients (`.135`, `.127`) are off (check with the `/ssh-matrix` IP recipe).
2. Snapshot for rollback:
   - VMware snapshot of the VM (best), OR at least `pg_dump`/a filesystem snapshot before the destructive ingest phases.
3. Record the starting state: `git rev-parse HEAD` on both repos, and the current `pg_settings` (from the `/ssh-matrix` Postgres recipe).
4. Start monitoring on the box: `bash bench/monitor.sh 5 /tmp/bench_$(date +%H%M).log` (leave it running; one log per phase is fine).
5. Kill switch: know that stopping the k6 process (Mac) and `sudo systemctl restart fastapirag fastapirag-worker` (box) returns things to normal.

## Phase B - read-path concurrency (concurrent tabs)

Goal: the max concurrent tabs at SLO, per archetype. Bump the peak across runs until a threshold fails.

```
# monitoring-heavy, step to 100 tabs
MON_VUS=100 ANA_VUS=0  k6 run bench/k6-read-load.js
# analyst-heavy, step to 40
MON_VUS=0   ANA_VUS=40 k6 run bench/k6-read-load.js
# mixed, production-like ratio
MON_VUS=60  ANA_VUS=15 k6 run bench/k6-read-load.js
```
Read the knee from the k6 summary (p95 per scenario, http_req_failed) cross-referenced with `monitor.sh`.
Hit `:8000` directly to isolate the app, and `https://IP` to include nginx+proxy.
Expected binding constraint: a single CPU core pegged during heavy (limit=500) renders (the render is Python, one core per worker). Note the tab count at first SLO breach.

## Phase C - ingestion throughput (more tenants / higher volume)

Use a THROWAWAY tenant `bench` so `tmp-live` data is never touched.
Ingestion dedupes by content hash, so entries must be unique: take a real source file and shift every timestamp forward (e.g. +30 days) to force new rows.

Driver: `bench/ingest_driver.py` (copy to the box, run there for disk-isolated numbers). Verified working (dedup + uniqueness confirmed).
```
export CUST=bench
python3 ingest_driver.py setup                                        # once: create the bench tenant
python3 ingest_driver.py seed --from-db --source tmp-live --entries 20000 --out seed.log
python3 ingest_driver.py ramp --seed seed.log --copies 20             # feeds 20 unique windows, prints entries/sec
```
Watch `bench/monitor.sh` for disk MB/s and `sda %util` while `ramp` runs.
Ramp `--copies` (and seed `--entries`) up until the worker falls behind (open `log_regroup_pending` for `bench` grows) or `sda %util` pins ~100%.
Then repeat with a baseline Phase-B read load running, to measure read/write contention on the one HDD.
Compare the achieved entries/sec (and its disk MB/s) against the Run-2a raw ceiling.

## Phase D - stitching + catch-up (downtime recovery)

Throughput: after a Phase-C ingest, time a `finalize` for `bench` and compute entries stitched/sec; watch `%util`.

Catch-up (the downtime scenario) - driven by `ingest_driver.py catchup`, which prompts you through the worker stop/start and times the drain:
```
export CUST=bench
python3 ingest_driver.py catchup --seed seed.log --copies 40
#   -> it pauses; you run:  sudo systemctl stop fastapirag-worker   then Enter
#   -> it feeds the backlog, then pauses; you run:  sudo systemctl start fastapirag-worker   then Enter
#   -> it polls /logs/regroup/status and prints the drain time
```
Run a light Phase-B read mix at the same time and watch feed p95 + iowait to see how badly catch-up degrades live reads.
This quantifies "how long until the feed is trustworthy again after an outage."

## Phase E - combined steady-state (the real limit)

Run a production-like read mix (Phase B) + a sustainable ingest (Phase C) + stitching all at once; ramp until an SLO breaks.
This is the number to dimension on, because reads, ingest, and stitch all share the single HDD.

## Phase F - soak

Hold the Phase-E "safe" level 1-4 h. Watch `monitor.sh` for memory growth, iowait creep, worker recycles, and latency drift.

## Teardown

1. Stop k6 and `bench/monitor.sh`.
2. Delete the `bench` tenant and its data: `DELETE FROM log_entries WHERE customer_code='bench'; DELETE FROM log_transactions WHERE customer_code='bench'; DELETE FROM log_regroup_pending WHERE customer_code='bench';` (or restore the pre-flight snapshot).
3. `sudo systemctl restart fastapirag fastapirag-worker`; confirm `/health` 200 and the real feed loads.
4. Bring real clients back; archive the `monitor.sh` logs + k6 summaries with the run date.

## Toolkit status

- `bench/monitor.sh` - server-side sampler. Ready.
- `bench/k6-read-load.js` - Phase B read load. Ready (needs `brew install k6` on the Mac).
- `bench/ingest_driver.py` - Phase C/D ingestion + catch-up. **Built and dry-run-verified** against the `bench` tenant (dedup and uniqueness confirmed; 500-entry feed completed cleanly).
- Raw disk ceiling (Run 2a): `fio` (may need `sudo apt install fio`) + `pg_test_fsync` (ships with Postgres). Commands in `docs/load-testing-and-dimensioning.md` Section 6, P0-B1.

## Reading the result -> dimensioning

Translate the knees into the tuple in `docs/load-testing-and-dimensioning.md` Section 8 ("N monitoring tabs AND M analysts AND R entries/sec"), note which resource capped first at each knee, and apply the ranked levers (SSD is #1).
