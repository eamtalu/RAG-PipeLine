#!/usr/bin/env python3
"""Ingestion load driver for the Matrix log-explorer benchmark suite.

Pushes real, controllable ingestion load by REPLAYING real log text with shifted
timestamps: shifting changes each entry's content hash (so rows are unique despite the
per-tenant dedup) and drops them into a fresh time window. Different offsets multiply
volume from one seed. See docs/load-testing-and-dimensioning.md and bench/RUNBOOK.md.

Run this ON the server (default BASE_URL=http://localhost:8000 + local psql) so disk-write
measurement is not clouded by the network. From another host, set BASE_URL and use
`seed --file <real.log>` instead of `seed --from-db`.

Env: BASE_URL (default http://localhost:8000), CUST (default bench), PGPASSWORD (default rag).
Stdlib only; shells out to `curl` for HTTP and `psql` for counts.

Subcommands:
  setup                      create the throwaway `bench` logspace
  seed   --from-db|--file    build a parseable seed .log (real log text)
  shift  --in --out --offset-days N   make a unique copy in a new time window
  feed   --file              ingest one file, wait for the job, print entries + time
  ramp   --seed --copies K   feed K unique copies, report cumulative entries/sec
  catchup --seed --copies K  feed a backlog for the worker-stopped recovery test
  count                      current entry count for the tenant
  teardown --yes             delete all rows for the tenant (cleanup)

WARNING: only ever point CUST at a throwaway tenant. Never `bench` against tmp-live.
"""
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time

BASE = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
CUST = os.environ.get("CUST", "bench")
PGENV = {**os.environ, "PGPASSWORD": os.environ.get("PGPASSWORD", "rag")}
PSQL = ["psql", "-h", "localhost", "-U", "rag", "-d", "rag", "-tA"]

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) ")
TS_FMT = "%Y-%m-%d %H:%M:%S,%f"


def psql(sql):
    r = subprocess.run(PSQL + ["-c", sql], capture_output=True, text=True, env=PGENV)
    if r.returncode:
        sys.exit(f"psql failed: {r.stderr.strip()}")
    return r.stdout.strip()


def curl(method, path, *, tenant=True, form_file=None, json_body=None):
    """curl wrapper. Returns (status_code:int, text:str)."""
    cmd = ["curl", "-s", "-k", "-o", "-", "-w", "\n%{http_code}", "-X", method, f"{BASE}{path}"]
    if tenant:
        cmd += ["-H", f"X-Customer-Code: {CUST}"]
    if json_body is not None:
        cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(json_body)]
    if form_file is not None:
        cmd += ["-F", f"file=@{form_file}"]
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    body, _, code = out.rpartition("\n")
    return int(code or 0), body


def count_entries():
    return int(psql(f"SELECT count(*) FROM log_entries WHERE customer_code='{CUST}'") or 0)


# ---- timestamp shifting --------------------------------------------------------------
def shift_text(text, delta):
    out = []
    for line in text.splitlines(keepends=True):
        m = TS_RE.match(line)
        if m:
            ts = dt.datetime.strptime(m.group(1), TS_FMT) + delta
            new = ts.strftime("%Y-%m-%d %H:%M:%S,") + f"{ts.microsecond // 1000:03d}"
            line = new + line[m.end(1):]
        out.append(line)
    return "".join(out)


# ---- subcommands ---------------------------------------------------------------------
def cmd_setup(a):
    code, body = curl("POST", "/api/v1/customers", tenant=False,
                      json_body={"customer_code": CUST, "display_name": CUST})
    print(f"setup {CUST}: HTTP {code} {body}")
    if code not in (200, 201, 409):
        print("  (if this needs admin, create the tenant via the UI, then re-run)")


def cmd_seed(a):
    if a.file:
        with open(a.file, "r", errors="replace") as f:
            text = f.read()
    else:
        # Reconstruct a parseable file from real log text (raw_body is the literal entry text).
        sql = (f"SELECT raw_body FROM log_entries WHERE customer_code='{a.source}' "
               f"AND raw_body IS NOT NULL ORDER BY timestamp, line_number LIMIT {a.entries}")
        text = psql(sql) + "\n"
    with open(a.out, "w") as f:
        f.write(text)
    n = sum(1 for ln in text.splitlines() if TS_RE.match(ln))
    print(f"seed -> {a.out}: {len(text)} bytes, ~{n} entry-start lines")


def cmd_shift(a):
    with open(a.inp, "r", errors="replace") as f:
        text = f.read()
    shifted = shift_text(text, dt.timedelta(days=a.offset_days, seconds=a.offset_seconds))
    with open(a.out, "w") as f:
        f.write(shifted)
    print(f"shift {a.inp} -> {a.out} (+{a.offset_days}d {a.offset_seconds}s)")


def _feed_one(path):
    t0 = time.monotonic()
    code, body = curl("POST", "/api/v1/logs/ingest", form_file=path)
    if code != 201:
        return None, f"ingest HTTP {code}: {body[:200]}"
    job_id = json.loads(body)["job_id"]
    # Stage-1 runs in the background; poll the job to completion.
    while True:
        c, b = curl("GET", f"/api/v1/logs/jobs/{job_id}")
        j = json.loads(b) if c == 200 and b.startswith("{") else {}
        st = j.get("status")
        if st in ("completed", "failed"):
            return {"job_id": job_id, "status": st, "entry_count": j.get("entry_count"),
                    "secs": round(time.monotonic() - t0, 2)}, None
        time.sleep(0.5)


def cmd_feed(a):
    res, err = _feed_one(a.file)
    print(err or f"feed {a.file}: {res}")


def cmd_ramp(a):
    before = count_entries()
    t0 = time.monotonic()
    results = []
    for i in range(a.copies):
        out = f"/tmp/ramp_{i:03d}.log"
        with open(a.seed, "r", errors="replace") as f:
            text = f.read()
        # each copy a distinct window: offset i*(offset_days) days back from a far-future base
        delta = dt.timedelta(days=a.offset_days * (i + 1) + a.base_days)
        with open(out, "w") as f:
            f.write(shift_text(text, delta))
        res, err = _feed_one(out)
        results.append(res or err)
        print(f"  [{i+1}/{a.copies}] {res or err}")
    elapsed = time.monotonic() - t0
    after = count_entries()
    added = after - before
    print(f"\nramp: +{added} entries in {elapsed:.1f}s = {added/elapsed:,.0f} entries/sec "
          f"(watch bench/monitor.sh for disk MB/s + %util; stop if open pending grows)")


def cmd_catchup(a):
    print("CATCH-UP TEST: ensure the worker is STOPPED first "
          "(operator: sudo systemctl stop fastapirag-worker), then press Enter.")
    input()
    before = count_entries()
    for i in range(a.copies):
        out = f"/tmp/catchup_{i:03d}.log"
        with open(a.seed, "r", errors="replace") as f:
            text = f.read()
        with open(out, "w") as f:
            f.write(shift_text(text, dt.timedelta(days=a.base_days + a.offset_days * (i + 1))))
        _feed_one(out)
    print(f"backlog fed (+{count_entries()-before} entries). "
          "Now START the worker (operator: sudo systemctl start fastapirag-worker); timing drain...")
    input("press Enter once the worker is started to begin the timer: ")
    t0 = time.monotonic()
    while True:
        c, b = curl("GET", "/api/v1/logs/regroup/status")
        j = json.loads(b) if c == 200 and b.startswith("{") else {}
        if not j.get("pending"):
            break
        print(f"  draining... pending_windows={j.get('pending_windows')} "
              f"({time.monotonic()-t0:.0f}s)")
        time.sleep(5)
    print(f"catch-up drained in {time.monotonic()-t0:.1f}s")


def cmd_count(a):
    print(f"{CUST}: {count_entries():,} entries")


def cmd_teardown(a):
    if not a.yes:
        sys.exit("refusing without --yes")
    for t in ("log_transactions", "log_entries", "log_regroup_pending"):
        psql(f"DELETE FROM {t} WHERE customer_code='{CUST}'")
    print(f"teardown: deleted all rows for {CUST}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("setup").set_defaults(fn=cmd_setup)

    s = sub.add_parser("seed"); s.set_defaults(fn=cmd_seed)
    s.add_argument("--from-db", dest="from_db", action="store_true")
    s.add_argument("--source", default="tmp-live", help="tenant to copy real log text from")
    s.add_argument("--entries", type=int, default=20000)
    s.add_argument("--file", help="use this real .log instead of --from-db")
    s.add_argument("--out", default="seed.log")

    s = sub.add_parser("shift"); s.set_defaults(fn=cmd_shift)
    s.add_argument("--in", dest="inp", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--offset-days", dest="offset_days", type=int, default=30)
    s.add_argument("--offset-seconds", dest="offset_seconds", type=int, default=0)

    s = sub.add_parser("feed"); s.set_defaults(fn=cmd_feed)
    s.add_argument("--file", required=True)

    s = sub.add_parser("ramp"); s.set_defaults(fn=cmd_ramp)
    s.add_argument("--seed", default="seed.log")
    s.add_argument("--copies", type=int, default=10)
    s.add_argument("--offset-days", dest="offset_days", type=int, default=1)
    s.add_argument("--base-days", dest="base_days", type=int, default=365,
                   help="push all test data this many days into the future (isolated window)")

    s = sub.add_parser("catchup"); s.set_defaults(fn=cmd_catchup)
    s.add_argument("--seed", default="seed.log")
    s.add_argument("--copies", type=int, default=10)
    s.add_argument("--offset-days", dest="offset_days", type=int, default=1)
    s.add_argument("--base-days", dest="base_days", type=int, default=365)

    sub.add_parser("count").set_defaults(fn=cmd_count)

    s = sub.add_parser("teardown"); s.set_defaults(fn=cmd_teardown)
    s.add_argument("--yes", action="store_true")

    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
