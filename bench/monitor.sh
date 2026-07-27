#!/usr/bin/env bash
# Server-side resource sampler for load/benchmark runs. Run this ON the box (via the
# /ssh-matrix connection) for the whole duration of a test; analyse the log afterwards.
#
#   ./monitor.sh [interval_seconds] [out_file]
#
# Emits a human-readable sample every INTERVAL seconds: load, CPU (us/sy/id/wa), disk sda
# (raw iostat line; last field is %util), memory, and Postgres connection counts by state.
# Ctrl-C to stop. Correlate the load-test knee with whichever line saturates first
# (expected on this box: disk sda %util for writes, or a single CPU core for heavy renders).
set -u
INT="${1:-5}"
OUT="${2:-/tmp/bench_monitor_$(date +%Y%m%d_%H%M%S).log}"
echo "sampling every ${INT}s -> $OUT   (Ctrl-C to stop)"
while true; do
  {
    echo "===== $(date '+%F %T') ====="
    echo "load:  $(cut -d' ' -f1-3 /proc/loadavg)"
    # vmstat's 2nd line is the average over INT seconds; columns 13-16 = us sy id wa.
    vmstat "$INT" 2 | tail -1 | awk '{printf "cpu:   us=%s sy=%s id=%s wa=%s   runq r=%s blocked b=%s\n",$13,$14,$15,$16,$1,$2}'
    # iostat 1s x2: the 2nd sda line is the current interval. Last field is %util.
    iostat -xd sda 1 2 2>/dev/null | awk '/^sda/{l=$0} END{if(l) print "disk:  "l}'
    free -m | awk '/Mem:/{printf "mem:   used=%sMB avail=%sMB\n",$3,$7}'
    PGPASSWORD=rag psql -h localhost -U rag -d rag -tA -c \
      "SELECT 'pg:    '||coalesce(string_agg(state||'='||c,' '),'(none)') FROM (SELECT coalesce(state,'null') state, count(*) c FROM pg_stat_activity WHERE datname='rag' GROUP BY 1) s;" 2>/dev/null
  } >> "$OUT" 2>&1
done
