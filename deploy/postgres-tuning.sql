-- deploy/postgres-tuning.sql
--
-- One-time Postgres provisioning for the RAG-Pipeline host (Change 3 of the
-- WORKER TIMEOUT outage work - see docs/ISSUE-worker-timeout-outage.md).
--
-- WHY: the box is bottlenecked on DISK WRITES, not CPU/RAM. The log ingest + Stage-2
-- stitch cycle writes in bursts; stock Postgres (shared_buffers 128MB, max_wal_size 1GB,
-- wal_compression off, synchronous_commit on) turns those into frequent, heavy checkpoint
-- + fsync storms on a slow disk. These settings smooth the writes and keep reads in cache
-- so they stop competing with writes.
--
-- HOW TO APPLY (as the postgres superuser; `rag` is NOT a superuser):
--     sudo -u postgres psql -d rag -f deploy/postgres-tuning.sql
--     sudo -u postgres psql -d rag -c "SELECT pg_reload_conf();"   # applies the reload-only settings
--     sudo systemctl restart postgresql                           # ONLY needed for shared_buffers
--
-- This is a ONE-TIME provisioning step, NOT part of deploy.sh (do not run it on every deploy).
-- Settings persist in postgresql.auto.conf and survive reboots.
--
-- VALUES ARE SIZED FOR THE PRODUCTION VM: 47 GB RAM, ~8 cores. On a box with different RAM, scale:
--     shared_buffers        ~= 15-25% of RAM
--     effective_cache_size  ~= 60-75% of RAM   (planner hint only; does not allocate memory)
-- On a small dev machine / laptop container, use smaller values or skip this file entirely.
--
-- ROLLBACK: `ALTER SYSTEM RESET <name>;` for each, then `SELECT pg_reload_conf();`
--           (plus a restart to revert shared_buffers).

-- ── Reload-only (take effect on pg_reload_conf(), no restart) ──────────────────────────
ALTER SYSTEM SET max_wal_size = '8GB';               -- was 1GB:  fewer, gentler checkpoints
ALTER SYSTEM SET min_wal_size = '2GB';               -- was 80MB: less WAL segment recycling churn
ALTER SYSTEM SET wal_compression = 'on';             -- was off:  smaller WAL = fewer bytes to fsync
ALTER SYSTEM SET checkpoint_timeout = '15min';       -- was 5min: spread checkpoint writes further apart
ALTER SYSTEM SET checkpoint_completion_target = 0.9; -- spread the checkpoint over 90% of the interval
ALTER SYSTEM SET effective_cache_size = '32GB';      -- was 4GB:  planner hint (box caches ~44GB)

-- synchronous_commit=off: drop the per-commit WAL fsync wait (the most direct fix for the
-- fsync-bound write latency). TRADE-OFF: on an OS/hardware crash the last <1s of committed
-- transactions may be lost. Low-risk HERE because log ingest is idempotent and re-fetches from
-- the source log files, so any lost tail is rebuilt on the next poll. Flip back to 'on' if this
-- DB ever holds data that is NOT reconstructable from an external source.
ALTER SYSTEM SET synchronous_commit = 'off';

-- ── Restart-only (postmaster context; requires `systemctl restart postgresql`) ─────────
ALTER SYSTEM SET shared_buffers = '8GB';             -- was 128MB

-- Verify after applying (fresh session reflects reloaded + restarted values):
--   SELECT name, setting, unit, pending_restart FROM pg_settings
--   WHERE name IN ('shared_buffers','max_wal_size','min_wal_size','wal_compression',
--                  'effective_cache_size','checkpoint_timeout','checkpoint_completion_target',
--                  'synchronous_commit') ORDER BY name;
