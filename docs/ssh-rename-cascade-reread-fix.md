# SSH poller: stop re-downloading rotated logs (rename-cascade fix)

## Problem (confirmed from production, see the postmortem below)

The remote (`C:/BEC Logs`, glob `*.txt*`) rotates logs by **renaming a whole chain**:
`eSmartServerLog.txt` (active) → `.txt.1` → `.txt.2` → … → `.txt.100`, oldest deleted.
On each rotation every file's content shifts up one path.

The poller checkpoints **by path** (`log_ssh_file_checkpoints`, unique on `source_id, remote_path`),
storing `(last_size, last_mtime, last_offset, head_fingerprint)`. After a rotation the content at each
path is different from its stored checkpoint, so the poller treated every path as "rotated/changed" and
**re-read the entire chain from offset 0** - ~100 files, ~500 MB - on every rotation.

Confirmed evidence (192.168.0.142, tenant `tmp-live`):
- 113 of 126 ingest jobs in a 45-min window produced **zero new entries** (pure re-download; content
  deduped by `entry_hash`).
- Files re-read in a burst right after a rotation, then quiet - episodic, once per rotation.
- Only 1-2 of 101 files were actually being written; the other ~99 were static (mtimes back to Jun 27)
  yet were re-downloaded.

Impact: each rotation caused a ~20-min fetch pass that held the per-host advisory lock, froze the
tenant's poll loop (stale `last_ok_at` → UI "needs attention"/"catching up"), spiked CPU (SSH
decryption of ~500 MB), and surfaced transient "No such file" when a file rotated mid-pass.

No data was lost (entry-level dedup kept transactions correct); the cost was wasted bandwidth/CPU and
loop stalls.

## Fix

Recognise content we have **already fully ingested** when it reappears at a **new path**, and skip
re-downloading it - keyed on content identity, not path.

- In `_fetch_source` (`app/services/mnp_log_ingestion/remote/remote_fetcher.py`), after loading this
  source's checkpoint snapshot, build a content-identity index once:
  `sig_consumed = {(head_fingerprint, size, mtime): max last_offset}`.
  It is built from the **pre-poll snapshot** and is immutable during the loop, so a file processed
  later still sees the signature of content whose old path we've already upserted this poll.
- The per-file decision is extracted into a **pure, unit-tested** function `_plan_incremental(...)`
  with this order:
  1. unchanged at this path → skip (backfill a legacy NULL fingerprint);
  2. **NEW - content-identity skip:** `size >= N` and `(head_fp, size, mtime)` was previously consumed
     to `>= size` → skip the transfer, just record the new path's checkpoint as consumed;
  3. rotation/replace/truncation on this path → re-read whole;
  4. otherwise tail from the last offset (or from 0 for a new path).
- Skips are counted and surfaced: `content_skipped` in the per-source stats, aggregated in
  `fetch_now`, and logged by the poller.

### Why it is safe (no false skip, no data loss)

- The skip matches the **full `(head_fingerprint, size, mtime)` triple**. A Windows rename preserves
  all three, so a genuine cascade matches; any mismatch falls through to a normal (re)read.
- `size >= N` (`ssh_fingerprint_bytes`, 4096) is required, so the head hash is a reliable content
  identity (same rule the existing rotation guard uses).
- A reused path holding **different** content of identical size+mtime is **not** skipped, because its
  head fingerprint differs (covered by a test).
- `entry_hash` content dedup remains the ultimate correctness backstop for anything transferred.
- If the head fingerprint were ever non-deterministic, the skip simply wouldn't fire (safe
  degradation to the current re-read behaviour) - it can never cause a wrongful skip.

### Scope / non-goals

- No schema change, no migration - uses the existing `head_fingerprint` column and checkpoint rows.
- Only `incremental` mode is affected; `seed` / `timestamp` / `full` are untouched.
- The small per-file head read (open+read 4 KB+close for rotation/identity detection) is unchanged.
  That is ~100 small round-trips per poll, far below the eliminated ~500 MB. Reducing it further
  (e.g. skipping the head read when path size+mtime are unchanged) is a possible future optimisation,
  deliberately out of scope to keep this change surgical.

## Tests

`tests/test_ssh_rotation_cascade_chunk12.py`:
- `test_plan_incremental_all_branches` - the pure planner across every branch, including the
  content-skip and its safety guards (different mtime / different size / sub-N file all NOT skipped).
- `test_rename_cascade_skips_already_ingested_content` - a full rename cascade over the fake SFTP:
  first poll ingests 3 files; after the cascade, `content_skipped == 3`, `files_fetched == 1` (only the
  new active file), and no content is re-ingested.
- `test_reused_path_different_content_is_not_skipped` - identical size+mtime but different bytes is
  re-read, never skipped.

Full suite: 73 passed, no regressions (the existing `chunk2`/`chunk3` SSH tests exercise the same
`_fetch_source` incremental path and still pass, confirming behaviour was preserved).

## Deployment

Code-only, no migration. `deploy.sh` (pull + restart) suffices; the worker restart picks it up. First
poll after deploy is cheap (existing checkpoints match current content → step 1 skips); the next
rotation exercises the content-skip instead of re-downloading the chain. Verify with the poller log
line showing `content_skipped` > 0 around a rotation, and by watching that per-rotation re-download
bytes drop (zero-new-entry ingest jobs should largely disappear).
