# Frontend task: a completed remote fetch no longer means transactions are current

Paste everything below into a session opened in `matrix-log-explorer`.

---

## Context

The backend changed how Stage 2 (stitching raw log entries into transactions) is triggered.

**Before:** `fetch_now` ran the stitch itself at the end of every poll. By the time a fetch run
reported `completed`, the transactions were already rebuilt. The frontend was built on that
guarantee and states it explicitly in two places:

`src/hooks/useRemoteFetch.ts:41-43`

```
 * The run FINALIZES INTERNALLY (phase "regrouping" IS the regroup) — so on
 * `completed` the transaction reads are already current and we just reload them
 * via `onCompleted`; never call finalize after a fetch.
```

`src/components/upload/RemoteFetchProgress.tsx:25-26` repeats it.

**Now:** stitching is owned by a dedicated backend worker that drains the `log_regroup_pending`
queue on its own ~1s cadence. The fetch path no longer stitches at all - the transport module does
not even import Stage 2 any more. This was deliberate: previously the SFTP transport, the directory
watcher and the parse worker each had to remember to trigger stitching, and any new ingestion path
that forgot silently left data unstitched.

**Consequence:** a fetch run reaches `completed` while the entries it just ingested are still
queued for stitching. `onCompleted` reloads the feed and the newly fetched data is not in it yet.
It appears a second or so later, on the next reload or via the pending banner.

The comment quoted above is now factually wrong and must not be left in place.

---

## What to change

### 1. Wait for stitching before firing `onCompleted`

`src/hooks/useRemoteFetch.ts` - the `settle` callback at line ~92.

Today:

```ts
const settle = useCallback(
  (r: RemoteFetchRun, opts: StartOptions | undefined) => {
    setIsFetching(false);
    if (r.status === "completed") onCompletedRef.current?.(r);
    opts?.onSettled?.(r);
  },
  []
);
```

Required behaviour on `status === "completed"`:

1. Poll `getRegroupStatus()` (already exported from `src/lib/logsApi.ts:835`) until
   `up_to_date === true`.
2. Then fire `onCompletedRef.current?.(r)`.
3. Fire `opts?.onSettled?.(r)` as it does today.

Details that matter:

- **Derive the flag defensively.** `RegroupStatus.up_to_date` is optional
  (`src/lib/logsApi.ts:76`) and the documented invariant is
  `up_to_date === !pending === (pending_windows === 0)`. Use
  `status.up_to_date ?? !status.pending`.
- **Bound the wait.** Cap it at roughly 15 seconds or ~10 polls. On timeout, fire `onCompleted`
  anyway rather than hanging - a stale feed is recoverable, a stuck spinner is not.
- **Never let this throw.** `getRegroupStatus` is a best-effort probe. On any error, stop waiting
  and fire `onCompleted` immediately.
- **Poll interval** ~750ms-1s. The backend worker ticks at 1s, so a shorter interval only adds load.
- **Cancellation.** If the component unmounts or a new run starts mid-wait, abandon the wait and do
  not fire the stale callback. The hook already has `pollRef` and a `stopPolling` helper - follow
  the same teardown pattern.
- `setIsFetching(false)` should still happen immediately, as it does now. The run genuinely has
  finished; only the feed reload is deferred.

Consider a small `waitForStitch()` helper next to the hook so it is unit-testable on its own.

### 2. Fix the two stale comments

- `src/hooks/useRemoteFetch.ts:41-43` - remove "never call finalize after a fetch" and the claim
  that the run finalizes internally. Replace with: the run reports the PULL only; stitching is done
  by a backend worker shortly afterwards, which is why the hook waits for `up_to_date` before
  reloading.
- `src/components/upload/RemoteFetchProgress.tsx:25-26` - same correction.

These comments are load-bearing: the next person to read them would otherwise reintroduce the bug.

### 3. `phase: "regrouping"` is now a misnomer

`FetchPhase` (`src/lib/logsApi.ts:242`) still includes `"regrouping"`, and the backend still emits
it, so **nothing breaks**. But it no longer means "a regroup is running" - the backend sets it right
before the run ends and nothing is stitching at that moment.

Do **not** remove it from the union - the backend still sends it and an unknown-phase branch would
regress. In `RemoteFetchProgress.tsx:183` the phase is grouped with `"done"` already, so no code
change is needed. Add a one-line comment noting the phase is retained for wire compatibility and no
longer indicates active stitching.

---

## Explicitly NOT changing

Verified against the backend diff - these are safe and need no work:

| Concern | Why it is fine |
| --- | --- |
| `run.result.already_local` | Still emitted. Used at `remoteFetchView.ts:15`, `RemoteFetchProgress.tsx:118` |
| `run.result.errors` | Still emitted. Used at `RemoteFetchProgress.tsx:117` |
| `run.result.finalize` / `finalize_error` | **Removed from the backend**, but nothing in this repo reads them - confirmed by grep |
| `shouldPromptRefresh` (`remoteFetchView.ts`) | Keys off `files_fetched > 0 \|\| entries_ingested > 0`; `files_fetched` is unaffected |
| `entries_ingested` | Unchanged **for now** - see the forward-looking note below |
| Run marked `failed` on a stitch failure | The backend no longer does this. Nothing here keys off a stitch-specific failure |
| Upload flow, `pending_regroup`, `FinalizeBanner` | Unchanged. The soft-pending flag and the banner work exactly as before |
| `GET /logs/regroup/status` shape | Additive only. Every field you already read is unchanged, including `abandoned_windows`. Two new optional fields are described below |

### 4. Two new optional status fields (use if useful; not required)

`GET /logs/regroup/status` now also returns:

- `backing_off_windows: number` — the SUBSET of `pending_windows` that has already FAILED and is
  serving a retry delay. `pending_windows` alone cannot tell "about to be stitched" from "failing
  repeatedly"; this splits them. A rising value is the early warning that precedes
  `abandoned_windows`.
- `next_retry_at: string | null` — when the earliest of those becomes eligible again.

Both are additive and safe to ignore. They are worth using in the stitch wait: if
`backing_off_windows > 0`, the wait is not going to clear quickly, so you may prefer to stop waiting
and reload immediately rather than burn the full timeout.

Also relevant: `read_pending_state(?finalize=true)` previously hardcoded `pending: false` after
finalizing. It now re-counts and reports the truth, so a window inside its retry delay correctly
keeps `pending: true`. If any code path relies on `?finalize=true` always coming back clear, it needs
to handle a truthful non-zero count.

---

## Forward-looking, do not implement yet

A second backend change (currently **disabled** behind `log_parse_worker_enabled=false`) will
decouple SSH downloading from parsing. When it is switched on:

- `entries_ingested` becomes **0** at fetch time, because parsing has not happened yet.
- A new `objects_queued` field carries "how many byte-ranges were queued for parsing".

Four display sites would then show "0 entries": `RemoteFetchProgress.tsx:84,143,158` and
`RemoteFetchHistory.tsx:202`. `shouldPromptRefresh` still works via `files_fetched`.

**Do not change these now.** The flag is off; changing them early would break today's correct
display. Flagged only so it is not a surprise later.

---

## Tests

These files encode the old assumption and will need updating:

- `src/hooks/useRemoteFetch.test.ts`
- `src/components/Explorer.remoteFetch.test.tsx`
- `src/components/upload/RemoteFetchProgress.test.tsx`

Add coverage for:

1. `onCompleted` does **not** fire while `getRegroupStatus()` reports `up_to_date: false`.
2. It fires once the status flips to `up_to_date: true`.
3. It fires after the timeout even if the status never flips.
4. It fires immediately if `getRegroupStatus()` rejects.
5. `up_to_date` absent from the response falls back to `!pending` correctly.
6. A wait in flight is abandoned on unmount / new run, with no stale callback.
7. `onSettled` still fires for `failed` and `cancelled` with no stitch wait at all.

Write the failing tests first, then implement.

Run `npm test`, `npm run typecheck` and `npm run lint` - all three must pass.

---

## Acceptance

Manually: trigger a remote fetch that pulls genuinely new data, and confirm the transaction feed
contains it the moment the progress card settles - with no second refresh needed.
