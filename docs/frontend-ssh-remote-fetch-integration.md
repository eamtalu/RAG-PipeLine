# Frontend integration spec — Remote SSH log-fetch + soft regroup gate

> Audience: the Next.js frontend (`matrix-log-explorer` repo).
> Status: backend implemented + verified end-to-end (2026-06-17). Frontend not yet built.
> Hand this whole file to the frontend implementer; it is self-contained.

The FastAPI backend gained a feature: pull M3 logs from one or more remote **Windows Servers**
(OpenSSH) over SFTP into Postgres, on-demand or via a background poller. As part of this, the
backend's "regroup pending" behavior changed from a hard block (409) to a soft, non-blocking flag.

All endpoints below are under `/api/v1/logs` and require the existing `X-Customer-Code` header
(reuse the current `logsFetch`/`withCustomer` wrapper). Integrate via the existing patterns:
`src/lib/logsApi.ts`, the `useUpload.ts` polling pattern, `RegroupContext.tsx` +
`regroup/FinalizeBanner.tsx`, and the `Sidebar.tsx` / `upload/UploadPanel.tsx` panels.

---

## 1. BREAKING CHANGE (only one) — transaction reads no longer return 409

Previously, when a regroup was pending, these endpoints returned **409**:
- `GET /logs/transactions`
- `GET /logs/transactions/{id}`
- `GET /logs/transactions/{id}/view`
- `GET /logs/transactions/view`
- `POST /logs/debug/ask`

NOW they ALWAYS return **200** with the last fully-stitched data, plus a flag. JSON endpoints add a
top-level field:

```jsonc
"pending_regroup": {
  "pending": true,                       // freshest ingested tail not yet stitched in
  "pending_windows": 1,
  "oldest_pending_at": "2026-06-17T15:09:58.339407+00:00"  // ISO8601 | null
}
```

When you call a read with `?finalize=true`, it stitches first and returns
`{ "pending": false, "pending_windows": 0, "oldest_pending_at": null, "finalized": true }`.

The two `/view` endpoints return `text/plain`; when pending they are PREFIXED with one line:
`⚠ N newer log window(s) since <ts> are not yet stitched in — re-request with finalize=true ...`

### Required frontend changes for this
- **Remove the read-path 409 handling.** Wherever `logsFetch` maps a 409 on a transaction read to
  `onRegroupPending()` / a blocking banner — delete that path. Reads no longer 409.
- **Drive the banner from the flag instead.** Make `FinalizeBanner` advisory (non-blocking): show it
  when `pending_regroup.pending === true` from any read response, and/or keep using the existing
  `GET /logs/regroup/status` poll (unchanged) which returns the same `{pending, pending_windows,
  oldest_pending_at}`. The "Finalize" action stays: either re-issue the read with `?finalize=true`,
  or call the existing `POST /logs/regroup/finalize` (202 + poll) — both work.
- The user can keep browsing while `pending` is true; the data is consistent, only missing the newest
  tail. Make the banner informational ("New data available — click to include"), not a modal/gate.

> ⚠ **DO NOT treat 409 globally as "regroup pending" anymore** — see §4; the new endpoints use 409
> for different meanings and a global interceptor will misfire.

No fields were removed from any response; everything else is additive.

---

## 2. NEW: SSH source CRUD (a tenant can have MANY Windows Servers)

A source is identified within a tenant by a unique `name` label.

### `GET /logs/ssh-sources`
→ 200 `{ "sources": SshSourceOut[] }`

### `POST /logs/ssh-sources` → 201 `SshSourceOut`
Request body (`SshSourceCreate`):
```jsonc
{
  "name": "prod-wms-1",            // required, 1..128, unique per tenant (409 if dup)
  "host": "10.0.0.5",             // required
  "port": 22,                      // default 22 (1..65535)
  "username": "svc_logs",         // required
  "remote_log_dir": "C:/logs/m3", // required; POSIX path even on Windows OpenSSH
  "file_glob": "*.log",           // default "*.log"
  "enabled": false,                // default false — include in the background poller
  "poll_interval_seconds": null,   // optional, >=5 (reserved; see §4.4)
  "private_key_path": "/keys/wms1",// path to a key file ON THE BACKEND HOST
  "private_key": null,             // OR inline PEM private key (stored encrypted)
  "key_passphrase": null
}
```
Rules: must supply `private_key_path` OR `private_key` (else **400**). **409** if `name` exists.

### `GET /logs/ssh-sources/{id}` → 200 `SshSourceOut` | 404
### `PATCH /logs/ssh-sources/{id}` → 200 `SshSourceOut` | 404
Partial update; any create field EXCEPT `name` (name is the identity, not editable here). Leaving key
fields out keeps the existing key. Sending `private_key` replaces it and clears `private_key_path`
(and vice-versa). Changing `host`/`port`/`username` clears the pinned host fingerprint (re-pins on
next test/connect).

### `DELETE /logs/ssh-sources/{id}` → 204 (cascades its checkpoints) | 404

### `POST /logs/ssh-sources/{id}/test` → 200 | 404 | 409 | 502
Connects, lists the remote dir, pins the host fingerprint on first success.
- → 200 `{ "ok": true, "fingerprint": "SHA256:...", "matched_files": 3, "sample": ["C:/logs/m3/a.log", ...] }`
- → 409 if the server's host key changed vs the pinned one ("host key changed")
- → 502 if connect/auth/SFTP fails (show `detail` text to the user)

### `SshSourceOut` (response — SECRETS ARE NEVER RETURNED)
```jsonc
{
  "id": "uuid", "customer_code": "acme", "name": "prod-wms-1",
  "host": "10.0.0.5", "port": 22, "username": "svc_logs",
  "remote_log_dir": "C:/logs/m3", "file_glob": "*.log",
  "enabled": false, "poll_interval_seconds": null,
  "auth_method": "path",            // "path" | "inline" | "none"
  "host_key_fingerprint": null,      // null until first successful connect
  "last_ok_at": null, "last_error": null,
  "created_at": "...", "updated_at": "..."
}
```

UI: a "Windows Servers (SSH sources)" panel — list, add, edit, delete, "Test connection" per source
(show fingerprint + sample files on success). Treat `private_key` / `key_passphrase` as **write-only**
inputs (never pre-fill; GET never returns them).

---

## 3. NEW: trigger a remote fetch (async, poll like a Job)

### `POST /logs/fetch-remote` → 202
Request body (`FetchRemoteRequest`, all optional):
```jsonc
{
  "source_id": null,           // one source; OMIT to fetch ALL of the tenant's sources
  "from_timestamp": null,      // ISO8601 (tz-aware/UTC); "ensure coverage from here"
  "mode": null                 // "incremental" | "timestamp" | "full"
}
```
Defaults: if `from_timestamp` set and `mode` omitted → `mode="timestamp"`; else `"incremental"`.
- → 202 `{ "run_id": "uuid", "status": "running", "mode": "incremental", "poll": "/api/v1/logs/fetch-remote/runs/{run_id}" }`
- → 404 if `source_id` doesn't belong to the tenant.

### `GET /logs/fetch-remote/runs/{run_id}` → 200 | 404
```jsonc
{
  "run_id": "uuid", "customer_code": "acme", "source_id": "uuid|null",
  "mode": "incremental", "requested_from": "ISO|null",
  "status": "running",            // "running" | "completed" | "failed"
  "phase": "fetching",            // "listing" | "fetching" | "regrouping" | "done"  (LIVE)
  "progress": {                   // LIVE during the run; null before listing finishes
    "current_source": "prod-wms-1", "source_index": 1, "sources_total": 1,
    "current_file": "C:/BEC Logs/app.txt3",
    "files_total": 12, "files_done": 3,      // per the CURRENT source
    "bytes_so_far": 184320, "entries_so_far": 512   // cumulative across sources so far
  },
  "files_considered": 12,         // set as soon as listing finishes (was null mid-run before)
  "files_fetched": null,          // FINAL totals — null until status=completed
  "bytes_fetched": null, "entries_ingested": null,
  "error": null,
  "result": { /* per-source/file stats; may include result.already_local / result.errors[] */ },
  "created_at": "...", "finished_at": "...|null"
}
```

**Live progress contract (`phase` + `progress`).** The run row is now updated *mid-flight* on its own
DB session, so each poll reflects real progress instead of a binary running→done flip:

| `phase` | What's happening | What to show |
|---------|------------------|--------------|
| `listing` | connecting + globbing each server's remote dir | "Connecting & listing files…" (indeterminate) |
| `fetching` | pulling + ingesting files (per-file loop) | progress bar `files_done/files_total`, "Fetching `current_file` (3/12)", live `bytes_so_far`/`entries_so_far` |
| `regrouping` | Stage 2 stitching of what was ingested (the internal finalize) | "Rebuilding transactions…" (indeterminate) |
| `done` | terminal — read `status` for outcome | hide progress; show result/ error |

Notes:
- `files_done` advances for **every** listed file, including unchanged ones skipped with no transfer —
  so the bar moves smoothly even on a no-op incremental poll.
- `files_total` / `files_done` / `current_source` are scoped to the **current** source; use
  `source_index` / `sources_total` to label multi-server fetches ("Server 1/2"). `bytes_so_far` /
  `entries_so_far` are cumulative across all sources processed so far.
- `progress` is `null` until the first `listing` completes, and the final aggregate totals still live
  in the top-level `files_fetched` / `bytes_fetched` / `entries_ingested` (populated only at `done`).

UI: a "Fetch from server" button (per source and/or "fetch all") + optional "from timestamp" picker.
On click → POST, then poll the run (~1500ms) until `status` is `completed`/`failed`, like
`useUpload.ts` polls jobs. Drive a banner off `phase` + `progress` (clone `regroup/FinalizeBanner.tsx`)
in `useRemoteFetch.ts`.

> IMPORTANT: the run **finalizes internally** — the `regrouping` phase IS that finalize, so when it
> reports `status: completed` transaction reads are already current (pending flag false). Do NOT call
> finalize separately after a fetch.
> A `completed` run can still carry **per-source failures** in `result.errors[]` (one server down while
> others succeeded) — show a partial-success warning rather than assuming every server was reached.
> If `result.already_local === true`, the requested timestamp was already covered locally and nothing
> was pulled — surface "already available locally" rather than an error.

---

## 4. ⚠ SIDE EFFECTS / SPECIAL CONSIDERATIONS

1. **Re-scope global error handling in `logsFetch`.** Today it likely maps `409 → regroup pending`
   and `404/422 → invalid tenant, bounce to picker`. After this change:
   - Transaction reads NEVER 409 anymore (the old 409 handler is dead for its original purpose).
   - The new endpoints use **409** for *duplicate source name* and *host-key mismatch* — do NOT route
     these to the regroup banner.
   - The new endpoints use **404** for *source not found* / *run not found* — do NOT bounce the user
     to the tenant picker. Only treat 404/422 from actual tenant-resolution calls as "invalid tenant"
     (the tenant-unknown 404 has `detail` like "Unknown customer: ..."). Easiest fix: stop applying
     the global 409/404/422 interceptors to the ssh-source / fetch-remote calls; handle their errors
     locally at the call site.
2. **Secrets are write-only.** Never display `private_key`/`key_passphrase`. The edit form shows
   `auth_method` + whether a fingerprint is pinned, and lets the user *replace* the key.
3. **Host-key pinning.** First successful test/connect pins `host_key_fingerprint`. A later change
   yields 409 ("host key changed") on test/fetch. To accept a legitimate new key the user must edit
   the source (changing host/username clears the pin) — surface this clearly.
4. **`enabled` = backend auto-poll.** It only takes effect if the server has the poller turned on
   (env `ssh_log_fetcher_enabled`, off by default). Label it "Auto-poll this server"; on-demand
   "Fetch now" works regardless. `poll_interval_seconds` is stored but the v1 poller uses a global
   cadence (reserved field).
5. **Timestamps:** send `from_timestamp` as tz-aware ISO8601 (UTC recommended); DB log timestamps are
   tz-aware.
6. **Multiple servers per tenant** is first-class: the source list can have N rows; "fetch all" omits
   `source_id`.
7. **`GET /logs/entries` is unchanged and never gated** — raw line-level search stays available even
   while a regroup is pending; rely on it during a burst.
8. Unchanged endpoints you already use: `POST /logs/ingest`, `POST /logs/scan`, `GET /logs/jobs/{id}`,
   `POST /logs/regroup`, `POST /logs/regroup/finalize`, `GET /logs/regroup/runs/{id}`,
   `GET /logs/regroup/status`. Only the 5 read endpoints in §1 changed behavior (409 → 200 + flag).

---

## Deliverables
- `src/lib/logsApi.ts`: add `listSshSources`, `createSshSource`, `getSshSource`, `updateSshSource`,
  `deleteSshSource`, `testSshSource`, `triggerRemoteFetch`, `getRemoteFetchRun`; re-scope global error
  handling per §4; add a `pending_regroup` type and read it from transaction responses.
- `src/hooks/useRemoteFetch.ts`: POST + poll-to-terminal (clone `useUpload.ts`).
- UI: SSH-sources panel (CRUD + Test) and a "Fetch from server" control with progress, in
  `Sidebar.tsx` / `upload/UploadPanel.tsx`.
- Make `FinalizeBanner` advisory (non-blocking), driven by `pending_regroup` / `/regroup/status`.

---
_Backend design + decisions: `docs/transaction-log-ingestion-design.md` and the plan
`~/.claude/plans/2026-06-17_15-32_ssh-windows-log-fetch-ingestion.md`._
