# Frontend Integration Spec — SSH Log-Source Admin & Fetch

Low-level, implementation-ready contract for the SSH log-fetch feature after the backend hardening.
Everything here matches the deployed backend exactly. Implement + verify against this in one pass.

- **Base URL:** `/api/v1`
- **Router prefix:** `/logs` (so every path below is `/api/v1/logs/...`)
- **Auth header (REQUIRED on every request):** `X-Customer-Code: <tenant-slug>`
  - Slug format: `^[a-z0-9][a-z0-9_-]{0,63}$` (lowercase). Malformed → **400**; unknown tenant → **404**; **omitted header → 422** (FastAPI validation).
  - Read endpoints require the tenant to exist; mutating endpoints (create/update/delete/test/fetch/cancel) additionally require it to be **active** (else 404).
- **Content type:** `application/json` for all request bodies. All timestamps are **ISO-8601 UTC** strings (e.g. `2026-07-08T09:15:00+00:00`) or `null`.

---

## 0. What changed vs the frontend's earlier SSH integration

The earlier integration already had: CRUD on `/ssh-sources`, `POST /ssh-sources/{id}/test`, `POST /fetch-remote`, and `GET /fetch-remote/runs/{id}`. Those paths are unchanged. The differences to implement:

1. **Source object (`SourceOut`) gained 5 fields** — render `status` (see §7) instead of deriving health locally:
   `status`, `effective_poll_seconds`, `last_attempt_at`, `consecutive_failures`, `auto_disabled_at`.
2. **`POST /fetch-remote` has two new 409s** to handle:
   - the target source is `enabled=true` (auto-polled) → must disable before manual fetch;
   - a run is already in progress → response echoes the in-flight `run_id` (attach to it, don't start a new one).
3. **Run status can now be `cancelled`** — a new terminal state. Polling loops must treat `completed | failed | cancelled` as terminal.
4. **Two NEW endpoints:** `GET /fetch-remote/runs` (history list) and `POST /fetch-remote/runs/{id}/cancel`.
5. **Behavioral:** the Fetch action should be offered only when a source is `enabled=false`; a "fetch all" (omit `source_id`) now pulls only the disabled sources.
6. **Start-point on enable:** enabling auto-poll on a fresh source would otherwise backfill all current files on the first poll. A new **`seed` mode** (and the existing `timestamp` mode) let you start "from now" or "from a date" with no/bounded backfill — the frontend must route the Auto-poll choice through §A step 3.

Nothing else in the request/response shapes changed.

---

## A. Onboarding flow (the recommended guided UX)

Auto-poll is controlled **entirely from the frontend** by each source's `enabled` flag — there is **no backend env to set**. The poll supervisor always runs and stays idle until a source is enabled. Guide the user: **Add → Test → Choose mode → Manage.**

1. **Add server** — `POST /ssh-sources` with `enabled:false` (create it inactive so it's verified before going live). Collect connection + auth + what-to-pull.
2. **Test** — `POST /ssh-sources/{id}/test`. Show the pinned `fingerprint` + `sample` files ("Found N files"). Gate step 3 on a green result. (`409` mismatch / `502` unreachable → show the reason; let them fix and re-test.)
3. **Choose how it runs:**
   - **Auto-poll** — ask **from when to start** (this is what avoids backfilling months of old logs):
     - **From now** *(recommended — zero backfill):* `POST /fetch-remote { "source_id": id, "mode": "seed" }` → poll the run to `done` → `PATCH /ssh-sources/{id} { "enabled": true }`. `seed` marks every current file as already-read and **ingests nothing**, so the poller then only picks up **new lines from here**.
     - **From a date/time:** `POST /fetch-remote { "source_id": id, "mode": "timestamp", "from_timestamp": "<ISO>" }` → poll to `done` → `PATCH { "enabled": true }`. Pulls only files at/after T and seeds the rest forward.
     - **All existing history:** `PATCH { "enabled": true }` directly — the first poll backfills every current file (dedup-safe, but can be large). Offer this only when the user really wants history.
   - **Manual** → leave `enabled:false`; show a **Fetch now** button (`POST /fetch-remote {source_id}` → poll the run).

   Important: without a "From now" / "From a date" seed step, flipping `enabled:true` on a fresh source makes the first poll **backfill all current files** — so always route the Auto-poll choice through one of the three options above.
4. **Manage** — drive each card off `status` (§7): auto cards show live/stale + last-synced + **Pause** (`PATCH enabled:false`); manual cards show **Fetch now** + **Turn on auto-poll** (`PATCH enabled:true`); an `auto_disabled` card shows **Resume** (the §8 windowed-resume recipe, then `PATCH enabled:true`).

**State → control cheatsheet:**
- `enabled:true` (auto) → the only control is **Pause** (disable). Never show **Fetch now** here (the API 409s it).
- `enabled:false` (manual) → controls are **Fetch now** and **Turn on auto-poll**.

---

## 1. `GET /logs/ssh-sources` — list sources

- **Auth:** current-customer. **Returns 200.**
- **Response:** `{ "sources": SourceOut[] }` (see §7 for `SourceOut`). Use this for the fleet/health dashboard — every item carries `status`, so no per-source calls are needed.

---

## 2. `POST /logs/ssh-sources` — create a source

- **Auth:** active-customer. **Returns 201** → `SourceOut`.
- **Request body** (`SshSourceCreate`):

| field | type | required | default | constraints |
|---|---|---|---|---|
| `name` | string | yes | — | 1–128 chars; unique per tenant |
| `host` | string | yes | — | non-empty |
| `port` | int | no | `22` | 1–65535 |
| `username` | string | yes | — | non-empty |
| `remote_log_dir` | string | yes | — | POSIX path, e.g. `C:/logs/m3` |
| `file_glob` | string | no | `"*.log"` | |
| `enabled` | bool | no | `false` | include in the poller |
| `poll_interval_seconds` | float\|null | no | `null` | `>= 5` if set |
| `private_key_path` | string\|null | no | `null` | key file **on the backend host** |
| `private_key` | string\|null | no | `null` | inline PEM (stored encrypted) |
| `key_passphrase` | string\|null | no | `null` | for the inline PEM |

- **Auth rule:** provide **exactly one** of `private_key_path` OR `private_key` (with optional `key_passphrase`). Providing neither → **400** `"Provide private_key_path or private_key for SSH key auth."`
- **Errors:** `409` if `name` already exists for the tenant; `400` on missing key or (if inline) when the server's encryption key is unconfigured.
- **Note:** key material is **never** returned in any response.

---

## 3. `GET /logs/ssh-sources/{source_id}` — one source

- **Auth:** current-customer. Path param `source_id` (UUID). **200** → `SourceOut`; **404** if not found for this tenant.

---

## 4. `PATCH /logs/ssh-sources/{source_id}` — update

- **Auth:** active-customer. **200** → `SourceOut`; **404** if not found.
- **Request body** (`SshSourceUpdate`) — all fields optional; send only what changes:
  `host, port, username, remote_log_dir, file_glob, enabled, poll_interval_seconds, private_key_path, private_key, key_passphrase` (same types/constraints as create).
- **Behavior to know:**
  - Setting `private_key_path` and `private_key` are mutually exclusive (setting one clears the other).
  - Changing `host`/`port`/`username` clears the pinned `host_key_fingerprint` (it re-pins on next connect/test).
  - **Setting `enabled: true` re-arms the circuit breaker** (resets `consecutive_failures` to 0 and clears `auto_disabled_at`). This is the "resume after an outage" action.

---

## 5. `DELETE /logs/ssh-sources/{source_id}`

- **Auth:** active-customer. **204** (no body); **404** if not found. Cascades the source's checkpoints.

---

## 6. `POST /logs/ssh-sources/{source_id}/test` — connectivity probe

- **Auth:** active-customer. Use as the **"is it reachable right now?"** button.
- **200** → `{ "ok": true, "fingerprint": "SHA256:…", "matched_files": <int>, "sample": ["C:/logs/m3/app.log", …] }` (sample capped at 25). Pins the host fingerprint on first success.
- **Errors:** `409` on host-key mismatch (server key changed vs pinned); `502` on connection/config/secret failure (detail is a human-readable reason). `404` if the source isn't the tenant's.

---

## 7. `SourceOut` object (returned by §1–§4)

```jsonc
{
  "id": "uuid",
  "customer_code": "acme",
  "name": "prod-wms-1",
  "host": "10.0.0.5",
  "port": 22,
  "username": "svc-logs",
  "remote_log_dir": "C:/logs/m3",
  "file_glob": "*.log",
  "enabled": false,
  "poll_interval_seconds": null,          // null = use the global cadence
  "effective_poll_seconds": 60.0,         // NEW: resolved cadence (source interval or global)
  "auth_method": "path",                  // "path" | "inline" | "none"
  "host_key_fingerprint": "SHA256:…",     // null until first successful connect
  "status": "disabled",                   // NEW: server-computed, see table below
  "last_ok_at": "2026-07-08T09:15:00+00:00",   // last SUCCESSFUL fetch, or null
  "last_attempt_at": "2026-07-08T09:16:00+00:00", // NEW: last ATTEMPT (success or fail), or null
  "last_error": null,                     // text; cleared on success
  "consecutive_failures": 0,              // NEW: breaker counter
  "auto_disabled_at": null,               // NEW: set when the breaker auto-disabled (else null)
  "created_at": "…",
  "updated_at": "…"
}
```

### `status` values → UI

| status | meaning | suggested UI |
|---|---|---|
| `live` | enabled, healthy, recent successful poll | green |
| `stale` | enabled, no recent successful poll (poller lagging / just resumed) | amber; offer **Test** |
| `degraded` | enabled but currently failing (`last_error` set, below the auto-disable threshold) | amber/red; show `consecutive_failures` |
| `pending` | enabled, never polled yet | grey; offer **Test** |
| `auto_disabled` | **broken** — breaker disabled it after a sustained outage (`enabled=false`, `auto_disabled_at` set) | red; show `last_error`; offer **Resume** then **Enable** |
| `disabled` | operator-disabled (manual-only), intentional (`enabled=false`, `auto_disabled_at=null`) | grey; offer **Fetch now** / **Enable** |

- **Live vs broken:** live ⇔ `status==="live"` (or run a `test`); broken ⇔ `status ∈ {degraded, auto_disabled}`.
- **When to offer manual Fetch:** only when `enabled===false` (i.e. `status ∈ {disabled, auto_disabled}`). If `enabled===true`, hide/disable Fetch (the API 409s it).
- **"When did it last run":** show `last_attempt_at`; pair with `last_ok_at` ("last succeeded") for unhealthy sources.

---

## 8. `POST /logs/fetch-remote` — trigger a fetch

- **Auth:** active-customer. **Returns 202** (non-blocking) → `{ "run_id": "uuid", "status": "running", "mode": "incremental", "poll": "/api/v1/logs/fetch-remote/runs/<run_id>" }`.
- **Request body** (`FetchRemoteRequest`):

| field | type | required | notes |
|---|---|---|---|
| `source_id` | uuid\|null | no | one source; **omit to fetch all the tenant's disabled sources** |
| `from_timestamp` | ISO-8601\|null | no | windowed catch-up lower bound |
| `mode` | enum\|null | no | `"incremental"` \| `"timestamp"` \| `"full"` \| `"seed"` |

- **Mode default:** if `mode` omitted → `timestamp` when `from_timestamp` is given, else `incremental`. Explicit `mode` wins.
- **`seed` mode** ingests nothing — it just marks every current file as already-read to its end, so a subsequent poll starts "from now" with zero backfill. Use it in the "From now" auto-poll onboarding (§A). Runs like any other run (poll it to `done`).
- **Errors (handle both):**
  - **409** — target `source_id` is `enabled=true`: `detail = "Source is auto-polled; disable it before fetching manually."` → prompt the user to disable it first (PATCH `enabled:false`), then retry.
  - **409** — a run is already in progress for this target: `detail = { "message": "A fetch is already in progress for this target", "run_id": "<uuid>" }` → **attach to that `run_id`** (poll it) instead of starting a new run.
  - **404** — `source_id` given but not found.
- **UX:** disable the Fetch button while a run for that source is active (you hold its `run_id`).

### Windowed resume recipe (after an outage / for `auto_disabled`)
1. Ensure the source is disabled (it already is if `auto_disabled`, else PATCH `enabled:false`).
2. `POST /fetch-remote { "source_id": "<id>", "mode": "timestamp", "from_timestamp": "<e.g. now-24h ISO>" }`.
3. Poll the run to completion.
4. PATCH `enabled:true` to resume auto-polling forward-only (this also re-arms the breaker).

---

## 9. `GET /logs/fetch-remote/runs/{run_id}` — poll one run

- **Auth:** current-customer. **200** → `RunOut` (see §11); **404** if not the tenant's.
- **Poll until** `status ∈ {"completed", "failed", "cancelled"}`. Use `phase` + `progress` for a live progress bar.

---

## 10. `GET /logs/fetch-remote/runs` — run history (NEW)

- **Auth:** current-customer. **200** → `{ "runs": RunOut[] }`, newest first, tenant-scoped.
- **Query params (all optional):**

| param | type | default | constraints |
|---|---|---|---|
| `source_id` | uuid | — | filter to one source |
| `status` | enum | — | `running`\|`completed`\|`failed`\|`cancelled` |
| `limit` | int | `50` | 1–200 |
| `offset` | int | `0` | `>= 0` |

Backs an audit/history panel.

---

## 11. `RunOut` object (returned by §8 partial, §9, §10)

```jsonc
{
  "run_id": "uuid",
  "customer_code": "acme",
  "source_id": "uuid",         // null = the run covered all (disabled) sources
  "mode": "incremental",       // incremental | timestamp | full | seed
  "requested_from": null,      // ISO-8601 or null (the from_timestamp)
  "status": "running",         // running | completed | failed | cancelled
  "phase": "fetching",         // listing | fetching | regrouping | done | null
                               // NOTE (2026-08-05): "regrouping" is retained for wire
                               // compatibility only. The stitch worker owns Stage 2, so a
                               // completed run does NOT mean transactions are rebuilt yet.
  "progress": {                // free-form live progress, or null
    "current_source": "prod-wms-1", "source_index": 1, "sources_total": 1,
    "files_total": 4, "files_done": 2, "current_file": "C:/logs/m3/app.log",
    "bytes_so_far": 12345, "entries_so_far": 210
  },
  "files_considered": 4,
  "files_fetched": 2,
  "bytes_fetched": 12345,
  "entries_ingested": 210,
  "error": null,               // text when status=failed/cancelled
  "result": { … },             // full aggregate stats when finished, else null
  "created_at": "…",
  "finished_at": null          // set when terminal
}
```

---

## 12. `POST /logs/fetch-remote/runs/{run_id}/cancel` — cancel an in-flight run (NEW)

- **Auth:** active-customer. **200** → `{ "run_id": "uuid", "status": "cancelled" }`.
- **Errors:** `404` if the run isn't the tenant's; **409** if the run is already terminal (`completed`/`failed`/`cancelled`) — `detail = "Run is already <status>; nothing to cancel"`.
- Already-ingested bytes stay (safe); the next fetch resumes from the last checkpoint.

---

## 13. Suggested frontend flows

- **Onboard:** the full guided flow is **§A** (Add → Test → Choose mode → Manage). For Auto-poll, always route through the start-point choice (From now `mode:seed` / From a date `mode:timestamp` / All history) so you don't backfill old logs; for Manual, leave `enabled:false`.
- **Health dashboard:** `GET /ssh-sources`, colour by `status`; show `last_attempt_at`/`last_ok_at`/`last_error`/`consecutive_failures`; badge `auto_disabled` distinctly (it's a breaker trip, not an operator disable).
- **Manual fetch:** only when `enabled===false`; `POST /fetch-remote` → store `run_id` → poll `runs/{id}` until terminal; offer **Cancel** while `running`.
- **Resume after outage:** the §8 windowed-resume recipe.
- **History panel:** `GET /fetch-remote/runs?source_id=…`.

---

## 14. Verification checklist (do all after implementing)

Send `X-Customer-Code` on every call.

1. List sources → renders `status` for each; no local health derivation.
2. Create with neither key → 400; duplicate name → 409; valid → 201, `auth_method` correct, no key material in the response.
3. `test` on a good source → 200 with `fingerprint` + `sample`; on an unreachable host → 502.
4. PATCH `enabled:true` on an `auto_disabled` source → response shows `consecutive_failures:0`, `auto_disabled_at:null`, `enabled:true`.
5. `POST /fetch-remote` on an `enabled:true` source → 409 ("auto-polled"); Fetch button hidden for enabled sources.
6. Two `POST /fetch-remote` in a row for the same disabled source → second returns 409 with `detail.run_id`; UI attaches to it.
7. Poll a run → transitions to `completed`/`failed`; polling loop treats `cancelled` as terminal too.
8. Start a run, `POST …/cancel` → 200 `status:"cancelled"`; cancelling a finished run → 409.
9. `GET /fetch-remote/runs?status=failed&limit=10` → newest-first, only this tenant's, only failed.
10. Omit `X-Customer-Code` on any call → 422; malformed → 400; unknown tenant → 404.
