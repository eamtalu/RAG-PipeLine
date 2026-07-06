# Backend spec — Logspace palette (permanent vs disposable)

Audience: backend engineer on the FastAPI service behind `matrix / log explorer`.
Goal: implement the backend so the new **Switch Logspace** command palette can be repointed from its interim behavior to real, persisted data in one pass.

This spec is written against the **actual current frontend** (verified in `src/lib/logspaces.ts` and `src/lib/customersApi.ts`). It states (A) what exists today and must keep working, (B) what the frontend already implemented and the contract it holds to, (C) exactly what to build, (D) the exact JSON the frontend will consume, and (E) how to verify.

All endpoints below live under `/api/v1/customers` and are **NOT tenant-scoped** — they must never require or read the `X-Customer-Code` header. JSON is snake_case (existing convention).

---

## A. What exists today — DO NOT BREAK

### A.1 Customer model (current JSON shape the frontend already parses)
```jsonc
// Customer
{
  "id": "uuid",
  "customer_code": "mnp",            // slug: ^[a-z0-9][a-z0-9_-]{0,63}$ (server lowercases)
  "display_name": "amin" ,           // primary label, or null
  "active": true,
  "created_at": "2026-06-01T00:00:00Z",
  "display_names": [                  // present on GET /{code}; array of aliases
    { "id": "uuid", "customer_code": "mnp", "display_name": "amin", "active": true, "created_at": "..." }
  ],
  "timezone": "Europe/London",       // or null
  "timezone_set": true,
  "effective_timezone": "Europe/London"
}
```

### A.2 Endpoints in use today (keep exact behavior + shapes)
| Method | Path | Notes the frontend relies on |
|---|---|---|
| `POST` | `/api/v1/customers` | Body `{customer_code, display_name?, timezone?}`. Create-or-attach: **201** = new code created, **200** = name attached to existing code, **400** = malformed code. Returns `Customer`. |
| `GET` | `/api/v1/customers?include_inactive=<bool>` | Returns `{count, customers: Customer[]}`. Each customer **must include `created_at`** (the palette joins on it). Also supports `?unset_timezone_only=true`. |
| `GET` | `/api/v1/customers/log-spaces?include_inactive=<bool>` | Returns `{count, log_spaces: LogSpaceOption[]}` where `LogSpaceOption = {label, customer_code, active}` — **one row per display name**. |
| `GET` | `/api/v1/customers/{code}` | Returns `Customer` incl. `display_names[]`, or **404**. |
| `PATCH` | `/api/v1/customers/{code}` | Body `{active}` and/or `{timezone}`. Returns `Customer`. **The palette uses `{active:false}` as its current "delete a disposable".** |
| `GET` | `/api/v1/customers/timezones` | `{current_default, timezones[]}`. |
| `GET/POST/DELETE` | `/api/v1/customers/{code}/display-names[/{name_id}]` | Alias CRUD (already present; not used by the palette's delete after we chose code-based delete, but keep them). |

Errors return `{detail: string}` and the frontend surfaces `detail`.

---

## B. What the frontend already implemented (the contract it holds to)

The palette currently runs against **only the existing endpoints above**, with the new concepts stubbed client-side. Confirmed behavior:

1. **Every current customer record is treated as a DISPOSABLE logspace.** The palette's DISPOSABLE group is built from `GET /customers/log-spaces?include_inactive=false` (one row per display name), joined to `GET /customers?include_inactive=false` for `created_at`.
2. **PERMANENT group is empty** and shows an empty state — permanents don't exist yet; they will be created via a future **Manage → Permanent logspace** admin UI.
3. **Create disposable** = `POST /customers {customer_code, display_name}` with a **brand-new customer_code every time** (1:1 code ⇄ disposable; the UI does not offer existing codes for disposables).
4. **Delete disposable** = `PATCH /customers/{code} {active:false}` — **by customer_code** (the whole code), currently a soft deactivate.
5. **Owner, expiry, TTL** for disposables are shown as "pending" (not stored server-side yet).
6. **Presence** (who is in a permanent space) is a client-only localStorage stub for now.
7. **Display-name suggestions** in the create form come from `GET /customers/log-spaces?include_inactive=true` labels.

All of the above lives behind one file (`src/lib/logspaces.ts`). When you ship the items in Part C, the frontend repoints there — no component changes.

---

## C. What to build

### C.1 Data model changes (customer table)

Add these columns to the customer record:

| Column | Type | Null? | Default | Applies to | Meaning |
|---|---|---|---|---|---|
| `kind` | enum(`permanent`,`disposable`) | NOT NULL | `disposable` | both | Which group the record belongs to. |
| `name` | varchar(128) | NULL | — | permanent | Human name of a permanent space (distinct from `customer_code` and from `display_name` aliases). |
| `description` | text | NULL | — | permanent | Free-text description. |
| `environment` | enum(`live`,`test`) | NULL | — | permanent | Admin-set. `inactive` is **derived** from `active=false`, so do NOT add `inactive` to this enum. |
| `owner_name` | varchar(128) | NULL | — | disposable | Who created the disposable. |
| `expires_at` | timestamptz | NULL | — | disposable | When the disposable auto-expires (see C.4). |

`ingest_rate` (permanent, e.g. `"1.2k/min"`): expose as a **computed/optional string** in responses (see D). It can be derived from ingestion metrics rather than stored; return `null` when unknown or when `active=false`.

**Migration:** backfill `kind='disposable'` for every existing customer (they are all disposables per product). Leave permanent-only columns null.

### C.2 New presence table

`logspace_presence`:
| Column | Type | Notes |
|---|---|---|
| `id` | uuid PK | |
| `customer_code` | varchar FK → customer | indexed |
| `name` | varchar(128) | who is present |
| `note` | varchar(256) NULL | optional |
| `since` | timestamptz | server-set on insert |

De-dupe by `(customer_code, name)` — a repeat insert refreshes `since` and `note` (upsert). Sweep entries older than a TTL (12h suggested) via the same worker as C.4, or on read.

### C.3 API additions / changes

**(1) Hard delete a logspace — the delete the palette wants**
```
DELETE /api/v1/customers/{code}
```
- Removes the customer_code **and all associated data**: its `display_names` aliases, its presence rows, and any owned records that should not outlive it. This is the real cleanup (today the palette only soft-deactivates via PATCH; it will switch to this).
- **204** on success, **404** if the code doesn't exist.
- Admin-authorized (see auth note). Applies to both disposable delete and permanent delete.
- Decide + document: hard delete vs. soft (tombstone). The frontend treats a deleted code as gone either way, but the product intent for **disposables is a real purge**.

**(2) Disposable create (refine `POST /customers`)**
- When creating a disposable, set `kind='disposable'`, stamp `owner_name` (from request), and set `expires_at = now + 30 days` (default TTL; see C.4).
- Accept an optional `owner_name` and optional `expires_at` in the body; default them server-side.
- Because a disposable owns a **brand-new** code, creating a disposable against an **already-existing code should 409** (not silently attach). Keep the existing create-or-attach 200/201 semantics for the non-disposable path.
- Suggested explicit body: `POST /customers {customer_code, display_name, kind:"disposable", owner_name?}`.

**(3) Permanent CRUD (Manage → Permanent logspace)**
- Create: `POST /customers {customer_code, kind:"permanent", name, description?, environment:"live"|"test"}` → **201**, returns `Customer`. 409 if code exists.
- Edit: extend `PATCH /customers/{code}` to accept `{name?, description?, environment?}` in addition to the existing `{active?, timezone?}`.
- Inactivate/reactivate: existing `PATCH {active}`.
- Delete: the `DELETE /customers/{code}` from (1), admin-only.

**(4) Presence endpoints**
- `POST /api/v1/customers/{code}/presence {name, note?}` → upsert, server sets `since`, returns the presence row. Used when a user opens a permanent space.
- `DELETE /api/v1/customers/{code}/presence/{id}` → **204** (leave).

**(5) Enrich the read responses** (so the palette can drop its client-side stubs — see Part D for exact shapes)
- Add `kind`, and permanent fields (`name`, `description`, `environment`, `ingest_rate`, `active_presence[]`) and disposable fields (`owner_name`, `expires_at`, `created_at`) to **`GET /customers/log-spaces`** rows. This is the palette's primary source; enriching it lets the frontend stop joining `listCustomers`.
- Add the same new columns to the `Customer` shape returned by `GET /customers` and `GET /customers/{code}` (for Manage + detail views).

### C.4 Auto-expiry / cleanup worker
- A scheduled job (cron/worker) that, for `kind='disposable'` rows with `expires_at <= now`, performs the **same hard delete as `DELETE /customers/{code}`** (code + aliases + presence + data).
- Default TTL = **30 days** from creation. Make it configurable.
- Same job (or a lighter one) sweeps stale `logspace_presence` (older than the presence TTL).

### C.5 Auth note
Permanent create/edit/delete and any logspace hard-delete are **admin-only**. Disposable create/delete may be allowed for regular users per your policy. There is no auth in the app yet; define the guard now so the Manage UI can wire to it. Presence writes are per-user (self-declared name today).

---

## D. Exact target JSON the frontend will consume

When the above ships, the palette repoints to these shapes. Match field names exactly.

### D.1 Enriched `GET /api/v1/customers/log-spaces?include_inactive=<bool>`
```jsonc
{
  "count": 2,
  "log_spaces": [
    {
      "label": "amin",              // display name (existing)
      "customer_code": "mnp",       // existing
      "active": true,               // existing
      "kind": "disposable",         // NEW — "permanent" | "disposable"
      "created_at": "2026-06-01T00:00:00Z",  // NEW on this row (was join-only)

      // disposable-only (null/absent for permanent):
      "owner_name": "amin",         // NEW
      "expires_at": "2026-07-01T00:00:00Z",  // NEW

      // permanent-only (null/absent for disposable):
      "name": null,                 // NEW
      "description": null,          // NEW
      "environment": null,          // NEW — "live" | "test"
      "ingest_rate": null,          // NEW — e.g. "1.2k/min", or null
      "active_presence": []          // NEW — [{name, note?, since}]
    }
  ]
}
```

### D.2 Presence object
```jsonc
{ "id": "uuid", "customer_code": "mnp", "name": "amin", "note": "debugging 15656", "since": "2026-07-06T12:00:00Z" }
```

### D.3 Enriched `Customer` (GET /customers, GET /customers/{code})
Existing fields (A.1) **plus** `kind`, `name`, `description`, `environment`, `owner_name`, `expires_at`, `ingest_rate`, `active_presence[]`. Keep `created_at`, `display_names[]`, timezone fields unchanged.

> Frontend mapping for reference (so field semantics are unambiguous): permanent `status` = `environment` when `active`, else `"inactive"`. Disposable rows show `created_at` + `expires_at`; `owner_name` is optional.

---

## E. Verification checklist (run after implementing)

Do all of these against a dev DB. Replace host as needed.

**E.1 Migration / regression (existing behavior intact)**
- [ ] `GET /api/v1/customers?include_inactive=true` still returns `{count, customers[]}`, every customer has `created_at`, and now also `kind` (all existing = `"disposable"`).
- [ ] `GET /api/v1/customers/log-spaces?include_inactive=true` still returns `{count, log_spaces[]}` one row per display name, now with the D.1 fields.
- [ ] `GET /api/v1/customers/{code}` still returns `display_names[]`; `PATCH {active}` and `PATCH {timezone}` still work and return `Customer`.
- [ ] `POST /customers {customer_code, display_name}` still returns 201 (new) / 200 (attach) for the non-disposable path.

**E.2 Hard delete**
- [ ] `DELETE /api/v1/customers/{code}` → 204; the code, its `display_names`, and its presence rows are gone from the DB.
- [ ] The deleted code no longer appears in `GET /customers` or `/log-spaces` (any `include_inactive`).
- [ ] `DELETE` on a nonexistent code → 404.

**E.3 Disposable lifecycle**
- [ ] Create disposable `POST /customers {customer_code:"repro-x", display_name:"bug-1", kind:"disposable", owner_name:"amin"}` → 201; row has `kind=disposable`, `owner_name=amin`, `expires_at ≈ now+30d`.
- [ ] Creating a disposable against an existing code → 409.
- [ ] `/log-spaces` row for it shows `kind:"disposable"`, `owner_name`, `expires_at`, `created_at`.
- [ ] Force `expires_at` into the past, run the worker → the code + aliases + presence are hard-deleted.

**E.4 Permanent CRUD**
- [ ] `POST /customers {customer_code:"becwhlo", kind:"permanent", name:"BEC Wholesale", description:"...", environment:"live"}` → 201; appears with `kind:"permanent"`, `environment:"live"` in `/log-spaces`.
- [ ] `PATCH /customers/becwhlo {environment:"test"}` and `{description:"..."}` update and return the customer.
- [ ] `PATCH {active:false}` → row reported `active:false`; palette will render it as `INACTIVE`.
- [ ] `DELETE /customers/becwhlo` (admin) → 204, fully removed.
- [ ] Non-admin cannot create/edit/delete permanents (once auth exists).

**E.5 Presence**
- [ ] `POST /customers/{code}/presence {name:"amin", note:"x"}` → returns row with server `since`; a second POST with same name upserts (no duplicate).
- [ ] The code's `/log-spaces` row (or `/customers/{code}`) includes it in `active_presence`.
- [ ] `DELETE /customers/{code}/presence/{id}` → 204; it's removed.
- [ ] Presence older than the TTL is swept.

**E.6 Contract shape**
- [ ] Every field name matches Part D exactly (snake_case): `kind`, `environment`, `name`, `description`, `owner_name`, `expires_at`, `ingest_rate`, `active_presence`, and presence `{id, customer_code, name, note, since}`.
- [ ] All these endpoints ignore/reject the `X-Customer-Code` header (non-tenant-scoped).

---

## F. One-line summary of the split (so nothing is ambiguous)
- **Disposable** = a throwaway logspace that owns a **brand-new customer_code** (1:1). Create = `POST /customers` (kind=disposable, stamp owner + 30d expiry). Delete = **by customer_code**, ideally the new `DELETE /customers/{code}` hard purge (frontend uses `PATCH active:false` until that ships). Auto-deleted at `expires_at`.
- **Permanent** = an admin-curated logspace created via **Manage** with `name`, `description`, `environment` (live/test); inactivate via `active`, delete via `DELETE /customers/{code}` (admin). Carries `ingest_rate` + presence.
- **`inactive`** is always derived from `active=false`; it is not a value of `environment`.
