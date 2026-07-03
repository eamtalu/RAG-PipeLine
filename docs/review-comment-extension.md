# Backend handoff — persist the Review rail (comments, notes, replies, annotations, hyperlinks, resolve, Matrix pins)

**Audience:** the FastAPI backend that serves `/api/v1/logs/*` (the LAN box the frontend proxies to via `next.config.mjs`).
**Goal:** durably persist everything the Review rail produces, so that after a page reload / re-open of a saved view, the full thread — general comments, line-anchored notes/annotations, threaded replies, resolved state, line-reference hyperlinks, quotes, and Matrix-pinned findings — comes back exactly as saved.

The frontend already implements the whole feature **optimistically in-session**. It is backward-compatible: it sends new fields and tolerates a backend that drops them. Once you implement the contract below, persistence becomes real and survives reload. **No frontend changes are required for this to work** (one optional cleanup is noted at the end).

---

## 0. Mental model (read this first)

- A **SavedView** (a.k.a. "snapshot"/"saved analysis") already exists and already persists the view *state* (filters, expanded rows, scroll anchor, search). That part is done; **do not change it**.
- The Review thread is a **collection of comments attached to a SavedView**. It is **already embedded** in every SavedView response as `comments: SavedViewComment[]`. Your job is to (a) extend that comment model with new fields, (b) add reply + resolve sub-resources, and (c) keep embedding the full thread in SavedView reads.
- **Persistence is incremental, not save-button-driven.** Each comment / reply / resolve is written the moment it is created via its own endpoint — there is no "bundle the whole thread into the save PATCH" step. "Everything is saved" because every mutation is a durable child row and every SavedView read returns the whole tree. This is deliberate: it avoids lost updates between concurrent reviewers and matches the frontend's optimistic-append design.
- A **note / annotation is simply a comment with a non-null `anchor`** (a line id). A **general comment** has `anchor = null`. There is no separate "notes" table or type — the frontend derives the two lists from the anchor. Do not model notes separately.

### What is persisted vs transient
| Persisted (your job) | Transient (never sent to you) |
|---|---|
| comment `body`, `author`, `created_at` | composer draft text |
| `anchor` (line id → makes it a note) | active tab / rail open state |
| `refs` (line-id hyperlinks in a comment) | text-selection popover |
| `quote` (quoted log snippet) | reply-box open/closed |
| `resolved` flag | hover state |
| `replies[]` (threaded) | |
| `source` (`user` \| `matrix`) | |

---

## 1. Exact data contract (authoritative — copy field names verbatim)

The frontend types live in `src/lib/savedViewsApi.ts`. Match them **exactly** (snake_case on the wire where shown; the frontend already uses `created_at`, `customer_code`, etc.).

```ts
type LineId = string;                    // "${transactionId}#${bodyLineIndex}"  e.g. "TX-1042#3"

interface CommentQuote {
  text: string;                          // quoted snippet (already truncated client-side to <=64 chars + "…")
  lineId: LineId;                        // NOTE: camelCase 'lineId' inside the quote object (see §7 casing note)
}

interface CommentReply {
  id: string;
  author: string;
  body: string;
  created_at: string;                    // ISO-8601 UTC
}

interface SavedViewComment {             // EXTENDED — new fields are optional + default-safe
  id: string;
  author: string;
  body: string;
  created_at: string;                    // ISO-8601 UTC
  anchor?: LineId | null;                // null/absent → general comment; set → line-anchored note/annotation
  refs?: LineId[];                       // default []  — jump-to-line hyperlink chips embedded in the comment
  quote?: CommentQuote | null;           // default null
  resolved?: boolean;                    // default false
  replies?: CommentReply[];              // default []
  source?: "user" | "matrix";            // default "user" — "matrix" marks a pinned assistant finding
}

interface SavedView {                    // UNCHANGED except `comments` now carries the richer objects
  id: string;
  customer_code: string;
  name: string;
  title?: string | null;
  notes?: string | null;
  saved_by?: string | null;
  assignee?: string | null;
  status: "open" | "in_progress" | "due" | "completed";
  due_date?: string | null;
  comments: SavedViewComment[];          // ← full thread embedded here, every read
  closure?: { summary?: string | null; closed_by?: string | null; closed_at: string } | null;
  created_at: string;
  updated_at: string;
  state: { /* schemaVersion, filters, collapsed, expandedTxnIds, scrollAnchorTxnId, scrollOffset?, scrollTop, search, activeMatch */ };
}
```

`LineId` is an **opaque client identifier**. Do **not** validate it against real log lines or try to resolve it — store and echo it as a string. (It is `"${transactionId}#${bodyLineIndex}"`, where `bodyLineIndex` is the line's index inside the transaction block as the frontend parses the feed text.)

### Wire calls the frontend makes today (`src/lib/savedViewsApi.ts`)
All under the tenant-scoped prefix `/api/v1/logs`, all carrying header `X-Customer-Code: <active customer>`, all expecting FastAPI `{ "detail": "..." }` on error.

```
POST   /api/v1/logs/saved-views/{id}/comments
       body: { author, body, anchor?, refs?, quote?, source? }
       → 200/201  SavedViewComment            (server sets id + created_at; fills defaults)

POST   /api/v1/logs/saved-views/{id}/comments/{cid}/replies      ← NEW
       body: { author, body }
       → 200/201  CommentReply                (server sets id + created_at)

PATCH  /api/v1/logs/saved-views/{id}/comments/{cid}              ← NEW
       body: { resolved: boolean }
       → 200      SavedViewComment            (the updated comment, full object)

DELETE /api/v1/logs/saved-views/{id}/comments/{cid}             ← OPTIONAL (frontend does not call it yet)
       → 204
```

Plus one additive change to an existing endpoint:
```
POST   /api/v1/logs/debug/ask
       body: { question }
       → { answer, tool_calls?, refs?: LineId[] }               ← ADD `refs`
```

---

## 2. Cross-cutting rules (apply to every endpoint below)

1. **Tenant isolation.** A SavedView belongs to a `customer_code`. Every comment/reply/resolve call must confirm the target SavedView belongs to the `X-Customer-Code` on the request; if not, return **404** (not 403 — do not leak existence across tenants). This mirrors the existing `getSavedView`/`updateSavedView` behavior (they pass `tenantSensitive404: false`, i.e. a real not-found is a genuine 404 and must NOT bounce the tenant).
2. **Completed-lock.** If the SavedView `status == "completed"`, reject comment create / reply / resolve with **409 Conflict** (`{"detail": "snapshot is completed — comments are locked"}`). The frontend already hides the composer for completed snapshots, but enforce server-side too.
3. **Server-authoritative fields.** `id` and `created_at` are always generated by the server and ignored if sent by the client. `author` is client-supplied free text (there is no auth; it is the active logspace label, defaulting to `"anonymous"`) — store it as-is.
4. **Defaults on write.** Missing `anchor`→`null`, `refs`→`[]`, `quote`→`null`, `resolved`→`false`, `replies`→`[]`, `source`→`"user"`.
5. **Non-empty guard.** A comment is valid if **at least one** of `body`, `anchor`, `refs` (non-empty), or `quote` is present (matches the frontend submit guard — an anchored note may have empty body). A reply requires non-empty `body`. Return **422** otherwise.
6. **`source` enum.** Accept only `"user"` | `"matrix"`; default `"user"`; reject others with 422.
7. **Ordering.** Return `comments` and `replies` in **ascending `created_at`** (stable insertion order) so the thread reads top-to-bottom like the UI expects.
8. **`updated_at` bump.** Any comment/reply/resolve mutation should bump the parent SavedView's `updated_at` (so list sorting/caching reflects activity). Do not change `created_at`.

---

## 3. Prompt 1 — Schema / migration

> **Task:** Extend the persistence layer to store the Review thread as durable child rows of a saved view. Follow this repo's existing ORM + migration conventions (same as the `saved_views` model/table). Additive and backward-compatible only.
>
> **Model A — `saved_view_comments`** (child of `saved_views`):
> - `id` — primary key, server-generated (same id strategy as existing tables: UUID or your standard).
> - `saved_view_id` — FK → `saved_views.id`, `ON DELETE CASCADE`, indexed.
> - `author` — text, not null.
> - `body` — text, not null (may be empty string when the comment is a pure anchor/quote note).
> - `anchor` — text, nullable (a `LineId`; null → general comment).
> - `refs` — JSON/JSONB array of text, not null, default `[]`.
> - `quote` — JSON/JSONB object `{ "text": str, "lineId": str }`, nullable, default null.
> - `resolved` — boolean, not null, default `false`.
> - `source` — text/enum in (`user`,`matrix`), not null, default `user`.
> - `created_at` — timestamptz, not null, server default now().
> - `updated_at` — timestamptz, not null (for resolve toggles).
> - Index on `(saved_view_id, created_at)` for ordered fetch.
>
> **Model B — `saved_view_comment_replies`** (child of `saved_view_comments`):
> - `id` — primary key, server-generated.
> - `comment_id` — FK → `saved_view_comments.id`, `ON DELETE CASCADE`, indexed.
> - `author` — text, not null.
> - `body` — text, not null.
> - `created_at` — timestamptz, not null, server default now().
> - Index on `(comment_id, created_at)`.
>
> Write a forward migration that creates both tables. **Existing saved views must keep working** — no data backfill needed; a view simply has zero comment rows until one is added.
>
> **Storage note:** `refs` and `quote` are stored as JSON columns (not normalized) because they are small, always read/written whole with the comment, and are opaque client identifiers. Replies ARE normalized (their own table) because they grow unbounded and are appended independently.

---

## 4. Prompt 2 — Request/response schemas (Pydantic)

> **Task:** Add Pydantic models mirroring the frontend contract in §1. Use the repo's existing schema style and JSON encoders (ISO-8601 UTC for datetimes).
>
> - `QuoteSchema` → `{ text: str, lineId: str }` — **keep the key `lineId` (camelCase)** inside quote; the frontend sends and reads it camelCase. (Everything else on the wire is snake_case: `created_at`, `customer_code`.)
> - `ReplySchema` (response) → `{ id, author, body, created_at }`.
> - `CommentCreateSchema` (request body for POST comments) → `{ author: str, body: str = "", anchor: str | None = None, refs: list[str] = [], quote: QuoteSchema | None = None, source: Literal["user","matrix"] = "user" }`.
> - `ReplyCreateSchema` (request) → `{ author: str, body: str }`.
> - `CommentResolveSchema` (request for PATCH) → `{ resolved: bool }`.
> - `CommentSchema` (response) → the full `SavedViewComment` shape incl. nested `replies: list[ReplySchema]`.
> - Ensure the existing `SavedView` response schema embeds `comments: list[CommentSchema]` (it already has a `comments` field — enrich it).
>
> Apply the §2 non-empty guard as a validator on `CommentCreateSchema` (at least one of body/anchor/refs/quote).

---

## 5. Prompt 3 — POST comment (extend existing endpoint)

> **Task:** Extend `POST /api/v1/logs/saved-views/{id}/comments` to accept and persist the new fields.
> - Resolve the SavedView by `id` **scoped to `X-Customer-Code`**; 404 if not found for this tenant.
> - Enforce the completed-lock (§2.2) → 409.
> - Validate body per `CommentCreateSchema` (§2.5) → 422.
> - Insert a `saved_view_comments` row with server-generated `id` + `created_at`, applying defaults (§2.4). Store `anchor`, `refs`, `quote`, `source` verbatim. `resolved=false`, no replies yet.
> - Bump the parent SavedView `updated_at`.
> - Return the full `CommentSchema` (id, author, body, created_at, anchor, refs, quote, resolved=false, replies=[], source). The frontend reconciles this echo over its optimistic row by id, so **return every field** — a partial echo makes fields flicker to defaults.

---

## 6. Prompt 4 — POST reply (new endpoint)

> **Task:** Implement `POST /api/v1/logs/saved-views/{id}/comments/{cid}/replies`.
> - Scope the SavedView to the tenant (404 if wrong tenant / missing). Confirm `cid` belongs to that SavedView (404 otherwise).
> - Completed-lock (§2.2) → 409.
> - Require non-empty `body` (§2.5) → 422.
> - Insert a `saved_view_comment_replies` row (server `id` + `created_at`). Bump parent SavedView `updated_at`.
> - Return the `ReplySchema` `{ id, author, body, created_at }`.
>
> **This is the endpoint whose absence currently causes replies to 404.** Until it exists the frontend keeps replies only in-session (it now deliberately does NOT roll them back). Once this ships, replies persist and reload-survive.

---

## 7. Prompt 5 — PATCH resolve (new endpoint)

> **Task:** Implement `PATCH /api/v1/logs/saved-views/{id}/comments/{cid}` accepting `{ resolved: bool }`.
> - Tenant-scope + locate the comment under the SavedView (404 otherwise).
> - Completed-lock (§2.2) → 409.
> - Set `resolved` to the provided value, bump the comment's `updated_at` and the SavedView's `updated_at`.
> - Return the **full updated `CommentSchema`** (not just the flag).
>
> Keep the verb generic (a PATCH that today only touches `resolved`) so future editable fields (e.g. body edit) can extend the same route. Ignore unknown fields rather than erroring, or 422 on them — pick the repo's convention; the frontend only ever sends `{ resolved }`.

---

## 8. Prompt 6 — Embed the full thread in every SavedView read

> **Task:** Ensure `GET /api/v1/logs/saved-views` (list) and `GET /api/v1/logs/saved-views/{id}` (single) both return each view's `comments` as an array of full `CommentSchema` objects, each with its nested `replies` (ascending `created_at`), including `anchor/refs/quote/resolved/source`. Eager-load to avoid N+1. This is what makes the thread survive reload and appear when a shared `/?view={id}` link is opened.
>
> **Regression check:** existing SavedView consumers already read `comments`; adding fields is additive. Confirm views with zero comments still return `comments: []`.

---

## 9. Prompt 7 — `refs` on `POST /debug/ask` (progressive enhancement)

> **Task:** Add an optional `refs: string[]` to the `POST /api/v1/logs/debug/ask` response, listing the `LineId`s the answer cites so the frontend can render jump-to-line chips and "pin as note".
> - A `LineId` is `"${transactionId}#${bodyLineIndex}"`, where `bodyLineIndex` is the zero-based index of the line **within that transaction's rendered block body** (the same text the feed renders). If the assistant references a step, map it to the transaction id + that step line's body index.
> - If you cannot reliably compute `bodyLineIndex`, **omit `refs` or return `[]`** — do not guess. The frontend already derives chips client-side from method names present in the loaded feed as a fallback, and prefers real backend `refs` when present. So this is safe to ship last / incrementally.

---

## 10. Prompt 8 — Tests

> **Task:** Add API tests (follow the repo's test framework) covering:
> - Create general comment (anchor null) → persisted, echoed with defaults; appears embedded in the SavedView GET.
> - Create anchored note (anchor set, empty body, with a quote) → persisted; passes the non-empty guard via anchor/quote.
> - Create comment with `refs` and `source: "matrix"` → round-trips verbatim.
> - Add reply → persisted, nested under the comment, ordered by created_at; survives a re-GET.
> - PATCH resolve true then false → flips and persists; returns the full comment.
> - Completed snapshot → comment/reply/resolve all 409.
> - Cross-tenant (`X-Customer-Code` mismatch) → 404 for comment/reply/resolve.
> - Empty comment (no body/anchor/refs/quote) → 422; empty reply body → 422.
> - SavedView with a mixed thread round-trips fully through list + single GET (eager-loaded, no N+1).

---

## 11. Acceptance criteria (definition of done)

1. Add a general comment, an anchored note (with quote + refs), a reply, and toggle resolve — then **reload the page / reopen the saved view**: the entire thread returns identical, notes still anchored to their lines, replies nested, resolved state intact, Matrix-pinned items marked `source: "matrix"`.
2. All new fields survive a full round-trip through the SavedView list + single GET.
3. Completed snapshots reject mutations (409); cross-tenant access 404s.
4. `POST /debug/ask` returns `refs` when computable (or `[]`), and the Matrix chips light up on real data.
5. Existing saved-view create/save/complete, filters, scroll-restore, and feed behavior are unaffected.

---

## 12. Frontend coordination notes (for whoever wires the two sides)

- **No frontend change is required** for persistence to start working — the client already sends every field and embeds/reads `comments` from the SavedView. Once the backend persists + embeds them, reload durability is automatic.
- **One optional cleanup once the backend is live:** the client currently keeps reply/resolve **locally authoritative** and does **not** refetch after comment mutations, specifically because an un-extended backend would clobber the new fields (see `src/hooks/useSavedViews.ts` — `addReply`/`setCommentResolved` swallow the error and keep optimistic state; `useSavedViews` intentionally skips a post-mutation refresh). After the backend fully implements + embeds the contract, you may:
  - re-enable a post-mutation refetch (server truth now matches), and
  - let reply/resolve surface genuine (non-404) errors again instead of always keeping optimistic state.
  These are optional hardening steps, not blockers.
- **Casing gotcha:** everything on the wire is snake_case **except** the `lineId` key **inside** the `quote` object, which the client sends/reads camelCase. Keep it camelCase there.
- **Author identity:** no auth; `author` is the active logspace label (`activeLabel || "anonymous"`), sent per request. Store verbatim; do not attempt to authenticate or dedupe.

---

### Reference — files that define the contract (in this frontend repo)
- `src/lib/savedViewsApi.ts` — types + the exact fetch calls (`addComment`, `addReply`, `setCommentResolved`, `SavedViewComment`, `CommentReply`, `CommentQuote`, `AddCommentInput`).
- `src/hooks/useSavedViews.ts` — optimistic behavior + the "locally authoritative until backend ships" comments.
- `src/lib/logsApi.ts` — `logsFetch` (X-Customer-Code injection, error typing), `debugAsk`, `DebugAnswer.refs`.
- `next.config.mjs` — the `/api/v1/logs/:path*` → FastAPI proxy.
- `src/contexts/LogReviewContext.tsx` — how anchor/refs/quote/source/replies are produced by the UI.
