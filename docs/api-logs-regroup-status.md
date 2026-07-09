# API spec - `GET /logs/regroup/status`

Low-level contract for the frontend "is the log server up to date?" widget.
This endpoint reports the Stage 2 (log entries → log transactions) stitching status for one tenant.

## Endpoint

```
GET /api/v1/logs/regroup/status
```

- Method: `GET`.
- Idempotent, read-only. It performs no writes and is safe to poll frequently.
- Cost: one indexed query over the small `log_regroup_pending` table. Cheap.

## Authentication / tenancy

Tenant is resolved from a required request header.

| Header | Required | Format | Notes |
|---|---|---|---|
| `X-Customer-Code` | yes | `^[a-z0-9][a-z0-9_-]{0,63}$` | Trimmed and lower-cased server-side; must be a registered customer. |

The tenant does NOT need to be "active" to read status (a paused tenant can still be inspected).

## Request

No query parameters. No request body.

```
GET /api/v1/logs/regroup/status
X-Customer-Code: tmp-live
```

## Response `200 OK`

`application/json`:

```json
{
  "customer_code": "tmp-live",
  "pending": false,
  "pending_windows": 0,
  "oldest_pending_at": null,
  "last_regroup_at": "2026-07-08T22:41:07.512000+00:00",
  "up_to_date": true
}
```

### Fields

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `customer_code` | string | no | The resolved (normalized) tenant slug, echoing the header. |
| `up_to_date` | boolean | no | **Primary "is it current?" signal.** `true` when there is no stitching backlog (`pending_windows == 0`), i.e. log transactions are current with everything ingested so far. |
| `pending_windows` | integer (>= 0) | no | **Backlog size.** Number of ingested time windows not yet stitched into transactions. `0` means fully caught up. |
| `oldest_pending_at` | string (ISO 8601, UTC) | yes | When the oldest un-stitched window was queued. `null` exactly when `pending_windows == 0`. Use it to show "catching up since ...". |
| `last_regroup_at` | string (ISO 8601, UTC) | yes | When a window was last stitched (the server last populated transactions). `null` only if nothing has ever been stitched for this tenant. |
| `pending` | boolean | no | Legacy alias, kept for backward compatibility. Always equals `!up_to_date` (i.e. `pending_windows > 0`). Prefer `up_to_date` in new code. |

### Invariants (safe to rely on)

- `pending === !up_to_date === (pending_windows > 0)`.
- `oldest_pending_at` is non-null if and only if `pending_windows > 0`.
- All timestamps are ISO 8601 with a UTC offset (`+00:00`). Convert to the user's locale client-side.

## Error responses

| Status | When | Body `detail` |
|---|---|---|
| `422` | `X-Customer-Code` header missing | FastAPI validation error (field `header -> x-customer-code`, "field required"). |
| `400` | Header present but malformed | `"Invalid X-Customer-Code (expected a slug like 'acme')."` |
| `404` | Header valid but tenant not registered | `"Unknown customer: '<code>'. Create its log space first (POST /api/v1/customers)."` |

## Rendering guidance

Two states drive the widget:

- `up_to_date === true` → green / "Up to date". Optionally show "last updated {last_regroup_at}".
- `up_to_date === false` → amber / "Catching up - {pending_windows} window(s) pending" and, if `oldest_pending_at` is set, "since {oldest_pending_at}".

This endpoint reports the STITCHING backlog only. It does not say whether ingestion (the SSH poller) is alive.
For a complete "log server health" view, combine it with each source's `status` / `last_ok_at` from `GET /api/v1/ssh-sources`:

- ingestion live AND `up_to_date` → fully healthy and current;
- ingestion live AND not `up_to_date` → healthy, catching up (transient, e.g. after a burst or a first backfill);
- ingestion stale/broken → surface the source status regardless of `up_to_date`.

## Polling

- Poll every 10-30 s while the status widget is visible. The endpoint is read-only and cheap.
- No caching headers are set; the frontend should treat each response as a point-in-time snapshot.

## Behavioral note (large one-time recovery)

In normal incremental operation each poll stitches a tiny window and marks it done immediately, so
`last_regroup_at` advances every cycle and `up_to_date` is `true` between polls.

During a large one-time recovery (for example a first backfill of weeks of history, or draining a
backlog), the whole span is processed as a single run that is marked consumed only when it finishes.
While that run is in progress, `pending_windows` and `last_regroup_at` can stay unchanged for several
minutes even though transactions are actively being created in the background, and `up_to_date` stays
`false` until it completes. This is expected. Treat `up_to_date === false` as "catching up"; do not
infer "stuck" from an unchanging `last_regroup_at` alone.

## Examples

Up to date (no backlog):

```bash
curl -s http://<host>/api/v1/logs/regroup/status -H 'X-Customer-Code: tmp-test'
# { "customer_code":"tmp-test","pending":false,"pending_windows":0,
#   "oldest_pending_at":null,"last_regroup_at":"2026-07-08T09:45:30.926880+00:00","up_to_date":true }
```

Catching up (backlog present):

```bash
curl -s http://<host>/api/v1/logs/regroup/status -H 'X-Customer-Code: tmp-live'
# { "customer_code":"tmp-live","pending":true,"pending_windows":450,
#   "oldest_pending_at":"2026-07-08T11:10:39.261252+00:00",
#   "last_regroup_at":"2026-07-08T11:11:24.778404+00:00","up_to_date":false }
```

## TypeScript type

```ts
interface RegroupStatus {
  customer_code: string;
  up_to_date: boolean;            // no backlog -> transactions current
  pending_windows: number;        // backlog size (>= 0)
  oldest_pending_at: string | null; // ISO 8601 UTC; null iff pending_windows === 0
  last_regroup_at: string | null;   // ISO 8601 UTC; null only if never stitched
  pending: boolean;               // legacy: === !up_to_date
}
```
