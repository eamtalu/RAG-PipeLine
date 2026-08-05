# Frontend: two changes from the daily-partitioning release

1. **The timezone picker can now be refused** - a real behaviour change, needs handling.
2. **Partition health on the AUTO-POLL card** - additive, one line.

Item 1 first, because it is the one that changes an existing flow.

---

# 1. `CustomerTimezonePanel` can now get a 409

`PATCH /customers/{code}` now REFUSES a timezone change once the tenant has ingested entries.

Why: `log_entries.timestamp` is derived at parse time using the customer's zone and is never
rewritten. Changing it splits the tenant's timeline - old entries keep the old derivation, new ones
get the new one - and because the entry-dedup key now includes the timestamp, re-ingesting an
already-loaded file inserts DUPLICATE rows instead of being skipped.

## What happens today, unchanged

`confirmChange` in `src/components/logspace/CustomerTimezonePanel.tsx` already catches the error and
toasts `LogsApiError.message`, and `fail()` in `customersApi.ts` already lifts `detail` into that
message. So **nothing crashes**. But the detail is a five-sentence paragraph, which reads badly in a
toast, and there is no way to proceed from the UI.

## What to change

**`customersApi.ts`** - let the caller opt in:

```ts
export async function setCustomerTimezone(
  code: string,
  timezone: string,
  opts?: { allowMixed?: boolean }
): Promise<Customer> {
  const qs = opts?.allowMixed ? "?allow_mixed_timezones=true" : "";
  const res = await resilientFetch(`${PREFIX}/${encodeURIComponent(code)}${qs}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ timezone }),
  });
  if (!res.ok) await fail(res);
  return (await res.json()) as Customer;
}
```

**`CustomerTimezonePanel.tsx`** - on a 409, do NOT toast. Keep the confirm modal open and show the
server's `detail` as the body, with two actions:

- **Purge log data first** (the safe route the message names) - or just close, if there is no purge
  affordance nearby.
- **Change anyway** - re-calls `setCustomerTimezone(code, tz, { allowMixed: true })`.

Branch on `err instanceof LogsApiError && err.status === 409`; every other error keeps the existing
toast path.

Do not paraphrase the reason - render `detail` as given. It names the exact zones, the consequence,
and the safe remedy, and it is the same text an operator hitting the API directly sees.

## Not affected

Setting a timezone on a tenant with **no entries** is unchanged - the normal post-creation flow never
sees a 409. Nor does re-selecting the zone the tenant already has.

---

# 2. Partition health on the AUTO-POLL card

One line to add.
`GET /api/v1/logs/regroup/status` — the endpoint `RegroupContext` already polls — now returns an extra `partitions` block.
No new request, no new polling, no change to anything already on the card.

## What the API returns now

```json
{
  "customer_code": "mnp",
  "pending": false,
  "pending_windows": 0,
  "oldest_pending_at": null,
  "last_regroup_at": null,
  "abandoned_windows": 0,
  "backing_off_windows": 0,
  "next_retry_at": null,
  "up_to_date": true,

  "partitions": {
    "days_ahead": 14,
    "oldest_day": "2026-05-19",
    "newest_day": "2026-08-19",
    "retention_days": 60,
    "default_partition_rows": 0,
    "healthy": true
  }
}
```

Every existing field is unchanged.

## 1. Type change — `logsApi.ts`

Make it optional and nullable, so nothing breaks against a backend that predates this or one whose catalogue read failed.

```ts
export interface PartitionStatus {
  days_ahead: number;
  oldest_day: string | null;
  newest_day: string | null;
  retention_days: number;
  default_partition_rows: number;
  healthy: boolean;
}

export interface RegroupStatus {
  // ...existing fields unchanged...
  partitions?: PartitionStatus | null;
}
```

`null` means the backend could not read partition health this poll.
`undefined` means an older backend.
Render nothing in both cases — the distinction only matters when someone is debugging.

## 2. One line — `PollingStatus.tsx`

Beneath the existing `pstat-sub`:

```tsx
{status?.partitions && (
  <div className={status.partitions.healthy ? "pstat-sub" : "pstat-warn"}>
    {status.partitions.healthy
      ? `storage ready ${status.partitions.days_ahead}d ahead`
      : `⚠ storage partitions only ${status.partitions.days_ahead}d ahead — ingestion stops when this reaches 0`}
  </div>
)}
```

Rendered, healthy:

> 🟢 **Up to date**
> last updated 8/6/2026, 8:27:38 PM
> 2 servers auto-polling · last poll 8/6/2026, 8:14:11 PM
> storage ready 14d ahead

## Do not compute `healthy` in the frontend

It is deliberately server-side.
The threshold has to match the partition worker's own CRITICAL alarm (`log_partition_min_runway_days`), and a threshold baked into a React component cannot be changed without a deploy.
The two would drift, and the card would show green while the worker was paging.

Use `healthy` as given. `days_ahead` is for display only.

## What the numbers mean, if you are ever debugging from the card

| Field | Meaning |
| --- | --- |
| `days_ahead` | Days of partitions provisioned ahead of today. **At 0, ingestion stops** — inserts fail outright. This is the one that pages someone. |
| `default_partition_rows` | Entries whose timestamp would not parse. Growth means the parser is silently failing on some log format; nothing else reports it. |
| `oldest_day` | Whether retention is actually running, or only the create half of the worker still works. |
| `retention_days` | The configured policy, echoed so the two can be compared. |

## Note

`partitions` is **global**, not per-tenant — partitions are cut per day, not per customer, so every log space shows the same block.
It is on this tenant-scoped endpoint only because the card already polls it.
Do not present it as a property of the selected log space; it is infrastructure health.
