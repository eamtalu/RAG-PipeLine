# Frontend: partition health on the AUTO-POLL card

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
