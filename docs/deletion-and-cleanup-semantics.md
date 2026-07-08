# Deletion & Cleanup Semantics

Reference for what each "delete" action removes vs keeps. There are **two distinct delete actions**;
they are very different in blast radius. Verify against the code before relying on details.

- Single SSH server delete → `app/api/v1/log_sources.py::delete_ssh_source` + `LogSshSourceRepository.delete`.
- Permanent logspace delete → `app/services/logspace_cleanup.py::purge_logspace` (called by `DELETE /api/v1/customers/{code}` and the auto-expiry worker).

---

## 1. Delete a single SSH server — `DELETE /api/v1/logs/ssh-sources/{id}`

**Scope: the connection and its resume state only. Collected logs are kept.**

Removes:
- The `log_ssh_sources` row (host/port/user, auth, pinned fingerprint).
- Its `log_ssh_file_checkpoints` — deleted automatically by the FK `ON DELETE CASCADE` (`log_ssh_file_checkpoint.py`: `ForeignKey("log_ssh_sources.id", ondelete="CASCADE")`).

Keeps:
- **All ingested logs** (`log_entries` / `log_transactions`). They are keyed by `(customer_code, entry_hash)` and only carry the source as a text label (`log_entries.source_file`) — there is **no FK to the source**, so deleting the server does not touch the data it collected.
- **Fetch-run history** (`log_ssh_fetch_runs`). `source_id` has **no FK**, so past runs remain (now referencing a deleted source id). The history list still shows them.

Functional impact:
- **One of several servers:** the other servers are unaffected and keep polling; the customer's poll loop continues with the remaining sources.
- **The only / last enabled server:** the customer now has 0 enabled sources → the poller supervisor reaps that customer's loop within one reconcile tick (~`ssh_poll_reconcile_seconds`). Polling stops cleanly for that customer.
- **Mid-fetch delete:** graceful/isolated — `_record_success`/`_record_failure` do `db.get → None → no-op`; a stray checkpoint write hits the FK and is caught by per-source error isolation; a tracked run loads 0 sources and completes. No damage to other servers. (There is no auto-cancel of an in-flight run on delete.)

Gotcha:
- **Re-adding the same server** is treated as brand-new (its checkpoints cascaded away) → the first fetch re-transfers/backfills all current files (content dedup drops duplicate rows, but bytes are re-pulled). Re-onboard with a start point (`mode:"seed"` = from now, or `mode:"timestamp"`) to avoid it — same as a fresh add.

---

## 2. Permanent logspace delete — `DELETE /api/v1/customers/{code}` (`purge_logspace`)

**Scope: a complete, irreversible hard-purge of everything keyed to that `customer_code`.** This is a real purge, not a soft deactivate.

In one committed transaction it deletes, for the `customer_code`:
1. **All ingested log data** — `log_entries` and `log_transactions` (via `Job` FK `ON DELETE CASCADE`), plus `jobs`, `chunks`, `chunks_entity`, the embedding queue, and the pgvector `embeddings` rows (embeddings have no FK, so their ids are gathered from the tenant's chunks/entities and deleted explicitly first).
2. **All SSH connections** — `log_ssh_sources`.
3. **All checkpoints** — `log_ssh_file_checkpoints`.
4. **All fetch-run history** — `log_ssh_fetch_runs`.
5. **Regroup state** — `log_regroup_runs`, `log_regroup_pending`.
6. **Saved views & notifications** — `saved_views`, notification events / rules / channels.
7. **The logspace itself** — the `customers` registry row for that `customer_code`.

Functional impact:
- **The `customer_code` no longer exists.** Every subsequent API call carrying that `X-Customer-Code` returns **404** (all endpoints are gated by the customer-exists dependency).
- **Polling stops for the tenant** (no enabled sources remain → its poll loop is reaped next reconcile tick).
- **In-flight fetches during a purge** degrade gracefully (source/customer gone → bookkeeping no-ops, stray writes fail in isolation, tracked runs load 0 sources and finish).
- **Irreversible** — nothing is recoverable. The frontend must gate this behind a strong confirmation ("permanently deletes all logs and servers for this space — cannot be undone").

---

## Contrast at a glance

| | Delete one SSH server | Delete logspace (purge) |
|---|---|---|
| SSH connection(s) | that one removed | all removed |
| Checkpoints | that source's removed (cascade) | all removed |
| Fetch-run history | **kept** (no FK) | all removed (by `customer_code`) |
| Ingested logs (`log_entries`/`log_transactions`) | **kept** | all removed (Job cascade) |
| Embeddings / chunks / jobs | untouched | all removed |
| Saved views / notifications | untouched | all removed |
| The logspace (`customers` row) | untouched | removed |
| Other servers of the tenant | untouched | n/a (tenant gone) |
| Reversible | n/a | no — hard purge |
