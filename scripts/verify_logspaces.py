"""End-to-end verification of the Logspace palette backend (permanent vs disposable + presence).

Drives the real FastAPI app in-process (httpx ASGITransport, no server) against the live Postgres,
covering the backend spec's Part E:
  E.1 regression (existing shapes intact + new kind field)
  E.2 hard delete (full purge: registry + aliases + presence + seeded job/log/saved-view data)
  E.3 disposable lifecycle (create/409/fields + worker auto-expiry)
  E.4 permanent CRUD
  E.5 presence (upsert/dedupe/list/leave/stale-sweep)
  E.6 contract shape (field names) + non-tenant-scoped (ignores X-Customer-Code)

The customer-registry endpoints are NOT tenant-scoped, so no X-Customer-Code header is needed.

Run:  PYTHONPATH=. python scripts/verify_logspaces.py
Creates log spaces under codes prefixed 'vlz-' and purges them (plus any seeded data) at the end.
Exit code 0 = all pass.
"""

import asyncio
import sys
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, update, func

from app.main import app
from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.logspace_presence import LogspacePresence
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.saved_view import SavedView
from app.services.logspace_cleanup import purge_logspace
from app.services.workers.logspace_cleanup_worker import run_logspace_cleanup_once

BASE = "/api/v1/customers"
PREFIX = "vlz-"

_passed = 0
_failed = 0
_codes: set[str] = set()


def check(cond: bool, label: str, extra: str = "") -> None:
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"  PASS  {label}")
    else:
        _failed += 1
        print(f"  FAIL  {label}   {extra}")


def track(code: str) -> str:
    _codes.add(code)
    return code


# D.1/D.3 field set every enriched customer/log-space row must carry.
_ENRICHED_FIELDS = {"kind", "owner_name", "expires_at", "name", "description", "environment",
                    "ingest_rate", "active_presence"}


async def _seed_purge_targets(code: str) -> None:
    """Insert a job + log entry + saved view under `code` so E.2 can prove the full purge removes them."""
    async with async_session() as s:
        job = Job(customer_code=code, filename="verify.log", storage_key="verify/verify.log")
        s.add(job)
        await s.flush()
        s.add(LogEntry(job_id=job.id, customer_code=code, source_file="verify.log"))
        s.add(SavedView(customer_code=code, name="vlz-view", status="open", state={"x": 1}))
        await s.commit()


async def _count_rows(code: str) -> dict:
    async with async_session() as s:
        return {
            "customers": await s.scalar(select(func.count()).select_from(Customer).where(Customer.customer_code == code)),
            "jobs": await s.scalar(select(func.count()).select_from(Job).where(Job.customer_code == code)),
            "log_entries": await s.scalar(select(func.count()).select_from(LogEntry).where(LogEntry.customer_code == code)),
            "saved_views": await s.scalar(select(func.count()).select_from(SavedView).where(SavedView.customer_code == code)),
            "presence": await s.scalar(select(func.count()).select_from(LogspacePresence).where(LogspacePresence.customer_code == code)),
        }


async def main() -> int:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:

        # ============================================================ E.1 regression
        print("\n[E.1 regression — existing shapes intact + new kind field]")
        # legacy create (no kind) → 201 and reads back kind='disposable'
        legacy = track(PREFIX + "legacy-1")
        r = await c.post(BASE, json={"customer_code": legacy, "display_name": "amin"})
        check(r.status_code == 201, "legacy POST (no kind) → 201", f"{r.status_code} {r.text}")
        body = r.json()
        check(body["kind"] == "disposable", "legacy create defaults kind=disposable", str(body.get("kind")))
        check(body["expires_at"] is None, "legacy create has no expiry (not auto-purged)", str(body.get("expires_at")))
        check("display_names" in body and body["display_names"][0]["display_name"] == "amin",
              "display_names[] still present on create")
        check(_ENRICHED_FIELDS.issubset(body.keys()), "create response carries all enriched fields",
              str(_ENRICHED_FIELDS - set(body.keys())))

        # legacy attach (existing code, no kind) → 200
        r = await c.post(BASE, json={"customer_code": legacy, "display_name": "amin-2"})
        check(r.status_code == 200, "legacy attach existing code → 200", f"{r.status_code} {r.text}")

        # list still {count, customers[]} with created_at + kind on every row
        r = await c.get(BASE, params={"include_inactive": "true"})
        lst = r.json()
        check(r.status_code == 200 and "count" in lst and isinstance(lst["customers"], list),
              "GET /customers → {count, customers[]}")
        check(all(x.get("created_at") for x in lst["customers"]), "every customer has created_at")
        check(all(x.get("kind") in ("disposable", "permanent") for x in lst["customers"]),
              "every customer has a valid kind")

        # log-spaces still one row per display name, now with D.1 fields
        r = await c.get(BASE + "/log-spaces", params={"include_inactive": "true"})
        ls = r.json()
        check(r.status_code == 200 and "log_spaces" in ls, "GET /log-spaces → {count, log_spaces[]}")
        legacy_rows = [x for x in ls["log_spaces"] if x["customer_code"] == legacy]
        check(len(legacy_rows) == 2, "one log-space row per display name (2 aliases)", str(len(legacy_rows)))
        check(all(_ENRICHED_FIELDS.issubset(x.keys()) for x in legacy_rows),
              "log-space rows carry all D.1 enriched fields")

        # GET /{code} still returns display_names[]; PATCH {timezone}/{active} still work
        r = await c.get(f"{BASE}/{legacy}")
        check(r.status_code == 200 and len(r.json()["display_names"]) == 2, "GET /{code} display_names[]")
        r = await c.patch(f"{BASE}/{legacy}", json={"timezone": "Europe/Berlin"})
        check(r.status_code == 200 and r.json()["timezone"] == "Europe/Berlin", "PATCH {timezone} works")
        r = await c.patch(f"{BASE}/{legacy}", json={"active": False})
        check(r.status_code == 200 and r.json()["active"] is False, "PATCH {active:false} works")
        r = await c.patch(f"{BASE}/{legacy}", json={})
        check(r.status_code == 400, "PATCH with nothing to update → 400", f"{r.status_code}")

        # ============================================================ E.3 disposable lifecycle
        print("\n[E.3 disposable lifecycle]")
        disp = track(PREFIX + "disp-1")
        r = await c.post(BASE, json={"customer_code": disp, "display_name": "bug-1",
                                     "kind": "disposable", "owner_name": "amin"})
        check(r.status_code == 201, "disposable create → 201", f"{r.status_code} {r.text}")
        d = r.json()
        check(d["kind"] == "disposable" and d["owner_name"] == "amin", "kind/owner_name stamped", str(d))
        exp = datetime.fromisoformat(d["expires_at"])
        target = datetime.now(timezone.utc) + timedelta(days=30)
        check(abs((exp - target).total_seconds()) < 3600, "expires_at ≈ now + 30d", d["expires_at"])
        # 409 on existing code
        r = await c.post(BASE, json={"customer_code": disp, "kind": "disposable"})
        check(r.status_code == 409, "disposable create on existing code → 409", f"{r.status_code}")
        # log-spaces row shows disposable fields
        r = await c.get(BASE + "/log-spaces", params={"include_inactive": "true"})
        row = next((x for x in r.json()["log_spaces"] if x["customer_code"] == disp), None)
        check(row and row["kind"] == "disposable" and row["owner_name"] == "amin" and row["expires_at"]
              and row["created_at"], "log-space row shows disposable fields", str(row))

        # worker auto-expiry: force expires_at into the past, run one cleanup pass → purged
        async with async_session() as s:
            await s.execute(update(Customer).where(Customer.customer_code == disp)
                            .values(expires_at=datetime.now(timezone.utc) - timedelta(days=1)))
            await s.commit()
        purged, _ = await run_logspace_cleanup_once()
        check(purged >= 1, "cleanup worker purged ≥1 expired disposable", f"purged={purged}")
        r = await c.get(f"{BASE}/{disp}")
        check(r.status_code == 404, "expired disposable is gone (404)", f"{r.status_code}")
        _codes.discard(disp)

        # ============================================================ E.4 permanent CRUD
        print("\n[E.4 permanent CRUD]")
        perm = track(PREFIX + "perm-1")
        r = await c.post(BASE, json={"customer_code": perm, "kind": "permanent", "name": "BEC Wholesale",
                                     "description": "wholesale ops", "environment": "live"})
        check(r.status_code == 201, "permanent create → 201", f"{r.status_code} {r.text}")
        p = r.json()
        check(p["kind"] == "permanent" and p["name"] == "BEC Wholesale" and p["environment"] == "live",
              "permanent fields stored", str(p))
        # missing required fields → 400
        r = await c.post(BASE, json={"customer_code": PREFIX + "perm-bad", "kind": "permanent"})
        check(r.status_code == 400, "permanent create missing name/environment → 400", f"{r.status_code}")
        # 409 on existing
        r = await c.post(BASE, json={"customer_code": perm, "kind": "permanent", "name": "x",
                                     "environment": "test"})
        check(r.status_code == 409, "permanent create on existing code → 409", f"{r.status_code}")
        # appears in log-spaces with environment
        r = await c.get(BASE + "/log-spaces", params={"include_inactive": "true"})
        prow = next((x for x in r.json()["log_spaces"] if x["customer_code"] == perm), None)
        check(prow and prow["kind"] == "permanent" and prow["environment"] == "live",
              "permanent shows in /log-spaces with environment", str(prow))
        # edit environment + description
        r = await c.patch(f"{BASE}/{perm}", json={"environment": "test", "description": "moved to test"})
        check(r.status_code == 200 and r.json()["environment"] == "test"
              and r.json()["description"] == "moved to test", "PATCH permanent fields", r.text[:200])
        # inactivate
        r = await c.patch(f"{BASE}/{perm}", json={"active": False})
        check(r.status_code == 200 and r.json()["active"] is False, "PATCH {active:false} on permanent")

        # ============================================================ E.5 presence
        print("\n[E.5 presence]")
        r = await c.post(f"{BASE}/{perm}/presence", json={"name": "amin", "note": "debugging 15656"})
        check(r.status_code == 200, "POST presence → 200", f"{r.status_code} {r.text}")
        pr = r.json()
        check(set(pr.keys()) == {"id", "customer_code", "name", "note", "since"},
              "presence object shape = {id, customer_code, name, note, since}", str(pr.keys()))
        check(pr["since"], "presence has server-set since", str(pr))
        pid = pr["id"]
        # upsert (same name) → no duplicate, since refreshed
        r = await c.post(f"{BASE}/{perm}/presence", json={"name": "amin", "note": "still here"})
        check(r.status_code == 200 and r.json()["id"] == pid and r.json()["note"] == "still here",
              "second POST same name upserts (same id, note refreshed)", r.text[:200])
        # appears in active_presence on GET /{code}
        r = await c.get(f"{BASE}/{perm}")
        ap = r.json()["active_presence"]
        check(len(ap) == 1 and ap[0]["name"] == "amin" and ap[0]["note"] == "still here",
              "presence in active_presence", str(ap))
        # leave → 204, removed
        r = await c.delete(f"{BASE}/{perm}/presence/{pid}")
        check(r.status_code == 204, "DELETE presence → 204", f"{r.status_code}")
        r = await c.get(f"{BASE}/{perm}")
        check(r.json()["active_presence"] == [], "presence removed after leave")
        # malformed presence id → 404 (not 422/500)
        r = await c.delete(f"{BASE}/{perm}/presence/not-a-uuid")
        check(r.status_code == 404, "DELETE malformed presence id → 404", f"{r.status_code}")
        # stale sweep: add presence, backdate `since`, run cleanup → swept + excluded from read
        await c.post(f"{BASE}/{perm}/presence", json={"name": "stale-user"})
        async with async_session() as s:
            await s.execute(update(LogspacePresence).where(LogspacePresence.customer_code == perm)
                            .values(since=datetime.now(timezone.utc) - timedelta(days=2)))
            await s.commit()
        r = await c.get(f"{BASE}/{perm}")
        check(r.json()["active_presence"] == [], "stale presence excluded from read (fresh_after filter)")
        _, swept = await run_logspace_cleanup_once()
        check(swept >= 1, "cleanup worker swept ≥1 stale presence row", f"swept={swept}")

        # ============================================================ E.2 hard delete (full purge)
        print("\n[E.2 hard delete — full tenant purge]")
        delcode = track(PREFIX + "del-1")
        await c.post(BASE, json={"customer_code": delcode, "display_name": "to-purge",
                                 "kind": "disposable", "owner_name": "amin"})
        await c.post(f"{BASE}/{delcode}/presence", json={"name": "amin"})
        await _seed_purge_targets(delcode)
        before = await _count_rows(delcode)
        check(all(before[k] >= 1 for k in ("customers", "jobs", "log_entries", "saved_views", "presence")),
              "seed present before delete", str(before))
        r = await c.delete(f"{BASE}/{delcode}")
        check(r.status_code == 204, "DELETE /{code} → 204", f"{r.status_code} {r.text}")
        after = await _count_rows(delcode)
        check(all(v == 0 for v in after.values()),
              "all rows purged (customer, aliases via cascade, presence, jobs, log_entries, saved_views)",
              str(after))
        r = await c.get(f"{BASE}/{delcode}")
        check(r.status_code == 404, "deleted code → 404 on GET")
        r = await c.get(BASE + "/log-spaces", params={"include_inactive": "true"})
        check(all(x["customer_code"] != delcode for x in r.json()["log_spaces"]),
              "deleted code absent from /log-spaces")
        r = await c.delete(f"{BASE}/{delcode}")
        check(r.status_code == 404, "DELETE nonexistent code → 404", f"{r.status_code}")
        _codes.discard(delcode)

        # ============================================================ E.6 contract / non-tenant-scoped
        print("\n[E.6 non-tenant-scoped — endpoints ignore X-Customer-Code]")
        r1 = await c.get(BASE, params={"include_inactive": "true"})
        r2 = await c.get(BASE, params={"include_inactive": "true"},
                         headers={"X-Customer-Code": "totally-bogus-tenant"})
        check(r2.status_code == 200 and r1.json()["count"] == r2.json()["count"],
              "GET /customers ignores X-Customer-Code header (same result)",
              f"{r1.status_code}/{r2.status_code}")

    # ---- cleanup: purge everything this run created ----
    async with async_session() as s:
        for code in list(_codes):
            await purge_logspace(s, code)

    print(f"\n==== {_passed} passed, {_failed} failed ====")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
