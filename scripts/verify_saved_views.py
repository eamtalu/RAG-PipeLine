"""End-to-end verification of the Saved Analyses ("Saved Views") API against the backend spec.

Drives the real FastAPI app in-process (httpx ASGITransport, no server) against the live Postgres,
exercising every endpoint AND the critical HTTP status-code contract (§0.1):
  - 422 only for a missing tenant header (never for body validation)
  - 400 for all body/field validation
  - tenant-sensitive 404 on list/create; by-id 404 on the rest
  - cross-tenant GET-by-id (share deep-link)
  - PATCH merge semantics (partial; comments untouched; created_at immutable; updated_at bumped)
  - never 409

Run:  PYTHONPATH=. python scripts/verify_saved_views.py
Creates and then deletes its own rows under existing tenants (mnp / asafe). Exit code 0 = all pass.
"""

import asyncio
import sys

import httpx

from app.main import app
from app.config.database import async_session
from app.persistence.models.saved_view import SavedView
from sqlalchemy import delete

TENANT = "mnp"          # must be an existing, active customer
OTHER_TENANT = "asafe"  # a different existing tenant, for the cross-tenant share test

_passed = 0
_failed = 0
_created_ids: list[str] = []


def check(cond: bool, label: str, extra: str = "") -> None:
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"  PASS  {label}")
    else:
        _failed += 1
        print(f"  FAIL  {label}   {extra}")


def hdr(t: str | None = TENANT) -> dict:
    return {"X-Customer-Code": t} if t is not None else {}


async def main() -> int:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        BASE = "/api/v1/logs/saved-views"

        sample_state = {
            "schemaVersion": 1,
            "filters": {"user": "amin", "status": "error", "limit": 50},
            "collapsed": False,
            "expandedTxnIds": ["TX1"],
            "scrollAnchorTxnId": "TX5",
            "scrollOffset": 18,
            "scrollTop": 420,
            "search": "timeout",
            "activeMatch": 2,
        }

        # ----------------------------------------------------------------- §2.2 Create
        print("\n[Create — POST /saved-views]")
        body = {
            "name": "analysis-mnp-checkout-2026-06-14-10-00-00",
            "title": "checkout", "notes": "first pass", "saved_by": "amin",
            "assignee": None, "status": "in_progress", "state": sample_state,
        }
        r = await c.post(BASE, json=body, headers=hdr())
        check(r.status_code == 201, "create → 201", f"got {r.status_code} {r.text}")
        v = r.json()
        _created_ids.append(v["id"])
        check(bool(v["id"]), "server-generated id present")
        check(v["customer_code"] == TENANT, "customer_code set from header")
        check(v["name"] == body["name"], "name stored verbatim")
        check(v["title"] == "checkout" and v["notes"] == "first pass", "title/notes stored")
        check(v["saved_by"] == "amin" and v["assignee"] is None, "saved_by/assignee stored")
        check(v["status"] == "in_progress", "status stored")
        check(v["comments"] == [], "comments == [] on create")
        check(v["closure"] is None, "closure null on create")
        check(v["due_date"] is None, "due_date null default")
        check(v["state"] == sample_state, "state round-tripped verbatim")
        check(v["created_at"] == v["updated_at"], "created_at == updated_at on create")
        check(v["created_at"].endswith("Z") and "T" in v["created_at"], "datetime is ISO-Z",
              v["created_at"])
        check(len(v["created_at"]) == len("2026-06-14T10:00:00.000Z"), "datetime has ms precision",
              v["created_at"])
        main_id = v["id"]

        # default status when omitted = "open"
        r = await c.post(BASE, json={"name": "n-default-status", "state": {"a": 1}}, headers=hdr())
        check(r.status_code == 201 and r.json()["status"] == "open", "status defaults to 'open'",
              f"{r.status_code} {r.text}")
        if r.status_code == 201:
            _created_ids.append(r.json()["id"])

        # ----------------------------------------------------------------- §0.1 status contract
        print("\n[Status-code contract — body validation must be 400, never 422]")
        r = await c.post(BASE, json={"state": sample_state}, headers=hdr())
        check(r.status_code == 400, "create missing name → 400", f"got {r.status_code}")
        check(r.json().get("detail"), "error body has .detail", r.text)

        r = await c.post(BASE, json={"name": "x"}, headers=hdr())
        check(r.status_code == 400, "create missing state → 400", f"got {r.status_code}")

        r = await c.post(BASE, json={"name": "", "state": {}}, headers=hdr())
        check(r.status_code == 400, "create empty name → 400", f"got {r.status_code}")

        r = await c.post(BASE, content=b"{not json", headers={**hdr(), "Content-Type": "application/json"})
        check(r.status_code == 400, "create bad JSON → 400", f"got {r.status_code}")

        r = await c.post(BASE, json={"name": "x", "state": {}, "status": "bogus"}, headers=hdr())
        check(r.status_code == 400, "create invalid status → 400", f"got {r.status_code}")

        # 422 reserved for missing tenant header
        print("\n[422 reserved for missing tenant header]")
        r = await c.get(BASE)
        check(r.status_code == 422, "list w/o X-Customer-Code → 422", f"got {r.status_code}")
        r = await c.post(BASE, json={"name": "x", "state": {}})
        check(r.status_code == 422, "create w/o X-Customer-Code → 422", f"got {r.status_code}")

        # tenant-sensitive 404 on list/create
        print("\n[Tenant-sensitive 404 on list/create]")
        r = await c.get(BASE, headers=hdr("no-such-tenant-zzz"))
        check(r.status_code == 404, "list unknown tenant → 404", f"got {r.status_code}")
        r = await c.post(BASE, json={"name": "x", "state": {}}, headers=hdr("no-such-tenant-zzz"))
        check(r.status_code == 404, "create unknown tenant → 404", f"got {r.status_code}")

        # ----------------------------------------------------------------- §2.1 List
        print("\n[List — GET /saved-views]")
        r = await c.get(BASE, headers=hdr())
        check(r.status_code == 200, "list → 200", f"got {r.status_code}")
        lst = r.json()
        check(isinstance(lst, list), "list returns a JSON array")
        # newest-first
        created_ats = [x["created_at"] for x in lst]
        check(created_ats == sorted(created_ats, reverse=True), "sorted by created_at desc")
        check(any(x["id"] == main_id for x in lst), "created view appears in list")
        first = next(x for x in lst if x["id"] == main_id)
        check("comments" in first and "state" in first, "list items fully hydrated")

        # status filter
        r = await c.get(BASE, params={"status": "in_progress"}, headers=hdr())
        check(r.status_code == 200 and all(x["status"] == "in_progress" for x in r.json()),
              "list ?status=in_progress filters", r.text[:200])
        r = await c.get(BASE, params={"status": "bogus"}, headers=hdr())
        check(r.status_code == 400, "list ?status=bogus → 400", f"got {r.status_code}")

        # ----------------------------------------------------------------- §2.6 Comments
        print("\n[Add comment — POST /saved-views/{id}/comments]")
        r = await c.post(f"{BASE}/{main_id}/comments", json={"author": "amin", "body": "looks off"},
                         headers=hdr())
        check(r.status_code == 201, "add comment → 201", f"got {r.status_code} {r.text}")
        cm = r.json()
        check(set(cm.keys()) == {"id", "author", "body", "created_at"},
              "comment response is the comment only", str(cm.keys()))
        check(cm["author"] == "amin" and cm["body"] == "looks off", "comment fields stored")

        # default author when absent/empty
        r = await c.post(f"{BASE}/{main_id}/comments", json={"body": "no author"}, headers=hdr())
        check(r.status_code == 201 and r.json()["author"] == "anonymous",
              "author defaults to 'anonymous'", r.text)
        r = await c.post(f"{BASE}/{main_id}/comments", json={"author": "  ", "body": "blank author"},
                         headers=hdr())
        check(r.status_code == 201 and r.json()["author"] == "anonymous",
              "blank author → 'anonymous'", r.text)

        # validation
        r = await c.post(f"{BASE}/{main_id}/comments", json={"author": "x"}, headers=hdr())
        check(r.status_code == 400, "comment missing body → 400", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{main_id}/comments", json={"body": ""}, headers=hdr())
        check(r.status_code == 400, "comment empty body → 400", f"got {r.status_code}")
        r = await c.post(f"{BASE}/00000000-0000-0000-0000-000000000000/comments",
                         json={"body": "x"}, headers=hdr())
        check(r.status_code == 404, "comment on missing view → 404", f"got {r.status_code}")

        # comments embedded in GET, and updated_at bumped
        r = await c.get(f"{BASE}/{main_id}", headers=hdr())
        gv = r.json()
        check(len(gv["comments"]) == 3, "3 comments embedded in GET", str(len(gv["comments"])))
        check(gv["updated_at"] > gv["created_at"], "updated_at bumped after add-comment",
              f"{gv['updated_at']} vs {gv['created_at']}")
        check(gv["created_at"] == v["created_at"], "created_at immutable after add-comment")

        # ----------------------------------------------------------------- §2.3 Get by id
        print("\n[Get one — GET /saved-views/{id}]")
        r = await c.get(f"{BASE}/{main_id}", headers=hdr())
        check(r.status_code == 200 and r.json()["id"] == main_id, "get by id → 200")
        r = await c.get(f"{BASE}/00000000-0000-0000-0000-000000000000", headers=hdr())
        check(r.status_code == 404, "get missing id → 404 (no bounce)", f"got {r.status_code}")
        # a malformed (non-UUID) id must be a by-id 404 — NEVER 422 (which would bounce the user)
        r = await c.get(f"{BASE}/not-a-uuid", headers=hdr())
        check(r.status_code == 404, "get malformed id → 404 (NOT 422)", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/not-a-uuid", json={"title": "x"}, headers=hdr())
        check(r.status_code == 404, "patch malformed id → 404 (NOT 422)", f"got {r.status_code}")
        r = await c.delete(f"{BASE}/not-a-uuid", headers=hdr())
        check(r.status_code == 404, "delete malformed id → 404 (NOT 422)", f"got {r.status_code}")
        r = await c.post(f"{BASE}/not-a-uuid/comments", json={"body": "x"}, headers=hdr())
        check(r.status_code == 404, "comment malformed id → 404 (NOT 422)", f"got {r.status_code}")

        # cross-tenant get-by-id (share deep-link) — view under mnp, requested with asafe header
        print("\n[Share — cross-tenant GET by id (§3)]")
        r = await c.get(f"{BASE}/{main_id}", headers=hdr(OTHER_TENANT))
        check(r.status_code == 200, "cross-tenant get by id → 200", f"got {r.status_code}")
        check(r.status_code == 200 and r.json()["customer_code"] == TENANT,
              "cross-tenant get returns owner customer_code", r.text[:200])

        # ----------------------------------------------------------------- §2.4 PATCH Save
        print("\n[Save — PATCH {name, state}]")
        new_state = {**sample_state, "search": "deadlock", "scrollTop": 999}
        r = await c.patch(f"{BASE}/{main_id}",
                          json={"name": "analysis-mnp-checkout-2026-06-15-04-30-00", "state": new_state},
                          headers=hdr())
        check(r.status_code == 200, "patch save → 200", f"got {r.status_code} {r.text}")
        pv = r.json()
        check(pv["name"] == "analysis-mnp-checkout-2026-06-15-04-30-00", "name replaced")
        check(pv["state"] == new_state, "state replaced wholesale")
        check(pv["status"] == "in_progress", "status preserved (not in body)")
        check(len(pv["comments"]) == 3, "comments untouched by PATCH")
        check(pv["created_at"] == v["created_at"], "created_at immutable on PATCH")
        check(pv["updated_at"] > gv["updated_at"], "updated_at bumped on PATCH")
        check(pv["title"] == "checkout", "absent key (title) unchanged")

        # partial: present null sets null
        r = await c.patch(f"{BASE}/{main_id}", json={"notes": None}, headers=hdr())
        check(r.status_code == 200 and r.json()["notes"] is None, "present null sets null (notes)")
        check(r.json()["title"] == "checkout", "other fields untouched on partial patch")

        # patch validation
        r = await c.patch(f"{BASE}/{main_id}", json={"status": "bogus"}, headers=hdr())
        check(r.status_code == 400, "patch invalid status → 400", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{main_id}", json={"name": ""}, headers=hdr())
        check(r.status_code == 400, "patch empty name → 400", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{main_id}", content=b"{bad", headers={**hdr(), "Content-Type": "application/json"})
        check(r.status_code == 400, "patch bad JSON → 400", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/00000000-0000-0000-0000-000000000000", json={"title": "x"}, headers=hdr())
        check(r.status_code == 404, "patch missing view → 404", f"got {r.status_code}")

        # ----------------------------------------------------------------- §2.4 PATCH Complete
        print("\n[Complete — PATCH {name, state, status:completed, closure}]")
        closure = {"summary": None, "closed_by": "amin", "closed_at": "2026-06-15T05:00:00.000Z"}
        r = await c.patch(f"{BASE}/{main_id}",
                          json={"name": "analysis-mnp-checkout-2026-06-15-05-00-00",
                                "state": new_state, "status": "completed", "closure": closure},
                          headers=hdr())
        check(r.status_code == 200, "patch complete → 200", f"got {r.status_code} {r.text}")
        cvv = r.json()
        check(cvv["status"] == "completed", "status flipped to completed")
        check(cvv["closure"] == closure, "closure stored verbatim")

        # closure-convenience fallback: completed with no closure, on a fresh view
        r = await c.post(BASE, json={"name": "to-complete", "state": {"x": 1},
                                     "status": "in_progress", "assignee": "bob"}, headers=hdr())
        cid = r.json()["id"]; _created_ids.append(cid)
        r = await c.patch(f"{BASE}/{cid}", json={"status": "completed"}, headers=hdr())
        cl = r.json().get("closure")
        check(r.status_code == 200 and cl is not None, "completed w/o closure stamps one", r.text)
        check(cl and cl["summary"] is None and cl["closed_by"] == "bob" and cl["closed_at"].endswith("Z"),
              "stamped closure = {summary:null, closed_by:assignee, closed_at:now}", str(cl))

        # ----------------------------------------------------------------- §2.5 Delete
        print("\n[Delete — DELETE /saved-views/{id}]")
        r = await c.post(BASE, json={"name": "to-delete", "state": {"x": 1}}, headers=hdr())
        did = r.json()["id"]
        r = await c.delete(f"{BASE}/{did}", headers=hdr())
        check(r.status_code == 204 and r.content == b"", "delete → 204 empty body", f"got {r.status_code}")
        r = await c.get(f"{BASE}/{did}", headers=hdr())
        check(r.status_code == 404, "deleted view → 404 on get")
        r = await c.delete(f"{BASE}/00000000-0000-0000-0000-000000000000", headers=hdr())
        check(r.status_code == 404, "delete missing view → 404", f"got {r.status_code}")

    # ---- cleanup: remove every row this script created ----
    async with async_session() as s:
        if _created_ids:
            await s.execute(delete(SavedView).where(SavedView.id.in_(_created_ids)))
            await s.commit()

    print(f"\n==== {_passed} passed, {_failed} failed ====")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
