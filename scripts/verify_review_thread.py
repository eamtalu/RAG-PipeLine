"""End-to-end verification of the Review-rail persistence (comments, notes, replies, quotes, refs,
resolve, Matrix pins) against docs/review-comment-extension.md.

Drives the real FastAPI app in-process (httpx ASGITransport, no server) against the live Postgres,
covering the §10 test matrix:
  - general comment (anchor null) → persisted, echoed with defaults, embedded in GET
  - anchored note (anchor set, empty body, with a quote) → passes the non-empty guard via anchor/quote
  - comment with refs + source:"matrix" → round-trips verbatim
  - reply → nested under the comment, ordered by created_at, survives a re-GET
  - PATCH resolve true then false → flips, persists, returns the full comment
  - completed snapshot → comment/reply/resolve all 409
  - cross-tenant → 404 for comment/reply/resolve
  - empty comment → 400; empty reply body → 400  (repo convention: body errors are 400, never 422)
  - a mixed thread round-trips fully through list + single GET (reload durability)

Run:  PYTHONPATH=. python scripts/verify_review_thread.py
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
OTHER_TENANT = "asafe"  # a different existing tenant, for the cross-tenant tests

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


async def _new_view(c: httpx.AsyncClient, name: str, status: str = "in_progress") -> str:
    r = await c.post("/api/v1/logs/saved-views",
                     json={"name": name, "state": {"x": 1}, "status": status}, headers=hdr())
    vid = r.json()["id"]
    _created_ids.append(vid)
    return vid


async def main() -> int:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        BASE = "/api/v1/logs/saved-views"
        vid = await _new_view(c, "review-thread-verify")

        # ----------------------------------------------------------------- general comment
        print("\n[General comment — anchor null, defaults]")
        r = await c.post(f"{BASE}/{vid}/comments", json={"author": "amin", "body": "top-level"},
                         headers=hdr())
        check(r.status_code == 201, "general comment → 201", f"{r.status_code} {r.text}")
        g = r.json()
        check(g["anchor"] is None, "anchor null → general comment")
        check(g["refs"] == [] and g["quote"] is None and g["resolved"] is False
              and g["replies"] == [] and g["source"] == "user", "defaults applied", str(g))
        check(bool(g["id"]) and g["created_at"].endswith("Z"), "server id + created_at set")
        gen_id = g["id"]

        # ----------------------------------------------------------------- anchored note (empty body + quote)
        print("\n[Anchored note — anchor set, empty body, quote (non-empty guard via anchor/quote)]")
        note_body = {"anchor": "TX-1042#3", "body": "",
                     "quote": {"text": "Location does not exist…", "lineId": "TX-1042#3"}}
        r = await c.post(f"{BASE}/{vid}/comments", json=note_body, headers=hdr())
        check(r.status_code == 201, "anchored empty-body note → 201", f"{r.status_code} {r.text}")
        n = r.json()
        check(n["anchor"] == "TX-1042#3", "anchor stored verbatim")
        check(n["body"] == "", "empty body allowed for anchored note")
        check(n["quote"] == {"text": "Location does not exist…", "lineId": "TX-1042#3"},
              "quote round-trips (lineId stays camelCase)", str(n["quote"]))
        note_id = n["id"]

        # ----------------------------------------------------------------- refs + source:matrix
        print("\n[Matrix-pinned finding — refs + source:'matrix']")
        mx = {"body": "pinned finding", "refs": ["TX-1042#3", "TX-2001#0"], "source": "matrix"}
        r = await c.post(f"{BASE}/{vid}/comments", json=mx, headers=hdr())
        check(r.status_code == 201, "matrix comment → 201", f"{r.status_code} {r.text}")
        m = r.json()
        check(m["refs"] == ["TX-1042#3", "TX-2001#0"], "refs round-trip verbatim", str(m["refs"]))
        check(m["source"] == "matrix", "source:'matrix' stored")
        mx_id = m["id"]

        # ----------------------------------------------------------------- replies (nested, ordered)
        print("\n[Replies — nested under a comment, ordered by created_at]")
        r = await c.post(f"{BASE}/{vid}/comments/{gen_id}/replies",
                         json={"author": "bob", "body": "first reply"}, headers=hdr())
        check(r.status_code == 201, "add reply → 201", f"{r.status_code} {r.text}")
        rep = r.json()
        check(set(rep.keys()) == {"id", "author", "body", "created_at"}, "reply is {id,author,body,created_at}",
              str(rep.keys()))
        r = await c.post(f"{BASE}/{vid}/comments/{gen_id}/replies",
                         json={"body": "second reply"}, headers=hdr())
        check(r.status_code == 201 and r.json()["author"] == "anonymous",
              "reply author defaults to 'anonymous'", r.text)
        # reply on a missing comment → 404
        r = await c.post(f"{BASE}/{vid}/comments/does-not-exist/replies",
                         json={"body": "x"}, headers=hdr())
        check(r.status_code == 404, "reply on unknown comment → 404", f"got {r.status_code}")
        # empty reply body → 400
        r = await c.post(f"{BASE}/{vid}/comments/{gen_id}/replies", json={"body": ""}, headers=hdr())
        check(r.status_code == 400, "empty reply body → 400 (never 422)", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{vid}/comments/{gen_id}/replies", json={"author": "x"}, headers=hdr())
        check(r.status_code == 400, "reply missing body → 400", f"got {r.status_code}")

        # ----------------------------------------------------------------- resolve toggle
        print("\n[Resolve — PATCH {resolved} true then false, returns full comment]")
        r = await c.patch(f"{BASE}/{vid}/comments/{note_id}", json={"resolved": True}, headers=hdr())
        check(r.status_code == 200, "patch resolve true → 200", f"{r.status_code} {r.text}")
        pc = r.json()
        check(pc["resolved"] is True, "resolved flipped true")
        check(set(pc.keys()) == {"id", "author", "body", "created_at", "anchor", "refs", "quote",
                                 "resolved", "replies", "source"}, "resolve returns full comment", str(pc.keys()))
        r = await c.patch(f"{BASE}/{vid}/comments/{note_id}", json={"resolved": False}, headers=hdr())
        check(r.status_code == 200 and r.json()["resolved"] is False, "patch resolve false → 200 flips back")
        # unknown key ignored (generic verb); bad type → 400
        r = await c.patch(f"{BASE}/{vid}/comments/{note_id}", json={"foo": "bar"}, headers=hdr())
        check(r.status_code == 200, "unknown key ignored (no error)", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{vid}/comments/{note_id}", json={"resolved": "yes"}, headers=hdr())
        check(r.status_code == 400, "non-boolean resolved → 400", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{vid}/comments/missing", json={"resolved": True}, headers=hdr())
        check(r.status_code == 404, "resolve unknown comment → 404", f"got {r.status_code}")

        # ----------------------------------------------------------------- empty comment guard
        print("\n[Non-empty guard — a comment with nothing → 400]")
        r = await c.post(f"{BASE}/{vid}/comments", json={"author": "amin"}, headers=hdr())
        check(r.status_code == 400, "empty comment (no body/anchor/refs/quote) → 400", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "  "}, headers=hdr())
        check(r.status_code == 400, "whitespace-only body, nothing else → 400", f"got {r.status_code}")
        # but refs-only (empty body) is valid
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "", "refs": ["TX-9#1"]}, headers=hdr())
        check(r.status_code == 201, "refs-only (empty body) → 201 (guard satisfied by refs)",
              f"got {r.status_code} {r.text}")
        # bad field types → 400 (never 422)
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "x", "refs": "nope"}, headers=hdr())
        check(r.status_code == 400, "refs not a list → 400", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "x", "source": "robot"}, headers=hdr())
        check(r.status_code == 400, "invalid source → 400", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "x", "quote": {"text": "t"}}, headers=hdr())
        check(r.status_code == 400, "quote missing lineId → 400", f"got {r.status_code}")

        # ----------------------------------------------------------------- full round-trip (reload durability)
        print("\n[Round-trip — mixed thread survives single GET + list GET]")
        r = await c.get(f"{BASE}/{vid}", headers=hdr())
        gv = r.json()
        by_id = {cm["id"]: cm for cm in gv["comments"]}
        check(gen_id in by_id and note_id in by_id and mx_id in by_id, "all comments embedded in GET")
        check(len(by_id[gen_id]["replies"]) == 2, "2 replies nested under general comment",
              str(by_id[gen_id]["replies"]))
        reps = by_id[gen_id]["replies"]
        check([x["body"] for x in reps] == ["first reply", "second reply"], "replies ordered by created_at",
              str([x["body"] for x in reps]))
        check(by_id[note_id]["quote"]["lineId"] == "TX-1042#3", "note quote survives GET")
        check(by_id[mx_id]["source"] == "matrix", "matrix source survives GET")
        # comments ascending by created_at
        cas = [cm["created_at"] for cm in gv["comments"]]
        check(cas == sorted(cas), "comments ascending by created_at", str(cas))
        # single-view GET has no leaked internal keys (no 'updated_at' on comments)
        check(all("updated_at" not in cm for cm in gv["comments"]), "no internal 'updated_at' leaked on comments")
        # appears in the list too
        r = await c.get(BASE, headers=hdr())
        lst = next(v for v in r.json() if v["id"] == vid)
        check(len(lst["comments"]) == len(gv["comments"]), "list GET embeds the same thread")

        # ----------------------------------------------------------------- cross-tenant guard
        print("\n[Cross-tenant — comment/reply/resolve with wrong tenant → 404]")
        r = await c.post(f"{BASE}/{vid}/comments", json={"body": "hijack"}, headers=hdr(OTHER_TENANT))
        check(r.status_code == 404, "cross-tenant add-comment → 404", f"got {r.status_code}")
        r = await c.post(f"{BASE}/{vid}/comments/{gen_id}/replies", json={"body": "hijack"},
                         headers=hdr(OTHER_TENANT))
        check(r.status_code == 404, "cross-tenant add-reply → 404", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{vid}/comments/{gen_id}", json={"resolved": True}, headers=hdr(OTHER_TENANT))
        check(r.status_code == 404, "cross-tenant resolve → 404", f"got {r.status_code}")

        # ----------------------------------------------------------------- completed-lock (409)
        print("\n[Completed-lock — comment/reply/resolve on a completed view → 409]")
        lv = await _new_view(c, "review-thread-completed")
        r = await c.post(f"{BASE}/{lv}/comments", json={"body": "before complete"}, headers=hdr())
        pre_cid = r.json()["id"]
        r = await c.patch(f"{BASE}/{lv}", json={"status": "completed"}, headers=hdr())
        check(r.status_code == 200 and r.json()["status"] == "completed", "view completed")
        r = await c.post(f"{BASE}/{lv}/comments", json={"body": "too late"}, headers=hdr())
        check(r.status_code == 409, "add-comment on completed → 409", f"got {r.status_code} {r.text}")
        check(r.status_code == 409 and "locked" in r.json().get("detail", ""), "409 detail mentions locked",
              r.text[:160])
        r = await c.post(f"{BASE}/{lv}/comments/{pre_cid}/replies", json={"body": "too late"}, headers=hdr())
        check(r.status_code == 409, "add-reply on completed → 409", f"got {r.status_code}")
        r = await c.patch(f"{BASE}/{lv}/comments/{pre_cid}", json={"resolved": True}, headers=hdr())
        check(r.status_code == 409, "resolve on completed → 409", f"got {r.status_code}")

    # ---- cleanup: remove every row this script created ----
    async with async_session() as s:
        if _created_ids:
            await s.execute(delete(SavedView).where(SavedView.id.in_(_created_ids)))
            await s.commit()

    print(f"\n==== {_passed} passed, {_failed} failed ====")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
