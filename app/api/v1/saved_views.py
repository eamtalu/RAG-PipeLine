"""Saved Analyses ("Saved Views") API — a persisted, shareable snapshot of an analysis session.

A saved view captures opaque feed/filter view-state (`state`) plus a lightweight collaboration
layer: a status workflow, an append-only comment thread, and a closure record. Six routes live
under the tenant-scoped logs namespace `/api/v1/logs/saved-views`.

⚠️ HTTP status-code contract (the frontend's shared logs client reacts to these globally):
  - 422  → reserved EXCLUSIVELY for a missing/invalid X-Customer-Code header (FastAPI raises it when
           the required header is absent). The client bounces to the logspace picker. We therefore
           NEVER let body/field validation surface as 422 — every body error is raised as 400.
  - 400  → all body/field validation errors (bad JSON, missing `name`/`state`/comment `body`, …).
  - 404 on list/create  → unknown/inactive tenant (get_current_customer); the client bounces.
  - 404 on by-id routes  → "not found"; handled gracefully, no bounce. NOTE the read/write split:
           GET-by-id resolves a view in ANY tenant (the Share deep-link) and takes NO tenant header at
           all — it never 422/404s on header grounds. The writes (PATCH/DELETE/add-comment) stay
           tenant-guarded, so a view owned by another tenant reads as a by-id 404 for them.
  - 409  → the completed-lock: once a view's status is "completed" its Review thread is frozen, so
           add-comment / add-reply / resolve return 409 (`{"detail": "snapshot is completed — comments
           are locked"}`). This is the ONLY 409 here; the saved-view lifecycle itself (create/save/
           complete) never 409s. The frontend hides the composer for completed views, but we enforce
           it server-side too.

`state`, `comments`, and `closure` are opaque JSONB and round-tripped verbatim; `name` is
client-generated and stored as-is. The Review thread lives inside `comments` — an append-only list of
rich comment objects (anchor/refs/quote/resolved/replies/source), embedded in every view read so it
survives reload. See the backend spec (docs/review-comment-extension.md) for the full contract.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from app.config.database import get_session
from app.api.deps import get_current_customer
from app.persistence.models.saved_view import SavedView, ANALYSIS_STATUSES, DEFAULT_STATUS

router = APIRouter(prefix="/logs/saved-views", tags=["saved-views"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: datetime) -> str:
    """Serialize a datetime as ISO-8601 UTC with millisecond precision + 'Z', e.g.
    `2026-06-14T10:00:00.000Z`. The client sorts/compares these as plain strings and parses with
    `Date`, so the format must be stable and zulu-suffixed."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


async def _read_json_object(request: Request) -> dict:
    """Parse a JSON request body into a dict, or raise 400 (never 422 — see module docstring).

    A non-object top-level body (array/scalar) is rejected, since every saved-views body is an
    object."""
    raw = await request.body()
    if not raw:
        raise HTTPException(400, detail="Request body must be a JSON object.")
    try:
        data = json.loads(raw)
    except ValueError:
        raise HTTPException(400, detail="Request body is not valid JSON.")
    if not isinstance(data, dict):
        raise HTTPException(400, detail="Request body must be a JSON object.")
    return data


def _require_str(body: dict, key: str) -> str:
    """A required, non-empty string field — else 400."""
    if key not in body:
        raise HTTPException(400, detail=f"Missing required field: {key!r}.")
    val = body[key]
    if not isinstance(val, str) or val.strip() == "":
        raise HTTPException(400, detail=f"Field {key!r} must be a non-empty string.")
    return val


def _optional_str_or_none(body: dict, key: str) -> str | None:
    """An optional string|null field. Absent → None. Present must be string or null, else 400."""
    if key not in body or body[key] is None:
        return None
    val = body[key]
    if not isinstance(val, str):
        raise HTTPException(400, detail=f"Field {key!r} must be a string or null.")
    return val


def _validate_status(value: Any) -> str:
    if not isinstance(value, str) or value not in ANALYSIS_STATUSES:
        raise HTTPException(
            400,
            detail=f"Field 'status' must be one of {', '.join(ANALYSIS_STATUSES)}.",
        )
    return value


def _serialize_reply(r: dict) -> dict:
    """A threaded reply — {id, author, body, created_at} in contract order."""
    return {
        "id": r.get("id"),
        "author": r.get("author"),
        "body": r.get("body"),
        "created_at": r.get("created_at"),
    }


def _serialize_comment(c: dict) -> dict:
    """A Review-thread comment in the contract's exact shape/order.

    Comments are stored verbatim as JSONB dicts. Every new field is read with a default so a comment
    written under the OLD shape ({id, author, body, created_at}) still serializes cleanly — anchor→
    null (general comment), refs→[], quote→null, resolved→false, replies→[], source→"user". Any
    internal-only keys (e.g. a comment-level `updated_at`) are intentionally NOT emitted — the wire
    contract has no such field.

    Replies are returned in stable insertion (created_at-ascending) order, matching how they were
    appended."""
    return {
        "id": c.get("id"),
        "author": c.get("author"),
        "body": c.get("body") or "",
        "created_at": c.get("created_at"),
        "anchor": c.get("anchor"),                       # null → general comment; set → note/annotation
        "refs": list(c.get("refs") or []),               # jump-to-line hyperlink chips
        "quote": c.get("quote"),                         # {text, lineId} | null
        "resolved": bool(c.get("resolved", False)),
        "replies": [_serialize_reply(r) for r in (c.get("replies") or [])],
        "source": c.get("source") or "user",             # "user" | "matrix" (pinned assistant finding)
    }


def _serialize(v: SavedView) -> dict:
    """Full, hydrated SavedView in the contract's exact field order."""
    return {
        "id": str(v.id),
        "customer_code": v.customer_code,
        "name": v.name,
        "title": v.title,
        "notes": v.notes,
        "saved_by": v.saved_by,
        "assignee": v.assignee,
        "status": v.status,
        "due_date": v.due_date,
        "comments": [_serialize_comment(c) for c in (v.comments or [])],
        "closure": v.closure,
        "created_at": _iso_z(v.created_at),
        "updated_at": _iso_z(v.updated_at),
        "state": v.state,
    }


async def _get_global(view_id: str, db: AsyncSession) -> SavedView:
    """Look a view up by id ACROSS ALL TENANTS (no customer_code filter), or raise a by-id 404.

    Required for the Share deep-link: a link to a view in another logspace must resolve so the client
    can switch tenants. This 404 is NOT tenant-sensitive — the client handles it gracefully without
    bouncing to the logspace picker.

    `view_id` is taken as a raw string (not a typed UUID path param) so a malformed id resolves to a
    plain 404 here rather than FastAPI's 422 — 422 is reserved exclusively for a missing tenant
    header (a 422 on a by-id route would wrongly bounce the user to the logspace picker, see §0.1)."""
    try:
        parsed = uuid.UUID(view_id)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(404, detail="Saved view not found.")
    view = await db.get(SavedView, parsed)
    if view is None:
        raise HTTPException(404, detail="Saved view not found.")
    return view


async def _get_tenant_scoped(view_id: str, customer: str, db: AsyncSession) -> SavedView:
    """Look a view up by id but ONLY within `customer`'s tenant — for the WRITE routes (PATCH/DELETE/
    add-comment). A view owned by another tenant is treated as not-found (a by-id 404, no bounce), so
    one logspace can never mutate another's saved view. This is the deliberate counterpart to the
    global GET-by-id: reads cross tenants for sharing, writes stay tenant-guarded.

    Like `_get_global`, a malformed id is a plain 404 (never FastAPI's 422 — 422 is reserved for a
    missing tenant header). The header itself is still validated upstream via get_current_customer, so
    writes keep requiring a well-formed, registered tenant header."""
    try:
        parsed = uuid.UUID(view_id)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(404, detail="Saved view not found.")
    view = await db.get(SavedView, parsed)
    if view is None or view.customer_code != customer:
        raise HTTPException(404, detail="Saved view not found.")
    return view


# ---------------------------------------------------------------------------
# Review-thread helpers (comments / replies / resolve)
#
# Every validation error below is raised as 400 (NOT 422): 422 is reserved exclusively for a missing
# X-Customer-Code header (see module docstring), so a body error must never surface as 422 or the
# frontend's global handler would bounce the user to the logspace picker.
# ---------------------------------------------------------------------------

_COMMENT_SOURCES: tuple[str, ...] = ("user", "matrix")
_COMPLETED_LOCK_DETAIL = "snapshot is completed — comments are locked"


def _require_not_completed(view: SavedView) -> None:
    """Completed-lock (§2.2): a completed view's Review thread is frozen → 409 on any mutation."""
    if view.status == "completed":
        raise HTTPException(409, detail=_COMPLETED_LOCK_DETAIL)


def _resolve_author(payload: dict) -> str:
    """Author is free text (no auth). Absent/blank/None → "anonymous"; non-string → 400."""
    author = payload.get("author")
    if author is None or (isinstance(author, str) and author.strip() == ""):
        return "anonymous"
    if not isinstance(author, str):
        raise HTTPException(400, detail="Field 'author' must be a string.")
    return author


def _coerce_comment_body(payload: dict) -> str:
    """`body` is optional (defaults to "") — an anchored note may have empty body. Non-string
    (and non-null) → 400. Null/absent → ""."""
    if "body" not in payload or payload["body"] is None:
        return ""
    val = payload["body"]
    if not isinstance(val, str):
        raise HTTPException(400, detail="Field 'body' must be a string.")
    return val


def _coerce_anchor(payload: dict) -> str | None:
    """`anchor` is an opaque LineId string or null. Present-non-string → 400. Blank string → null
    (treated as no anchor → a general comment)."""
    if "anchor" not in payload or payload["anchor"] is None:
        return None
    val = payload["anchor"]
    if not isinstance(val, str):
        raise HTTPException(400, detail="Field 'anchor' must be a string or null.")
    return val or None


def _coerce_refs(payload: dict) -> list[str]:
    """`refs` is a list of opaque LineId strings (default []). Non-list, or any non-string item → 400."""
    if "refs" not in payload or payload["refs"] is None:
        return []
    val = payload["refs"]
    if not isinstance(val, list) or not all(isinstance(x, str) for x in val):
        raise HTTPException(400, detail="Field 'refs' must be an array of strings.")
    return val


def _coerce_quote(payload: dict) -> dict | None:
    """`quote` is {text, lineId} or null (default null). NOTE: `lineId` stays camelCase inside the
    quote object — it is the one camelCase key on the wire. Malformed → 400. Stored normalized to
    exactly {text, lineId}."""
    if "quote" not in payload or payload["quote"] is None:
        return None
    val = payload["quote"]
    if not isinstance(val, dict):
        raise HTTPException(400, detail="Field 'quote' must be an object or null.")
    text = val.get("text")
    line_id = val.get("lineId")
    if not isinstance(text, str) or not isinstance(line_id, str):
        raise HTTPException(400, detail="Field 'quote' must be {text: string, lineId: string}.")
    return {"text": text, "lineId": line_id}


def _coerce_source(payload: dict) -> str:
    """`source` is "user" | "matrix" (default "user"). Anything else → 400."""
    if "source" not in payload or payload["source"] is None:
        return "user"
    val = payload["source"]
    if val not in _COMMENT_SOURCES:
        raise HTTPException(400, detail=f"Field 'source' must be one of {', '.join(_COMMENT_SOURCES)}.")
    return val


def _require_comment_content(body: str, anchor: str | None, refs: list[str], quote: dict | None) -> None:
    """Non-empty guard (§2.5): a comment is valid if AT LEAST ONE of body (non-blank), anchor, refs
    (non-empty), or quote is present. A pure anchor/quote note with empty body is valid; a fully
    empty comment → 400."""
    if body.strip() or anchor or refs or quote is not None:
        return
    raise HTTPException(400, detail="A comment needs at least one of: body, anchor, refs, or quote.")


def _find_comment(view: SavedView, cid: str) -> dict:
    """Locate a comment dict by id within the view's embedded thread, or by-id 404."""
    for c in (view.comments or []):
        if c.get("id") == cid:
            return c
    raise HTTPException(404, detail="Comment not found.")


def _replace_comment(view: SavedView, cid: str, updated: dict) -> None:
    """Swap the comment `cid` for `updated` and persist the JSONB change.

    Critical: we must NOT mutate the loaded comment dicts in place. SQLAlchemy's default JSON(B)
    change detection compares the new attribute value against the value it snapshotted at load time
    BY REFERENCE — an in-place edit mutates that very snapshot, so before==after and the write is
    silently skipped (the bug that made replies/resolves not survive reload). Building a fresh list
    with a brand-new dict for the target (loaded objects untouched) makes new != old, and
    flag_modified forces the column into the UPDATE regardless. Reassign (not .append) so the ORM
    sees a new list object too."""
    view.comments = [updated if c.get("id") == cid else c for c in (view.comments or [])]
    flag_modified(view, "comments")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("")
async def list_saved_views(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    status: str | None = Query(default=None, description="Filter by status (open|in_progress|due|completed)."),
):
    """List this tenant's saved views, newest-first (by created_at desc), each fully hydrated.

    Populates the Load chip dropdown (the client shows the 10 newest). An unknown/inactive tenant
    is a tenant-sensitive 404 (via get_current_customer) and bounces the client to the picker."""
    if status is not None:
        _validate_status(status)  # an invalid ?status= value is a body/field error → 400

    stmt = select(SavedView).where(SavedView.customer_code == customer)
    if status is not None:
        stmt = stmt.where(SavedView.status == status)
    stmt = stmt.order_by(SavedView.created_at.desc())
    rows = (await db.execute(stmt)).scalars().all()
    return [_serialize(v) for v in rows]


@router.post("", status_code=201)
async def create_saved_view(
    request: Request,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Create a new saved view (Feature 1 — "Create a snapshot"). Always inserts a NEW row.

    `name` and `state` are required; `state` is stored opaque. Server sets id, customer_code,
    comments=[], closure=null, created_at=updated_at=now."""
    body = await _read_json_object(request)

    name = _require_str(body, "name")
    if "state" not in body or body["state"] is None:
        raise HTTPException(400, detail="Missing required field: 'state'.")
    state = body["state"]  # opaque — stored verbatim, never inspected

    status = DEFAULT_STATUS
    if "status" in body and body["status"] is not None:
        status = _validate_status(body["status"])

    now = _now()
    view = SavedView(
        customer_code=customer,
        name=name,
        title=_optional_str_or_none(body, "title"),
        notes=_optional_str_or_none(body, "notes"),
        saved_by=_optional_str_or_none(body, "saved_by"),
        assignee=_optional_str_or_none(body, "assignee"),
        status=status,
        due_date=_optional_str_or_none(body, "due_date"),
        comments=[],
        closure=None,
        state=state,
        created_at=now,
        updated_at=now,
    )
    db.add(view)
    await db.commit()
    await db.refresh(view)
    return _serialize(view)


@router.get("/{view_id}")
async def get_saved_view(
    view_id: str,
    db: AsyncSession = Depends(get_session),
):
    """Get one saved view by id — GLOBAL lookup (cross-tenant), used by the Share deep-link resolver.

    The X-Customer-Code header is intentionally NOT a dependency here. The Share resolver opens a link
    whose view often lives in a different logspace than the opener's currently-active one — that is the
    whole point of sharing — so the read must resolve by id regardless of which tenant (if any) the
    header names. The client depends on the returned `customer_code` to decide whether to switch
    logspace. Therefore this route never filters or denies on header grounds: a missing or garbage
    header does NOT 422/400/404 the read. 404 here means the id exists in NO tenant — a plain by-id
    not-found the client handles gracefully (no bounce to the logspace picker)."""
    view = await _get_global(view_id, db)
    return _serialize(view)


@router.patch("/{view_id}")
async def update_saved_view(
    view_id: str,
    request: Request,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Partially update a saved view (Feature 2 "Save" and Feature 6 "Complete").

    Only keys present in the body are written (name, title, notes, saved_by, assignee, status,
    due_date, closure, state). Absent keys are unchanged; a present key with null sets null.
    `comments` are never touched here; `created_at` is immutable; `updated_at` is bumped to now.
    `state` is replaced wholesale (opaque). Closure convenience: if status flips to "completed" and
    no closure was provided, the server stamps {summary: null, closed_by: <assignee or null>,
    closed_at: now}.

    Writes are tenant-guarded: the view must belong to the request's tenant, else by-id 404."""
    body = await _read_json_object(request)
    view = await _get_tenant_scoped(view_id, customer, db)

    if "name" in body:
        view.name = _require_str(body, "name")  # if present it must be a non-empty string
    if "title" in body:
        view.title = _coerce_optional_str(body["title"], "title")
    if "notes" in body:
        view.notes = _coerce_optional_str(body["notes"], "notes")
    if "saved_by" in body:
        view.saved_by = _coerce_optional_str(body["saved_by"], "saved_by")
    if "assignee" in body:
        view.assignee = _coerce_optional_str(body["assignee"], "assignee")
    if "due_date" in body:
        view.due_date = _coerce_optional_str(body["due_date"], "due_date")
    if "status" in body:
        if body["status"] is None:
            raise HTTPException(400, detail="Field 'status' may not be null.")
        view.status = _validate_status(body["status"])
    if "closure" in body:
        view.closure = _validate_closure(body["closure"])
    if "state" in body:
        if body["state"] is None:
            raise HTTPException(400, detail="Field 'state' may not be null.")
        view.state = body["state"]  # opaque — replaced wholesale

    # Closure convenience fallback (rule §4.5): completed + no closure supplied → stamp one.
    if view.status == "completed" and "closure" not in body and view.closure is None:
        view.closure = {
            "summary": None,
            "closed_by": view.assignee,
            "closed_at": _iso_z(_now()),
        }

    view.updated_at = _now()
    await db.commit()
    await db.refresh(view)
    return _serialize(view)


@router.delete("/{view_id}", status_code=204)
async def delete_saved_view(
    view_id: str,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Delete a saved view by id (tenant-guarded). 204 on success; by-id 404 if it doesn't exist in
    the request's tenant (missing, or owned by another tenant)."""
    view = await _get_tenant_scoped(view_id, customer, db)
    await db.delete(view)
    await db.commit()
    return Response(status_code=204)


@router.post("/{view_id}/comments", status_code=201)
async def add_comment(
    view_id: str,
    request: Request,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Append a Review-thread comment to a view (append-only). Bumps the view's updated_at.

    Body: { author?, body?, anchor?, refs?, quote?, source? }. A comment is valid if AT LEAST ONE of
    body/anchor/refs/quote is present (§2.5) — so a line-anchored note may have an empty body. `anchor`
    (a LineId) makes it a note; absent → a general comment. `author` defaults to "anonymous"; `source`
    to "user". Rejects mutations on a completed view (409).

    Returns the FULL created comment (id, author, body, created_at, anchor, refs, quote, resolved=false,
    replies=[], source) — never a partial echo, so the frontend reconciling by id doesn't flicker
    fields to defaults. Comments are read back embedded in every SavedView response (no GET-comments
    endpoint). Tenant-guarded like the other writes: a view owned by another tenant is a by-id 404."""
    payload = await _read_json_object(request)
    view = await _get_tenant_scoped(view_id, customer, db)
    _require_not_completed(view)

    author = _resolve_author(payload)
    body = _coerce_comment_body(payload)
    anchor = _coerce_anchor(payload)
    refs = _coerce_refs(payload)
    quote = _coerce_quote(payload)
    source = _coerce_source(payload)
    _require_comment_content(body, anchor, refs, quote)

    now = _iso_z(_now())
    comment = {
        "id": str(uuid.uuid4()),
        "author": author,
        "body": body,
        "created_at": now,
        "updated_at": now,      # internal only (for resolve toggles); never serialized
        "anchor": anchor,
        "refs": refs,
        "quote": quote,
        "resolved": False,
        "replies": [],
        "source": source,
    }
    # Reassign (not .append) so SQLAlchemy detects the JSONB mutation and writes it back.
    view.comments = [*(view.comments or []), comment]
    view.updated_at = _now()
    await db.commit()
    return _serialize_comment(comment)


@router.post("/{view_id}/comments/{cid}/replies", status_code=201)
async def add_reply(
    view_id: str,
    cid: str,
    request: Request,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Append a threaded reply to a comment (append-only). Bumps the view's updated_at.

    Body: { author?, body }. `body` is required/non-empty (§2.5) → else 400. `author` defaults to
    "anonymous". Rejects on a completed view (409). Returns the created reply
    {id, author, body, created_at}. Tenant-guarded; a missing view OR a `cid` not under it → by-id
    404."""
    payload = await _read_json_object(request)
    view = await _get_tenant_scoped(view_id, customer, db)
    _require_not_completed(view)
    comment = _find_comment(view, cid)

    author = _resolve_author(payload)
    text = _require_str(payload, "body")  # reply requires a non-empty body → else 400

    reply = {
        "id": str(uuid.uuid4()),
        "author": author,
        "body": text,
        "created_at": _iso_z(_now()),
    }
    # Append to a COPY of the comment (never mutate the loaded dict — see _replace_comment).
    updated = dict(comment)
    updated["replies"] = [*(comment.get("replies") or []), reply]
    _replace_comment(view, cid, updated)
    view.updated_at = _now()
    await db.commit()
    return reply


@router.patch("/{view_id}/comments/{cid}")
async def update_comment(
    view_id: str,
    cid: str,
    request: Request,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Update a comment — today only its `resolved` flag (Feature: resolve/unresolve).

    Body: { resolved: bool }. The verb is kept generic so future editable fields (e.g. a body edit)
    can extend this same route; unknown keys are ignored rather than erroring. Rejects on a completed
    view (409). Bumps the comment's (internal) updated_at and the view's updated_at. Returns the FULL
    updated comment. Tenant-guarded; missing view / `cid` → by-id 404."""
    payload = await _read_json_object(request)
    view = await _get_tenant_scoped(view_id, customer, db)
    _require_not_completed(view)
    comment = _find_comment(view, cid)

    updated = dict(comment)
    if "resolved" in payload:
        val = payload["resolved"]
        if not isinstance(val, bool):
            raise HTTPException(400, detail="Field 'resolved' must be a boolean.")
        updated["resolved"] = val
        updated["updated_at"] = _iso_z(_now())

    # Swap in the new dict (loaded objects untouched) so the JSONB write is not skipped.
    _replace_comment(view, cid, updated)
    view.updated_at = _now()
    await db.commit()
    return _serialize_comment(updated)


# ---------------------------------------------------------------------------
# PATCH field coercion helpers (defined after routes for readability)
# ---------------------------------------------------------------------------

def _coerce_optional_str(val: Any, key: str) -> str | None:
    """A PATCH string|null value: null stays null, a string is kept, anything else → 400."""
    if val is None:
        return None
    if not isinstance(val, str):
        raise HTTPException(400, detail=f"Field {key!r} must be a string or null.")
    return val


def _validate_closure(val: Any) -> dict | None:
    """A PATCH closure value: null, or an object stored verbatim. Non-object/non-null → 400."""
    if val is None:
        return None
    if not isinstance(val, dict):
        raise HTTPException(400, detail="Field 'closure' must be an object or null.")
    return val
