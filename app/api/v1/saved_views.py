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
  - 404 on by-id routes  → "not found"; handled gracefully, no bounce.
  - 409  → never returned here (saved-views are independent of the regroup lifecycle).

`state`, `comments`, and `closure` are opaque JSONB and round-tripped verbatim; `name` is
client-generated and stored as-is. See the backend spec for the full contract.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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


def _serialize_comment(c: dict) -> dict:
    """Comments are stored verbatim as JSONB dicts; emit the contract's key order."""
    return {
        "id": c.get("id"),
        "author": c.get("author"),
        "body": c.get("body"),
        "created_at": c.get("created_at"),
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
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Get one saved view by id — GLOBAL lookup (cross-tenant), used by the Share deep-link resolver.

    A valid tenant header is still required (so logsFetch doesn't 422-bounce), but the row is found
    by id regardless of which tenant owns it; the response carries its `customer_code` so the client
    can switch logspace. 404 here is a by-id not-found (no bounce)."""
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
    closed_at: now}."""
    body = await _read_json_object(request)
    view = await _get_global(view_id, db)

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
    """Delete a saved view by id (global lookup). 204 on success, by-id 404 if it doesn't exist."""
    view = await _get_global(view_id, db)
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
    """Append a comment to a view (Feature 5 — append-only). Bumps the view's updated_at.

    Body: { author?: string, body: string }. `body` is required/non-empty; `author` defaults to
    "anonymous". Returns the created comment (NOT the whole view). Comments are read back embedded in
    every SavedView response — there is no GET-comments endpoint."""
    payload = await _read_json_object(request)
    view = await _get_global(view_id, db)

    text = _require_str(payload, "body")  # required, non-empty → else 400
    author = payload.get("author")
    if author is None or (isinstance(author, str) and author.strip() == ""):
        author = "anonymous"
    elif not isinstance(author, str):
        raise HTTPException(400, detail="Field 'author' must be a string.")

    comment = {
        "id": str(uuid.uuid4()),
        "author": author,
        "body": text,
        "created_at": _iso_z(_now()),
    }
    # Reassign (not .append) so SQLAlchemy detects the JSONB mutation and writes it back.
    view.comments = [*(view.comments or []), comment]
    view.updated_at = _now()
    await db.commit()
    return comment


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
