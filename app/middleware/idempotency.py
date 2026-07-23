"""IdempotencyMiddleware — server-side Idempotency-Key de-duplication for mutating POSTs.

Best-practice, opt-in request de-duplication (Stripe-style). A client sends `Idempotency-Key: <uuid>`
on a mutating POST; the first request runs and its JSON response is cached; a retry / double-submit
carrying the SAME key replays that response instead of duplicating the side effect.

STRICTLY OPT-IN / NO REGRESSION: this middleware is a pure pass-through unless ALL of:
  - method is POST,
  - the path is on the ALLOWLIST (small JSON-body create endpoints), and
  - an `Idempotency-Key` header is present (with an `X-Customer-Code` tenant).
Every other request (all GETs incl. the feed, uploads, non-allowlisted routes, keyless POSTs) is
forwarded untouched — the request body is not even read.

Duplicate handling (same tenant + key):
  - completed + same request fingerprint  -> replay the stored status + JSON body,
  - in_progress                           -> 409 (the first request is still running),
  - different request fingerprint          -> 422 (a key reused for a different request = client bug).

Only JSON, non-5xx responses are cached; a 5xx / non-JSON response drops the claim so a genuine retry
can proceed. Keys expire after settings.idempotency_ttl_hours.
"""

import hashlib
import json
import re
from datetime import datetime, timezone, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import Message

from app.config.database import async_session
from app.persistence.models.idempotency_key import IdempotencyKey, IdempotencyStatus
from app.settings import settings

# Allowlist: small JSON-body create endpoints that can duplicate on double-submit / retry.
# Deliberately excludes /ingest + /upload (multipart / large bodies — covered by the frontend
# in-flight button disable + downstream entry_hash dedup) and endpoints with natural guards
# (/fetch-remote 409, /ssh-sources 409, /regroup/finalize no-op).
_ALLOWLIST = [
    re.compile(r"^/api/v1/logs/saved-views/?$"),
    re.compile(r"^/api/v1/logs/saved-views/[^/]+/comments/?$"),
    re.compile(r"^/api/v1/logs/saved-views/[^/]+/comments/[^/]+/replies/?$"),
    re.compile(r"^/api/v1/logs/debug/ask/?$"),
]


def _allowed(path: str) -> bool:
    return any(p.match(path) for p in _ALLOWLIST)


async def _load(customer: str, key: str) -> IdempotencyKey | None:
    async with async_session() as s:
        return (
            await s.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.customer_code == customer,
                    IdempotencyKey.idem_key == key,
                )
            )
        ).scalar_one_or_none()


class IdempotencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Fast pass-through: never touch anything that isn't an allowlisted, keyed POST.
        if request.method != "POST" or not _allowed(request.url.path):
            return await call_next(request)
        key = request.headers.get("Idempotency-Key")
        customer = request.headers.get("X-Customer-Code")
        if not key or not customer:
            return await call_next(request)

        # Read the body (to fingerprint) and RE-INJECT it so the downstream handler can still read it
        # (BaseHTTPMiddleware would otherwise leave the receive stream exhausted).
        body = await request.body()

        async def _replay() -> Message:
            return {"type": "http.request", "body": body, "more_body": False}

        request._receive = _replay  # type: ignore[attr-defined]

        fingerprint = hashlib.sha256(
            f"{request.method}|{request.url.path}|".encode() + body
        ).hexdigest()
        now = datetime.now(timezone.utc)
        expires = now + timedelta(hours=settings.idempotency_ttl_hours)

        # 1) Try to CLAIM the key (atomic: UNIQUE(customer_code, idem_key)).
        claimed = False
        async with async_session() as s:
            s.add(
                IdempotencyKey(
                    customer_code=customer, idem_key=key, method=request.method,
                    path=request.url.path, request_fingerprint=fingerprint,
                    status=IdempotencyStatus.in_progress.value, created_at=now, expires_at=expires,
                )
            )
            try:
                await s.commit()
                claimed = True
            except IntegrityError:
                await s.rollback()

        # 2) Duplicate — decide replay / 409 / 422 from the existing row.
        if not claimed:
            existing = await _load(customer, key)
            if existing is None:
                # Row vanished between the failed insert and the read (TTL sweep) — just proceed.
                return await call_next(request)
            if existing.request_fingerprint != fingerprint:
                return JSONResponse(
                    {"detail": "Idempotency-Key was reused with a different request."},
                    status_code=422,
                )
            if existing.status == IdempotencyStatus.completed.value and existing.response_status:
                return JSONResponse(existing.response_body, status_code=existing.response_status)
            return JSONResponse(
                {"detail": "A request with this Idempotency-Key is already in progress."},
                status_code=409,
            )

        # 3) We claimed it — run the handler once and capture the response body.
        response = await call_next(request)
        chunks = [chunk async for chunk in response.body_iterator]
        raw = b"".join(chunks)

        content_type = response.headers.get("content-type", "")
        cacheable = response.status_code < 500 and "application/json" in content_type
        parsed = None
        if cacheable:
            try:
                parsed = json.loads(raw.decode() or "null")
            except ValueError:
                cacheable = False

        async with async_session() as s:
            row = (
                await s.execute(
                    select(IdempotencyKey).where(
                        IdempotencyKey.customer_code == customer,
                        IdempotencyKey.idem_key == key,
                    )
                )
            ).scalar_one_or_none()
            if row is not None:
                if cacheable:
                    row.status = IdempotencyStatus.completed.value
                    row.response_status = response.status_code
                    row.response_body = parsed
                    row.completed_at = datetime.now(timezone.utc)
                else:
                    # 5xx / non-JSON: don't cache — drop the claim so a genuine retry can proceed.
                    await s.delete(row)
                await s.commit()

        # Rebuild the response (its body_iterator is now consumed).
        return Response(
            content=raw,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
