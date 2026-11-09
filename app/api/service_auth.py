"""Who is calling the HTTP API: a named service token on every request under /api/v1.

The router in `app/api/v1/router.py` runs `require_service_caller` on every route; `require_admin`
in `app/api/deps.py` reads the caller it resolved. Tokens come from settings as `name=token,...`;
the name says which system is calling (the web app, an operator) and `api_admin_callers` says
which names may use the admin routes. While `api_auth_required` is off, a call without a token
passes with a warning so the log shows who still needs one; a wrong token never passes.
"""

from __future__ import annotations

import logging
import secrets
from contextvars import ContextVar
from dataclasses import dataclass

from fastapi import Header, HTTPException, Request

from app.settings import settings

logger = logging.getLogger(__name__)

ANONYMOUS = "anonymous"


@dataclass(frozen=True)
class Caller:
    """The system behind a request, by token name; `anonymous` only while auth is optional."""

    name: str
    is_admin: bool


#: The caller of the request in flight, set by the router dependency so that code called inside a
#: handler (`require_admin()` is called directly in two places) can read it without plumbing.
current_caller: ContextVar[Caller | None] = ContextVar("current_caller", default=None)


def parse_service_tokens(raw: str) -> dict[str, str]:
    """Named tokens from the setting, blanks ignored.

    >>> parse_service_tokens("web=a1,ops=b2")
    {'web': 'a1', 'ops': 'b2'}
    >>> parse_service_tokens(" web = a1 , ")
    {'web': 'a1'}
    >>> parse_service_tokens("")
    {}
    """
    out: dict[str, str] = {}
    for part in raw.split(","):
        if "=" not in part:
            continue
        name, token = (s.strip() for s in part.split("=", 1))
        if name and token:
            out[name] = token
    return out


def _admin_names() -> set[str]:
    return {n.strip() for n in settings.api_admin_callers.split(",") if n.strip()}


def resolve_caller(authorization: str | None, path: str) -> Caller:
    """The caller a bearer header names, or anonymous while auth is optional; 401 otherwise.

    ("Bearer tok-web", "/api/v1/x") -> Caller("web", False) ; (None, path) -> anonymous or 401 ;
    ("Bearer wrong", path) -> 401 in both modes
    """
    # Step 1: A missing token is a rollout gap while optional and a refusal once required, so
    # the warning names the path and the switch to required breaks nothing unexpected.
    if not authorization:
        if settings.api_auth_required:
            raise HTTPException(401,
                                detail="a service token is required: Authorization: Bearer <token>")
        logger.warning("api.auth.anonymous path=%s", path)
        return Caller(ANONYMOUS, is_admin=True)
    scheme, _, presented = authorization.partition(" ")
    if scheme.lower() != "bearer" or not presented.strip():
        raise HTTPException(401, detail="malformed Authorization header; expected Bearer <token>")
    # Step 2: Compare against every configured token in constant time, because a timing
    # difference on the first mismatching byte would leak the token one byte at a time.
    presented = presented.strip()
    matched: str | None = None
    for name, token in parse_service_tokens(settings.api_service_tokens).items():
        if secrets.compare_digest(presented.encode(), token.encode()):
            matched = name
    if matched is None:
        logger.warning("api.auth.refused path=%s reason=unknown_token", path)
        raise HTTPException(401, detail="unknown service token")
    return Caller(matched, is_admin=matched in _admin_names())


async def require_service_caller(request: Request,
                                 authorization: str | None = Header(default=None)) -> Caller:
    """Router dependency: resolve the caller and remember it for the rest of the request."""
    caller = resolve_caller(authorization, request.url.path)
    current_caller.set(caller)
    return caller


def require_admin_caller() -> None:
    """Refuse unless the request's caller is an admin; outside a request, follow the mode.

    (caller ops, admin) -> None ; (caller web) -> 403 ; (no request, auth optional) -> None ;
    (no request, auth required) -> 403
    """
    caller = current_caller.get()
    if caller is None:
        if settings.api_auth_required:
            raise HTTPException(403,
                                detail="admin access needs a token named in API_ADMIN_CALLERS")
        return
    if not caller.is_admin:
        logger.warning("api.auth.refused caller=%s reason=not_admin", caller.name)
        raise HTTPException(403, detail=f"caller {caller.name!r} is not an admin")
