"""Every HTTP request gets a request id and a tenant in the log context, and the id in its response.

Pure ASGI middleware, outermost in `app/main.py`, so even the idempotency layer logs with the
id. The id is the caller's X-Request-ID when it sends one (the web app or the operator can then
find the line), otherwise minted here; the tenant is the X-Customer-Code header as sent, before
validation, because a refused code is exactly what a reader of the log wants to see.
"""

from __future__ import annotations

import logging
import time

from starlette.datastructures import Headers, MutableHeaders

from app import log_context

logger = logging.getLogger(__name__)

HEADER = "x-request-id"
QUIET_PATHS = frozenset({"/health"})


class RequestContextMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        """Bind the context, forward the request, write one `api.request` line when it ends.

        (GET /api/v1/x, X-Request-ID abc) -> handler lines carry abc; response X-Request-ID abc
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        request_id = (headers.get(HEADER) or "").strip()[:128] or log_context.new_request_id()
        tenant = (headers.get("x-customer-code") or "").strip()[:64] or None
        status = {"code": 0}
        started = time.monotonic()

        async def send_with_id(message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
                MutableHeaders(scope=message).append("X-Request-ID", request_id)
            await send(message)

        with log_context.bind(request_id=request_id, tenant=tenant):
            try:
                await self.app(scope, receive, send_with_id)
            finally:
                path = scope.get("path", "")
                if path not in QUIET_PATHS:
                    logger.info(
                        "api.request method=%s path=%s status=%d ms=%d",
                        scope.get("method"),
                        path,
                        status["code"],
                        int((time.monotonic() - started) * 1000),
                    )
