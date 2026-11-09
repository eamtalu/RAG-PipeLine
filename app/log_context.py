"""The request id and tenant every log line carries, kept in context variables.

`bind` sets them for a block of work (one HTTP request, one queued job, one tenant's pass in a
worker); the log record factory installed by `app.background.setup_logging` stamps the current
values on every record, so the fields are there whichever logger wrote the line. Tasks started
inside a bound block inherit the values, because asyncio copies the context into new tasks.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator

request_id_var: ContextVar[str | None] = ContextVar("log_request_id", default=None)
tenant_var: ContextVar[str | None] = ContextVar("log_tenant", default=None)

_UNSET = object()


def new_request_id() -> str:
    """A fresh id for work that arrived without one (no X-Request-ID, a worker pass)."""
    return uuid.uuid4().hex


def clear() -> None:
    """Drop both values, for the end of a loop that set them per pass without a block.

    >>> _ = request_id_var.set("r")
    >>> _ = tenant_var.set("t")
    >>> clear()
    >>> current()
    {'request_id': None, 'tenant': None}
    """
    request_id_var.set(None)
    tenant_var.set(None)


def current() -> dict[str, str | None]:
    """What the next log line will carry.

    >>> current()
    {'request_id': None, 'tenant': None}
    >>> with bind(request_id="r1", tenant="acme"):
    ...     current()
    {'request_id': 'r1', 'tenant': 'acme'}
    """
    return {"request_id": request_id_var.get(), "tenant": tenant_var.get()}


@contextmanager
def bind(
    request_id: str | None | object = _UNSET, tenant: str | None | object = _UNSET
) -> Iterator[None]:
    """Set the request id and/or the tenant for the block; a field left out keeps its value.

    >>> with bind(request_id="r1", tenant="t1"):
    ...     with bind(tenant="t2"):
    ...         current()["tenant"]
    't2'
    """
    tokens = []
    if request_id is not _UNSET:
        tokens.append((request_id_var, request_id_var.set(request_id)))
    if tenant is not _UNSET:
        tokens.append((tenant_var, tenant_var.set(tenant)))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)
