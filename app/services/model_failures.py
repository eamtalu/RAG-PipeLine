"""What a failed model-provider call was: throttled, a server error, a timeout, auth, billing,
unreachable, or something else.

Used by the agent's provider guard (`app/agent/router`), which retries only a throttled call,
and by the Teams consumer, which turns the kind into the sentence a person sees. It lives here
rather than under `app/agent` because the services side may not import the agent package. The
classification reads what the SDKs expose: an HTTP status on the exception or its response,
botocore's error code, and failing those, the exception's class name and message.
"""

from __future__ import annotations

from typing import Literal

FailureKind = Literal[
    "throttled", "server_error", "timeout", "auth", "billing", "unreachable", "other"
]
_AUTH_CODES = {
    "accessdeniedexception",
    "accessdenied",
    "unrecognizedclientexception",
    "expiredtokenexception",
    "expiredtoken",
    "invalidsignatureexception",
    "authenticationerror",
    "permissiondeniederror",
}
_BILLING_WORDS = ("credit", "billing", "insufficient funds", "quota exceeded", "payment")


class ModelCallTimeout(Exception):
    """A provider call that ran past the call timeout and was cancelled; a plain exception, not
    `asyncio.TimeoutError`, so the run's own deadline and the fallback rerun tell them apart."""

    def __init__(self, provider: str, seconds: float, what: str = "the call"):
        self.provider, self.seconds = provider, seconds
        super().__init__(f"{provider}: {what} did not finish within {seconds:g}s and was cancelled")


def _status_code(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):  # botocore.exceptions.ClientError
        code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code if isinstance(code, int) else None
    for holder in (exc, response):
        code = getattr(holder, "status_code", None)
        if isinstance(code, int):
            return code
    return None


def _code_name(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code") or type(exc).__name__)
    return type(exc).__name__


def classify(exc: BaseException) -> FailureKind:
    """The kind of failure a provider call raised.

    >>> classify(ModelCallTimeout("ollama", 60))
    'timeout'
    >>> classify(type("RateLimitError", (Exception,), {})("slow down"))
    'throttled'
    >>> classify(type("ServiceUnavailableError", (Exception,), {})("down"))
    'server_error'
    >>> classify(ValueError("x"))
    'other'
    >>> classify(type("EndpointConnectionError", (Exception,), {})("no route"))
    'unreachable'
    """
    if isinstance(exc, ModelCallTimeout):
        return "timeout"
    status = _status_code(exc)
    name = _code_name(exc).lower().replace("_", "")
    message = str(exc).lower()
    if status in (401, 403) or name in _AUTH_CODES or any(
        k in name for k in ("authentication", "permissiondenied", "unauthorized", "forbidden")
    ):
        return "auth"
    if status == 400 and any(k in message for k in _BILLING_WORDS):
        return "billing"
    if status == 429 or "throttl" in name or "ratelimit" in name or "toomanyrequests" in name:
        return "throttled"
    if (status is not None and 500 <= status < 600) or any(
        k in name for k in ("serviceunavailable", "internalserver", "overloaded", "servererror")
    ):
        return "server_error"
    if status is None and any(k in name for k in ("connection", "connect", "endpoint")):
        return "unreachable"
    return "other"
