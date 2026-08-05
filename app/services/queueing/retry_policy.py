"""One retry policy, shared by both durable queues.

The pipeline has two work queues with the same shape:

    SSH fetch ──ticket──► log_source_objects  ──► parse worker   (Stage 1)
    Stage 1   ──ticket──► log_regroup_pending ──► stitch worker  (Stage 2)

They must answer the same two questions identically, or the same failure behaves differently
depending on which queue it happened to land in:

  1. How long before the next attempt?   -> backoff_seconds()
  2. Is another attempt even worth it?   -> is_transient()

Both used to live only in the parse worker. They are here so the stitch worker uses exactly the same
rules rather than a second copy that drifts.
"""

import asyncio
import random
import socket

from app.services.mnp_log_ingestion.io_errors import is_disk_io_error

# Errors that are pointless to retry: the same input will fail the same way every time. Named
# explicitly rather than caught as OSError, because FileNotFoundError and ConnectionResetError are
# both OSError subclasses and belong on opposite sides of this line.
PERMANENT_TYPES = (
    UnicodeError,        # includes UnicodeDecodeError — corrupt or mis-encoded bytes
    FileNotFoundError,   # the stored file is gone; re-reading will not bring it back
    IsADirectoryError,
    NotADirectoryError,
    PermissionError,
    ValueError,          # a parser or builder rejected the content
    TypeError,
    KeyError,
)

# Errors that plausibly succeed later.
TRANSIENT_TYPES = (
    asyncio.TimeoutError,
    TimeoutError,
    ConnectionError,     # includes ConnectionResetError / BrokenPipeError
    socket.timeout,
)


def backoff_seconds(attempts: int, *, base: float, cap: float) -> float:
    """Delay before the next attempt: base * 2^(attempts-1), plus up to 25% jitter, capped.

    Why the delay matters: most transient problems need TIME to clear — a busy disk, a held lock,
    backed-up I/O. Retrying three times within a few seconds spends the whole budget before the
    condition can possibly resolve, so the retries achieve nothing.

    Why the jitter matters: rows typically fail together (one bad disk, one dead source). Without it
    they would all come back on the same tick, forever.
    """
    n = max(1, int(attempts))
    raw = min(float(base) * (2 ** (n - 1)), float(cap))
    return raw + random.random() * raw * 0.25


# Ordered rules: the first whose test matches decides. Expressed as data rather than a chain of
# `if`s so adding a rule is a one-line edit and the function that walks them stays trivial.
#
# The disk classifier goes first: it recognises the real Postgres "could not read block" and
# QueryCanceledError MESSAGES, which no isinstance check would catch.
_RULES: tuple[tuple[str, object, bool], ...] = (
    ("disk or timeout", is_disk_io_error, True),
    ("permanent type", lambda e: isinstance(e, PERMANENT_TYPES), False),
    ("transient type", lambda e: isinstance(e, TRANSIENT_TYPES), True),
)


def _cause_chain(exc: BaseException):
    """`exc`, then its __cause__, and so on — so a wrapped error is classified by what actually
    went wrong rather than by the wrapper. Guards against a self-referential cause."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = getattr(current, "__cause__", None)


def _classify(exc: BaseException) -> bool | None:
    """One pass of the rules. True = transient, False = permanent, None = unrecognised."""
    for _name, matches, verdict in _RULES:
        if matches(exc):
            return verdict
    return None


def is_transient(exc: BaseException) -> bool:
    """True if the failure is worth another attempt.

    An unrecognised error defaults to TRANSIENT. The attempt budget bounds it either way, so giving
    an unknown failure a couple of retries is safer than discarding work that might have succeeded.
    """
    for candidate in _cause_chain(exc):
        verdict = _classify(candidate)
        if verdict is not None:
            return verdict
    return True
