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


def is_transient(exc: BaseException) -> bool:
    """True if the failure is worth another attempt.

    Order: the existing disk/timeout classifier first (it recognises the real Postgres
    "could not read block" and QueryCanceledError messages), then explicit permanent types, then
    explicit transient types, then follow the cause chain.

    An unrecognised error defaults to TRANSIENT. The attempt budget bounds it either way, so giving
    an unknown failure a couple of retries is safer than discarding work that might have succeeded.
    """
    if is_disk_io_error(exc):
        return True
    if isinstance(exc, PERMANENT_TYPES):
        return False
    if isinstance(exc, TRANSIENT_TYPES):
        return True
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        return is_transient(cause)
    return True
