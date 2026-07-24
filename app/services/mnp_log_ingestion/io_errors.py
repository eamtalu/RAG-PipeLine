# Disk I/O (bad-sector) error detection for the ingestion pipeline.
#
# The production host runs on a failing HDD with unrecoverable bad sectors. When a query touches a
# dead block Postgres raises `could not read block N in file "…": Input/output error`
# (asyncpg PostgresIOError, wrapped by SQLAlchemy). We cannot repair the disk, so the pipeline must
# TREAT these as skippable: catch the error on the affected unit (file / stitch window / read), log
# and report it, and keep processing everything else. This module centralises the detection so every
# stage classifies the error the same way. See docs/load-testing-and-dimensioning.md and the
# disk-io-resilience plan.

import re

# Substrings that identify a failing-disk fault we should skip-and-continue on. Two kinds:
#  1. a device/page READ failure (a dead sector), and
#  2. a statement timeout — on this degraded disk a large INSERT/query can crawl past the
#     statement_timeout because of bad-sector retries + saturation, and Postgres cancels it.
# Both mean "this unit couldn't complete because of the disk"; treat them the same (skip + report).
_IO_SIGNATURES = (
    "could not read block",     # Postgres, the exact one we see
    "input/output error",       # the OS EIO underneath
    "unrecovered read error",   # kernel/SCSI medium error text, if surfaced
    "medium error",
    "invalid page",             # a corrupt (not just unreadable) page
    "canceling statement due to statement timeout",  # slow disk -> the statement was cancelled
)

_BLOCK_RE = re.compile(r'could not read block (\d+) in file "([^"]+)"')


def is_disk_io_error(exc: BaseException) -> bool:
    """True if the exception (or anything in its cause chain) is a disk read / bad-sector failure.

    Deliberately broad: SQLAlchemy folds the driver's message into str(exc), so a substring match on
    the full text is the most robust signal across asyncpg/SQLAlchemy wrapping. Also checks the
    exception class names in the chain as a backstop.
    """
    seen = 0
    e: BaseException | None = exc
    while e is not None and seen < 6:
        text = str(e).lower()
        if any(sig in text for sig in _IO_SIGNATURES):
            return True
        if type(e).__name__ in ("PostgresIOError", "DiskError"):
            return True
        # Walk down the wrapper chain (SQLAlchemy .orig, or a normal __cause__/__context__).
        nxt = getattr(e, "orig", None)
        if nxt is None or nxt is e:
            nxt = e.__cause__ or e.__context__
        e = nxt
        seen += 1
    return False


def disk_io_detail(exc: BaseException) -> str:
    """A short locator for logs/labels: the 'block N in file X' for a read failure, a note for a
    slow-disk statement timeout, or a trimmed message otherwise."""
    text = str(exc)
    m = _BLOCK_RE.search(text)
    if m:
        return f"block {m.group(1)} in file {m.group(2)}"
    if "statement timeout" in text.lower():
        return "statement timeout (disk too slow to finish in time)"
    return text.splitlines()[0][:200]
