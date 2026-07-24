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

# Substrings that identify a device/page read failure (lower-cased match against the whole chain).
_IO_SIGNATURES = (
    "could not read block",     # Postgres, the exact one we see
    "input/output error",       # the OS EIO underneath
    "unrecovered read error",   # kernel/SCSI medium error text, if surfaced
    "medium error",
    "invalid page",             # a corrupt (not just unreadable) page
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
    """A short 'block N in file X' locator for logs/labels, or a trimmed message if not parseable."""
    m = _BLOCK_RE.search(str(exc))
    if m:
        return f"block {m.group(1)} in file {m.group(2)}"
    return str(exc).splitlines()[0][:200]
