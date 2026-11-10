"""The two versions stamped on every result: the profile file's hash and the code's git hash."""

from __future__ import annotations

import hashlib
import subprocess
from functools import cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def file_version(data: bytes) -> str:
    """A short, stable name for exactly these bytes; one changed byte gives another name.

    >>> file_version(b"name: analytics\\n")
    '6e0f33532c10'
    >>> file_version(b"name: analytics\\n") != file_version(b"name: Analytics\\n")
    True
    """
    return hashlib.sha256(data).hexdigest()[:12]


@cache
def code_version() -> str:
    """The short git hash of the running code, read once per process; "unknown" without git.

    (a checkout) -> "1f91180" ; (no git binary, no .git) -> "unknown"
    """
    try:
        done = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.TimeoutExpired, subprocess.CalledProcessError):
        return "unknown"
    return done.stdout.strip() if done.returncode == 0 and done.stdout.strip() else "unknown"
