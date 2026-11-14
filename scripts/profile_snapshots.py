"""Regenerate `tests/profile_snapshots/*.json` after a prompt or fragment edit, or report the drift.

    PYTHONPATH=. .venv/bin/python scripts/profile_snapshots.py          # prints the drift, exit 1 when any
    PYTHONPATH=. .venv/bin/python scripts/profile_snapshots.py --write  # rewrites the files

The snapshot holds the composed system prompt (the profile's own text plus its fragments), the two
nudges with the blank line the runtime puts before them, the domain and the decline sentence; the
test in chunk 162 compares byte for byte, so this script is the only way the files should change.
"""

from __future__ import annotations

import difflib
import json
import sys
from pathlib import Path

from app.agent.profiles import load_profile, profile_names

SNAPSHOTS = Path(__file__).resolve().parents[1] / "tests" / "profile_snapshots"


def snapshot_of(name: str) -> dict[str, str]:
    """The snapshot dict for one profile, in the shape chunk 162 reads.

    ("analytics") -> {"system_prompt": ..., "retry_nudge": "\\n\\n(...)", ...}
    """
    profile = load_profile(name)
    return {
        "system_prompt": profile.system_prompt,
        "retry_nudge": "\n\n" + profile.retry_nudge,
        "ungrounded_nudge": "\n\n" + profile.ungrounded_nudge,
        "domain": profile.domain,
        "decline": profile.decline,
    }


def rendered(name: str) -> str:
    return json.dumps(snapshot_of(name), indent=2) + "\n"


def write_snapshots(directory: Path = SNAPSHOTS) -> list[Path]:
    """Write one file per profile and return the paths written.

    (tmp) -> [tmp/analytics.json, tmp/logspace.json]
    """
    written = []
    for name in profile_names():
        path = directory / f"{name}.json"
        path.write_text(rendered(name))
        written.append(path)
    return written


def drift() -> list[str]:
    """One unified diff per profile whose snapshot differs from the loaded profile.

    (all in step) -> [] ; (a prompt edited) -> ["--- analytics.json ..."]
    """
    out = []
    for name in profile_names():
        path = SNAPSHOTS / f"{name}.json"
        have = path.read_text() if path.is_file() else ""
        want = rendered(name)
        if have != want:
            out.append("".join(difflib.unified_diff(have.splitlines(True), want.splitlines(True), f"{name}.json", f"{name} (loaded)")))
    return out


def main(argv: list[str]) -> int:
    if "--write" in argv:
        for path in write_snapshots():
            print(f"wrote {path}")
        return 0
    diffs = drift()
    for text in diffs:
        print(text)
    print("snapshots in step" if not diffs else f"{len(diffs)} snapshot(s) drift; run with --write")
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
