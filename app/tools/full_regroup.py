"""Chunk 69: run one tracked FULL rebuild in its own process.

    PYTHONPATH=<repo> python -m app.tools.full_regroup <run_id>

Spawned by `POST /logs/regroup/full`; also runnable by hand against an existing run row, which is
exactly how a rebuild interrupted by a service restart is resumed (mark the stale row failed, create
a fresh one, run this). A separate process because a full-history grouping is tens of minutes of pure
Python CPU - inside any shared event loop it would freeze everything else that loop hosts.
"""

import asyncio
import sys
import uuid

from app.services.mnp_log_ingestion.pipeline.derive_transactions import run_full_regroup_tracked


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m app.tools.full_regroup <run_id>")
    asyncio.run(run_full_regroup_tracked(uuid.UUID(sys.argv[1])))


if __name__ == "__main__":
    main()
