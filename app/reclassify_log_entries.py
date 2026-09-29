"""One-off repair for entries stored before the parser knew the new log format (chunk 128):

    python -m app.reclassify_log_entries --customer tmp-live --since 2026-09-29T12:00:00+01:00 [--dry-run]

Re-parses the stamped request, response and M3 lines in place and tickets the window for the Stage 2
worker to re-stitch. Run it once per customer after deploying chunk 128; safe to run again (a row
already right is skipped).
"""

import argparse
import asyncio
from datetime import datetime

from app.background import setup_logging
from app.config.database import async_session, engine
from app.services.mnp_log_ingestion.pipeline.reclassify import reclassify_stamped


async def _amain(args) -> None:
    setup_logging()
    since = datetime.fromisoformat(args.since)
    until = datetime.fromisoformat(args.until) if args.until else None
    try:
        async with async_session() as db:
            result = await reclassify_stamped(db, args.customer, since=since, until=until, dry_run=args.dry_run)
        print(result)
    finally:
        await engine.dispose()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer", required=True)
    ap.add_argument("--since", required=True, help="ISO timestamp with offset, e.g. 2026-09-29T12:00:00+01:00")
    ap.add_argument("--until", default=None)
    ap.add_argument("--dry-run", action="store_true")
    asyncio.run(_amain(ap.parse_args()))


if __name__ == "__main__":
    main()
