"""One-off repair for alerts stored before chunk 131 carried secrets (M3Credentials in error texts):

    python -m app.redact_notification_events --customer tmp-live            # dry run: ids and counts
    python -m app.redact_notification_events --customer tmp-live --apply    # write

Cards already posted to a channel are not touched by this; delete those in the channel itself.
"""

import argparse
import asyncio

from app.background import setup_logging
from app.config.database import async_session, engine
from app.services.notifications.redact_store import redact_stored_events


async def _amain(args) -> None:
    setup_logging()
    try:
        async with async_session() as db:
            result = await redact_stored_events(db, args.customer, apply=args.apply)
            if args.apply:
                await db.commit()
        print(result)
    finally:
        await engine.dispose()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer", required=True)
    ap.add_argument("--apply", action="store_true", help="write the redaction (default: dry run)")
    asyncio.run(_amain(ap.parse_args()))


if __name__ == "__main__":
    main()
