"""Repair after chunk 211: refill the `pick line` lookup and re-settle the releases since a date.

From 2 October 2026 19:00 to the deploy of chunk 211 the live fold harvested no list lookup, so the
`pick line` lookup (planned location, delivery, expected quantity, status) has nothing for those
weeks, and every `pick_release` row settled since then has no planned location. This script fills the
lookup from the stored facts and their raw response entries (kept 60 days), then re-settles every
release with a call since the date, upserting in place so a reader never sees a row missing.

    PYTHONPATH=. venv/bin/python scripts/repair_pick_line_since.py tmp-live 2026-10-02T17:00:00+00:00
    PYTHONPATH=. venv/bin/python scripts/repair_pick_line_since.py tmp-live 2026-10-02T17:00:00+00:00 --write

Without `--write` it only counts: how many facts the backfill would read and how many releases it
would re-settle. Run it AFTER chunk 211 is deployed, or the fold keeps leaving new gaps.
"""

import argparse
import asyncio
import math
import time
from datetime import datetime, timezone

from app.config.database import async_session
from app.services.analytics import lookup_store, settle_store

LOOKUP = "pick line"
SETTLEMENT = "pick_release"
#: Keys settled per transaction, as `resettle_all` does, so no transaction stays open for long.
BATCH = 500


async def repair(customer_code: str, since: datetime, *, write: bool) -> None:
    """Refill the lookup since `since`, then re-settle the releases since then, one batch per commit.

    (tmp-live, 2 October, write=False) -> prints the counts and writes nothing
    """
    # Step 1: The declarations, so a missing lookup or settlement stops the run before any write
    days = math.ceil((datetime.now(timezone.utc) - since).total_seconds() / 86400)
    async with async_session() as db:
        declared = (await lookup_store.load(db, customer_code, enabled_only=False)).get(LOOKUP)
        settlement = (await settle_store.load(db, customer_code)).get(SETTLEMENT)
        if declared is None or settlement is None:
            raise SystemExit(f"{customer_code} has no {LOOKUP!r} lookup or no enabled {SETTLEMENT!r} settlement")
        keys = await settle_store.keys_since(db, customer_code, settlement, since)
    print(f"lookup {LOOKUP!r}: backfill {days} day(s) · settlement {SETTLEMENT!r}: {len(keys)} release(s) since {since.isoformat()}")
    if not write:
        print("dry run: nothing written (add --write)")
        return
    # Step 2: The lookup first, because the releases read their planned location from it when settled
    t0 = time.monotonic()
    async with async_session() as db:
        report = await lookup_store.backfill(db, customer_code, declared, days=days)
        await db.commit()
    print(f"backfill: {report} in {time.monotonic() - t0:.1f} s")
    # Step 3: The releases in batches, each committed on its own so the vacuum horizon is never pinned
    written = 0
    for i in range(0, len(keys), BATCH):
        async with async_session() as db:
            written += await settle_store.settle_keys(db, customer_code, settlement, keys[i:i + BATCH])
            await db.commit()
        print(f"re-settled {written} of {len(keys)} ({time.monotonic() - t0:.1f} s)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("customer_code")
    parser.add_argument("since", type=datetime.fromisoformat, help="aware ISO time, e.g. 2026-10-02T17:00:00+00:00")
    parser.add_argument("--write", action="store_true", help="write; without it the run only counts")
    args = parser.parse_args()
    if args.since.tzinfo is None:
        raise SystemExit("since must carry its offset, e.g. +00:00")
    asyncio.run(repair(args.customer_code, args.since, write=args.write))


if __name__ == "__main__":
    main()
