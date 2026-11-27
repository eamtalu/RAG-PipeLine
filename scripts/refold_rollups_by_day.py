"""Rebuild stored rollups from facts, one local day at a time, watching the disk.

    PYTHONPATH=. venv/bin/python scripts/refold_rollups_by_day.py tmp-live 2026-09-03 2026-11-27
    PYTHONPATH=. venv/bin/python scripts/refold_rollups_by_day.py tmp-live 2026-09-03 2026-09-10 \\
        --pause 10 --max-write-wait-ms 100

For each day it queues one refold ticket, waits until the fact writer has consumed it, and reads
the disk's write wait before the next day. It stops when that wait is above the limit, or when a
ticket is not consumed in time; run it again from the day it names. Safe to repeat: a refold
recomputes a day's rows from its facts.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import date
from pathlib import Path

from app.config.database import async_session
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.analytics import rollup_refold as refold

DISKSTATS = Path("/proc/diskstats")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("customer_code")
    parser.add_argument("start", type=date.fromisoformat)
    parser.add_argument("end", type=date.fromisoformat)
    parser.add_argument("--pause", type=float, default=5.0, help="seconds between days")
    parser.add_argument("--max-write-wait-ms", type=float, default=200.0,
                        help="stop when a disk write waits longer than this on average")
    parser.add_argument("--device", default="sda", help="the disk to watch in /proc/diskstats")
    parser.add_argument("--sample", type=float, default=2.0, help="seconds per disk reading")
    parser.add_argument("--timeout", type=float, default=300.0,
                        help="seconds to wait for the fact writer to consume one day")
    return parser


async def run(args: argparse.Namespace) -> int:
    async def read_write_wait() -> float | None:
        if not DISKSTATS.exists():
            return None
        before = DISKSTATS.read_text()
        await asyncio.sleep(args.sample)
        return refold.write_wait_ms(before, DISKSTATS.read_text(), args.device)

    async def wait_until_consumed() -> bool:
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            if await refold.open_tickets(async_session, args.customer_code) == 0:
                return True
            await asyncio.sleep(2.0)
        return False

    started, last = time.monotonic(), [time.monotonic()]

    def report(day: date, waited: float | None) -> None:
        now = time.monotonic()
        disk = "n/a" if waited is None else f"{waited:.1f} ms"
        print(f"{day}: refolded in {now - last[0]:.1f}s, disk write wait before it {disk}",
              flush=True)
        last[0] = now

    async with async_session() as db:
        tz = await get_customer_timezone(db, args.customer_code)
    progress = await refold.refold_range(
        async_session, args.customer_code, start=args.start, end=args.end, tz=tz,
        max_write_wait_ms=args.max_write_wait_ms, read_write_wait=read_write_wait,
        wait_until_consumed=wait_until_consumed, pause_seconds=args.pause, on_day=report)
    print(f"{args.customer_code}: {len(progress.days_done)} days refolded in "
          f"{time.monotonic() - started:.0f}s")
    if progress.stopped_at is not None:
        print(f"STOPPED before {progress.stopped_at}: {progress.reason}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
