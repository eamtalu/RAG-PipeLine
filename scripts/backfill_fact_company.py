"""Fill the WMS company on facts and ledger versions written before chunk 226.

    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live --dry-run
    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live
    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live \
        --table analytics_fact_ledger --batch 2000 --pause 1 --vacuum

A row takes the company of its source transaction. A row whose transaction is gone takes the
customer's only company for its warehouse, and stays empty when there are two. Each batch is its
own transaction, so the run can be stopped with Ctrl-C at any time and started again: it picks up
the rows that still lack a company. `--vacuum` cleans each partition of the table afterwards,
because an update leaves the old copy of every row behind.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

from sqlalchemy import text

from app.config.database import async_session, engine
from app.services.analytics import company_backfill as backfill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("customer_code")
    parser.add_argument("--table", choices=backfill.TABLES, default=backfill.TABLES[0])
    parser.add_argument("--batch", type=int, default=5000, help="rows per transaction")
    parser.add_argument("--pause", type=float, default=0.5, help="seconds between batches")
    parser.add_argument("--max-batches", type=int, default=None, help="stop after this many")
    parser.add_argument("--dry-run", action="store_true", help="count, write nothing")
    parser.add_argument("--vacuum", action="store_true", help="vacuum the partitions afterwards")
    return parser


async def vacuum(table: str) -> None:
    """Vacuum every partition of the table, outside a transaction and with no statement timeout."""
    async with async_session() as db:
        partitions = await backfill.partitions_of(db, table)
    async with engine.connect() as raw:
        conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("SET statement_timeout = 0"))
        for partition in partitions:
            started = time.monotonic()
            await conn.execute(text(f"VACUUM (ANALYZE) {partition}"))
            print(f"vacuumed {partition} in {time.monotonic() - started:.1f}s", flush=True)


async def run(args: argparse.Namespace) -> int:
    started, last = time.monotonic(), [time.monotonic()]

    def report(partition: str, done: backfill.Batch) -> None:
        now = time.monotonic()
        print(f"{partition}: read {done.read}, filled {done.filled}, {now - last[0]:.2f}s",
              flush=True)
        last[0] = now

    totals = await backfill.fill_table(
        async_session, args.table, args.customer_code, batch=args.batch, pause_seconds=args.pause,
        dry_run=args.dry_run, max_batches=args.max_batches, on_batch=report)
    word = "would fill" if args.dry_run else "filled"
    print(f"{args.table} for {args.customer_code}: read {totals.read}, {word} {totals.filled}, "
          f"left empty {totals.read - totals.filled}, {totals.batches} batches, "
          f"{'finished' if totals.is_finished else 'stopped early'}, "
          f"{time.monotonic() - started:.1f}s")
    if args.vacuum and not args.dry_run and totals.is_finished:
        await vacuum(args.table)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
