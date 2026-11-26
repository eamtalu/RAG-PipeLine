"""Fill the WMS company on facts, ledger versions and settled rows written before they had one.

    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live --dry-run
    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live
    PYTHONPATH=. venv/bin/python scripts/backfill_fact_company.py tmp-live \
        --table analytics_fact_ledger --blocks 500 --pause 1 --vacuum

A row takes the company its own WMS call carried (`Company` or `CONO` in its attributes), which
is the value ingestion files the transaction under; a call that carried none stays empty. Each
batch is its own transaction, so the run can be stopped with Ctrl-C at any time and started
again: it picks up the rows that still lack a company. A settled row (`--table
analytics_settled_rows`) takes the customer's only company for its warehouse, from the facts.
`--vacuum` cleans the table afterwards, because an update leaves the old copy of every row
behind.
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
    parser.add_argument("--table",
                        choices=(*backfill.TABLES, backfill.SETTLED_ROWS, backfill.ADVISORIES),
                        default=backfill.TABLES[0])
    parser.add_argument("--blocks", type=int, default=1000,
                        help="table blocks (8 kB each) read per transaction")
    parser.add_argument("--pause", type=float, default=0.5, help="seconds between batches")
    parser.add_argument("--max-batches", type=int, default=None,
                        help="stop after this many batches that found rows to fill")
    parser.add_argument("--dry-run", action="store_true", help="count, write nothing")
    parser.add_argument("--vacuum", action="store_true", help="vacuum the partitions afterwards")
    return parser


async def vacuum(table: str) -> None:
    """Vacuum every partition of the table, outside a transaction and with no statement timeout."""
    async with async_session() as db:
        partitions = await backfill.partitions_of(db, table) or [table]
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

    if args.table == backfill.ADVISORIES:
        filled = await backfill.fill_advisories(async_session, args.customer_code,
                                                dry_run=args.dry_run)
        print(f"advisories for {args.customer_code}: "
              f"{'would fill' if args.dry_run else 'filled'} {filled}")
        return 0
    options = dict(blocks=args.blocks, pause_seconds=args.pause, dry_run=args.dry_run,
                   max_batches=args.max_batches, on_batch=report)
    if args.table == backfill.SETTLED_ROWS:
        totals = await backfill.fill_settled_rows(async_session, args.customer_code, **options)
    else:
        totals = await backfill.fill_table(async_session, args.table, args.customer_code, **options)
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
