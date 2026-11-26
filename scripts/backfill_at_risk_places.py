"""Give older at-risk deliveries, and their checks, the company and warehouse of their route rows.

    PYTHONPATH=. venv/bin/python scripts/backfill_at_risk_places.py tmp-live --dry-run
    PYTHONPATH=. venv/bin/python scripts/backfill_at_risk_places.py tmp-live

A delivery takes its place from its newest settled route row. A delivery that already has a
warehouse keeps it and gains only the company; one with no settled route row is left as it is.
Safe to run again: it only fills what is still empty.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from app.config.database import async_session
from app.services.analytics_at_risk import place_backfill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("customer_code")
    parser.add_argument("--settlement", default=None, help="the route settlement's name")
    parser.add_argument("--dry-run", action="store_true", help="count, write nothing")
    return parser


async def run(args: argparse.Namespace) -> int:
    filled = await place_backfill.fill(async_session, args.customer_code,
                                       settlement=args.settlement, dry_run=args.dry_run)
    word = "would fill" if args.dry_run else "filled"
    print(f"{args.customer_code}: {word} {filled.deliveries} deliveries and {filled.checks} checks")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
