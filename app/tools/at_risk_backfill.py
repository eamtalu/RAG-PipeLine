"""Deliveries at risk: write the closed rows of past days from the logs (chunk 151).

    PYTHONPATH=<repo> python -m app.tools.at_risk_backfill <customer_code> <start YYYY-MM-DD> <end YYYY-MM-DD>

Run by hand on the server once, after the feature is switched on, so the history has the days before
the departure fields were approved. Safe to re-run: a delivery that already has a row for that
departure date is left alone, live or reconstructed. Each day is committed on its own, so an
interrupted run resumes by running it again. A day whose deliveries have not all closed is skipped.
"""

import asyncio
import sys
from datetime import date

from app.services.analytics_at_risk.backfill import backfill_tenant


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: python -m app.tools.at_risk_backfill <customer_code> <start YYYY-MM-DD> <end YYYY-MM-DD>")
    cc, start, end = sys.argv[1], date.fromisoformat(sys.argv[2]), date.fromisoformat(sys.argv[3])
    out = asyncio.run(backfill_tenant(cc, start=start, end=end))
    for day in out["days"]:
        if "skipped" in day:
            print(f'{day["date"]}  skipped: {day["skipped"]}')
        else:
            print(f'{day["date"]}  deliveries {day["deliveries"]:>4}  written {day["written"]:>4}  existing {day["existing"]:>4}  '
                  f'missed {day["missed"]:>3}  delayed {day["delayed"]:>3}  fine {day["fine"]:>3}  unreadable calls {day["unreadable"]}')


if __name__ == "__main__":
    main()
