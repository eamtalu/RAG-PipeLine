"""The developer's view of the agent's runs: list them, show one, rank what went wrong.

    PYTHONPATH=. venv/bin/python scripts/trace_review.py list tmp-live --days 2
    PYTHONPATH=. venv/bin/python scripts/trace_review.py list tmp-live --limited --role supervisor
    PYTHONPATH=. venv/bin/python scripts/trace_review.py show 9f3c2a...
    PYTHONPATH=. venv/bin/python scripts/trace_review.py findings tmp-live --days 14

Read-only: it reads `agent_traces` through the trace store and prints what
`app/services/agent_traces/review.py` makes of it. It proposes nothing and changes nothing.
`--json` prints the same as JSON for further work.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from app.config.database import async_session
from app.services.agent_traces import review
from app.services.agent_traces.store import PostgresTraceStore
from app.services.teams.sites import list_sites

FINDINGS = (("repeated_calls", "the same tool call made again"),
            ("round_limit", "ran out of tool rounds"),
            ("timeouts", "timed out"),
            ("limited_by_scope", "asked outside the asker's sites or hit a closed table"),
            ("retried", "answer redrafted once"),
            ("withheld", "figures withheld"),
            ("declined", "declined"),
            ("errors", "raised an error"))


def build_parser() -> argparse.ArgumentParser:
    """The three commands and their filters: list, show, findings."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("list", "findings"):
        sub = commands.add_parser(name)
        sub.add_argument("customer")
        sub.add_argument("--days", type=float, default=7.0, help="how far back to read (default 7)")
        sub.add_argument("--user", help="the asker's object id")
        sub.add_argument("--role", help="the asker's role at the time")
        sub.add_argument("--trigger", choices=("ask", "routine", "watch", "brief"))
        sub.add_argument("--stop", help="stop reason: end_turn, max_iterations, timeout, ...")
        sub.add_argument("--json", action="store_true")
    listing = commands.choices["list"]
    listing.add_argument("--tool", help="only runs that called this tool")
    listing.add_argument("--limited", action="store_true", help="only runs the scope limited")
    listing.add_argument("--limit", type=int, default=50, help="most lines to print (default 50)")
    show = commands.add_parser("show")
    show.add_argument("trace_id")
    show.add_argument("--json", action="store_true")
    return parser


async def _window(args: argparse.Namespace) -> tuple[list[dict[str, Any]], list[str]]:
    """The runs the filters select and the customer's site ids, read once.

    (list tmp-live --days 2 --role supervisor) -> (the supervisors' runs of two days, ["BRI", ...])
    """
    store = PostgresTraceStore(async_session)
    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    runs = await store.all_runs(args.customer, since=since, principal_id=args.user,
                                role_code=args.role, trigger=args.trigger, stop_reason=args.stop)
    async with async_session() as db:
        sites = [site.site_id for site in await list_sites(db, args.customer)]
    return runs, sites


def _print_findings(found: dict[str, Any]) -> None:
    print(f"{found['runs']} runs")
    for key, label in FINDINGS:
        entry = found[key]
        print(f"  {entry['runs']:>5}  {label:<55} {' '.join(entry['examples'])}")
    for title, key in (("tool errors", "tool_errors"), ("slowest tools", "slowest_tools"),
                       ("by prompt version", "by_prompt_version")):
        print(title)
        for row in found[key]:
            print("  " + "  ".join(f"{k}={v}" for k, v in row.items()))


async def run(args: argparse.Namespace) -> int:
    """Carry out one command; 0 when it printed something, 1 when the run to show is unknown."""
    if args.command == "show":
        trace = await PostgresTraceStore(async_session).get(args.trace_id)
        if trace is None:
            print(f"no run with trace id {args.trace_id}", file=sys.stderr)
            return 1
        print(json.dumps(trace, default=str, indent=2) if args.json
              else "\n".join(review.timeline(trace)))
        return 0
    runs, sites = await _window(args)
    if args.command == "findings":
        found = review.findings(runs, sites)
        print(json.dumps(found, default=str, indent=2)) if args.json else _print_findings(found)
        return 0
    if args.tool:
        runs = [r for r in runs if any(c["name"] == args.tool for c in r.get("tool_calls") or [])]
    if args.limited:
        runs = [r for r in runs if review.is_limited_by_scope(r, sites)]
    runs = runs[:max(1, args.limit)]
    if args.json:
        print(json.dumps(runs, default=str, indent=2))
    else:
        print("\n".join(review.list_line(r, sites) for r in runs) or "no runs")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
