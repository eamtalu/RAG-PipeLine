"""Say which WMS company and warehouse pairs a site stands for.

    PYTHONPATH=. venv/bin/python scripts/site_places.py list tmp-live
    PYTHONPATH=. venv/bin/python scripts/site_places.py set tmp-live BRI 915:BRI
    PYTHONPATH=. venv/bin/python scripts/site_places.py set tmp-test BRI 911:BRI 920:BRI
    PYTHONPATH=. venv/bin/python scripts/site_places.py set tmp-live BRI --clear

A pair is `company:warehouse`; a bare `warehouse` means any company. A site with no pair stands
for the warehouse of its own id under any company. `set` replaces the site's pairs and writes one
line to the access audit log. The Teams screen for sites comes with the Admin pages; until then
this is how an operator sets the pairs.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from app.config.database import async_session
from app.persistence.repositories.site_repository import Pair, SiteRepository
from app.services import access

ACTOR = "script:site_places"


def parse_pair(text: str) -> Pair:
    """One `company:warehouse` argument as a pair; no company part means any company.

    >>> parse_pair("915:BRI"), parse_pair("BRI"), parse_pair(":BRI")
    (('915', 'BRI'), (None, 'BRI'), (None, 'BRI'))
    >>> parse_pair("915:")
    Traceback (most recent call last):
    ...
    ValueError: '915:' names no warehouse; write company:warehouse or warehouse
    """
    company, _, warehouse = text.rpartition(":")
    if not warehouse.strip():
        raise ValueError(f"{text!r} names no warehouse; write company:warehouse or warehouse")
    return (company.strip() or None, warehouse.strip())


def pair_text(pair: Pair) -> str:
    """A pair as it is typed: ('915', 'BRI') -> '915:BRI' ; (None, 'BRI') -> 'any:BRI'.

    >>> pair_text(("915", "BRI")), pair_text((None, "BRI"))
    ('915:BRI', 'any:BRI')
    """
    return f"{pair[0] or 'any'}:{pair[1]}"


async def list_places(customer_code: str) -> list[str]:
    """Every site of a customer with the pairs it stands for, one line each.

    ("tmp-live") -> ["BRI  Brighton  915:BRI", "LON  London  any:LON (default)"]
    """
    async with async_session() as db:
        repo = SiteRepository(db)
        sites = await repo.list_all(customer_code)
        places = await repo.places_of(customer_code, [site.site_id for site in sites])
    return [f"{site.site_id}  {site.name}  "
            + (" ".join(pair_text(pair) for pair in places[site.site_id])
               if site.site_id in places else f"any:{site.site_id} (default)")
            for site in sites]


async def set_places(customer_code: str, site_id: str, pairs: list[Pair]) -> list[Pair]:
    """Replace a site's pairs and audit it; ValueError for a site the customer does not have.

    ("tmp-live", "BRI", [("915", "BRI")]) -> [("915", "BRI")] ; ([]) -> [] (the default again)
    """
    async with async_session() as db:
        saved = await SiteRepository(db).set_places(customer_code, site_id, pairs, actor=ACTOR)
        text = (f"site {site_id} now stands for {', '.join(pair_text(p) for p in saved)}"
                if saved
                else f"site {site_id} stands for its own warehouse under any company again")
        await access.record_audit_event(
            db, customer_code, actor_object_id=ACTOR, actor_kind="service",
            action="sites.places_set", decision="allowed", site_id=site_id, text=text,
            detail={"places": [list(pair) for pair in saved]})
        await db.commit()
    return saved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list").add_argument("customer")
    setter = commands.add_parser("set")
    setter.add_argument("customer")
    setter.add_argument("site_id")
    setter.add_argument("pairs", nargs="*", help="company:warehouse, or warehouse for any company")
    setter.add_argument("--clear", action="store_true", help="remove the site's pairs")
    return parser


async def run(args: argparse.Namespace) -> int:
    """Carry out one command; 0 when done, 2 when the arguments make no sense."""
    if args.command == "list":
        print("\n".join(await list_places(args.customer)) or "no sites")
        return 0
    if bool(args.pairs) == bool(args.clear):
        print("give at least one pair, or --clear, not both", file=sys.stderr)
        return 2
    try:
        saved = await set_places(args.customer, args.site_id, [parse_pair(p) for p in args.pairs])
    except ValueError as problem:
        print(str(problem), file=sys.stderr)
        return 2
    print(f"{args.site_id}: " + (" ".join(pair_text(p) for p in saved) or "default"))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run(build_parser().parse_args())))
