"""The warehouses (sites) of a customer for the eSmart Eye Teams app (stage D, step 4).

    PYTHONPATH=. .venv/bin/python scripts/teams_sites.py discover tmp-live
    PYTHONPATH=. .venv/bin/python scripts/teams_sites.py set tmp-live 1 Brighton --order 1
    PYTHONPATH=. .venv/bin/python scripts/teams_sites.py list tmp-live
    PYTHONPATH=. .venv/bin/python scripts/teams_sites.py remove tmp-live 1

`discover` prints the distinct `warehouse` values on the customer's settled pick-release rows and on
its facts, with counts, so the site ids are taken from the data and never assumed. A settled row
carries the warehouse only when the settlement's `carry` list names it: when the facts have values
and the settled rows have none, add "warehouse" to the pick_release settlement's carry list and
backfill before configuring sites (docs/stage-d-deploy-runbook.md, D4).
"""

import argparse
import asyncio

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.persistence.repositories.site_repository import SiteRepository


async def discover(customer_code: str) -> None:
    """Print the warehouse values the data carries, settled rows first, then facts.

    (tmp-live with BRI on 4,120 rows) -> "settled rows: BRI 4120" and the facts' counts
    """
    async with async_session() as db:
        for label, model in (("settled rows", AnalyticsSettledRow), ("facts", AnalyticsFact)):
            stmt = (select(model.warehouse, func.count()).where(model.customer_code == customer_code)
                    .group_by(model.warehouse).order_by(func.count().desc()))
            rows = (await db.execute(stmt)).all()
            print(f"{label}:")
            for value, count in rows:
                print(f"  {value if value is not None else '<none>'}  {count}")
            if not rows:
                print("  (no rows)")


async def list_sites(customer_code: str) -> None:
    async with async_session() as db:
        for site in await SiteRepository(db).list_all(customer_code):
            flag = "" if site.active else "  (inactive)"
            print(f"{site.sort_order:>3}  {site.site_id:<8} {site.name}{flag}")


async def set_site(customer_code: str, site_id: str, name: str, *, order: int, inactive: bool) -> None:
    async with async_session() as db:
        try:
            await SiteRepository(db).upsert(customer_code, site_id, name, sort_order=order, active=not inactive)
            await db.commit()
        except IntegrityError:
            raise SystemExit(f"{customer_code} is not a customer; a site belongs to an existing log space") from None
    print(f"{customer_code}: site {site_id} = {name!r}, order {order}{' (inactive)' if inactive else ''}")


async def remove_site(customer_code: str, site_id: str) -> None:
    async with async_session() as db:
        await SiteRepository(db).remove(customer_code, site_id)
        await db.commit()
    print(f"{customer_code}: site {site_id} removed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("discover", help="the warehouse values the customer's data carries")
    p.add_argument("customer_code")
    p = sub.add_parser("list", help="the configured sites")
    p.add_argument("customer_code")
    p = sub.add_parser("set", help="create or update one site")
    p.add_argument("customer_code")
    p.add_argument("site_id", help="the warehouse value as it appears on the rows")
    p.add_argument("name")
    p.add_argument("--order", type=int, default=0)
    p.add_argument("--inactive", action="store_true")
    p = sub.add_parser("remove", help="delete one site")
    p.add_argument("customer_code")
    p.add_argument("site_id")
    args = parser.parse_args()
    if args.command == "discover":
        asyncio.run(discover(args.customer_code))
    elif args.command == "list":
        asyncio.run(list_sites(args.customer_code))
    elif args.command == "set":
        asyncio.run(set_site(args.customer_code, args.site_id, args.name, order=args.order, inactive=args.inactive))
    else:
        asyncio.run(remove_site(args.customer_code, args.site_id))


if __name__ == "__main__":
    main()
