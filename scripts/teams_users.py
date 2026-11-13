"""The people of a customer in the eSmart Eye Teams app, from the server (stage D, step 7).

    PYTHONPATH=. venv/bin/python scripts/teams_users.py list tmp-live
    PYTHONPATH=. venv/bin/python scripts/teams_users.py set tmp-live <object id> "Amin Talukder" \\
        --role admin --all-sites --email amin@example.com
    PYTHONPATH=. venv/bin/python scripts/teams_users.py set tmp-live <object id> "Jo Bloggs" \\
        --role supervisor --sites BRI,BIC
    PYTHONPATH=. venv/bin/python scripts/teams_users.py deactivate tmp-live <object id>
    PYTHONPATH=. venv/bin/python scripts/teams_users.py matrix tmp-live

`set` creates the user when new and changes them otherwise; it is how the first Admin of a
customer is made at initialisation. Every change is audited as the superadmin and the customer's
access rows are mirrored to the edge at once. The object id is the person's Entra object id, as
the edge logs it (`tab.intake ... sender=`).
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config.database import async_session, engine  # noqa: E402
from app.services.access import mirror as access_mirror  # noqa: E402
from app.services.access import records, users  # noqa: E402
from app.services.teams.mirror import build_mirror_writer_from_settings  # noqa: E402


async def list_users(customer_code: str) -> None:
    async with async_session() as db:
        rows = await users.list_users(db, customer_code)
        if not rows:
            print(f"no users for {customer_code}")
        for user in rows:
            record = await records.record_for_user(db, user)
            sites = "all sites" if user.all_sites else ", ".join(record.site_ids) or "no sites"
            print(f"{user.object_id}  {user.display_name!r}  {user.role_code}  {user.status}  {sites}")


async def set_user(customer_code: str, object_id: str, name: str, *, role: str, email: str | None,
                   all_sites: bool, sites: list[str] | None, actor: str) -> None:
    change = users.UserChange(display_name=name, email=email, role_code=role, all_sites=all_sites,
                              site_ids=tuple(sites) if sites is not None else None, status="active")
    async with async_session() as db:
        try:
            if await users.get_user(db, customer_code, object_id) is None:
                await users.create_user(db, customer_code, object_id, change=change,
                                        actor=users.Actor.system(actor))
                verb = "created"
            else:
                await users.update_user(db, customer_code, object_id, change=change,
                                        actor=users.Actor.system(actor))
                verb = "updated"
            await db.commit()
        except ValueError as exc:
            raise SystemExit(f"refused: {exc}")
        await _mirror(db, customer_code)
    print(f"{verb} {object_id} as {role} for {customer_code}")


async def deactivate(customer_code: str, object_id: str, *, actor: str) -> None:
    async with async_session() as db:
        try:
            await users.update_user(db, customer_code, object_id,
                                    change=users.UserChange(status="deactivated"),
                                    actor=users.Actor.system(actor))
            await db.commit()
        except ValueError as exc:
            raise SystemExit(f"refused: {exc}")
        await _mirror(db, customer_code)
    print(f"deactivated {object_id} for {customer_code}")


async def show_matrix(customer_code: str) -> None:
    async with async_session() as db:
        payload = await access_mirror.roles_payload(db, customer_code)
        await db.commit()
    roles = [r["code"] for r in payload["roles"]]
    print("capability".ljust(18) + "".join(r.ljust(14) for r in roles))
    for cap in payload["capabilities"]:
        cells = payload["matrix"][cap["code"]]
        print(cap["code"].ljust(18) + "".join(cells[r].ljust(14) for r in roles))


async def _mirror(db, customer_code: str) -> None:
    """Mirror the customer's access rows now; a failure is a line, never a crash, because the row
    is already saved and the consumer's sweep mirrors it within a minute anyway."""
    mirror = build_mirror_writer_from_settings()
    if mirror is None:
        print("(no mirror table configured; the consumer's sweep writes the edge rows)")
        return
    try:
        written = await access_mirror.mirror_customer(db, mirror, customer_code)
        await db.commit()
    except Exception as exc:  # noqa: BLE001 - the shell may lack the service's AWS credentials
        print(f"saved; not mirrored from this shell ({type(exc).__name__}: {str(exc)[:120]}). "
              "The consumer's sweep mirrors it within a minute.")
        return
    print(f"mirrored {written} access row(s) to the edge")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("list", help="the customer's users")
    p.add_argument("customer_code")
    p = sub.add_parser("set", help="create or change one user")
    p.add_argument("customer_code")
    p.add_argument("object_id")
    p.add_argument("name")
    p.add_argument("--role", required=True)
    p.add_argument("--email")
    p.add_argument("--all-sites", action="store_true")
    p.add_argument("--sites", help="comma-separated site ids, e.g. BRI,BIC")
    p.add_argument("--actor", default="superadmin")
    p = sub.add_parser("deactivate", help="switch one user off")
    p.add_argument("customer_code")
    p.add_argument("object_id")
    p.add_argument("--actor", default="superadmin")
    p = sub.add_parser("matrix", help="the customer's matrix")
    p.add_argument("customer_code")
    args = parser.parse_args()

    async def run() -> None:
        try:
            if args.command == "list":
                await list_users(args.customer_code)
            elif args.command == "set":
                sites = [s.strip() for s in args.sites.split(",") if s.strip()] if args.sites else None
                await set_user(args.customer_code, args.object_id, args.name, role=args.role,
                               email=args.email, all_sites=args.all_sites, sites=sites, actor=args.actor)
            elif args.command == "deactivate":
                await deactivate(args.customer_code, args.object_id, actor=args.actor)
            elif args.command == "matrix":
                await show_matrix(args.customer_code)
        finally:
            await engine.dispose()

    asyncio.run(run())


if __name__ == "__main__":
    main()
