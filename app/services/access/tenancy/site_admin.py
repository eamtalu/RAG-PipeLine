"""An Admin sets up the customer's sites and their places from Teams (program step 4).

A site is a named set of WMS company and warehouse pairs; it is what a person's access and
every read are limited by. This module is what the Admin tab's "Sites" section reads and
changes: the sites with their places, the pairs the data really carries (so a place is ticked
from a list and never typed), and two commands, `site_set` and `site_places_set`. A site is
retired, never deleted, because history refers to it. The actor's own record decides; every
change is one audit line. Used by app/services/teams/commands.py and the consumer's sweep.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.tenant_site import TenantSite
from app.persistence.repositories.site_repository import Pair, SiteRepository
from app.services.access import audit
from app.services.access.authorization import records
from app.services.teams.command_outcome import CommandOutcome, Refused
from app.services.teams.contracts import QuestionJob
from app.services.teams.mirror import (ChangeTracker, MirrorWriter,
                                       build_mirror_writer_from_settings, envelope,
                                       write_if_changed)

logger = logging.getLogger(__name__)

KINDS = ("site_set", "site_places_set")
CAPABILITY = "manage_users"
ROW_TYPE = "site_admin"
#: How far back the data is read for the pairs an Admin can tick.
PAIR_DAYS = 30
#: The most pairs offered; a customer with more has something else to sort out first.
MAX_PAIRS = 200
_SITE_ID = re.compile(r"^[A-Z0-9][A-Z0-9_-]{0,15}$")
ALL_SITES_ID = "all"


async def pairs_seen(db: AsyncSession, customer_code: str, *, now: datetime | None = None,
                     days: int = PAIR_DAYS) -> list[tuple[str | None, str, int]]:
    """The company and warehouse pairs on the customer's recent transactions, busiest first.

    (30 days with 3 calls at 915/BRI and 2 at 915/LON) -> [("915", "BRI", 3), ("915", "LON", 2)]
    ; calls with no warehouse are left out, they belong to no place
    """
    since = (now or datetime.now(timezone.utc)) - timedelta(days=days)
    calls = func.count().label("calls")
    rows = (await db.execute(
        select(LogTransaction.company, LogTransaction.warehouse, calls)
        .where(LogTransaction.customer_code == customer_code,
               LogTransaction.started_at >= since, LogTransaction.warehouse.isnot(None))
        .group_by(LogTransaction.company, LogTransaction.warehouse)
        .order_by(calls.desc(), LogTransaction.warehouse, LogTransaction.company)
        .limit(MAX_PAIRS))).all()
    return [(company, warehouse, int(count)) for company, warehouse, count in rows]


def site_of_pair(pair: Pair, places: dict[str, list[Pair]], site_ids: list[str]) -> str | None:
    """The site a pair belongs to: by a site's places, else a site that is its own warehouse.

    >>> places = {"BRI": [("915", "BRI")]}
    >>> site_of_pair(("915", "BRI"), places, ["BRI", "LON"])
    'BRI'
    >>> site_of_pair(("920", "BRI"), places, ["BRI", "LON"]) is None   # BRI names its pairs
    True
    >>> site_of_pair(("915", "LON"), places, ["BRI", "LON"])           # LON has none: any company
    'LON'
    """
    company, warehouse = pair
    for site_id, pairs in places.items():
        if any(w == warehouse and (c is None or c == company) for c, w in pairs):
            return site_id
    return warehouse if warehouse in site_ids and warehouse not in places else None


async def overview(db: AsyncSession, customer_code: str, *, now: datetime | None = None) -> dict:
    """What the Sites section draws: every site with its places, and the pairs seen in the data
    with the site each belongs to.

    -> {"sites": [{site_id, name, active, sort_order, places, stands_for_own_warehouse}],
        "pairs": [{company, warehouse, transactions, site_id}], "unplaced": n, "days": 30}
    """
    repo = SiteRepository(db)
    sites = await repo.list_all(customer_code)
    places = await repo.places_of(customer_code, [s.site_id for s in sites])
    site_ids = [s.site_id for s in sites]
    pairs = [{"company": company, "warehouse": warehouse, "transactions": count,
              "site_id": site_of_pair((company, warehouse), places, site_ids)}
             for company, warehouse, count in await pairs_seen(db, customer_code, now=now)]
    return {
        "sites": [{"site_id": s.site_id, "name": s.name, "active": s.active,
                   "sort_order": s.sort_order,
                   "places": [{"company": c, "warehouse": w} for c, w in places.get(s.site_id, [])],
                   "stands_for_own_warehouse": s.site_id not in places} for s in sites],
        "pairs": pairs,
        # rows at these pairs are read by all-sites people only, until a site takes them
        "unplaced": sum(1 for p in pairs if p["site_id"] is None),
        "days": PAIR_DAYS,
    }


async def run(job: QuestionJob, *, session_factory=None, mirror: MirrorWriter | None = None,
              now: datetime | None = None) -> CommandOutcome:
    """Run one site command for the job's sender; the line the page shows.

    (site_set LON "London" by an Admin) -> "Site LON added · London" ; (by a supervisor) ->
    Refused ; (a pair of another site) -> Refused naming that site
    """
    command = job.command
    if command is None or command.kind not in KINDS:
        raise ValueError("the job carries no site command")
    now = now or datetime.now(timezone.utc)
    payload = dict(command.payload)
    async with (session_factory or async_session)() as db:
        # Step 1: The sender's own record decides, so a forged job cannot change what every
        # person of the customer is limited by
        record = await records.access_record_for(db, job.customer_code, job.sender_object_id)
        if record is None or not record.is_active:
            raise Refused("you have no active access record")
        if not records.decide(record, CAPABILITY).is_allowed:
            raise Refused("sites are set up by people who may manage users")
        site_id = str(payload.get("site_id") or "").strip()
        try:
            if command.kind == "site_set":
                action, text = await _set_site(db, job.customer_code, site_id, payload)
            else:
                action, text = await _set_places(db, job.customer_code, site_id, payload,
                                                 actor=record.object_id)
        except ValueError as exc:
            raise Refused(str(exc)) from None
        await audit.record_event(
            db, job.customer_code, actor_object_id=record.object_id, actor_name=job.sender_name,
            action=action, capability_code=CAPABILITY, decision="allowed", subject_kind="site",
            subject_id=site_id, site_id=site_id, request_id=job.job_id,
            text=f"{job.sender_name or record.object_id}: {text}", detail=payload, at=now)
        await db.commit()
        # Step 2: Mirror at once, so the Admin sees the change and the switcher offers the site
        writer = mirror if mirror is not None else build_mirror_writer_from_settings()
        if writer is not None:
            try:
                await mirror_customer(db, writer, job.customer_code, now=now)
            except Exception:
                logger.warning("site rows of %s not mirrored after %s", job.customer_code,
                               command.kind, exc_info=True)
    return CommandOutcome(text)


async def _set_site(db: AsyncSession, customer_code: str, site_id: str,
                    payload: dict) -> tuple[str, str]:
    """Add a site, or change its name, order or whether it is active; the audit action and line.

    (new LON "London") -> ("sites.site_added", "Site LON added · London")
    ; (LON active False) -> ("sites.site_retired", "Site LON retired · London")
    """
    if not _SITE_ID.match(site_id):
        raise ValueError("the site id is 1 to 16 capital letters or digits, as the WMS "
                         "writes the warehouse, for example BRI")
    row = await db.get(TenantSite, (customer_code, site_id))
    name = str(payload.get("name") or (row.name if row else "")).strip()
    if not name or len(name) > 64:
        raise ValueError("a site needs a name of at most 64 characters")
    is_active = bool(payload["active"]) if payload.get("active") is not None \
        else (row.active if row else True)
    order = int(payload["sort_order"]) if payload.get("sort_order") is not None \
        else (row.sort_order if row else await _next_order(db, customer_code))
    was_active = row.active if row else None
    await SiteRepository(db).upsert(customer_code, site_id, name, sort_order=order,
                                    active=is_active)
    if was_active is None:
        return "sites.site_added", f"Site {site_id} added · {name}"
    if was_active and not is_active:
        return "sites.site_retired", f"Site {site_id} retired · {name}"
    return "sites.site_changed", f"Site {site_id} saved · {name}"


async def _next_order(db: AsyncSession, customer_code: str) -> int:
    highest = await db.scalar(select(func.max(TenantSite.sort_order)).where(
        TenantSite.customer_code == customer_code))
    return int(highest or 0) + 1


async def _set_places(db: AsyncSession, customer_code: str, site_id: str, payload: dict, *,
                      actor: str) -> tuple[str, str]:
    """Replace the pairs a site stands for; refuse a pair that another site already has.

    (BRI [915/BRI, 920/BRI]) -> ("sites.places_set", "Site BRI now stands for 915/BRI, 920/BRI")
    ; (LON [915/BRI] while BRI has it) -> ValueError naming Brighton
    """
    repo = SiteRepository(db)
    sites = {s.site_id: s for s in await repo.list_all(customer_code)}
    if site_id not in sites:
        raise ValueError(f"{site_id} is not a site of this customer")
    wanted: list[Pair] = []
    for place in payload.get("places") or []:
        warehouse = str((place or {}).get("warehouse") or "").strip()
        company = str((place or {}).get("company") or "").strip() or None
        if not warehouse:
            raise ValueError("every place names a warehouse")
        wanted.append((company, warehouse))
    others = await repo.places_of(customer_code, [s for s in sites if s != site_id])
    for company, warehouse in wanted:
        for other, pairs in others.items():
            if any(w == warehouse and (c is None or company is None or c == company)
                   for c, w in pairs):
                raise ValueError(f"{company or 'any company'}/{warehouse} already belongs to "
                                 f"{sites[other].name} ({other}); take it out there first")
    saved = await repo.set_places(customer_code, site_id, wanted, actor=actor)
    if not saved:
        return "sites.places_set", (f"Site {site_id} stands for its own warehouse under any "
                                    "company again")
    return "sites.places_set", (f"Site {site_id} now stands for "
                                + ", ".join(f"{c or 'any'}/{w}" for c, w in saved))


async def mirror_customer(db: AsyncSession, writer: MirrorWriter, customer_code: str, *,
                          tracker: ChangeTracker | None = None,
                          now: datetime | None = None) -> int:
    """Write the Admin tab's Sites row and the switcher's site list; rows written.

    (after a new site) -> 2 ; (nothing changed, with a tracker) -> 0
    """
    now = now or datetime.now(timezone.utc)
    as_of, stamp = now.isoformat(), now.timestamp()
    admin_row = envelope(ROW_TYPE, customer_code, as_of=as_of, now=stamp,
                         payload=await overview(db, customer_code, now=now))
    written = int(await write_if_changed(writer, tracker, admin_row, now=stamp))
    active = await SiteRepository(db).list_active(customer_code)
    display_name = await db.scalar(select(Customer.display_name).where(
        Customer.customer_code == customer_code))
    listing = {"customer_code": customer_code,
               "customer_name": display_name or customer_code,
               "default_site_id": active[0].site_id if active else ALL_SITES_ID,
               "sites": [{"site_id": s.site_id, "name": s.name} for s in active]}
    switcher = envelope("sites", customer_code, payload=listing, as_of=as_of, now=stamp)
    return written + int(await write_if_changed(writer, tracker, switcher, now=stamp))


async def sweep_once(writer: MirrorWriter, *, only: list[str] | None = None,
                     tracker: ChangeTracker | None = None, session_factory=None,
                     now: datetime | None = None) -> dict[str, int]:
    """Write the Sites rows of every bound customer; rows written per customer.

    The pairs come from the data, which moves without any command, so the consumer runs this
    now and then (not every minute: the read is about a second on a busy customer).
    """
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    written: dict[str, int] = {}
    async with (session_factory or async_session)() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                written[code] = await mirror_customer(db, writer, code, tracker=tracker, now=now)
            except Exception:
                logger.exception("site rows for %s not mirrored", code)
    return written

