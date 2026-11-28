"""Scopes with the company and warehouse pairs of their sites: a person's (`scope_for`), a set
of sites' with no person behind it (`sites_scope`), one cut down to another (`narrow`), and
whether one row is inside a scope (`can_read`)."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.site_repository import SiteRepository
from app.services import access
from app.services.scoped_data import registry
from app.services.scoped_data.scope import DataScope, Place


async def scope_for(db: AsyncSession, record: access.AccessRecord | None, *,
                    customer_code: str) -> DataScope:
    """The scope a record gives, each site resolved to the pairs it stands for.

    (supervisor of BRI, BRI = 911/BRI and 920/BRI) -> DataScope(places=(911/BRI, 920/BRI)) ;
    (a site with no pairs) -> that site as its own warehouse ; (no record) -> nothing
    """
    return await _with_places(db, DataScope.from_record(record, customer_code=customer_code))


async def sites_scope(db: AsyncSession, customer_code: str, *, all_sites: bool,
                      site_ids: tuple[str, ...] | list[str]) -> DataScope:
    """The scope of a set of sites with no person behind it, as a notification group has.

    ("acme", all_sites=True) -> every site ; ("acme", site_ids=["BRI"]) with BRI = 915/BRI ->
    DataScope(sites=("BRI",), places=(915/BRI,)) ; (no site) -> a scope that reads nothing
    """
    if all_sites:
        return DataScope.for_system(customer_code)
    return await _with_places(db, DataScope(customer_code=customer_code,
                                            site_ids=tuple(sorted(set(site_ids)))))


async def narrow(db: AsyncSession, scope: DataScope, *, all_sites: bool,
                 site_ids: tuple[str, ...] | list[str]) -> DataScope:
    """A scope cut down to the sites it shares with a set of sites; never wider than either.

    (all sites, group BRI) -> BRI with its places ; (BRI, an all-sites group) -> BRI unchanged ;
    (BRI, group LON) -> no site, so nothing is read
    """
    # Step 1: A set that is every site takes nothing away
    if all_sites:
        return scope
    # Step 2: Keep the sites both have, then resolve their places again, because the pairs of
    # a dropped site must not stay readable
    wanted = sorted(set(site_ids))
    kept = wanted if scope.all_sites else [site for site in wanted if site in scope.site_ids]
    return await _with_places(db, DataScope(customer_code=scope.customer_code,
                                            site_ids=tuple(kept), categories=scope.categories))


async def can_read(db: AsyncSession, scope: DataScope, model, row_id) -> bool:
    """Whether one row of a classified table is inside a scope, by the table's own rule.

    (a BRI scope, Advisory, a London advisory's id) -> False ; (the same, a Brighton one) -> True

    For deciding where something is sent: the rule is the one that limits what a person reads,
    so a channel is never sent what its site's people could not open.
    """
    rule = registry.BY_TABLE[model.__tablename__]
    found = await db.scalar(select(model.id).where(model.id == row_id, rule.condition(scope)))
    return found is not None


async def _with_places(db: AsyncSession, scope: DataScope) -> DataScope:
    """The scope with each of its sites resolved to the pairs it stands for."""
    # an all-sites scope reads by customer alone and an empty one reads nothing, so neither
    # needs pairs
    if scope.all_sites or not scope.site_ids:
        return scope
    listed = await SiteRepository(db).places_of(scope.customer_code, list(scope.site_ids))
    if not listed:
        return scope
    places = tuple(
        place
        for site in scope.site_ids
        for place in ([Place(company, warehouse) for company, warehouse in listed[site]]
                      if site in listed else [Place(None, site)]))
    return DataScope(customer_code=scope.customer_code, all_sites=False, site_ids=scope.site_ids,
                     categories=scope.categories, places=places)
