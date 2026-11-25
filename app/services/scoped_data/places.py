"""`scope_for`: a person's data scope with the company and warehouse pairs of their sites."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.site_repository import SiteRepository
from app.services import access
from app.services.scoped_data.scope import DataScope, Place


async def scope_for(db: AsyncSession, record: access.AccessRecord | None, *,
                    customer_code: str) -> DataScope:
    """The scope a record gives, each site resolved to the pairs it stands for.

    (supervisor of BRI, BRI = 911/BRI and 920/BRI) -> DataScope(places=(911/BRI, 920/BRI)) ;
    (a site with no pairs) -> that site as its own warehouse ; (no record) -> nothing
    """
    scope = DataScope.from_record(record, customer_code=customer_code)
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
