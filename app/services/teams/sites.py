"""A customer's sites for the Teams app (stage D, step 4).

A site is one `warehouse` value on the rows, named in `tenant_sites`. The Home snapshot takes a
`Site` and narrows every settlement read to it; `None` means the whole customer, which the mirror
stores under the id "all". The model never chooses the site: the snapshot writer and, from step 7,
the execution context do.
"""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.site_repository import SiteRepository

#: The id the whole customer is mirrored under, beside the real sites.
ALL_SITES_ID = "all"


@dataclass(frozen=True)
class Site:
    """One warehouse as the switcher shows it: the value on the rows, and its display name."""

    site_id: str
    name: str


def with_site(args: dict, site: Site | None) -> dict:
    """The same settlement read, narrowed to one site's rows.

    >>> with_site({"day": "today"}, Site("BRI", "Brighton"))
    {'day': 'today', 'where': ['warehouse==BRI']}
    >>> with_site({"day": "today", "where": ["picked==0"]}, Site("LON", "London"))["where"]
    ['picked==0', 'warehouse==LON']
    >>> with_site({"day": "today"}, None)
    {'day': 'today'}
    """
    if site is None:
        return args
    return {**args, "where": [*(args.get("where") or []), f"warehouse=={site.site_id}"]}


async def list_sites(db: AsyncSession, customer_code: str) -> list[Site]:
    """The active sites in switcher order, as plain values.

    (BRI order 1, LON order 2) -> [Site("BRI", "Brighton"), Site("LON", "London")] ; (none) -> []
    """
    return [Site(row.site_id, row.name) for row in await SiteRepository(db).list_active(customer_code)]
