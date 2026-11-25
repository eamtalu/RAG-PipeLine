"""Repository for `tenant_sites`: the warehouses a customer runs (stage D, step 4)."""

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.tenant_site import TenantSite, TenantSitePlace

#: One company and warehouse pair as plain values; the company is None for "any company".
Pair = tuple[str | None, str]


class SiteRepository:
    """Database access for a customer's sites. The caller commits."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_active(self, customer_code: str) -> list[TenantSite]:
        """The sites the switcher offers, in their configured order.

        (BRI order 1, LON order 2, BIC inactive) -> [BRI, LON] ; (no sites) -> []
        """
        stmt = (select(TenantSite).where(TenantSite.customer_code == customer_code, TenantSite.active.is_(True))
                .order_by(TenantSite.sort_order, TenantSite.site_id))
        return list((await self.db.execute(stmt)).scalars().all())

    async def list_all(self, customer_code: str) -> list[TenantSite]:
        stmt = (select(TenantSite).where(TenantSite.customer_code == customer_code)
                .order_by(TenantSite.sort_order, TenantSite.site_id))
        return list((await self.db.execute(stmt)).scalars().all())

    async def upsert(self, customer_code: str, site_id: str, name: str, *, sort_order: int = 0,
                     active: bool = True) -> TenantSite:
        """Create or update one site; the same id twice updates the name, order and flag.

        ("acme", "BRI", "Brighton", sort_order=1) -> TenantSite ; again with active=False -> hidden
        """
        row = await self.db.get(TenantSite, (customer_code, site_id))
        if row is None:
            row = TenantSite(customer_code=customer_code, site_id=site_id, name=name, sort_order=sort_order,
                             active=active)
            self.db.add(row)
        else:
            row.name, row.sort_order, row.active = name, sort_order, active
        await self.db.flush()
        return row

    async def remove(self, customer_code: str, site_id: str) -> None:
        await self.db.execute(delete(TenantSite).where(TenantSite.customer_code == customer_code,
                                                       TenantSite.site_id == site_id))

    async def set_places(self, customer_code: str, site_id: str, pairs: list[Pair], *,
                         actor: str | None = None) -> list[Pair]:
        """Replace the pairs a site stands for; ValueError for a site the customer does not have.

        ("acme", "BRI", [("911", "BRI"), ("920", "BRI")]) -> the two pairs ; ([]) -> the site is
        its own warehouse under any company again
        """
        if await self.db.get(TenantSite, (customer_code, site_id)) is None:
            raise ValueError(f"{site_id} is not a site of {customer_code}")
        wanted = sorted({((company or "").strip() or None, warehouse.strip())
                         for company, warehouse in pairs if warehouse and warehouse.strip()},
                        key=lambda pair: (pair[0] or "", pair[1]))
        await self.db.execute(delete(TenantSitePlace).where(
            TenantSitePlace.customer_code == customer_code, TenantSitePlace.site_id == site_id))
        self.db.add_all([TenantSitePlace(customer_code=customer_code, site_id=site_id,
                                         company=company, warehouse=warehouse, created_by=actor)
                         for company, warehouse in wanted])
        await self.db.flush()
        return wanted

    async def places_of(self, customer_code: str, site_ids: list[str]) -> dict[str, list[Pair]]:
        """The pairs of the sites that have any, by site id; a site with none is not in the result.

        ("acme", ["BRI", "LON"]) with BRI = 915/BRI -> {"BRI": [("915", "BRI")]}
        """
        if not site_ids:
            return {}
        rows = (await self.db.execute(
            select(TenantSitePlace).where(TenantSitePlace.customer_code == customer_code,
                                          TenantSitePlace.site_id.in_(site_ids))
            .order_by(TenantSitePlace.site_id, TenantSitePlace.company,
                      TenantSitePlace.warehouse))).scalars().all()
        found: dict[str, list[Pair]] = {}
        for row in rows:
            found.setdefault(row.site_id, []).append((row.company, row.warehouse))
        return found
