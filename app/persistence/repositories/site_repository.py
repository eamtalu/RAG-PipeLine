"""Repository for `tenant_sites`: the warehouses a customer runs (stage D, step 4)."""

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.tenant_site import TenantSite


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
