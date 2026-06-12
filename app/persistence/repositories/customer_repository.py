"""Repository for the Customer (tenant) registry."""

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.customer import Customer


class CustomerRepository:
    """Database access for the tenant registry."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create(self, customer_code: str, display_name: str | None = None) -> Customer:
        cust = Customer(customer_code=customer_code, display_name=display_name)
        self.db.add(cust)
        await self.db.commit()
        await self.db.refresh(cust)
        return cust

    async def get_by_code(self, customer_code: str) -> Customer | None:
        return await self.db.scalar(select(Customer).where(Customer.customer_code == customer_code))

    async def list_all(self, include_inactive: bool = True) -> list[Customer]:
        stmt = select(Customer)
        if not include_inactive:
            stmt = stmt.where(Customer.active.is_(True))
        stmt = stmt.order_by(Customer.customer_code.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def exists(self, customer_code: str, *, must_be_active: bool = False) -> bool:
        cust = await self.get_by_code(customer_code)
        if cust is None:
            return False
        return cust.active if must_be_active else True

    async def set_active(self, customer_code: str, active: bool) -> Customer | None:
        cust = await self.get_by_code(customer_code)
        if cust is None:
            return None
        cust.active = active
        await self.db.commit()
        await self.db.refresh(cust)
        return cust


def get_customer_repository(db: AsyncSession = Depends(get_session)) -> CustomerRepository:
    """FastAPI dependency — provides CustomerRepository with the request session injected."""
    return CustomerRepository(db)
