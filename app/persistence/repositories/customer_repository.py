"""Repository for the Customer (tenant) registry."""

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.customer import Customer
from app.persistence.models.customer_display_name import CustomerDisplayName


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

    # --- additional display names (usernames) per tenant ---------------------------------------

    async def list_display_names(
        self, customer_code: str, *, include_inactive: bool = True
    ) -> list[CustomerDisplayName]:
        stmt = select(CustomerDisplayName).where(CustomerDisplayName.customer_code == customer_code)
        if not include_inactive:
            stmt = stmt.where(CustomerDisplayName.active.is_(True))
        stmt = stmt.order_by(CustomerDisplayName.created_at.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def get_display_name(self, customer_code: str, display_name: str) -> CustomerDisplayName | None:
        return await self.db.scalar(
            select(CustomerDisplayName).where(
                CustomerDisplayName.customer_code == customer_code,
                CustomerDisplayName.display_name == display_name,
            )
        )

    async def add_display_name(self, customer_code: str, display_name: str) -> CustomerDisplayName:
        row = CustomerDisplayName(customer_code=customer_code, display_name=display_name)
        self.db.add(row)
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def remove_display_name(self, customer_code: str, name_id: str) -> bool:
        row = await self.db.scalar(
            select(CustomerDisplayName).where(
                CustomerDisplayName.id == name_id,
                CustomerDisplayName.customer_code == customer_code,
            )
        )
        if row is None:
            return False
        await self.db.delete(row)
        await self.db.commit()
        return True


def get_customer_repository(db: AsyncSession = Depends(get_session)) -> CustomerRepository:
    """FastAPI dependency — provides CustomerRepository with the request session injected."""
    return CustomerRepository(db)
