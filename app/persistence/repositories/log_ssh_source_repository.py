"""Repository for LogSshSource — per-tenant CRUD of Windows-Server SSH log sources.

Every method is scoped to a customer_code (tenant isolation): a source belonging to another
customer is invisible here, so id-probing can't reach across tenants.
"""

from uuid import UUID

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.log_ssh_source import LogSshSource


class LogSshSourceRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list_for_customer(self, customer_code: str) -> list[LogSshSource]:
        return list((await self.db.execute(
            select(LogSshSource).where(LogSshSource.customer_code == customer_code)
            .order_by(LogSshSource.name.asc())
        )).scalars().all())

    async def get(self, customer_code: str, source_id: UUID) -> LogSshSource | None:
        src = await self.db.get(LogSshSource, source_id)
        return src if src and src.customer_code == customer_code else None

    async def get_by_name(self, customer_code: str, name: str) -> LogSshSource | None:
        return await self.db.scalar(select(LogSshSource).where(
            LogSshSource.customer_code == customer_code, LogSshSource.name == name,
        ))

    async def create(self, **values) -> LogSshSource:
        src = LogSshSource(**values)
        self.db.add(src)
        await self.db.commit()
        await self.db.refresh(src)
        return src

    async def update(self, src: LogSshSource, **values) -> LogSshSource:
        for k, v in values.items():
            setattr(src, k, v)
        await self.db.commit()
        await self.db.refresh(src)
        return src

    async def delete(self, src: LogSshSource) -> None:
        await self.db.delete(src)  # checkpoints cascade via FK ON DELETE CASCADE
        await self.db.commit()


def get_log_ssh_source_repository(db: AsyncSession = Depends(get_session)) -> LogSshSourceRepository:
    return LogSshSourceRepository(db)
