# tenant_site.py - the warehouses (sites) a customer runs, for the Teams app (stage D, step 4)
#
#   A customer's rows carry a `warehouse` value (settled rows, facts, log transactions). This table
#   names the values that are sites of their own: the code as it appears on the rows, a display name
#   and an order for the site switcher. The Home snapshot is computed once per active site and once
#   for the whole customer; a user's site scope (step 7) is a list of these ids.
#
#   customer_code is a real foreign key: a site of a log space that does not exist is a config error.
#
#   tenant_site_places (chunk 223): a data row is identified by customer, company and warehouse, and
#   one site can stand for several such pairs (Brighton under two WMS companies while testing). A
#   site with no place row stands for the warehouse of its own id under any company, which is what
#   every site meant before the table existed.

from datetime import datetime, timezone

import uuid

from sqlalchemy import (Boolean, DateTime, ForeignKey, ForeignKeyConstraint, Integer, String,
                        UniqueConstraint)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class TenantSite(Base):
    __tablename__ = "tenant_sites"

    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True)
    #: The `warehouse` value on the rows, exactly as stored ("1", "BRI", ...). Never renamed here.
    site_id: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), nullable=False)


class TenantSitePlace(Base):
    """One company and warehouse pair a site stands for; no company means any company."""

    __tablename__ = "tenant_site_places"
    __table_args__ = (
        ForeignKeyConstraint(["customer_code", "site_id"],
                             ["tenant_sites.customer_code", "tenant_sites.site_id"],
                             ondelete="CASCADE", name="fk_tenant_site_places_site"),
        # NULLS NOT DISTINCT so the same warehouse cannot be listed twice with no company
        UniqueConstraint("customer_code", "site_id", "company", "warehouse",
                         name="uq_tenant_site_places_pair", postgresql_nulls_not_distinct=True),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    site_id: Mapped[str] = mapped_column(String(16), nullable=False)
    #: The WMS company number on the rows (M3 CONO, "915"); null = any company.
    company: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: The `warehouse` value on the rows, exactly as stored.
    warehouse: Mapped[str] = mapped_column(String(16), nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
