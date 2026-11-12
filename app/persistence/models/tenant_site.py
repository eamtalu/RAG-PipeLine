# tenant_site.py - the warehouses (sites) a customer runs, for the Teams app (stage D, step 4)
#
#   A customer's rows carry a `warehouse` value (settled rows, facts, log transactions). This table
#   names the values that are sites of their own: the code as it appears on the rows, a display name
#   and an order for the site switcher. The Home snapshot is computed once per active site and once
#   for the whole customer; a user's site scope (step 7) is a list of these ids.
#
#   customer_code is a real foreign key: a site of a log space that does not exist is a config error.

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
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
