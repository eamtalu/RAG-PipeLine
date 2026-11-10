# quota_window.py - the agent's hourly admission counters, one row per tenant or person per hour
#
#   The gateway admits a run before any model call (app/services/agent_quotas/admission.py). The
#   count lives here, not in a worker's memory, because five workers each counting locally cannot
#   enforce one tenant-wide limit: every admission changes the row by one atomic operation under
#   a per-tenant advisory lock. `user_id` is "" for the tenant-wide row and the Entra object id (or
#   the service caller name) for a person's row. Rows older than a day are swept on admission.
#
#   customer_code is the soft tenant key, like agent_runs; a tenant purge deletes the rows.

from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class QuotaWindow(Base):
    __tablename__ = "quota_windows"

    customer_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)  # "" = the whole tenant
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
