# agent_in_flight.py - one row per admitted agent run, from admission to release
#
#   The gateway inserts the row when it admits a run and deletes it when the run ends, however it
#   ends. The count of a tenant's rows is its in-flight number, shared by every worker and web
#   process. A crashed worker never releases, so every row carries the run's deadline and the next
#   admission for the tenant sweeps the expired ones: a tenant can be pinned for at most one run
#   deadline, with no sweeper process to run.
#
#   customer_code is the soft tenant key, like agent_runs; a tenant purge deletes the rows.

from datetime import datetime

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class AgentInFlight(Base):
    __tablename__ = "agent_in_flight"
    __table_args__ = (
        # admission reads "this tenant's live rows" and sweeps "this tenant's expired rows"
        Index("ix_agent_in_flight_customer_deadline", "customer_code", "deadline_at"),
    )

    trace_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
