# usage_daily.py - the agent's model usage summed per tenant, UTC day, provider and model
#
#   The gateway adds every run's model calls here after the run, whatever way it ended: calls and
#   failed calls, tokens in, out and read from cache, the latency, and the cost priced from the
#   operator's price list (NULL when the model has no price). One upsert per (provider, model)
#   with atomic increments, so every worker and web process adds into the same row.
#
#   customer_code is the soft tenant key, like agent_runs; a tenant purge deletes the rows.

from datetime import date
from decimal import Decimal

from sqlalchemy import BigInteger, Date, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class UsageDaily(Base):
    __tablename__ = "usage_daily"

    customer_code: Mapped[str] = mapped_column(String(64), primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)  # the UTC date the run started
    provider: Mapped[str] = mapped_column(String(32), primary_key=True)
    model: Mapped[str] = mapped_column(String(128), primary_key=True)
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    cached_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    # USD, summed; NULL once any call of the row ran on a model without a price
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(14, 6), nullable=True)
    latency_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
