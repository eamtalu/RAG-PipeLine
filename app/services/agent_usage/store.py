"""The Postgres usage store: the agent's model calls summed per tenant, day, provider and model.

The gateway hands over plain rows (`app/agent/gateway/accounting.py` `usage_rows`), so this
module imports nothing from the agent package. `add` is one upsert per row with atomic
increments; `usage_days` is what the admin endpoint reads.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Callable, Mapping, Sequence

from sqlalchemy import case, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.usage_daily import UsageDaily

COUNTERS = ("calls", "failed_calls", "input_tokens", "output_tokens", "cached_tokens", "latency_ms")


class PostgresUsageStore:
    """Adds rows in one short session; a row for an existing key is added into it."""

    def __init__(self, session_factory: Callable[[], AsyncSession]):
        self._session_factory = session_factory

    async def add(self, rows: Sequence[Mapping[str, Any]]) -> None:
        """Add each row's counters into its (tenant, day, provider, model) row, creating it.

        ([{tenant, day, provider, model, calls: 2, ...}]) -> the row's calls grow by 2 ;
        a row with cost_usd None -> the stored cost becomes NULL and stays NULL
        """
        if not rows:
            return
        async with self._session_factory() as db:
            for row in rows:
                values = {k: row[k] for k in COUNTERS}
                values.update(
                    customer_code=row["tenant"],
                    day=row["day"],
                    provider=row["provider"],
                    model=row["model"],
                    cost_usd=row["cost_usd"],
                )
                statement = pg_insert(UsageDaily).values(**values)
                # Step 1: Add into the row under the key, and let one unpriced call make the
                # cost NULL for good, because a partial sum would read as a real total.
                increments = {k: getattr(UsageDaily, k) + statement.excluded[k] for k in COUNTERS}
                increments["cost_usd"] = case(
                    (UsageDaily.cost_usd.is_(None), None),
                    (statement.excluded.cost_usd.is_(None), None),
                    else_=UsageDaily.cost_usd + statement.excluded.cost_usd,
                )
                await db.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            UsageDaily.customer_code,
                            UsageDaily.day,
                            UsageDaily.provider,
                            UsageDaily.model,
                        ],
                        set_=increments,
                    )
                )
            await db.commit()


async def usage_days(db: AsyncSession, customer_code: str, *, since: date) -> list[UsageDaily]:
    """The tenant's rows from `since` on, newest day first, then by provider and model."""
    result = await db.execute(
        select(UsageDaily)
        .where(UsageDaily.customer_code == customer_code, UsageDaily.day >= since)
        .order_by(UsageDaily.day.desc(), UsageDaily.provider, UsageDaily.model)
    )
    return list(result.scalars())
