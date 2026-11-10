"""The Postgres trace store: one row per agent run, written by the gateway after the answer.

The gateway hands over the plain row of a `TraceRecord` (`as_row()`), already redacted and
capped, so this module imports nothing from the agent package and the layout rule holds. The
Teams consumer and the API compose one store each over the app's session factory.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.agent_trace import AgentTrace


class PostgresTraceStore:
    """Writes each row in its own short session; a repeated trace id is ignored."""

    def __init__(self, session_factory: Callable[[], AsyncSession]):
        self._session_factory = session_factory

    async def save(self, row: Mapping[str, Any]) -> None:
        """Insert the run's row, keyed by the record's tenant as `customer_code`.

        (row for trace t1) -> one row ; (the same row again) -> still one row
        """
        # Step 1: Rename the record's tenant to the table's soft tenant key, so that the agent
        # package keeps its own vocabulary and the table keeps the database's.
        values = dict(row)
        values["customer_code"] = values.pop("tenant")
        # Step 2: Insert and let the unique trace id absorb a repeat, because the gateway may be
        # asked to store the same run twice and the first row is the truth.
        async with self._session_factory() as db:
            await db.execute(
                pg_insert(AgentTrace)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[AgentTrace.trace_id])
            )
            await db.commit()
