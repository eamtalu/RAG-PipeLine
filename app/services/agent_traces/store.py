"""The Postgres trace store: one row per agent run, written by the gateway after the answer,
read back by the developer's review and purged after the retention.

The gateway hands over the plain row of a `TraceRecord` (`as_row()`), already redacted and
capped, so this module imports nothing from the agent package and the layout rule holds. The
Teams consumer and the API compose one store each over the app's session factory.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Mapping

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.agent_trace import AgentTrace


#: How many old rows one purge call deletes, so a long backlog never holds one long transaction.
PURGE_BATCH = 1000
#: The most rows one listing returns, whatever the caller asks for.
LIST_MAX = 500


def _as_dict(trace: AgentTrace) -> dict[str, Any]:
    """A stored run as a plain dict, by column name.

    (AgentTrace row) -> {"trace_id": ..., "role_code": ..., "tool_calls": [...], ...}
    """
    return {column.name: getattr(trace, column.name) for column in AgentTrace.__table__.columns}


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

    async def get(self, trace_id: str) -> dict[str, Any] | None:
        """One run by its trace id, or None.

        ("t1") -> {"trace_id": "t1", ...} ; ("unknown") -> None
        """
        async with self._session_factory() as db:
            trace = (await db.execute(
                select(AgentTrace).where(AgentTrace.trace_id == trace_id))).scalar_one_or_none()
        return _as_dict(trace) if trace is not None else None

    async def list_runs(self, customer_code: str, *, since: datetime, limit: int = 100,
                        until: datetime | None = None, principal_id: str | None = None,
                        role_code: str | None = None, trigger: str | None = None,
                        stop_reason: str | None = None) -> list[dict[str, Any]]:
        """A customer's runs in a window, newest first, at most `limit` (never over LIST_MAX).

        ("acme", since=a week ago, trigger="routine") -> the routine runs of the week, newest first
        """
        wanted = {AgentTrace.principal_id: principal_id, AgentTrace.role_code: role_code,
                  AgentTrace.trigger: trigger, AgentTrace.stop_reason: stop_reason}
        conditions = [AgentTrace.customer_code == customer_code, AgentTrace.started_at >= since]
        conditions += [column == value for column, value in wanted.items() if value is not None]
        if until is not None:
            conditions.append(AgentTrace.started_at < until)
        async with self._session_factory() as db:
            traces = (await db.execute(
                select(AgentTrace).where(*conditions).order_by(AgentTrace.started_at.desc())
                .limit(max(1, min(int(limit), LIST_MAX))))).scalars().all()
        return [_as_dict(trace) for trace in traces]

    async def all_runs(self, customer_code: str, *, since: datetime, page: int = LIST_MAX,
                       most: int = 20_000, **filters: Any) -> list[dict[str, Any]]:
        """Every run of a window, newest first, read a page at a time and never more than `most`.

        ("acme", since=30 days ago) -> all of the month's runs ; (most=1000) -> the newest 1000
        """
        runs: list[dict[str, Any]] = []
        until: datetime | None = None
        while len(runs) < most:
            batch = await self.list_runs(customer_code, since=since, until=until, limit=page,
                                         **filters)
            runs.extend(batch)
            if len(batch) < min(page, LIST_MAX):
                break
            # the next page is everything strictly older than the oldest run read so far
            until = batch[-1]["started_at"]
        return runs[:most]

    async def purge_once(self, *, now: datetime, retention_days: int) -> int:
        """Delete one batch of runs older than the retention; how many went (0 = none left).

        (now, 90) with one run of 91 days and one of 89 -> 1 ; called again -> 0
        """
        cutoff = now - timedelta(days=retention_days)
        async with self._session_factory() as db:
            old = (select(AgentTrace.id).where(AgentTrace.started_at < cutoff)
                   .limit(PURGE_BATCH).scalar_subquery())
            result = await db.execute(delete(AgentTrace).where(AgentTrace.id.in_(old)))
            await db.commit()
        return int(result.rowcount or 0)
