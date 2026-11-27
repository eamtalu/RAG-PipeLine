"""`limit_session`: make one database session read inside a `DataScope`."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable

from sqlalchemy import Table, event
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import ORMExecuteState, with_loader_criteria
from sqlalchemy.sql import visitors

from app.services.scoped_data import registry
from app.services.scoped_data.scope import DataScope


#: The key under which a limited session keeps the names of the tables it has read.
TABLES_READ = "scoped_data.tables_read"


#: The key under which a limited session keeps the scope it reads within.
SCOPE = "scoped_data.scope"


def is_site_limited(db: AsyncSession) -> bool:
    """Whether this session reads for a person who may not see every site.

    (a BRI session) -> True ; (an all-sites session, or one that was never limited) -> False

    For a tool that can answer faster when it knows; the limit itself never depends on it.
    """
    scope = db.info.get(SCOPE)
    return scope is not None and not scope.all_sites


def tables_read(db: AsyncSession) -> set[str]:
    """Every table a limited session has read so far, for the run's trace.

    (a session that ran select(Advisory)) -> {"advisories"} ; (an unused session) -> set()
    """
    return set(db.info.get(TABLES_READ, ()))


def limit_session(db: AsyncSession, scope: DataScope) -> AsyncSession:
    """The same session, now adding the scope to every select it runs.

    (db, BRI scope) -> db ; then select(AnalyticsSettledRow) returns BRI rows of that customer only
    """

    def add_scope(state: ORMExecuteState) -> None:
        # Step 1: Leave writes and the follow-up loads of an already limited select alone, because
        # the limit is about what a tool may read and those loads inherit the first select's limit
        if not state.is_select or state.is_column_load or state.is_relationship_load:
            return
        # Step 2: Refuse a table nobody classified, otherwise new data would be open by default
        tables = _table_names(state.statement)
        registry.refuse_unclassified(tables)
        db.info.setdefault(TABLES_READ, set()).update(tables)
        # Step 3: Add each classified table's condition wherever it appears in the statement, so
        # that a query built deep in a shared service is limited without that service knowing
        state.statement = state.statement.options(*(
            with_loader_criteria(entry.model, entry.condition(scope), include_aliases=True)
            for entry in registry.RULES))

    event.listen(db.sync_session, "do_orm_execute", add_scope)
    db.info[SCOPE] = scope
    return db


def _table_names(statement) -> set[str]:
    """Every database table a statement reads, subqueries and joins included.

    (select(LogTransaction.id).join(Advisory, ...)) -> {"log_transactions", "advisories"}
    """
    return {node.name for node in visitors.iterate(statement) if isinstance(node, Table)}


def scope_of(data_scope: DataScope | None, *, customer_code: str, is_service: bool) -> DataScope:
    """The scope a run reads within: the person's own, every site for a system caller, else nothing.

    (BRI scope, ...) -> the BRI scope ; (None, is_service=True) -> every site of the customer ;
    (None, is_service=False) -> no sites, because a person without a record may read nothing
    """
    if data_scope is not None:
        return data_scope
    return DataScope.for_system(customer_code) if is_service else DataScope(customer_code)


@asynccontextmanager
async def open_session(session_factory: Callable[[], AsyncSession],
                       scope: DataScope) -> AsyncIterator[AsyncSession]:
    """One short session that reads inside the scope, closed on the way out.

    (async_session, BRI scope) -> a session whose selects return BRI rows of that customer only
    """
    async with session_factory() as db:
        yield limit_session(db, scope)
