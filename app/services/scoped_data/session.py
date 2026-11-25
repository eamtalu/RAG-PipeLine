"""`limit_session`: make one database session read inside a `DataScope`."""

from __future__ import annotations

from sqlalchemy import Table, event
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import ORMExecuteState, with_loader_criteria
from sqlalchemy.sql import visitors

from app.services.scoped_data import registry
from app.services.scoped_data.scope import DataScope


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
        registry.refuse_unclassified(_table_names(state.statement))
        # Step 3: Add each classified table's condition wherever it appears in the statement, so
        # that a query built deep in a shared service is limited without that service knowing
        state.statement = state.statement.options(*(
            with_loader_criteria(entry.model, entry.condition(scope), include_aliases=True)
            for entry in registry.SITE_LIMITED))

    event.listen(db.sync_session, "do_orm_execute", add_scope)
    return db


def _table_names(statement) -> set[str]:
    """Every database table a statement reads, subqueries and joins included.

    (select(LogTransaction.id).join(Advisory, ...)) -> {"log_transactions", "advisories"}
    """
    return {node.name for node in visitors.iterate(statement) if isinstance(node, Table)}
