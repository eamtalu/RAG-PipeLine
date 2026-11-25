"""Which column limits each table a data tool may read.

A table is classified once, here. A site-limited table names the column that carries the site (the
warehouse code, which is the site id). A table that is not listed cannot be read through a scoped
session, so new data stays closed until someone classifies it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import ColumnElement, and_, false

from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow
from app.persistence.models.log_transaction import LogTransaction
from app.services.scoped_data.scope import DataScope


class UnclassifiedTable(PermissionError):
    """A scoped session was asked to read a table that has no entry in the registry."""


@dataclass(frozen=True, slots=True)
class SiteLimited:
    """A table whose rows belong to one customer and one site."""

    model: Any
    site_column: str
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        """The rows of this table the scope may read.

        (BRI scope) -> customer_code = 'c' AND warehouse IN ('BRI') ; (no sites) -> false
        """
        mine = self.model.customer_code == scope.customer_code
        if scope.category_is_closed(self.category):
            return false()
        if scope.all_sites:
            return mine
        if not scope.site_ids:
            return false()
        return and_(mine, getattr(self.model, self.site_column).in_(scope.site_ids))


SITE_LIMITED: tuple[SiteLimited, ...] = (
    SiteLimited(LogTransaction, "warehouse"),
    SiteLimited(AnalyticsFact, "warehouse"),
    SiteLimited(AnalyticsSettledRow, "warehouse"),
    SiteLimited(AnalyticsAtRiskDelivery, "warehouse"),
    SiteLimited(Advisory, "site_id"),
)

BY_TABLE: dict[str, SiteLimited] = {entry.model.__tablename__: entry for entry in SITE_LIMITED}


def refuse_unclassified(table_names: set[str]) -> None:
    """Stop a read of any table the registry does not know, naming it.

    ({"log_transactions"}) -> None ; ({"log_ssh_sources"}) -> raises UnclassifiedTable
    """
    unknown = sorted(table_names - BY_TABLE.keys())
    if unknown:
        raise UnclassifiedTable(
            f"{', '.join(unknown)} cannot be read by a data tool: not classified in "
            "app/services/scoped_data/registry.py")
