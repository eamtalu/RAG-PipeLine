"""Which rule limits each table a data tool may read.

A table is classified once, here, with one of these rules:
- `SiteLimited`: rows belong to one customer and one site, named by a column (the warehouse code,
  which is the site id).
- `SubjectLimited`: forecast rows, whose subject is a site or the customer's total.
- `AllSitesOnly`: operational rows that carry no site yet (step 1d adds the column), so only a
  person who may see every site reads them.
- `CustomerWide`: settings and reference data, the same for every site of a customer.
- `OwnedBy`: a child table with no customer column, limited through the row that owns it.
- `Shared`: a list that is the same for every customer and names no one.
The data category of a rule is carried for step 4; step 1 opens only `operations`, which every
rule uses today, so a tool that guards a sensitive figure itself (usage) keeps doing so.
A table that is not listed cannot be read through a scoped session, so new data stays closed until
someone classifies it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy import ColumnElement, and_, false, or_, select, true

from app.persistence.models import analytics_at_risk as at_risk
from app.persistence.models import analytics_forecast as forecast
from app.persistence.models import analytics_rollup as rollup
from app.persistence.models.advisory import Advisory, AdvisoryAgent, AdvisoryAgentRun
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_field_meaning import AnalyticsFieldMeaning
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_ml import AnalyticsFeatureSet, AnalyticsPrediction
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.customer_display_name import CustomerDisplayName
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.teams_conversation import TeamsConversation
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.models.tenant_site import TenantSite, TenantSitePlace
from app.services import access
from app.services.scoped_data.scope import DataScope

#: The forecast's word for a subject that is one site; its other subject is the customer's total.
SITE_SUBJECT = "warehouse"


class UnclassifiedTable(PermissionError):
    """A scoped session was asked to read a table that has no entry in the registry."""


class Rule(Protocol):
    """How one table is limited; every rule names its model and its data category."""

    model: Any
    category: str

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        """The rows of the table this scope may read: (BRI scope) -> a WHERE condition."""


def _in_sites(scope: DataScope, mine: ColumnElement[bool], site: Any) -> ColumnElement[bool]:
    """The customer's rows whose column is one of the scope's site ids, or all for every site.

    (all sites) -> mine ; (BRI) -> mine AND site IN ('BRI') ; (no sites) -> false
    """
    if scope.all_sites:
        return mine
    if not scope.site_ids:
        return false()
    return and_(mine, site.in_(scope.site_ids))


@dataclass(frozen=True, slots=True)
class SiteLimited:
    """A table whose rows belong to one customer and one site.

    The site column holds the warehouse code. With `company_column` the row is matched by the
    company and warehouse pair; without one, by the warehouse alone. With `holds_site_id` the
    column holds a site id (written from the warehouse by the advisory agents), so it is matched
    against the person's site ids and their places' warehouses. With both, a row is matched by
    the pair where it has a company and by its site where it has none (chunk 231).
    """

    model: Any
    site_column: str
    category: str = "operations"
    company_column: str | None = None
    holds_site_id: bool = False

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        mine = self.model.customer_code == scope.customer_code
        if scope.category_is_closed(self.category):
            return false()
        if scope.all_sites:
            return mine
        places = scope.site_places
        if not places:
            return false()
        site = getattr(self.model, self.site_column)
        warehouses = sorted({place.warehouse for place in places})
        if self.company_column is None:
            if self.holds_site_id:
                return and_(mine, site.in_(sorted({*warehouses, *scope.site_ids})))
            return and_(mine, site.in_(warehouses))
        # the pair decides: the same warehouse code under another company is another place
        company = getattr(self.model, self.company_column)
        if not self.holds_site_id:
            return and_(mine, or_(*(
                site == place.warehouse if place.company is None
                else and_(site == place.warehouse, company == place.company)
                for place in places)))
        # A row written before it had a company still follows its site, otherwise older
        # advisories would vanish for the people of that site.
        by_place = [
            site == place.warehouse if place.company is None
            else and_(site == place.warehouse, or_(company == place.company, company.is_(None)))
            for place in places]
        # A site id that is not one of the warehouses is matched by id alone. One that IS a
        # warehouse is not, or the pair above would be bypassed for another company's rows.
        other_ids = sorted(set(scope.site_ids) - set(warehouses))
        if other_ids:
            by_place.append(site.in_(other_ids))
        return and_(mine, or_(*by_place))


@dataclass(frozen=True, slots=True)
class SubjectLimited:
    """A forecast table: a site-limited person reads their sites' subjects, never the total."""

    model: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        mine = self.model.customer_code == scope.customer_code
        if scope.category_is_closed(self.category):
            return false()
        if scope.all_sites:
            return mine
        site_subjects = and_(mine, self.model.subject_kind == SITE_SUBJECT)
        return _in_sites(scope, site_subjects, self.model.subject)


@dataclass(frozen=True, slots=True)
class AllSitesOnly:
    """Operational rows with no site column yet: read only by a person who may see every site."""

    model: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        if scope.category_is_closed(self.category) or not scope.all_sites:
            return false()
        return self.model.customer_code == scope.customer_code


@dataclass(frozen=True, slots=True)
class CustomerWide:
    """Settings and reference data: every row of the customer, whatever the person's sites."""

    model: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        return self.model.customer_code == scope.customer_code


@dataclass(frozen=True, slots=True)
class OwnedBy:
    """A child table without a customer column: its rows follow the customer of their owner."""

    model: Any
    owner_key: str
    owner: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        owners = select(self.owner.id).where(self.owner.customer_code == scope.customer_code)
        return getattr(self.model, self.owner_key).in_(owners)


@dataclass(frozen=True, slots=True)
class Shared:
    """A list that is the same for every customer and names no one (roles, capabilities)."""

    model: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        return true()


RULES: tuple[Rule, ...] = (
    # the WMS activity and what is derived from it, each row with its warehouse
    SiteLimited(LogTransaction, "warehouse", company_column="company"),
    SiteLimited(AnalyticsFact, "warehouse", company_column="company"),
    # by warehouse alone: part of the ledger's history has no company, and no data tool reads it
    SiteLimited(AnalyticsFactLedger, "warehouse"),
    SiteLimited(AnalyticsSettledRow, "warehouse", company_column="company"),
    SiteLimited(at_risk.AnalyticsAtRiskDelivery, "warehouse", company_column="company"),
    SiteLimited(at_risk.AnalyticsAtRiskCheck, "warehouse", company_column="company"),
    SiteLimited(at_risk.AnalyticsAtRiskRouteProfile, "warehouse", company_column="company"),
    SiteLimited(Advisory, "site_id", company_column="company", holds_site_id=True),
    SiteLimited(TeamsConversation, "site_id", holds_site_id=True),
    SiteLimited(TeamsConversationTurn, "site_id", holds_site_id=True),
    # the forecast, by subject
    SubjectLimited(forecast.AnalyticsForecastSeries),
    SubjectLimited(forecast.AnalyticsForecastAccuracy),
    SubjectLimited(AnalyticsPrediction),
    # no site column until step 1d
    AllSitesOnly(LogEntry),
    AllSitesOnly(LogEntryAssignment),
    AllSitesOnly(AnalyticsRecordFact),
    AllSitesOnly(rollup.AnalyticsHourlyRollup),
    AllSitesOnly(rollup.AnalyticsDailyRollup),
    AllSitesOnly(rollup.AnalyticsMonthlyRollup),
    # settings and reference data
    CustomerWide(Customer),
    CustomerWide(CustomerDisplayName),
    CustomerWide(TenantSite),
    CustomerWide(TenantSitePlace),
    CustomerWide(AnalyticsFieldMeaning),
    CustomerWide(AnalyticsFieldRegistry),
    CustomerWide(AnalyticsLookup),
    CustomerWide(AnalyticsLookupValue),
    CustomerWide(AnalyticsMetric),
    CustomerWide(AnalyticsSettlement),
    CustomerWide(AnalyticsTenantState),
    CustomerWide(AnalyticsTransactionRegistry),
    CustomerWide(AnalyticsFeatureSet),
    CustomerWide(at_risk.AnalyticsAtRiskSettings),
    CustomerWide(at_risk.AnalyticsAtRiskTenantState),
    CustomerWide(forecast.AnalyticsForecastRun),
    CustomerWide(AdvisoryAgent),
    CustomerWide(AdvisoryAgentRun),
)


def _access_rule(table: access.AccessTable) -> Rule:
    """The rule for one access table, from what access control says about whose its rows are.

    (TeamsUser, "customer") -> CustomerWide ; (AuditEvent, "site", "site_id") -> SiteLimited
    """
    if table.ownership == "site":
        return SiteLimited(table.model, table.key, holds_site_id=True)
    if table.ownership == "owner":
        return OwnedBy(table.model, table.key, table.owner)
    return Shared(table.model) if table.ownership == "shared" else CustomerWide(table.model)


#: Who people are, read by the usage tool through access control; the package classifies its own.
RULES = RULES + tuple(_access_rule(table) for table in access.ACCESS_TABLES)

BY_TABLE: dict[str, Rule] = {rule.model.__tablename__: rule for rule in RULES}


def closed_tables(table_names: set[str], scope: DataScope) -> list[str]:
    """The tables of a read that this scope gets no row from, because they carry no site yet.

    ({"log_entries", "advisories"}, BRI scope) -> ["log_entries"] ; (the same, all sites) -> []
    """
    if scope.all_sites:
        return []
    return sorted(name for name in table_names if isinstance(BY_TABLE.get(name), AllSitesOnly))


def refuse_unclassified(table_names: set[str]) -> None:
    """Stop a read of any table the registry does not know, naming it.

    ({"log_transactions"}) -> None ; ({"log_ssh_sources"}) -> raises UnclassifiedTable
    """
    unknown = sorted(table_names - BY_TABLE.keys())
    if unknown:
        raise UnclassifiedTable(
            f"{', '.join(unknown)} cannot be read by a data tool: not classified in "
            "app/services/scoped_data/registry.py")
