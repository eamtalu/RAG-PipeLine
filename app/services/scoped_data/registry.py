"""Which rule limits each table a data tool may read.

A table is classified once, here, with one of these rules:
- `SiteLimited`: rows belong to one customer and one site, named by a column (the warehouse code,
  which is the site id).
- `SubjectLimited`: forecast rows, whose subject is a site or the customer's total.
- `ThroughTransaction`: log lines and their assignment rows, which carry no site and are read
  by whoever may read the transaction they belong to.
- `AllSitesOnly`: operational rows that carry no site, so only a person who may see every site
  reads them. No table uses it today; a new table without a site starts here.
- `CustomerWide`: settings and reference data, the same for every site of a customer.
- `OwnedBy`: a child table with no customer column, limited through the row that owns it.
- `Shared`: a list that is the same for every customer and names no one.
The data category of a rule is carried for step 4; step 1 opens only `operations`, which every
rule uses today, so a tool that guards a sensitive figure itself (usage) keeps doing so.
A table that is not listed cannot be read through a scoped session, so new data stays closed until
someone classifies it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Protocol

from sqlalchemy import ColumnElement, and_, exists, false, literal_column, or_, select, true

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
class ThroughTransaction:
    """Log lines and their assignment rows: read by whoever may read their transaction.

    A log line has no site and gets none (`log_entries` is insert-only), but the assignment row
    names the transaction it belongs to and the transaction has its company and warehouse. The
    transaction's own rule is applied to it here, so a change to that rule changes both. A line
    that belongs to no transaction yet is read by everyone of the customer who has a site (the
    owner's decision, 27 Nov 2026: nothing is hidden for lack of grouping). A line with no
    timestamp cannot be matched to its transaction here, so only a person who may see every
    site reads it. That person's condition is the customer alone, as it was before.
    """

    model: Any
    category: str = "operations"

    def condition(self, scope: DataScope) -> ColumnElement[bool]:
        mine = self.model.customer_code == scope.customer_code
        if scope.category_is_closed(self.category):
            return false()
        if scope.all_sites:
            return mine
        if not scope.site_places:
            return false()
        # Plain table aliases, so that the session's own limit is not added a second time to
        # the transaction inside this condition.
        owner = LogTransaction.__table__.alias("owner_txn")
        may_read_owner = replace(TRANSACTION_RULE, model=owner.c).condition(scope)

        def owned(transaction_id, line_time) -> ColumnElement[bool]:
            # A transaction starts at its first line, so it is looked for only in the day up
            # to the line's own time; otherwise every check opens all the daily partitions
            # (measured on live data: 10 s against 2 s for one search).
            return exists().where(owner.c.id == transaction_id,
                                  owner.c.started_at <= line_time,
                                  owner.c.started_at >= line_time - _LONGEST_SQL,
                                  may_read_owner)

        if self.model is LogEntryAssignment:
            return and_(mine, owned(LogEntryAssignment.transaction_id,
                                    LogEntryAssignment.entry_ts))
        link = LogEntryAssignment.__table__.alias("owner_link")
        # "No link to a transaction this person may not read": true for a line of their own
        # transaction and for a line with no transaction at all. Equality on both partition
        # keys of the link, entry id and timestamp, so that one partition is opened per line.
        is_someone_elses = exists().where(link.c.entry_id == LogEntry.id,
                                          link.c.entry_ts == LogEntry.timestamp,
                                          ~owned(link.c.transaction_id, link.c.entry_ts))
        return and_(mine, LogEntry.timestamp.isnot(None), ~is_someone_elses)


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


#: No transaction lasts longer than this. Measured on live data: the longest of 444,403 took
#: under five minutes, and none took an hour.
LONGEST_TRANSACTION = timedelta(days=1)
#: The same length for a condition; written out so the SQL reads the same in every dialect
#: of test and log output.
_LONGEST_SQL = literal_column("INTERVAL '1 day'")

#: How a transaction is limited; its log lines follow it (`ThroughTransaction`).
TRANSACTION_RULE = SiteLimited(LogTransaction, "warehouse", company_column="company")

RULES: tuple[Rule, ...] = (
    # the WMS activity and what is derived from it, each row with its warehouse
    TRANSACTION_RULE,
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
    # log lines carry no site: they follow their transaction
    ThroughTransaction(LogEntry),
    ThroughTransaction(LogEntryAssignment),
    SiteLimited(AnalyticsRecordFact, "warehouse", company_column="company"),
    SiteLimited(rollup.AnalyticsHourlyRollup, "warehouse", company_column="company"),
    SiteLimited(rollup.AnalyticsDailyRollup, "warehouse", company_column="company"),
    SiteLimited(rollup.AnalyticsMonthlyRollup, "warehouse", company_column="company"),
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
