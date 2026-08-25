"""Chunk 41, Phase 1: the analytics schema, and the wiring that stops it losing data.

Nine tables. What this file mostly pins is not their columns -- a wrong column type fails loudly the
first time something writes -- but the four pieces of WIRING that fail silently:

1. **The fact row must be wide.** Anything missing from it is unrecoverable after 60 days, because the
   raw entries it was derived from are gone. This is the one irreversible decision in the plan, so the
   fact table is asserted against `contract.FACT_FIELDS` rather than against a hand-written list.
2. **Every partitioned table must declare an explicit grain.** Registering one without a grain gives it
   DAILY partitions, so a table meant to be cut monthly gets thirty times as many.
3. **Every partitioned table must declare an explicit retention policy.** Omitting it silently inherits
   `log_partition_retention_days` (60), and the retention worker would then drop the fact table and its
   ledger a month at a time. Those are the two tables nothing can rebuild.
4. **Every analytics table must be in the tenant purge map (F13).** Missing from it, a purged tenant
   leaves its rows behind forever, and nobody looks at a deleted tenant's data again.

The first three are why this phase came after the E4 extension rather than including it: none of them
was expressible before `partitioning.Grain` and `log_partition_worker.KEEP_FOREVER` existed.

Rollups store additive ROLES, never finished answers (invariant 8, and C5 in the correction log). The
columns are named for the roles, so a finished answer has no column to be written into.
"""

import inspect

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import Numeric

from app.persistence import models as m
from app.persistence import partitioning as pt
from app.services.analytics import contract, definition
from app.services.workers import log_partition_worker as pw

#: Every table Phase 1 creates. The ML pair (`analytics_feature_sets`, `analytics_predictions`) is
#: deliberately absent: M1 is marked "later" in the component map and Phase 1's own list does not
#: include it.
ANALYTICS_TABLES = (
    "analytics_pending_windows",
    "analytics_facts",
    "analytics_fact_ledger",
    "analytics_metrics",
    "analytics_hourly_rollups",
    "analytics_daily_rollups",
    "analytics_monthly_rollups",
    "analytics_tenant_state",
    "analytics_quality_issues",
)

#: Grain per partitioned table, from section 6's ownership table. `analytics_monthly_rollups` is
#: deliberately NOT partitioned ("none needed"): at 300K rows over five years there is nothing to prune.
EXPECTED_GRAIN = {
    "analytics_facts": pt.Grain.monthly,
    "analytics_fact_ledger": pt.Grain.monthly,
    "analytics_hourly_rollups": pt.Grain.daily,
    "analytics_daily_rollups": pt.Grain.yearly,
    "analytics_quality_issues": pt.Grain.monthly,
    # R4. Monthly for the same reason as the fact table it hangs off: kept forever, so the partition
    # count has to stay bounded. Expansion is opt-in per transaction because the volume is not -
    # measured at ~200k records/day on the deployed database.
    "analytics_record_facts": pt.Grain.monthly,
}

#: Kept forever versus a finite window. Split exactly as section 6 states, plus R4's record grain -
#: whose raw source is `mi_result.records[]`, gone with the entries at 60 days, so a dropped partition
#: here could not be rebuilt from anything either.
EXPECTED_FOREVER = {"analytics_facts", "analytics_fact_ledger", "analytics_daily_rollups",
                    "analytics_record_facts"}
EXPECTED_RETENTION_DAYS = {"analytics_hourly_rollups": 90, "analytics_quality_issues": 365}


def _model(table: str):
    for cls in (getattr(m, n) for n in m.__all__):
        if getattr(cls, "__tablename__", None) == table:
            return cls
    raise AssertionError(f"{table} has no model registered in app/persistence/models/__init__.py")


def _cols(table: str) -> dict:
    return {c.key: c for c in sa_inspect(_model(table)).columns}


# ==================================================== the tables exist and are registered
@pytest.mark.parametrize("table", ANALYTICS_TABLES)
def test_every_analytics_table_has_a_registered_model(table):
    """Registered in `models/__init__.py`, not merely defined. An unregistered model is invisible to
    Alembic's autogenerate and to anything importing by name."""
    assert _model(table) is not None


@pytest.mark.parametrize("table", ANALYTICS_TABLES)
def test_every_analytics_table_is_tenant_scoped(table):
    """`customer_code` is the soft tenant key used by every other log table, and F13's purge deletes by
    it. A table without one cannot be purged and cannot be read tenant-safely."""
    assert "customer_code" in _cols(table)


def test_the_ml_tables_are_not_in_phase_1():
    """M1 is "later" in the component map, and Phase 1's list stops at the ticket table. Building them
    now would freeze a feature-set shape before the ML work has said what it needs."""
    for absent in ("analytics_feature_sets", "analytics_predictions"):
        with pytest.raises(AssertionError):
            _model(absent)


# ==================================================== 1. the fact row is wide
def test_the_fact_table_carries_every_field_the_contract_pins():
    """The irreversible decision. Asserted against `contract.FACT_FIELDS` rather than a list written
    here, so the schema and the contract cannot drift: a field added to one without the other fails."""
    cols = _cols("analytics_facts")
    for field in contract.FACT_FIELDS:
        assert field in cols, f"{field} is in FACT_FIELDS but not in analytics_facts"


def test_the_quantity_column_is_numeric_and_never_float():
    """Quantities are fractional (0.333333, 2.666664 are live values) and get summed over a month.
    Float would drift with nothing reporting it."""
    assert isinstance(_cols("analytics_facts")["quantity"].type, Numeric)


def test_the_fact_table_identity_spans_the_id_and_the_event_instant_per_f3():
    """F3: two source rows can share an id in different partitions, so the id alone is not an identity.

    The key is (customer_code, source_transaction_id, event_time), not the literal
    (source_transaction_id, source_started_at) F3 names, for a reason worth stating rather than
    hiding: PostgreSQL requires a unique constraint on a partitioned table to contain EVERY
    partition-key column. `event_time` holds the source transaction's `started_at`, so this is the same
    identity -- and it keeps the partition key in the predicate reads carry, so reads prune. Keying on
    `source_started_at` would satisfy F3 word-for-word and prune nothing.

    Asserted as the PROPERTY (id + the partition key + the tenant) rather than a column list, so a
    future change of partition key cannot leave the constraint behind.
    """
    cols = _cols("analytics_facts")
    assert "source_transaction_id" in cols
    assert "source_started_at" in cols, "kept as explicit provenance of the source row's own key"

    key = pt.BY_TABLE["analytics_facts"].key
    uniques = [
        {c.name for c in con.columns}
        for con in _model("analytics_facts").__table__.constraints
        if con.__class__.__name__ == "UniqueConstraint"
    ]
    assert any({"customer_code", "source_transaction_id", key} <= u for u in uniques), \
        f"no unique constraint spans the tenant, the source id and the partition key {key!r}: {uniques}"


@pytest.mark.parametrize("table", sorted(EXPECTED_GRAIN))
def test_every_partitioned_unique_constraint_contains_the_partition_key(table):
    """PostgreSQL rejects one that does not, so this is really a guard against writing a constraint that
    cannot be created -- caught here rather than at migration time."""
    key = pt.BY_TABLE[table].key
    for con in _model(table).__table__.constraints:
        if con.__class__.__name__ == "UniqueConstraint":
            names = {c.name for c in con.columns}
            assert key in names, f"{table}.{con.name} omits the partition key {key!r}"


def test_the_ledger_is_append_only_shaped_and_carries_a_revision():
    """F10. `analytics_facts` holds the latest value; the ledger holds EVERY version, so a training set
    can pin a revision and be rebuilt identically months later."""
    cols = _cols("analytics_fact_ledger")
    for field in ("source_transaction_id", "source_started_at", "source_version_hash",
                  "revision", "recorded_at"):
        assert field in cols, f"{field} missing from the ledger"


# ==================================================== 2. explicit grain
@pytest.mark.parametrize("table,grain", sorted(EXPECTED_GRAIN.items()))
def test_every_partitioned_analytics_table_declares_its_grain(table, grain):
    assert pt.grain_of(table) is grain, (
        f"{table} must be {grain.value}; a wrong grain is not a failure, it is thirty times the "
        f"partitions or a thirtieth of them")


def test_the_monthly_rollup_table_is_deliberately_not_partitioned():
    """300K rows over five years. Partitioning it would add planning cost for nothing to prune."""
    assert "analytics_monthly_rollups" not in pt.BY_TABLE


def test_no_analytics_table_was_registered_without_being_listed_here():
    """Guard against a table appearing in PARTITIONED that this file has not considered, which is how
    one ends up silently on the log tables' 60-day retention."""
    registered = {t.table for t in pt.PARTITIONED if t.table.startswith("analytics_")}
    assert registered == set(EXPECTED_GRAIN)


def test_the_log_tables_still_have_their_original_grain():
    """Regression: Phase 1 must not disturb the three tables already in production."""
    for table in ("log_entries", "log_transactions", "log_entry_assignment"):
        assert pt.grain_of(table) is pt.Grain.daily


# ==================================================== 3. explicit retention
@pytest.mark.parametrize("table", sorted(EXPECTED_FOREVER))
def test_the_irreplaceable_tables_are_kept_forever(table):
    """Their raw source is dropped at 60 days, so a dropped partition here cannot be rebuilt from
    anything. `droppable_days` must return nothing for them whatever retention is configured."""
    assert table in pw.KEEP_FOREVER
    assert pw.retention_days_for(table) is None
    from datetime import date
    assert pw.droppable_days(table, [date(2019, 1, 1), date(2020, 6, 1)], date(2026, 8, 21)) == []


@pytest.mark.parametrize("table,days", sorted(EXPECTED_RETENTION_DAYS.items()))
def test_the_finite_tables_declare_their_own_window(table, days):
    assert pw.RETENTION_DAYS.get(table) == days
    assert pw.retention_days_for(table) == days


def test_no_partitioned_analytics_table_silently_inherits_the_log_retention():
    """The failure this whole phase was sequenced around. A table registered in PARTITIONED with no
    policy of its own gets `log_partition_retention_days` (60) applied to it, and the worker drops it a
    month at a time."""
    from app.settings import settings
    for table in EXPECTED_GRAIN:
        explicit = table in pw.KEEP_FOREVER or table in pw.RETENTION_DAYS
        assert explicit, f"{table} has no explicit retention policy"
        if table not in pw.KEEP_FOREVER:
            assert pw.retention_days_for(table) != settings.log_partition_retention_days, \
                f"{table}'s retention coincides with the log default; state it deliberately"


# ==================================================== 4. the tenant purge map (F13)
# Asserted BEHAVIOURALLY, by purging a throwaway tenant and checking the rows are gone. An earlier
# version of this grepped `logspace_cleanup.py` for each table NAME, which passed for the wrong reason:
# the purge loop names model CLASSES, so the literal string is absent and the grep proved nothing about
# whether a delete actually runs.

_ANALYTICS_MODELS = {
    "analytics_pending_windows": "AnalyticsPendingWindow",
    "analytics_facts": "AnalyticsFact",
    "analytics_fact_ledger": "AnalyticsFactLedger",
    "analytics_metrics": "AnalyticsMetric",
    "analytics_hourly_rollups": "AnalyticsHourlyRollup",
    "analytics_daily_rollups": "AnalyticsDailyRollup",
    "analytics_monthly_rollups": "AnalyticsMonthlyRollup",
    "analytics_tenant_state": "AnalyticsTenantState",
    "analytics_quality_issues": "AnalyticsQualityIssue",
}


def test_the_purge_names_every_analytics_model():
    """A cheap structural guard that runs without a database, so a table added later without being
    purged fails fast rather than waiting for the behavioural test's fixtures to be extended."""
    from app.services import logspace_cleanup
    src = inspect.getsource(logspace_cleanup)
    for table, model in sorted(_ANALYTICS_MODELS.items()):
        assert model in src, f"{table} ({model}) is not deleted by purge_logspace"


async def test_the_purge_deletes_from_every_analytics_table(db):
    """F13, for real: plant one row per analytics table for a throwaway tenant, purge it, assert none
    survive. Nothing cascades on their behalf, so each has to be deleted explicitly."""
    import uuid as _uuid
    from datetime import date, datetime, timezone
    from sqlalchemy import func, select as _select
    from app.persistence.models.customer import Customer, LogSpaceKind
    from app.services.logspace_cleanup import purge_logspace

    cc = f"purge-probe-{_uuid.uuid4().hex[:8]}"
    now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)
    db.add(Customer(customer_code=cc, kind=LogSpaceKind.disposable, notifications_enabled=False))

    planted = [
        m.AnalyticsPendingWindow(customer_code=cc, range_start=now, range_end=now),
        m.AnalyticsFact(customer_code=cc, source_transaction_id=_uuid.uuid4(),
                        source_started_at=now, source_version_hash="h", event_time=now),
        m.AnalyticsFactLedger(customer_code=cc, source_transaction_id=_uuid.uuid4(),
                              source_started_at=now, source_version_hash="h", recorded_at=now),
        m.AnalyticsMetric(customer_code=cc, name="probe"),
        m.AnalyticsHourlyRollup(customer_code=cc, definition_id=_uuid.uuid4(),
                                measure_name="q", bucket_start=now),
        m.AnalyticsDailyRollup(customer_code=cc, definition_id=_uuid.uuid4(),
                               measure_name="q", business_date=date(2026, 8, 21)),
        m.AnalyticsMonthlyRollup(customer_code=cc, definition_id=_uuid.uuid4(),
                                 measure_name="q", month_start=date(2026, 8, 1)),
        m.AnalyticsTenantState(customer_code=cc),
        m.AnalyticsQualityIssue(customer_code=cc, reason="probe", detected_at=now),
    ]
    for row in planted:
        db.add(row)
    await db.flush()

    models = {type(r) for r in planted}
    assert len(models) == len(ANALYTICS_TABLES), "one row planted per analytics table"

    async def counts():
        return {mo.__tablename__: await db.scalar(
            _select(func.count()).select_from(mo).where(mo.customer_code == cc)) for mo in models}

    before = await counts()
    assert all(n == 1 for n in before.values()), before

    await purge_logspace(db, cc)
    await db.flush()

    after = await counts()
    survivors = sorted(t for t, n in after.items() if n)
    assert not survivors, f"survived the tenant purge: {survivors}"


def test_the_purge_does_not_insert_a_ticket_for_a_departing_tenant():
    """The distinction between F12 and F13. On the ordinary delete paths a window's contents changed and
    the fix is to publish a ticket so the range diff corrects the totals. Here the TENANT is going away,
    so correcting its totals is meaningless: the worker would try to fold a tenant that no longer
    exists. So the ticket table is DELETED FROM on this path, never written to."""
    from app.services import logspace_cleanup
    src = inspect.getsource(logspace_cleanup)
    assert "AnalyticsPendingWindow" in src, "the ticket table must be purged too"
    assert "AnalyticsPendingWindow(" not in src, \
        "constructing a ticket here would ask the worker to fold a tenant that is gone"


# ==================================================== rollups store roles, not answers
ROLLUPS = ("analytics_hourly_rollups", "analytics_daily_rollups", "analytics_monthly_rollups")


@pytest.mark.parametrize("table", ROLLUPS)
def test_every_rollup_carries_the_six_additive_roles(table):
    cols = _cols(table)
    for role in definition.Role:
        assert role.value in cols, f"{table} is missing the {role.value} column"


@pytest.mark.parametrize("table", ROLLUPS)
def test_no_rollup_has_a_column_a_finished_answer_could_go_in(table):
    """Invariant 8 made structural. With role columns there is no `average` to write, so twelve monthly
    averages cannot be stored and then wrongly averaged into a year."""
    cols = set(_cols(table))
    for forbidden in ("average", "mean", "rate", "median", "p95", "stddev", "percentile",
                      "zero_pick_rate"):
        assert forbidden not in cols, f"{table}.{forbidden} is not additive"


@pytest.mark.parametrize("table", ROLLUPS)
def test_a_rollup_row_is_keyed_per_definition_and_measure(table):
    """C5. A definition needing one sum and two counts cannot share one set of role columns, so the key
    is (definition, measure, dimensions, bucket) and consumption emits three rows per bucket."""
    cols = _cols(table)
    assert "definition_id" in cols
    assert "measure_name" in cols


@pytest.mark.parametrize("table", ROLLUPS)
def test_a_rollup_has_a_fixed_number_of_dimension_slots(table):
    """Generic storage: "a definition identifier, a fixed number of dimension slots and additive
    measure slots, rather than a bespoke table per metric". Adding a metric is a row, never a
    migration."""
    cols = _cols(table)
    slots = [k for k in cols if k.startswith("dim")]
    assert len(slots) == 4, f"{table} has {len(slots)} dimension slots, expected 4"


@pytest.mark.parametrize("table", ROLLUPS)
def test_the_histogram_is_jsonb_because_bucket_counts_add(table):
    """Percentiles are stored as a 20-bucket log histogram, because bucket counts compose and
    percentiles do not. No hll or tdigest is available on this server."""
    assert isinstance(_cols(table)["histogram"].type, JSONB)


@pytest.mark.parametrize("table", ROLLUPS)
def test_rollup_numeric_roles_are_numeric_not_float(table):
    cols = _cols(table)
    for role in ("sum_value", "sum_sq", "min_value", "max_value"):
        assert isinstance(cols[role].type, Numeric), f"{table}.{role} must be NUMERIC"


# ==================================================== the ticket table mirrors the proven one
def test_the_ticket_table_mirrors_log_regroup_pending_field_for_field():
    """N1: a SEPARATE table with an identical shape. Separate because `consumed_at` is single-consumer,
    so a second consumer stamping it would find the window closed and skip work it never did."""
    ours = set(_cols("analytics_pending_windows"))
    theirs = set(_cols("log_regroup_pending"))
    assert theirs <= ours, f"missing from the analytics ticket table: {sorted(theirs - ours)}"


def test_the_ticket_table_has_no_constraint_a_retry_could_violate():
    """A3. It is written inside the ingestion transaction, so a failed insert fails INGESTION. No
    foreign key, no unique constraint, no trigger."""
    table = _model("analytics_pending_windows").__table__
    kinds = {c.__class__.__name__ for c in table.constraints}
    assert "ForeignKeyConstraint" not in kinds
    assert "UniqueConstraint" not in kinds


# ==================================================== the definition table drives the registry
def test_the_definition_table_holds_what_n4_specifies():
    cols = _cols("analytics_metrics")
    for field in ("name", "dimensions", "measures", "filter", "grains", "status",
                  "created_by", "backfilled_through"):
        assert field in cols, f"{field} missing from analytics_metrics"


def test_a_definition_is_stored_as_data_not_code():
    """The registry requirement. Dimensions, measures and the filter are JSONB, so a user-defined
    metric is a row the interface writes -- never a code change and never a migration."""
    cols = _cols("analytics_metrics")
    for field in ("dimensions", "measures", "filter", "grains"):
        assert isinstance(cols[field].type, JSONB), f"{field} must be JSONB to be user-definable"


def test_the_state_table_is_one_row_per_tenant_for_the_polled_card():
    """F5. The browser polls every 2s per tab across four web workers, so the status endpoint must read
    exactly ONE row; the worker writes every field it needs each cycle."""
    cols = _cols("analytics_tenant_state")
    assert "customer_code" in cols
    uniques = [tuple(c.name for c in con.columns)
               for con in _model("analytics_tenant_state").__table__.constraints
               if con.__class__.__name__ == "UniqueConstraint"]
    assert any(u == ("customer_code",) for u in uniques), \
        f"one row per tenant must be enforced, got {uniques}"
