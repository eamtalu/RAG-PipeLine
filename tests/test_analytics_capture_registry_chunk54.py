"""Chunk 54 (R1 of docs/analytics-ml-architecture/final_architecture.md, section 18b/18d): the
transaction registry, the field discovery registry, and the ONE capture predicate all three readers
share.

What R1 is
----------
A per-transaction registry with three switches, keyed on `transaction_name` because the mapping to
`method` is many-to-many - `ConfirmPickLine` appears under both "Brighton Stock Pick" and "JIT and
Shorts Pick (Brighton)", so a method-keyed switch cannot express one on and the other off.

    capture  gates whether a fact row exists at all. Irreversible: entries drop at 60 days.
    show     gates whether facts reach charts. Free and retroactive, and it already existed as
             `method_filter`; R1 adds `transactions` beside it.
    expand   gates R4's per-record rows. Not built.

The thing this chunk is really about
------------------------------------
THREE queries decide independently what analytics thinks should exist:

    consume._read_source              what the fold reads from log_transactions
    consume._read_stored              what it compares that against, in analytics_facts
    reconcile.facts_vs_transactions   what the auditor expects to find a fact for

Any two of them disagreeing is permanent and loud: a transaction the fold skips but the auditor expects
is reported as a missing fact on EVERY run, forever. A permanently red check is worse than no check.
So the predicate is defined once in `capture.py` and the tests below assert all three use it.

Applying it to the STORED side is a decision, not an oversight
-------------------------------------------------------------
The fold is a range diff, so anything in stored and absent from source is REVERSED. Predicate on source
alone would make un-ticking `capture` DELETE the facts that transaction already has - "stop capturing"
silently meaning "destroy what you have". Applying it to both sides makes those facts invisible to the
diff instead: not compared, not reversed, just left alone. That is asserted here, because it is the
difference between a switch and a delete button.
"""

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text

from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import capture
from app.services.analytics import definition as d

CC = "test_chunk54"


# =============================================================== fixtures
async def _job(db, cc=CC):
    j = Job(customer_code=cc, filename="c54.log", storage_key=f"{cc}/{uuid.uuid4().hex}/c54.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _txn(db, job, *, name, cc=CC, started_at=None):
    started_at = started_at or datetime.now(timezone.utc) - timedelta(minutes=5)
    t = LogTransaction(id=uuid.uuid4(), customer_code=cc, job_id=job.id, sealed=True,
                       status=LogTransactionStatus.success, transaction_name=name,
                       method="ConfirmPickLine", started_at=started_at, ended_at=started_at,
                       date=started_at.date(), created_at=started_at, updated_at=started_at)
    db.add(t)
    await db.flush()
    return t


async def _register(db, name, *, cc=CC, capture_on=True, show=False, expand=False):
    row = AnalyticsTransactionRegistry(customer_code=cc, transaction_name=name,
                                       capture=capture_on, show=show, expand=expand)
    db.add(row)
    await db.flush()
    return row


# =============================================================== 1. defaults
def test_a_new_transaction_defaults_to_captured_and_shown():
    """Both defaults ON, and `show` being one of them is a CORRECTION made during R2.

    The original reasoning - "show off cannot surprise a reader" - had it backwards. Discovery registers
    every transaction it sees, so `show` defaulting false meant the first fold after deploying R1 marked
    every EXISTING transaction hidden, and R2's rollup gate then blanked every existing chart. Twenty-
    three tests caught it by folding to zero.

    The failure modes are not symmetric, and neither default is safe in the abstract:

        capture off by default  ->  loses history IRREVERSIBLY, entries expire at 60 days
        show off by default     ->  UNDER-COUNTS every chart, silently, until someone reviews a row

    An under-counting total is the exact failure this architecture exists to prevent. So the review is
    SURFACED (`reviewed_at IS NULL`) rather than enforced by hiding data.
    """
    assert capture.DEFAULT_CAPTURE is True
    assert capture.DEFAULT_SHOW is True
    assert capture.DEFAULT_EXPAND is False


async def test_discovery_does_not_hide_an_existing_transaction(db):
    """The regression the default change fixes, asserted end to end rather than only as a constant.

    R1's discovery runs on every fold. If it registered a transaction in a state that R2's rollup gate
    treats as hidden, deploying R1 would silently zero every chart that already worked.
    """
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    await capture.observe_names(db, CC, {"Brighton Stock Pick", "Full Stock Count"})
    assert await capture.hidden_names(db, CC) == frozenset(), \
        "a freshly discovered transaction must not be hidden, or discovery blanks existing charts"


async def test_the_model_defaults_match_the_constants(db):
    """A default enforced only in Python is a default a raw INSERT can bypass. The server defaults must
    agree with the constants, or the same transaction gets different treatment depending on which code
    path created its row."""
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Defaults Probe"))
    await db.flush()
    row = (await db.execute(
        select(AnalyticsTransactionRegistry.capture, AnalyticsTransactionRegistry.show,
               AnalyticsTransactionRegistry.expand)
        .where(AnalyticsTransactionRegistry.customer_code == CC))).one()
    assert (row.capture, row.show, row.expand) == (
        capture.DEFAULT_CAPTURE, capture.DEFAULT_SHOW, capture.DEFAULT_EXPAND)


# =============================================================== 2. the rule, in memory
def test_an_unregistered_transaction_is_captured():
    """An empty registry must capture everything. The predicate returns EXCLUSIONS for exactly this
    reason - an inclusion list would have to enumerate every name ever seen, so anything new would
    silently not be captured, which is the one mistake that cannot be undone."""
    assert capture.is_captured("Brand New Transaction", frozenset()) is True


def test_a_suppressed_transaction_is_not_captured():
    assert capture.is_captured("Full Stock Count", frozenset({"Full Stock Count"})) is False


def test_an_unnamed_transaction_is_always_captured():
    """57 live transactions have no name (`CheckOperator`, `CheckServer`). They cannot be keyed by
    name, so the rule is fixed in code: always captured, never shown. Captured because a probe that
    starts failing is exactly what someone will want to measure later, and the entries are gone in 60
    days."""
    assert capture.is_captured(None, frozenset({"Full Stock Count"})) is True
    assert capture.is_captured(None, frozenset()) is True


# =============================================================== 3. the rule, in SQL
async def test_the_sql_and_the_python_agree(db):
    """The rule now has two implementations. They must agree, or a transaction's fate depends on
    whether it was decided by a query or by a loop. Same discipline as `_is_sealed` and the sealer."""
    job = await _job(db)
    names = ["Brighton Stock Pick", "Full Stock Count", None]
    for n in names:
        await _txn(db, job, name=n)
    suppressed = frozenset({"Full Stock Count"})

    pred = capture.source_predicate(suppressed)
    kept = (await db.execute(
        select(LogTransaction.transaction_name).where(
            LogTransaction.customer_code == CC, pred))).scalars().all()

    assert sorted(str(x) for x in kept) == sorted(
        str(n) for n in names if capture.is_captured(n, suppressed))


async def test_no_suppression_adds_no_clause():
    """The common case must cost nothing. Every fold of every window runs this, so an empty registry
    has to produce the exact query that existed before R1 rather than a tautology the planner has to
    reason about."""
    assert capture.source_predicate(frozenset()) is None
    assert capture.fact_predicate(frozenset(), AnalyticsFact) is None


async def test_suppressed_names_returns_only_capture_off(db):
    """`show = false` must NOT suppress capture. Conflating them would make the free, reversible switch
    silently do the irreversible thing."""
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    await _register(db, "Shown And Captured", capture_on=True, show=True)
    await _register(db, "Hidden But Captured", capture_on=True, show=False)
    await _register(db, "Not Captured", capture_on=False, show=False)
    assert await capture.suppressed_names(db, CC) == frozenset({"Not Captured"})


# =============================================================== 4. the three readers agree
async def test_all_three_readers_use_the_same_predicate():
    """The one test that matters most in this chunk.

    If the fold skips a transaction the auditor still expects, `facts_vs_transactions` reports a
    discrepancy on every run forever. Asserted on the SOURCE rather than by behaviour, because the
    failure is a missing call - and a behavioural test can only catch that once someone has actually
    turned a switch off in a fixture that also happens to run the auditor.
    """
    import inspect
    from app.services.analytics import consume, reconcile

    src = inspect.getsource(consume)
    assert "capture.source_predicate" in src, "_read_source must gate on the shared predicate"
    assert "capture.fact_predicate" in src, "_read_stored must gate on it too, or a suppressed " \
                                            "transaction's existing facts get REVERSED"
    assert "capture.source_predicate" in inspect.getsource(reconcile), \
        "facts_vs_transactions must use the same predicate or it reddens permanently"


async def test_suppressing_a_transaction_hides_it_from_the_fold_source(db):
    from app.services.analytics.consume import _read_source
    from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _txn(db, job, name="Kept", started_at=now - timedelta(minutes=5))
    await _txn(db, job, name="Dropped", started_at=now - timedelta(minutes=5))
    await _register(db, "Dropped", capture_on=False)

    window = UtcWindow(start=now - timedelta(hours=1), end=now)
    rows = await _read_source(db, CC, window, await capture.suppressed_names(db, CC))
    names = {r["transaction_name"] for r in rows}
    assert "Kept" in names
    assert "Dropped" not in names


async def test_suppressing_does_not_delete_existing_facts(db):
    """THE non-destructive property. The diff reverses anything in stored and absent from source, so
    predicate-on-source-alone would turn un-ticking `capture` into a delete. Both sides are gated, so
    the facts become invisible to the diff and simply stay."""
    from app.services.analytics.consume import _read_source, _read_stored
    from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _txn(db, job, name="Dropped", started_at=now - timedelta(minutes=5))
    fact_id = uuid.uuid4()
    db.add(AnalyticsFact(id=fact_id, customer_code=CC, source_transaction_id=uuid.uuid4(),
                         source_started_at=now - timedelta(minutes=5),
                         source_version_hash="x" * 8, revision=1,
                         event_time=now - timedelta(minutes=5), business_date=now.date(),
                         transaction_name="Dropped", method="ConfirmPickLine", status="success",
                         quantity_classification="non_quantity", attributes={},
                         created_at=now))
    await db.flush()
    await _register(db, "Dropped", capture_on=False)

    window = UtcWindow(start=now - timedelta(hours=1), end=now)
    suppressed = await capture.suppressed_names(db, CC)
    source = await _read_source(db, CC, window, suppressed)
    stored = await _read_stored(db, CC, window, suppressed)

    assert "Dropped" not in {r["transaction_name"] for r in source}
    assert "Dropped" not in {r["transaction_name"] for r in stored}, \
        "the stored side must be gated too, or the diff REVERSES this fact and the history is gone"

    still_there = await db.scalar(
        select(func.count()).select_from(AnalyticsFact).where(AnalyticsFact.id == fact_id))
    assert still_there == 1


async def test_the_auditor_does_not_report_a_suppressed_transaction(db):
    """Without this the auditor reports the suppressed transaction as a missing fact on every run,
    forever - which is precisely how a check gets ignored."""
    from app.services.analytics.reconcile import facts_vs_transactions
    from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _txn(db, job, name="Dropped", started_at=now - timedelta(minutes=5))
    await _register(db, "Dropped", capture_on=False)

    findings = await facts_vs_transactions(
        db, CC, window=UtcWindow(start=now - timedelta(hours=1), end=now))
    assert findings == [], f"the auditor must not cry wolf over a deliberately suppressed row: {findings}"


# =============================================================== 5. discovery
async def test_observing_a_new_name_registers_it_at_the_defaults(db):
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    added = await capture.observe_names(db, CC, {"Newly Seen", None})
    assert added == ["Newly Seen"], "NULL must not be registered; its rule lives in code"
    row = (await db.execute(
        select(AnalyticsTransactionRegistry.capture, AnalyticsTransactionRegistry.show)
        .where(AnalyticsTransactionRegistry.customer_code == CC,
               AnalyticsTransactionRegistry.transaction_name == "Newly Seen"))).one()
    assert (row.capture, row.show) == (True, True)


async def test_observing_never_overwrites_a_human_decision(db):
    """The failure this prevents: a transaction someone deliberately turned off comes back on by
    itself the next time it appears in the logs, which is every tick."""
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    await _register(db, "Deliberately Off", capture_on=False, show=False)
    assert await capture.observe_names(db, CC, {"Deliberately Off"}) == []
    assert await capture.suppressed_names(db, CC) == frozenset({"Deliberately Off"})


async def test_observing_is_idempotent(db):
    await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                     {"c": CC})
    assert await capture.observe_names(db, CC, {"Once"}) == ["Once"]
    assert await capture.observe_names(db, CC, {"Once"}) == []


async def test_one_tenants_registry_does_not_speak_for_another(db):
    other = "test_chunk54_other"
    for cc in (CC, other):
        await db.execute(text("DELETE FROM analytics_transaction_registry WHERE customer_code = :c"),
                         {"c": cc})
    await _register(db, "Shared Name", capture_on=False)
    assert await capture.suppressed_names(db, CC) == frozenset({"Shared Name"})
    assert await capture.suppressed_names(db, other) == frozenset()


# =============================================================== 6. the field registry
async def test_the_field_registry_has_nowhere_to_put_a_value(db):
    """The safety property, asserted structurally rather than trusted. An unknown key is recorded by
    NAME so a person can review it; if this table had a value column, a discovery record could leak a
    credential by accident. `AccessToken` and `M3UserCredentials` are the two most frequent response
    keys of the 145 measured."""
    cols = set((await db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'analytics_field_registry'"))).scalars().all())
    for forbidden in ("value", "sample", "sample_value", "example", "last_value", "observed_value"):
        assert forbidden not in cols, f"analytics_field_registry must not be able to store a value: {forbidden}"
    assert {"method", "source", "field", "captured"} <= cols


async def test_a_discovered_field_is_not_captured_by_default(db):
    """The whole difference between this and a denylist. An unknown field is neither captured nor
    silently ignored - it is reported, with `captured = false`."""
    await db.execute(text("DELETE FROM analytics_field_registry WHERE customer_code = :c"), {"c": CC})
    db.add(AnalyticsFieldRegistry(customer_code=CC, method="MMS060MI", source="response",
                                  field="SomeNewlyAppearedKey"))
    await db.flush()
    row = (await db.execute(
        select(AnalyticsFieldRegistry.captured)
        .where(AnalyticsFieldRegistry.customer_code == CC))).one()
    assert row.captured is False


# =============================================================== 7. `show`, on the definition
def test_a_definition_can_filter_by_transaction():
    """`method_filter` cannot express "Brighton Stock Pick on, JIT and Shorts Pick off" because both
    use `ConfirmPickLine`. `transaction_filter` is what makes the registry's `show` switch
    expressible at all."""
    defn = d.MetricDefinition(
        name="brighton_only", dimensions=("item_number",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",),
        transaction_filter=("Brighton Stock Pick",))
    assert d.validate(defn) == []

    inside = {"method": "ConfirmPickLine", "transaction_name": "Brighton Stock Pick",
              "status": "success", "quantity_classification": "pick"}
    outside = {**inside, "transaction_name": "JIT and Shorts Pick (Brighton)"}
    assert d._contributes(inside, defn, defn.measures[0]) is True
    assert d._contributes(outside, defn, defn.measures[0]) is False


def test_an_empty_transaction_filter_means_every_transaction():
    """Same convention as `method_filter`, so a definition that does not care about transactions is
    written by omitting the field rather than by listing all seven."""
    defn = d.MetricDefinition(
        name="all", dimensions=("method",), measures=(d.Measure("n", d.Aggregation.count),),
        grains=("daily",))
    row = {"method": "ConfirmPickLine", "transaction_name": "Anything At All",
           "status": "success", "quantity_classification": "pick"}
    assert d._contributes(row, defn, defn.measures[0]) is True


def test_the_transaction_filter_round_trips_through_the_registry():
    """A definition that serialises lossily is worse than one that fails to serialise: the fold would
    quietly use a different filter from the one that was saved, and the chart would be confidently
    wrong. Same property `registry.py` already asserts for measures."""
    from app.services.analytics import registry

    original = d.MetricDefinition(
        name="rt", dimensions=("item_number",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",),
        method_filter=("ConfirmPickLine",), transaction_filter=("Brighton Stock Pick",))
    row = registry.to_row(original, customer_code=CC)

    class _Row:
        def __init__(self, r):
            self.name, self.dimensions, self.measures = r["name"], r["dimensions"], r["measures"]
            self.grains, self.filter, self.status = r["grains"], r["filter"], r["status"]

    assert registry.from_row(_Row(row)) == original
