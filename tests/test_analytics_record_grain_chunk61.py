"""Chunk 61 (R4 of docs/analytics-ml-architecture/final_architecture.md, 18a): per-record expansion of
`mi_result.records[]`, into a SEPARATE table, opt-in per transaction.

The open decision, settled by measurement
-----------------------------------------
18a left it open whether the record grain should be a new table or a second row type in
`analytics_facts`, pending a read of `rollups.recompute`. The answer is not a matter of taste:

`_read_dirty_facts` selects the whole table with no grain predicate, and `group_fold` has no notion of
grain. So record rows fold into the SAME buckets as their parent transaction. Feeding the seed
definition one transaction plus three of its records inflated the quantity total from 10 to 40 - **4x,
silently**. Avoiding that would need EVERY definition to carry a grain filter, and forgetting one on any
single definition produces a plausible-looking wrong total.

A separate table makes the mistake structurally impossible: the existing fold cannot see these rows.

Why it is opt-in
----------------
~200k records a day on the deployed database, against ~1,400 transaction facts. Expansion is per
transaction so the volume is CHOSEN rather than inherited - which is also why `expand` is the one switch
returned as an INCLUSION list, and the one whose default is off.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select

from app.config.database import async_session
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.services.analytics import capture, payload as pl
from app.services.analytics import definition as d, rollups as r5

CC = "test_chunk61"
T0 = datetime(2026, 8, 17, 9, 0, tzinfo=timezone.utc)

MI = ("mi_result", {"result": "OK", "program": "MMS060MI", "transaction": "LstBalID",
                    "records": [{"BANO": "2608031215", "STQT": "624", "ITNO": "101978"},
                                {"BANO": "2608031216", "STQT": "12", "ITNO": "101978"}]})


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsRecordFact, AnalyticsTransactionRegistry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


# =============================================================== 1. the decision, re-asserted
def test_a_second_row_type_in_analytics_facts_would_double_count():
    """The measurement that settled 18a's open question, kept as a test so nobody re-opens it by
    accident. If this ever stops inflating, the separate table can be reconsidered - and until then the
    4x is the argument."""
    def fact(**kw):
        base = {"method": "ConfirmPickLine", "transaction_name": "Pick", "status": "success",
                "quantity_classification": "pick", "quantity": Decimal("10"),
                "event_time": T0, "business_date": T0.date(), "attributes": {}}
        base.update(kw)
        return base

    seed = d.CONSUMPTION
    alone = r5.group_fold([fact()], seed, lambda r: r["business_date"])
    mixed = r5.group_fold([fact()] + [fact() for _ in range(3)], seed, lambda r: r["business_date"])
    qa = list(alone.values())[0]["quantity"][d.Role.sum_value]
    qb = list(mixed.values())[0]["quantity"][d.Role.sum_value]
    assert qb == qa * 4, "the inflation that rules out a shared table"


def test_the_record_table_is_not_visible_to_the_transaction_fold():
    """Structural, not conventional. `_read_dirty_facts` names `AnalyticsFact` and nothing else, so an
    existing metric cannot see a record row even if somebody forgets a filter."""
    import inspect
    src = inspect.getsource(r5._read_dirty_facts)
    assert "AnalyticsRecordFact" not in src
    assert "AnalyticsFact" in src


# =============================================================== 2. extraction
def test_records_are_flattened_and_namespaced():
    recs = pl.records([MI])
    assert len(recs) == 2
    assert recs[0]["attributes"] == {"rec.BANO": "2608031215", "rec.STQT": "624",
                                     "rec.ITNO": "101978"}


def test_each_record_carries_the_call_that_answered():
    """A transaction can hold several M3 calls, so a record is meaningless without knowing which one
    produced it."""
    for r in pl.records([MI]):
        assert r["mi_program"] == "MMS060MI"
        assert r["mi_transaction"] == "LstBalID"


def test_the_record_namespace_cannot_collide_with_the_other_two():
    """`ItNO` from a record, `ItemNumber` from a response and `ItemNumber` from a request are three
    different observations. A third prefix makes that structural."""
    assert pl.RECORD_PREFIX not in (pl.RESPONSE_PREFIX, pl.MI_PREFIX)
    recs = pl.records([("mi_result", {"records": [{"ItemNumber": "REC"}]})])
    assert list(recs[0]["attributes"]) == ["rec.ItemNumber"]


def test_a_record_field_is_addressable_as_an_attr_path():
    """The contract with R1b. What is stored has to be exactly the key `resolve_field` looks up."""
    from app.services.analytics import contract as c
    recs = pl.records([MI])
    assert c.resolve_field({"attributes": recs[0]["attributes"]}, "attr:rec.STQT") == "624"


def test_a_nested_value_inside_a_record_is_dropped_not_flattened():
    """3,765 record field values measured, ALL scalar - so this has never been observed. It is dropped
    rather than flattened because inventing a key name no registry row could match would make the field
    permanently unapprovable."""
    recs = pl.records([("mi_result", {"records": [{"ok": 1, "nested": {"a": 1}, "list": [1]}]})])
    assert recs[0]["attributes"] == {"rec.ok": 1}


def test_a_response_entry_contributes_no_records():
    assert pl.records([("response", {"response": {"X": 1}})]) == []


def test_malformed_records_do_not_raise():
    assert pl.records([("mi_result", {"records": "not-a-list"}),
                       ("mi_result", {"records": [None, 7, {"ok": 1}]}),
                       ("mi_result", None)]) == [{"mi_program": None, "mi_transaction": None,
                                                  "attributes": {"rec.ok": 1}}]


def test_records_from_several_calls_all_appear():
    """Unlike the scalar grain, where the last `mi.program` wins, every record is its own observation and
    none is discarded."""
    two = pl.records([MI, ("mi_result", {"program": "P2", "records": [{"A": 1}]})])
    assert len(two) == 3
    assert two[-1]["mi_program"] == "P2"


# =============================================================== 3. the expand switch
async def test_expand_defaults_off():
    """The only switch that does. ~200k records/day against ~1,400 transaction facts, so the volume must
    be chosen rather than inherited."""
    assert capture.DEFAULT_EXPAND is False


async def test_expanded_names_is_an_inclusion_list():
    """The OPPOSITE direction to `capture` and `show`, and deliberately so. For those, a name missing
    from the registry must still be captured or shown, so exclusions are the safe default. For this one
    a missing name must NOT be expanded - defaulting on would grow a table by hundreds of thousands of
    rows a day that nobody asked for."""
    async with async_session() as db:
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="On", expand=True))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Off", expand=False))
        await db.commit()
        assert await capture.expanded_names(db, CC) == frozenset({"On"})


async def test_an_unregistered_transaction_is_not_expanded():
    """The consequence of the inclusion list: silence means no."""
    async with async_session() as db:
        assert await capture.expanded_names(db, CC) == frozenset()


# =============================================================== 4. approval
def test_an_unapproved_record_field_is_not_stored():
    """The same allowlist the scalar grain uses, so a record field is reviewed on the same screen and by
    the same rule."""
    recs = pl.records([MI])
    kept, unknown = pl.select(recs[0]["attributes"], frozenset({"rec.STQT"}))
    assert kept == {"rec.STQT": "624"}
    assert unknown == ["rec.BANO", "rec.ITNO"]


def test_no_record_field_is_seeded():
    """Deliberate. The seed list exists so the SCALAR grain produces history from day one without a
    screen; a record field only exists at all once somebody ticked `expand`, so they are already making
    a decision and can make this one too."""
    for name in ("rec.STQT", "rec.BANO", "rec.ITNO"):
        assert pl.seeded(name) is False


def test_a_credential_shaped_record_field_is_still_vetoed():
    """The veto is applied to the namespaced name, so it covers all three namespaces at once rather
    than needing to be repeated per prefix."""
    assert pl.never_auto_approve("rec.AccessToken") is True
    assert pl.seeded("rec.AccessToken") is False


async def test_a_record_field_is_registered_under_the_record_source():
    """Derived from the namespace rather than passed, so a stored row cannot disagree with the prefix the
    field is addressed by."""
    from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
    async with async_session() as db:
        await db.execute(delete(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == CC))
        await capture.observe_fields(db, CC, {"LstBalID": {"rec.STQT", "resp.BaseUoM", "mi.result"}})
        await db.commit()
        rows = {f: sc for f, sc in (await db.execute(
            select(AnalyticsFieldRegistry.field, AnalyticsFieldRegistry.source)
            .where(AnalyticsFieldRegistry.customer_code == CC))).all()}
    assert rows == {"rec.STQT": "record", "resp.BaseUoM": "response", "mi.result": "mi_result"}
    async with async_session() as db:
        await db.execute(delete(AnalyticsFieldRegistry).where(
            AnalyticsFieldRegistry.customer_code == CC))
        await db.commit()


# =============================================================== 5. the loud-expansion guard
def test_the_threshold_is_far_above_anything_measured():
    """2.3 records per entry on average and 26 at most, so this only fires when somebody has ticked
    `expand` on something that returns a catalogue."""
    assert pl.LOUD_EXPANSION >= 100


def test_the_guard_warns_rather_than_truncates():
    """Silently truncating would produce a record count that looks complete and is not - which is worse
    than a large table somebody was told about."""
    import inspect
    from app.services.analytics import consume
    src = inspect.getsource(consume._expand_records)
    assert "LOUD_EXPANSION" in src
    assert "logger.warning" in src
    assert "[:pl.LOUD_EXPANSION]" not in src and "[: pl.LOUD_EXPANSION]" not in src


# =============================================================== 6. it does not undo S3
def test_expansion_is_driven_by_the_diff_not_the_source_rows():
    """S3's whole gain is that a settled window writes nothing. Expanding from the source rows would put
    that write straight back - a transaction the diff called `unchanged` has records that are also
    unchanged."""
    import inspect
    from app.services.analytics import consume
    src = inspect.getsource(consume._expand_records)
    assert "dd.Action.insert" in src and "dd.Action.update" in src
    # 18x amends this pin rather than replacing it: reversals now DELETE their record rows (a
    # vanished parent must take its records with it - invariant 5 one grain down), and a presence
    # diff re-expands settled transactions whose rows are missing or stale. Neither reads source
    # rows the diff called unchanged for its own sake, so the original property - S3's skip
    # survives - still holds and is pinned byte-identically in chunk 78.
    assert "dd.Action.reverse" in src, \
        "expansion must key off the diff's verdicts, or it re-writes what S3 skipped"


def test_replace_per_transaction_rather_than_upsert_per_record():
    """A re-expansion can produce FEWER records than the last one, and an upsert keyed on
    (transaction, index) would leave the previous tail behind forever."""
    import inspect
    from app.services.analytics import consume
    src = inspect.getsource(consume._expand_records)
    assert "delete(AnalyticsRecordFact)" in src


# =============================================================== 7. schema obligations
async def test_the_table_is_partitioned_kept_forever_and_purged_with_its_tenant():
    """Three registrations that are each silent if forgotten: partitioning (or the worker cannot
    provision a month), KEEP_FOREVER (or it inherits the log tables' 60 days and drops what nothing can
    rebuild), and the tenant purge list (or a delete orphans the rows)."""
    import inspect
    from app.persistence import partitioning as pt
    from app.services.workers import log_partition_worker as pw
    from app.services import logspace_cleanup

    assert "analytics_record_facts" in pt.BY_TABLE
    assert pt.BY_TABLE["analytics_record_facts"].grain is pt.Grain.monthly
    assert "analytics_record_facts" in pw.KEEP_FOREVER
    assert "AnalyticsRecordFact" in inspect.getsource(logspace_cleanup)


async def test_the_identity_is_transaction_plus_index():
    """Two records of one transaction are genuinely different observations and can be identical in every
    field, so the index is part of the key rather than a convenience."""
    async with async_session() as db:
        from sqlalchemy.exc import IntegrityError
        tid = uuid.uuid4()
        for _ in range(2):
            db.add(AnalyticsRecordFact(id=uuid.uuid4(), customer_code=CC,
                                       source_transaction_id=tid, record_index=0,
                                       event_time=T0, business_date=T0.date(), attributes={}))
        with pytest.raises(IntegrityError):
            await db.commit()
